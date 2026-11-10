use super::limits::RESERVED_PREFIX;
use super::{
    Binding, Expr, Literal, Logical, LogicalCatalog, LogicalOperation, LogicalPlan, LogicalType,
    PlanError, Predicate, Rows, Step, filter,
};
use super::{MATCH_LINE, Structural};
use crate::plan::code::{Language as CodeLanguage, Pattern};
use crate::plan::tracking::{self, Body, Construction, Declare, Declared, Outputs, Ref};
use crate::{DataType, Direction, EntityId, RelationshipId};

pub struct LogicalBuilder<'m> {
    catalog: LogicalCatalog<'m>,
    construction: Construction<Logical>,
    internal: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InternalName(String);

impl std::ops::Deref for InternalName {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

impl<'m> LogicalBuilder<'m> {
    pub(super) fn new(catalog: LogicalCatalog<'m>, construction: Construction<Logical>) -> Self {
        Self {
            catalog,
            construction,
            internal: Vec::new(),
        }
    }

    pub(super) fn construction(&mut self) -> &mut Construction<Logical> {
        &mut self.construction
    }

    pub(super) fn construction_ref(&self) -> &Construction<Logical> {
        &self.construction
    }

    pub(super) fn view(&self, rows: tracking::Rows) -> Result<Rows<'m>, PlanError> {
        Rows::view(self.catalog, &self.construction, rows)
    }

    pub(super) fn finish(
        self,
        root: Result<tracking::Rows, PlanError>,
    ) -> Result<LogicalPlan, PlanError> {
        let plan = self.construction.finish(root?)?;
        super::shape::plan(&plan, &self.internal)?;
        Ok(plan)
    }

    pub(super) fn connect(
        &mut self,
        operation: LogicalOperation,
        inputs: Vec<Rows<'m>>,
        check: impl FnOnce(
            LogicalCatalog<'m>,
            &mut Construction<Logical>,
            &[Rows<'m>],
        ) -> Result<Outputs<LogicalType>, PlanError>,
    ) -> Result<Rows<'m>, PlanError> {
        let catalog = self.catalog;
        self.construction.attempt(|construction| {
            let outputs = check(catalog, construction, &inputs)?;
            let inputs = inputs.into_iter().map(Rows::into_inner).collect();
            let connected = construction.connect(operation, inputs, outputs)?;
            Rows::view(catalog, construction, connected.rows)
        })
    }

    pub(super) fn attempt<T>(
        &mut self,
        step: impl FnOnce(LogicalCatalog<'m>, &mut Construction<Logical>) -> Result<T, PlanError>,
    ) -> Result<T, PlanError> {
        let catalog = self.catalog;
        self.construction
            .attempt(|construction| step(catalog, construction))
    }

    pub fn catalog(&self) -> LogicalCatalog<'m> {
        self.catalog
    }

    pub fn internal_name(&mut self, purpose: &str) -> Result<InternalName, PlanError> {
        let plain = purpose
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
        if purpose.is_empty() || !plain || purpose.starts_with('_') {
            return Err(PlanError::shape(format!(
                "internal name {purpose:?} must be a lowercase identifier"
            )));
        }
        let name = format!("{RESERVED_PREFIX}{purpose}");
        self.internal.push(name.clone());
        Ok(InternalName(name))
    }

    pub fn entities(&mut self, entity: Declared<EntityId>) -> Result<Rows<'m>, PlanError> {
        let name = self.attempt(|catalog, construction| {
            let id = construction.declared(entity)?;
            Ok(catalog.model().graph().entity(id).name.clone())
        })?;
        self.entities_as(entity, &name)
    }

    pub fn entities_as(
        &mut self,
        entity: Declared<EntityId>,
        name: &str,
    ) -> Result<Rows<'m>, PlanError> {
        let id = self.attempt(|_, construction| Ok(construction.declared(entity)?))?;
        self.connect(LogicalOperation::Entities(id), Vec::new(), |_, _, _| {
            Ok(Outputs::extend([Declare::named(
                name,
                LogicalType::Entity(id),
            )]))
        })
    }

    pub fn filter(&mut self, rows: Rows<'m>, predicate: Predicate) -> Result<Rows<'m>, PlanError> {
        let checked = predicate.clone();
        self.connect(
            LogicalOperation::Filter(predicate),
            vec![rows],
            |catalog, construction, _| {
                super::limits::in_values(catalog.model(), &checked, catalog.limits())?;
                super::conditions::conditions(catalog.model(), construction, &checked)?;
                filter::predicate(catalog.model(), construction, &checked)?;
                Ok(Outputs::preserve())
            },
        )
    }

    /// Matches an ast-grep pattern against a string property and declares `match.line` and one
    /// `match.<NAME>` column per capture.
    pub fn structural(
        &mut self,
        rows: Rows<'m>,
        target: Expr,
        pattern: &str,
        language: &str,
    ) -> Result<Rows<'m>, PlanError> {
        let invalid = |reason: String| PlanError::Invalid {
            expr: pattern.to_string(),
            reason,
        };
        let parsed = Pattern::ast_grep(pattern).map_err(|error| invalid(error.to_string()));
        let parsed = self.attempt(|_, _| parsed)?;
        let language = language
            .parse::<CodeLanguage>()
            .map_err(|_| invalid(format!("unknown language {language}")));
        let language = self.attempt(|_, _| language)?;
        let declared: Vec<Declare<LogicalType>> = std::iter::once(Declare::named(
            MATCH_LINE,
            LogicalType::Scalar(DataType::Int),
        ))
        .chain(parsed.captures().iter().map(|capture| {
            Declare::named(
                format!("match.{capture}"),
                LogicalType::Scalar(DataType::String),
            )
        }))
        .collect();
        let operation = LogicalOperation::Structural(Structural {
            target,
            pattern: parsed,
            language,
        });
        self.connect(operation, vec![rows], move |catalog, _, inputs| {
            if inputs.iter().any(|rows| rows.column(MATCH_LINE).is_ok()) {
                return Err(PlanError::Duplicate("a structural match".into()));
            }
            if inputs.iter().any(|rows| rows.bound().count() != 1) {
                return Err(PlanError::shape(
                    "MATCHES ast(...) must be a top-level condition of a single-node match",
                ));
            }
            let property = match target {
                Expr::Property { property, .. } => catalog.model().graph().property(property),
                other => {
                    return Err(PlanError::Type {
                        expr: format!("{other:?}"),
                        expected: "a property".into(),
                        found: "another expression".into(),
                    });
                }
            };
            if property.data_type != DataType::String {
                return Err(PlanError::Type {
                    expr: property.name.clone(),
                    expected: "a string".into(),
                    found: format!("{:?}", property.data_type),
                });
            }
            Ok(Outputs::extend(declared))
        })
    }

    pub fn pin(&mut self, rows: Rows<'m>, key: Expr, ids: &[i64]) -> Result<Rows<'m>, PlanError> {
        let predicate = key.one_of(ids.iter().copied().map(Literal::Int));
        let checked = predicate.clone();
        let count = ids.len();
        self.connect(
            LogicalOperation::Filter(predicate),
            vec![rows],
            |catalog, construction, _| {
                super::limits::at_most("pinned ids", count, catalog.limits().pins)?;
                filter::predicate(catalog.model(), construction, &checked)?;
                Ok(Outputs::preserve())
            },
        )
    }

    pub fn follow(
        &mut self,
        rows: Rows<'m>,
        relationship: Declared<RelationshipId>,
        direction: Direction,
    ) -> Result<Rows<'m>, PlanError> {
        let from = rows.endpoint()?;
        self.traverse(rows, from, Step::new(direction).via([relationship]))
    }

    pub fn follow_to(
        &mut self,
        rows: Rows<'m>,
        relationship: Declared<RelationshipId>,
        direction: Direction,
        target: Option<Declared<EntityId>>,
    ) -> Result<Rows<'m>, PlanError> {
        let from = rows.endpoint()?;
        let step = Step::new(direction).via([relationship]);
        let step = match target {
            Some(target) => step.to(target),
            None => step,
        };
        self.traverse(rows, from, step)
    }

    pub fn share(
        &mut self,
        name: &str,
        body: impl FnOnce(&mut Self) -> Result<Rows<'m>, PlanError>,
    ) -> Result<Body, PlanError> {
        let opening = self.construction.open(name)?;
        let root = body(self);
        let root = self.construction.attempt(|_| root)?;
        Ok(self.construction.close(opening, root.into_inner())?)
    }

    pub fn read(&mut self, body: Body) -> Result<Rows<'m>, PlanError> {
        let connected = self
            .construction
            .read(body, LogicalOperation::Read(body.id()))?;
        self.view(connected.rows)
    }

    pub fn declare_order(
        &mut self,
        bindings: impl IntoIterator<Item = Binding<'m>>,
    ) -> Result<(), PlanError> {
        let outputs: Vec<Ref> = bindings.into_iter().map(Binding::output).collect();
        Ok(self.construction.declare_order(&outputs)?)
    }

    pub(super) fn bound(binding: Binding<'m>) -> Result<EntityId, PlanError> {
        binding.entity().ok_or(PlanError::NoEntity)
    }
}
