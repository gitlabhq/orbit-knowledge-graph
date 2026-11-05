use std::collections::BTreeSet;

use super::{
    Expr, Logical, LogicalCatalog, LogicalOperation, LogicalPlan, LogicalType, Measure, Named,
    Order, PlanError, Predicate, Rows, check,
};
use crate::plan::tracking::{self, Body, Construction, Declare, Declared, Output, Outputs, Ref};
use crate::{Direction, EntityId, Model, RelationshipId};

pub struct LogicalBuilder<'m> {
    catalog: LogicalCatalog<'m>,
    construction: Construction<Logical>,
}

impl<'m> LogicalBuilder<'m> {
    pub(super) fn new(catalog: LogicalCatalog<'m>, construction: Construction<Logical>) -> Self {
        Self {
            catalog,
            construction,
        }
    }

    pub(super) fn construction(&mut self) -> &mut Construction<Logical> {
        &mut self.construction
    }

    pub(super) fn view(&self, rows: tracking::Rows) -> Result<Rows<'m>, PlanError> {
        Rows::view(self.catalog.model(), &self.construction, rows)
    }

    pub(super) fn finish(
        self,
        root: Result<tracking::Rows, PlanError>,
    ) -> Result<LogicalPlan, PlanError> {
        Ok(self.construction.finish(root?)?)
    }

    fn connect(
        &mut self,
        operation: LogicalOperation,
        inputs: Vec<Rows<'m>>,
        check: impl FnOnce(&Model, &Construction<Logical>) -> Result<Outputs<LogicalType>, PlanError>,
    ) -> Result<Rows<'m>, PlanError> {
        let model = self.catalog.model();
        self.construction.attempt(|construction| {
            let outputs = check(model, construction)?;
            let inputs = inputs.into_iter().map(Rows::into_inner).collect();
            let connected = construction.connect(operation, inputs, outputs)?;
            Rows::view(model, construction, connected.rows)
        })
    }

    pub fn catalog(&self) -> LogicalCatalog<'m> {
        self.catalog
    }

    pub fn operation(&self, rows: &Rows<'m>) -> Result<&LogicalOperation, PlanError> {
        Ok(self.construction.operation(rows.inner())?)
    }

    pub fn entities(&mut self, entity: Declared<EntityId>) -> Result<Rows<'m>, PlanError> {
        let id = self
            .construction
            .attempt(|construction| construction.declared(entity))?;
        self.connect(LogicalOperation::Entities(id), Vec::new(), |_, _| {
            Ok(Outputs::extend([Declare::anonymous(LogicalType::Entity(
                id,
            ))]))
        })
    }

    pub fn filter(&mut self, rows: Rows<'m>, predicate: Predicate) -> Result<Rows<'m>, PlanError> {
        let checked = predicate.clone();
        self.connect(
            LogicalOperation::Filter(predicate),
            vec![rows],
            |model, construction| {
                check::predicate(model, construction, &checked)?;
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
        self.follow_to(rows, relationship, direction, None)
    }

    pub fn follow_to(
        &mut self,
        rows: Rows<'m>,
        relationship: Declared<RelationshipId>,
        direction: Direction,
        target: Option<Declared<EntityId>>,
    ) -> Result<Rows<'m>, PlanError> {
        let model = self.catalog.model();
        let (from, hops, to) = self.construction.attempt(|construction| {
            let relationship = construction.declared(relationship)?;
            let target = target
                .map(|target| construction.declared(target))
                .transpose()?;
            let from = rows.endpoint()?;
            let hops = model.connect(from.entity(), &[relationship], target, direction)?;
            let reached: BTreeSet<EntityId> = hops.iter().map(|hop| hop.to).collect();
            match Vec::from_iter(reached).as_slice() {
                [to] => Ok((from, hops, *to)),
                reached => Err(PlanError::Ambiguous {
                    relationship: model.graph().relationship(relationship).name.clone(),
                    endpoints: reached
                        .iter()
                        .map(|entity| model.graph().entity(*entity).name.clone())
                        .collect(),
                }),
            }
        })?;
        let operation = LogicalOperation::Follow {
            from: from.output(),
            hops,
        };
        self.connect(operation, vec![rows], |_, _| {
            Ok(Outputs::extend([Declare::anonymous(LogicalType::Entity(
                to,
            ))]))
        })
    }

    pub fn aggregate(
        &mut self,
        rows: Rows<'m>,
        groups: impl IntoIterator<Item = Named<Expr>>,
        measures: impl IntoIterator<Item = Named<Measure>>,
    ) -> Result<Rows<'m>, PlanError> {
        let (groups, group_names): (Vec<_>, Vec<_>) = groups
            .into_iter()
            .map(|named| (named.item, named.name))
            .unzip();
        let (measures, measure_names): (Vec<_>, Vec<_>) = measures
            .into_iter()
            .map(|named| (named.item, named.name))
            .unzip();
        let operation = LogicalOperation::Aggregate {
            groups: groups.clone(),
            measures: measures.clone(),
        };
        self.connect(operation, vec![rows], |model, construction| {
            let group_types = groups
                .iter()
                .map(|group| Ok(check::scalar(model, construction, *group)?.data_type));
            let measure_types = measures
                .iter()
                .map(|measure| check::measure(model, construction, *measure));
            declare(
                group_names.into_iter().chain(measure_names),
                group_types.chain(measure_types),
            )
        })
    }

    pub fn select(
        &mut self,
        rows: Rows<'m>,
        items: impl IntoIterator<Item = Named<Expr>>,
    ) -> Result<Rows<'m>, PlanError> {
        let (exprs, names): (Vec<_>, Vec<_>) = items
            .into_iter()
            .map(|named| (named.item, named.name))
            .unzip();
        let operation = LogicalOperation::Select(exprs.clone());
        self.connect(operation, vec![rows], |model, construction| {
            let types = exprs
                .iter()
                .map(|expr| Ok(check::scalar(model, construction, *expr)?.data_type));
            declare(names, types)
        })
    }

    pub fn order(
        &mut self,
        rows: Rows<'m>,
        keys: impl IntoIterator<Item = (Expr, Order)>,
    ) -> Result<Rows<'m>, PlanError> {
        let keys: Vec<_> = keys.into_iter().collect();
        let checked: Vec<Expr> = keys.iter().map(|(key, _)| *key).collect();
        self.connect(
            LogicalOperation::Order(keys),
            vec![rows],
            |model, construction| {
                for key in checked {
                    check::scalar(model, construction, key)?;
                }
                Ok(Outputs::preserve())
            },
        )
    }

    pub fn limit(&mut self, rows: Rows<'m>, count: u64) -> Result<Rows<'m>, PlanError> {
        self.connect(LogicalOperation::Limit(count), vec![rows], |_, _| {
            Ok(Outputs::preserve())
        })
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
        let connected = self.construction.read(body)?;
        self.view(connected.rows)
    }

    pub fn reconnect(&mut self, from: Ref, to: Ref) -> Result<(), PlanError> {
        Ok(self.construction.reconnect(from, to)?)
    }
}

fn declare(
    names: impl IntoIterator<Item = String>,
    types: impl Iterator<Item = Result<ontology::DataType, PlanError>>,
) -> Result<Outputs<LogicalType>, PlanError> {
    let outputs = names
        .into_iter()
        .zip(types)
        .map(|(name, data_type)| {
            Ok(Output::Declare(Declare::named(
                name,
                LogicalType::Scalar(data_type?),
            )))
        })
        .collect::<Result<Vec<_>, PlanError>>()?;
    Ok(Outputs::replace(outputs))
}
