use super::{EdgeField, Expr, Logical, LogicalCatalog, LogicalType, PlanError, TimeUnit};
use crate::plan::tracking::{self, Construction, Ref};
use crate::{EntityId, PropertyId};

#[derive(Debug, Clone, Copy)]
pub struct Binding<'m> {
    catalog: LogicalCatalog<'m>,
    kind: LogicalType,
    output: Ref,
}

impl<'m> Binding<'m> {
    pub fn kind(self) -> LogicalType {
        self.kind
    }

    pub fn entity(self) -> Option<EntityId> {
        match self.kind {
            LogicalType::Entity(entity) => Some(entity),
            _ => None,
        }
    }

    pub fn output(self) -> Ref {
        self.output
    }

    pub fn node(self) -> Expr {
        Expr::Node(self.output)
    }

    pub fn property(self, name: &str) -> Result<Expr, PlanError> {
        Ok(Expr::Property {
            binding: self.output,
            property: self.resolve(name)?,
        })
    }

    pub fn truncate(self, name: &str, unit: TimeUnit) -> Result<Expr, PlanError> {
        Ok(Expr::Truncate {
            binding: self.output,
            property: self.resolve(name)?,
            unit,
        })
    }

    pub fn field(self, field: EdgeField) -> Result<Expr, PlanError> {
        match self.kind {
            LogicalType::Edge => Ok(Expr::Edge {
                binding: self.output,
                field,
            }),
            found => Err(PlanError::Type {
                expr: format!("{field:?}"),
                expected: "a relationship".to_string(),
                found: format!("{found:?}"),
            }),
        }
    }

    fn resolve(self, name: &str) -> Result<PropertyId, PlanError> {
        let entity = self.entity().ok_or(PlanError::NoEntity)?;
        let model = self.catalog.model();
        let property = model.property(entity, name)?;
        let access = model.access().property(property.id);
        let qualified = || format!("{}.{name}", model.graph().entity(entity).name);
        if access.hidden {
            return Err(PlanError::Hidden(qualified()));
        }
        if access.admin_only && !self.catalog.admin() {
            return Err(PlanError::AdminOnly(qualified()));
        }
        Ok(property.id)
    }
}

#[derive(Debug)]
pub struct Rows<'m> {
    rows: tracking::Rows,
    bindings: Vec<(Option<String>, Binding<'m>)>,
    columns: Vec<(String, Ref)>,
}

impl<'m> Rows<'m> {
    pub(super) fn view(
        catalog: LogicalCatalog<'m>,
        construction: &Construction<Logical>,
        rows: tracking::Rows,
    ) -> Result<Self, PlanError> {
        let mut bindings = Vec::new();
        let mut columns = Vec::new();
        for output in construction.outputs(&rows)? {
            match construction.value(output)? {
                (Some(name), LogicalType::Scalar(_)) => columns.push((name.to_string(), output)),
                (_, LogicalType::Scalar(_)) => {}
                (name, kind) => bindings.push((
                    name.map(str::to_string),
                    Binding {
                        catalog,
                        kind: *kind,
                        output,
                    },
                )),
            }
        }
        Ok(Self {
            rows,
            bindings,
            columns,
        })
    }

    pub(super) fn into_inner(self) -> tracking::Rows {
        self.rows
    }

    pub(super) fn inner(&self) -> &tracking::Rows {
        &self.rows
    }

    pub fn endpoint(&self) -> Result<Binding<'m>, PlanError> {
        self.bindings
            .iter()
            .rev()
            .map(|(_, binding)| *binding)
            .find(|binding| matches!(binding.kind, LogicalType::Entity(_) | LogicalType::Node))
            .ok_or(PlanError::NoEntity)
    }

    pub fn edge(&self) -> Option<Binding<'m>> {
        self.bindings
            .iter()
            .rev()
            .map(|(_, binding)| *binding)
            .find(|binding| binding.kind == LogicalType::Edge)
    }

    pub fn binding(&self, name: &str) -> Result<Binding<'m>, PlanError> {
        self.bindings
            .iter()
            .find(|(bound, _)| bound.as_deref() == Some(name))
            .map(|(_, binding)| *binding)
            .ok_or_else(|| PlanError::UnknownBinding {
                name: name.to_string(),
                available: self
                    .bindings
                    .iter()
                    .filter_map(|(name, _)| name.clone())
                    .collect(),
            })
    }

    pub fn property(&self, name: &str) -> Result<Expr, PlanError> {
        self.endpoint()?.property(name)
    }

    pub fn column(&self, name: &str) -> Result<Expr, PlanError> {
        self.columns
            .iter()
            .find(|(column, _)| column == name)
            .map(|(_, output)| Expr::Output(*output))
            .ok_or_else(|| PlanError::UnknownColumn {
                name: name.to_string(),
                available: self.columns.iter().map(|(name, _)| name.clone()).collect(),
            })
    }
}
