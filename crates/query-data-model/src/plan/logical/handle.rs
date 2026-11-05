use super::{Expr, Logical, LogicalType, PlanError};
use crate::plan::tracking::{self, Construction, Ref};
use crate::{EntityId, Model};

#[derive(Debug, Clone, Copy)]
pub struct Binding<'m> {
    model: &'m Model,
    entity: EntityId,
    output: Ref,
}

impl<'m> Binding<'m> {
    pub fn entity(self) -> EntityId {
        self.entity
    }

    pub fn output(self) -> Ref {
        self.output
    }

    pub fn property(self, name: &str) -> Result<Expr, PlanError> {
        let property = self.model.property(self.entity, name)?;
        if self.model.access().property(property.id).hidden {
            let entity = &self.model.graph().entity(self.entity).name;
            return Err(PlanError::Hidden(format!("{entity}.{name}")));
        }
        Ok(Expr::Property {
            binding: self.output,
            property: property.id,
        })
    }
}

#[derive(Debug)]
pub struct Rows<'m> {
    rows: tracking::Rows,
    endpoint: Option<Binding<'m>>,
    columns: Vec<(String, Ref)>,
}

impl<'m> Rows<'m> {
    pub(super) fn view(
        model: &'m Model,
        construction: &Construction<Logical>,
        rows: tracking::Rows,
    ) -> Result<Self, PlanError> {
        let mut endpoint = None;
        let mut columns = Vec::new();
        for output in construction.outputs(&rows)? {
            match construction.value(output)? {
                (_, LogicalType::Entity(entity)) => {
                    endpoint = Some(Binding {
                        model,
                        entity: *entity,
                        output,
                    });
                }
                (Some(name), LogicalType::Scalar(_)) => columns.push((name.to_string(), output)),
                (None, LogicalType::Scalar(_)) => {}
            }
        }
        Ok(Self {
            rows,
            endpoint,
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
        self.endpoint.ok_or(PlanError::NoEntity)
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
