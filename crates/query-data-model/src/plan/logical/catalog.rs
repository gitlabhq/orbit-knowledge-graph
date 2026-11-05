use super::{LogicalBuilder, LogicalPlan, PlanError, Rows};
use crate::plan::tracking::{self, Declared};
use crate::{EntityId, Model, RelationshipId};

#[derive(Debug, Clone, Copy)]
pub struct LogicalCatalog<'m> {
    model: &'m Model,
    admin: bool,
}

impl Model {
    pub fn logical(&self) -> LogicalCatalog<'_> {
        LogicalCatalog {
            model: self,
            admin: false,
        }
    }
}

impl<'m> LogicalCatalog<'m> {
    pub fn model(self) -> &'m Model {
        self.model
    }

    #[must_use]
    pub fn as_admin(self) -> Self {
        Self {
            admin: true,
            ..self
        }
    }

    pub fn admin(self) -> bool {
        self.admin
    }

    pub fn entity(self, name: &str) -> Result<Declared<EntityId>, PlanError> {
        Ok(Declared::new(self.model.id(), self.model.entity(name)?))
    }

    pub fn relationship(self, name: &str) -> Result<Declared<RelationshipId>, PlanError> {
        Ok(Declared::new(
            self.model.id(),
            self.model.relationship(name)?,
        ))
    }

    pub fn build(
        self,
        construct: impl FnOnce(&mut LogicalBuilder<'m>) -> Result<Rows<'m>, PlanError>,
    ) -> Result<LogicalPlan, PlanError> {
        let mut builder = LogicalBuilder::new(self, tracking::Construction::new(self.model.id()));
        let root = construct(&mut builder).map(Rows::into_inner);
        builder.finish(root)
    }

    pub fn rewrite(
        self,
        plan: LogicalPlan,
        mut visit: impl FnMut(&mut LogicalBuilder<'m>, Rows<'m>) -> Result<Rows<'m>, PlanError>,
    ) -> Result<LogicalPlan, PlanError> {
        let construction = tracking::Construction::rewrite(plan, self.model.id())?;
        let mut builder = LogicalBuilder::new(self, construction);
        let root = tracking::rewrite(&mut builder, LogicalBuilder::construction, |q, rows| {
            let rows = q.view(rows)?;
            visit(q, rows).map(Rows::into_inner)
        });
        builder.finish(root)
    }
}
