use super::limits::Limits;
use super::{LogicalBuilder, LogicalPlan, PlanError, Rows};
use crate::catalog::Store;
use crate::plan::tracking::{self, Declared, Each, Step};
use crate::{AttributeId, EntityId, Model, PropertyId, RelationshipId, RelationshipVariantId};

#[derive(Debug, Clone, Copy)]
pub struct LogicalCatalog<'m> {
    model: &'m Model,
    admin: bool,
    connection: Option<&'m str>,
    limits: Limits,
    all_columns: bool,
}

impl Model {
    pub fn logical(&self) -> LogicalCatalog<'_> {
        LogicalCatalog {
            model: self,
            admin: false,
            connection: None,
            limits: Limits::QUERY,
            all_columns: false,
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

    #[must_use]
    pub fn with_all_columns(self) -> Self {
        Self {
            all_columns: true,
            ..self
        }
    }

    /// The columns a node returns when the query lists none.
    pub fn default_columns(self) -> super::Columns {
        match self.all_columns {
            true => super::Columns::All,
            false => super::Columns::Default,
        }
    }

    #[must_use]
    pub fn with_limits(self, limits: Limits) -> Self {
        Self { limits, ..self }
    }

    pub fn limits(self) -> Limits {
        self.limits
    }

    #[must_use]
    pub fn on(self, connection: &'m str) -> Self {
        Self {
            connection: Some(connection),
            ..self
        }
    }

    fn stores(self) -> impl Iterator<Item = &'m Store> {
        let connection = self.connection;
        self.model
            .stores()
            .iter()
            .filter(move |store| Some(store.connection.as_str()) == connection)
    }

    pub(super) fn realized(self, entity: EntityId, property: PropertyId) -> bool {
        if self.connection.is_none() {
            return true;
        }
        let Some(home) = self.stores().find(|store| store.source(entity).is_some()) else {
            return true;
        };
        !home.excludes(property)
            && (self.model.lookup(property).is_some() || home.column(property).is_some())
    }

    pub(super) fn attribute_realized(
        self,
        variants: &[RelationshipVariantId],
        attribute: AttributeId,
    ) -> bool {
        self.connection.is_none()
            || variants.iter().all(|variant| {
                self.stores()
                    .any(|store| store.attribute_column(*variant, attribute).is_some())
            })
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

    pub fn build<E: From<PlanError>>(
        self,
        construct: impl FnOnce(&mut LogicalBuilder<'m>) -> Result<Rows<'m>, E>,
    ) -> Result<LogicalPlan, E> {
        let mut builder = LogicalBuilder::new(self, tracking::Construction::new(self.model.id()));
        let root = construct(&mut builder)?;
        Ok(builder.finish(Ok(root.into_inner()))?)
    }

    pub fn rebuild(
        self,
        plan: LogicalPlan,
        visit: impl FnMut(
            &mut LogicalBuilder<'m>,
            &mut Step<'_, '_, LogicalBuilder<'m>>,
        ) -> Result<Rows<'m>, PlanError>,
    ) -> Result<LogicalPlan, PlanError> {
        let mut builder = LogicalBuilder::new(self, tracking::Construction::new(self.model.id()));
        let root = tracking::rebuild(&mut builder, &plan, &mut Each(visit));
        builder.finish(root.map(Rows::into_inner))
    }
}
