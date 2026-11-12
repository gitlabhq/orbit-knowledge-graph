use orbit_server_config::QueryLimits;

use super::{LogicalBuilder, LogicalPlan, PlanError, Rows};
use crate::catalog::Store;
use crate::plan::tracking::{self, Declared, Each, Step};
use crate::{EntityId, Model, PropertyId, RelationshipId, RelationshipVariantId};

#[derive(Debug, Clone, Copy)]
pub struct LogicalCatalog<'m> {
    model: &'m Model,
    admin: bool,
    served: &'m [String],
    restricted: bool,
    limits: &'m QueryLimits,
    all_columns: bool,
}

impl Model {
    pub fn logical<'m>(&'m self, limits: &'m QueryLimits) -> LogicalCatalog<'m> {
        LogicalCatalog {
            model: self,
            admin: false,
            served: &[],
            restricted: false,
            limits,
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

    pub fn limits(self) -> &'m QueryLimits {
        self.limits
    }

    /// Reads only what the stores of the `served` connections hold, as a query's frontend does.
    #[must_use]
    pub fn serving(self, served: &'m [String]) -> Self {
        Self {
            served,
            restricted: true,
            ..self
        }
    }

    /// Plans across the `served` connections, whose stage plans read what the frontend resolved.
    #[must_use]
    pub fn across(self, served: &'m [String]) -> Self {
        Self { served, ..self }
    }

    /// Whether the deployment serves several connections, whose stages the engine joins.
    pub fn serves_peers(self) -> bool {
        self.served.len() > 1
    }

    fn stores(self) -> impl Iterator<Item = &'m Store> {
        self.model
            .stores()
            .iter()
            .filter(move |store| self.served.contains(&store.connection))
    }

    /// The entity's home store among the served ones holds the property, or a served store
    /// searches it.
    pub(super) fn realized(self, entity: EntityId, property: PropertyId) -> bool {
        if !self.restricted {
            return true;
        }
        let mut holders = self.stores().filter(|store| store.source(entity).is_some());
        let Some(first) = holders.next() else {
            return true;
        };
        let home = std::iter::once(first)
            .chain(holders)
            .find(|store| store.complete(entity))
            .unwrap_or(first);
        let searched = self
            .stores()
            .any(|store| store.source(entity).is_some() && store.searches(property));
        searched
            || !home.excludes(property)
                && (home.complete(entity) && self.model.lookup(property).is_some()
                    || home.column(property).is_some())
    }

    /// Every variant keeps its rows in an edge table, which the loader checks holds each
    /// attribute of the relationship.
    pub(super) fn attribute_realized(self, variants: &[RelationshipVariantId]) -> bool {
        !self.restricted
            || variants.iter().all(|variant| {
                self.stores()
                    .any(|store| store.edge_table(*variant).is_some())
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
