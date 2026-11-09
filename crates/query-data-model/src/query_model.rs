use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock};

use ontology::TraversalPathKind;

use crate::catalog::{Catalog, Model, StorageBackend};
use crate::implementations::relational::RelationalFacts;
use crate::{
    DataModelError, DenormalizedCatalog, EntityId, GraphCatalog, MaterializedJoin, OrbitQueryModel,
    PropertyId, PropertyRealization, TraversalPathLookup,
};

pub trait QueryFacts: RelationalFacts + std::fmt::Debug + Send + Sync {
    fn text_index(&self, property: PropertyId) -> bool;
    fn null_as_epoch(&self, property: PropertyId) -> bool;
    fn high_fan_out(&self, entity: EntityId) -> bool;
    fn denormalized(&self) -> &DenormalizedCatalog;
    fn path_lookup(
        &self,
        entity: EntityId,
        kind: TraversalPathKind,
    ) -> Option<&TraversalPathLookup>;
    fn copies_of<'a>(&'a self, table: &str) -> Vec<&'a str>;
    fn joins(&self) -> &[MaterializedJoin];
}

#[derive(Debug)]
struct Bindings<B: StorageBackend>
where
    B::Store: QueryFacts,
{
    catalog: Catalog<B>,
    homes: Vec<Option<usize>>,
    realizations: Vec<Option<PropertyRealization>>,
}

impl<B: StorageBackend> Bindings<B>
where
    B::Store: QueryFacts,
{
    fn new(model: &Arc<Model>, connection: &str) -> Result<Self, DataModelError> {
        let catalog = model.physical::<B>(connection)?;
        let graph = catalog.graph();
        let stores: Vec<_> = catalog.stores().collect();
        let homes: Vec<Option<usize>> = graph
            .entities()
            .map(|entity| {
                stores
                    .iter()
                    .position(|(store, _)| store.source(entity.id).is_some())
            })
            .collect();
        let realizations = graph
            .properties()
            .map(|property| {
                let (store, _) = stores.get(homes[property.entity.index()]?)?;
                if store.excludes(property.id) {
                    return None;
                }
                if let Some(lookup) = model.lookup(property.id) {
                    return Some(PropertyRealization::Virtual(ontology::VirtualSource {
                        service: lookup.service.clone(),
                        lookup: lookup.operation.clone(),
                        disabled: lookup.disabled,
                        depends_on: lookup
                            .keys
                            .iter()
                            .map(|key| graph.property(*key).name.clone())
                            .collect(),
                        allowed_ops: lookup.operators.clone(),
                    }));
                }
                Some(PropertyRealization::Stored {
                    column: store.column(property.id)?.to_string(),
                })
            })
            .collect();
        Ok(Self {
            catalog,
            homes,
            realizations,
        })
    }

    pub(crate) fn home(&self, entity: EntityId) -> Option<(&crate::catalog::Store, &B::Store)> {
        self.catalog.stores().nth(self.homes[entity.index()]?)
    }

    pub(crate) fn facts_of(&self, property: PropertyId) -> Option<&B::Store> {
        let entity = self.graph_catalog().property(property).entity;
        self.home(entity).map(|(_, facts)| facts)
    }

    pub(crate) fn graph_catalog(&self) -> &GraphCatalog {
        self.catalog.graph()
    }

    pub(crate) fn table(
        &self,
        name: &str,
    ) -> Option<&crate::implementations::relational::TableFacts> {
        self.catalog
            .stores()
            .find_map(|(_, facts)| facts.table(name))
    }
}

impl<B: StorageBackend> Bindings<B>
where
    B::Store: QueryFacts,
{
    pub(crate) fn equivalent_layouts(&self, table: &str) -> Vec<&str> {
        self.catalog
            .stores()
            .flat_map(|(_, facts)| facts.copies_of(table))
            .collect()
    }

    pub(crate) fn materialized_joins(&self) -> &[MaterializedJoin] {
        self.catalog
            .stores()
            .map(|(_, facts)| facts.joins())
            .find(|joins| !joins.is_empty())
            .unwrap_or_default()
    }

    pub(crate) fn entity_high_fan_out(&self, entity: EntityId) -> bool {
        self.home(entity)
            .is_some_and(|(_, facts)| facts.high_fan_out(entity))
    }

    pub(crate) fn property_is_foreign_key(&self, property: PropertyId) -> bool {
        !self
            .graph_catalog()
            .property(property)
            .references
            .is_empty()
    }

    pub(crate) fn property_column(&self, property: PropertyId) -> Option<&str> {
        match self.property_realization(property)? {
            PropertyRealization::Stored { column } => Some(column),
            PropertyRealization::Virtual(_) => None,
        }
    }

    pub(crate) fn property_realization(
        &self,
        property: PropertyId,
    ) -> Option<&PropertyRealization> {
        self.realizations[property.index()].as_ref()
    }

    pub(crate) fn table_column_type(
        &self,
        table: &str,
        column: &str,
    ) -> Option<ontology::DataType> {
        self.table(table)?.column_types.get(column).copied()
    }

    pub(crate) fn has_text_index(&self, property: PropertyId) -> bool {
        self.facts_of(property)
            .is_some_and(|facts| facts.text_index(property))
    }

    pub(crate) fn null_as_epoch(&self, property: PropertyId) -> bool {
        self.facts_of(property)
            .is_some_and(|facts| facts.null_as_epoch(property))
    }

    pub(crate) fn table_columns(&self, table: &str) -> Option<&HashSet<String>> {
        Some(&self.table(table)?.columns)
    }

    pub(crate) fn table_sort_key(&self, table: &str) -> Option<&[String]> {
        Some(&self.table(table)?.sort_key)
    }

    pub(crate) fn denormalized(&self) -> &DenormalizedCatalog {
        static EMPTY: LazyLock<DenormalizedCatalog> = LazyLock::new(DenormalizedCatalog::default);
        self.catalog
            .stores()
            .next()
            .map_or(&EMPTY, |(_, facts)| facts.denormalized())
    }

    pub(crate) fn traversal_path_lookup(
        &self,
        entity: EntityId,
        kind: TraversalPathKind,
    ) -> Option<&TraversalPathLookup> {
        self.home(entity)?.1.path_lookup(entity, kind)
    }
}

#[derive(Debug)]
pub struct QueryModel<B: StorageBackend>
where
    B::Store: QueryFacts,
{
    bindings: Bindings<B>,
}

impl<B: StorageBackend> QueryModel<B>
where
    B::Store: QueryFacts,
{
    pub fn new(model: &Arc<Model>, connection: &str) -> Result<Self, DataModelError> {
        Ok(Self {
            bindings: Bindings::new(model, connection)?,
        })
    }

    pub fn catalog(&self) -> &Catalog<B> {
        &self.bindings.catalog
    }
}

impl<B: StorageBackend> OrbitQueryModel for QueryModel<B>
where
    B::Store: QueryFacts,
{
    fn graph(&self) -> &GraphCatalog {
        self.bindings.graph_catalog()
    }

    fn property_is_foreign_key(&self, property: PropertyId) -> bool {
        self.bindings.property_is_foreign_key(property)
    }

    fn materialized_joins(&self) -> &[MaterializedJoin] {
        self.bindings.materialized_joins()
    }

    fn null_as_epoch(&self, property: PropertyId) -> bool {
        self.bindings.null_as_epoch(property)
    }

    fn null_as_epoch_properties(&self, entity: EntityId) -> HashMap<String, ontology::DataType> {
        self.graph()
            .entity(entity)
            .properties
            .iter()
            .filter(|property| self.null_as_epoch(**property))
            .map(|property| {
                let property = self.graph().property(*property);
                (property.name.clone(), property.data_type)
            })
            .collect()
    }

    fn high_fan_out(&self, entity: EntityId) -> bool {
        self.bindings.entity_high_fan_out(entity)
    }

    fn table_column_type(&self, table: &str, column: &str) -> Option<ontology::DataType> {
        self.bindings.table_column_type(table, column)
    }

    fn table_columns(&self, table: &str) -> Option<&HashSet<String>> {
        self.bindings.table_columns(table)
    }

    fn table_sort_key(&self, table: &str) -> Option<&[String]> {
        self.bindings.table_sort_key(table)
    }

    fn equivalent_layouts(&self, table: &str) -> Vec<&str> {
        self.bindings.equivalent_layouts(table)
    }

    fn in_sort_key(&self, table: &str, column: &str) -> bool {
        self.table_sort_key(table)
            .is_some_and(|key| key.iter().any(|sort_column| sort_column == column))
    }

    fn has_text_index(&self, property: PropertyId) -> bool {
        self.bindings.has_text_index(property)
    }

    fn denormalized(&self) -> &DenormalizedCatalog {
        self.bindings.denormalized()
    }

    fn traversal_path_lookup(
        &self,
        entity: &str,
        kind: ontology::TraversalPathKind,
    ) -> Option<(&str, &str)> {
        let entity = self.graph().entity_id(entity)?;
        let lookup = self.bindings.traversal_path_lookup(entity, kind)?;
        let column = self.bindings.property_column(lookup.property)?;
        Some((&lookup.table, column))
    }
}
