use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::implementations::clickhouse::ClickHouse;
use crate::implementations::duckdb::DuckDb;
use crate::{
    DataModelError, DenormalizedCatalog, Entity, EntityId, ForeignKey, GraphCatalog,
    MaterializedJoin, Model, Packages, Property, PropertyId, QueryModel, RelationshipId,
    RelationshipVariant, RelationshipVariantId,
};

pub type ClickHouseDataModel = QueryModel<ClickHouse>;
pub type DuckDbDataModel = QueryModel<DuckDb>;

impl ClickHouseDataModel {
    pub const CONNECTION: &'static str = "main";

    pub fn from_model(model: &Arc<Model>) -> Result<Self, DataModelError> {
        Self::new(model, Self::CONNECTION)
    }

    pub fn embedded() -> Result<Self, DataModelError> {
        Self::from_model(&Packages::embedded().load()?)
    }

    pub fn versioned(packages: &Packages, table_prefix: &str) -> Result<Self, DataModelError> {
        let fragments =
            crate::implementations::clickhouse::versioned(packages.fragments()?, table_prefix)?;
        Self::from_model(&Arc::new(Model::load(&fragments)?))
    }
}

impl DuckDbDataModel {
    pub const CONNECTION: &'static str = "local";

    pub fn from_model(model: &Arc<Model>) -> Result<Self, DataModelError> {
        Self::new(model, Self::CONNECTION)
    }

    pub fn embedded() -> Result<Self, DataModelError> {
        Self::from_model(&Packages::embedded().load()?)
    }
}

#[derive(Debug, Clone)]
pub struct VariantRoute {
    pub table: String,
    pub foreign_key: Option<ForeignKey>,
}

#[derive(Debug, Clone)]
pub enum PropertyRealization {
    Stored { column: String },
    Virtual(ontology::VirtualSource),
}

#[derive(Debug)]
pub struct RelationshipRoute<'a> {
    pub table: &'a str,
    pub(crate) variants: Vec<&'a RelationshipVariant>,
}

impl RelationshipRoute<'_> {
    pub fn variants(&self) -> impl Iterator<Item = &RelationshipVariant> + '_ {
        self.variants.iter().copied()
    }

    pub fn source_entities(&self) -> impl Iterator<Item = EntityId> + '_ {
        self.variants.iter().map(|variant| variant.source)
    }

    pub fn target_entities(&self) -> impl Iterator<Item = EntityId> + '_ {
        self.variants.iter().map(|variant| variant.target)
    }

    pub fn has_source(&self, entity: EntityId) -> bool {
        self.source_entities().any(|source| source == entity)
    }

    pub fn has_target(&self, entity: EntityId) -> bool {
        self.target_entities().any(|target| target == entity)
    }
}

pub trait OrbitQueryModel {
    fn graph(&self) -> &GraphCatalog;
    fn entity(&self, name: &str) -> Option<&Entity>;
    fn property(&self, entity: &str, property: &str) -> Option<&Property>;
    fn property_is_nullable(&self, entity: &str, property: &str) -> bool;
    fn null_as_epoch(&self, property: PropertyId) -> bool;
    fn null_as_epoch_properties(&self, entity: EntityId) -> HashMap<String, ontology::DataType>;
    fn property_for_entity_id(&self, entity: EntityId, property: &str) -> Option<&Property>;
    fn high_fan_out(&self, entity: EntityId) -> bool;
    fn property_selectivity(&self, property: PropertyId) -> ontology::FieldSelectivity;
    fn relationship_exists(&self, relationship: &str) -> bool;
    fn table_column_type(&self, table: &str, column: &str) -> Option<ontology::DataType>;
    fn table_columns(&self, table: &str) -> Option<&HashSet<String>>;
    fn table_sort_key(&self, table: &str) -> Option<&[String]>;
    fn equivalent_layouts(&self, table: &str) -> Vec<&str>;
    fn in_sort_key(&self, table: &str, column: &str) -> bool;
    fn has_text_index(&self, property: PropertyId) -> bool;
    fn property_unique(&self, property: PropertyId) -> bool;
    fn property_mutable(&self, property: PropertyId) -> bool;
    fn property_is_free_text(&self, entity: &str, property: &str) -> bool;
    fn property_free_text(&self, property: PropertyId) -> bool;
    fn relationship_table(&self, relationship: &str) -> Option<&str>;
    fn default_edge_table(&self) -> &str;
    fn denormalized(&self) -> &DenormalizedCatalog;
    fn relationship_tables(&self, relationships: &[String]) -> Vec<String>;
    fn edge_tables(&self, relationships: &[RelationshipId]) -> Vec<String>;
    fn routes(&self, relationship: RelationshipId) -> Vec<RelationshipRoute<'_>>;
    fn variant_foreign_key(&self, variant: RelationshipVariantId) -> Option<ForeignKey>;
    fn relationship_tables_between(
        &self,
        relationships: &[String],
        source: &str,
        target: &str,
    ) -> Vec<String>;
    fn relationship_route(&self, relationship: &str) -> Option<RelationshipRoute<'_>>;
    fn variant_route(
        &self,
        relationship: &str,
        source: &str,
        target: &str,
    ) -> Option<&VariantRoute>;
    fn relationship_routes(&self, relationship: &str) -> Vec<RelationshipRoute<'_>>;
    fn foreign_key(
        &self,
        relationships: &[String],
        source: &str,
        target: &str,
    ) -> Option<ForeignKey>;
    fn traversal_path_lookup(
        &self,
        entity: &str,
        kind: ontology::TraversalPathKind,
    ) -> Option<(&str, &str)>;
    fn property_is_foreign_key(&self, property: PropertyId) -> bool;
    fn materialized_joins(&self) -> &[MaterializedJoin];
}
