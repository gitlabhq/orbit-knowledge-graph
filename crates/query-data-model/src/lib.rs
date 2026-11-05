pub mod catalog;
mod error;
pub mod from_ontology;
pub mod generic;
pub mod implementations;
pub mod models;
pub mod packages;
pub mod query_model;
pub mod spec;

pub use catalog::{Catalog, Model, StorageBackend};
pub use error::DataModelError;
pub use generic::{
    Direction, Endpoint, Entity, EntityId, ForeignKey, GraphCatalog, Hop, Property, PropertyId,
    Reach, Relationship, RelationshipId, RelationshipVariant, RelationshipVariantId, Walk,
};
pub use implementations::clickhouse::facts::{
    DenormalizedCatalog, DenormalizedDirection, DenormalizedKey, DenormalizedProperty,
    MaterializedJoin, MaterializedNode, MaterializedRelationship, TraversalPathLookup,
};
pub use implementations::relational::PathColumn;
pub use models::orbit::{
    ClickHouseDataModel, DuckDbDataModel, OrbitQueryModel, PropertyRealization, RelationshipRoute,
    VariantRoute,
};
pub use packages::Packages;
pub use query_model::{EntityAuthConfig, QueryFacts, QueryModel};
