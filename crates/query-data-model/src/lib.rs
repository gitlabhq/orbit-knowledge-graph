pub mod catalog;
mod error;
#[cfg(feature = "from-ontology")]
pub mod from_ontology;
pub mod generic;
pub mod implementations;
pub mod models;
pub mod packages;
pub mod plan;
pub mod query_model;
pub mod spec;
pub mod surface;

pub use catalog::{Catalog, Model, StorageBackend};
pub use error::DataModelError;
pub use generic::{
    Attribute, AttributeId, Direction, Endpoint, Entity, EntityId, ForeignKey, GraphCatalog, Hop,
    Property, PropertyId, Reach, Relationship, RelationshipId, RelationshipVariant,
    RelationshipVariantId, Walk,
};
pub use implementations::clickhouse::facts::{
    DenormalizedCatalog, DenormalizedDirection, DenormalizedKey, DenormalizedProperty,
    MaterializedJoin, MaterializedNode, MaterializedRelationship, TraversalPathLookup,
};
pub use implementations::relational::PathColumn;
pub use implementations::relational::RelationshipRoute;
pub use models::orbit::{ClickHouseDataModel, DuckDbDataModel, OrbitQueryModel};
pub use packages::Packages;
pub use query_model::QueryModel;
