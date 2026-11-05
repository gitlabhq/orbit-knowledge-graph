pub mod catalog;
mod derive;
mod error;
pub mod from_ontology;
pub mod generic;
pub mod implementations;
pub mod layout;
pub mod models;
pub mod query_model;
pub mod spec;
pub use layout::{Relational, RelationalBackend};

pub use catalog::{Catalog, Model, StorageBackend};
pub use error::DataModelError;
pub use generic::{
    Direction, Entity, EntityId, GraphCatalog, Hop, Property, PropertyId, Reach, Relationship,
    RelationshipId, RelationshipVariant, RelationshipVariantId, Walk,
};
pub use layout::relational::{
    MaterializedJoin, MaterializedNode, MaterializedRelationship, VariantRoute,
};
pub use query_model::{EntityAuthConfig, QueryFacts, QueryModel};

pub use layout::relational::{
    DenormalizedCatalog, DenormalizedDirection, DenormalizedKey, DenormalizedProperty, Endpoint,
    ForeignKey, PathColumn, PropertyRealization, RelationshipRoute, TraversalPathLookup,
};
pub use models::orbit::{ClickHouseDataModel, DuckDbDataModel, OrbitQueryModel};
