pub mod catalog;
pub mod constants;
mod error;
#[cfg(feature = "from-ontology")]
pub mod from_ontology;
pub mod generic;
pub mod implementations;
pub mod introspection;
pub mod packages;
pub mod plan;
pub mod spec;
pub mod sql_template;
pub mod surface;
mod types;

pub use catalog::{Catalog, Model, StorageBackend};
pub use constants::{DEFAULT_PRIMARY_KEY, DELETED_COLUMN, TRAVERSAL_PATH_COLUMN, VERSION_COLUMN};
pub use error::DataModelError;
pub use generic::{
    Attribute, AttributeId, Direction, Domain, Endpoint, Entity, EntityId, ForeignKey,
    GraphCatalog, Hop, Property, PropertyId, Reach, Relationship, RelationshipId,
    RelationshipVariant, RelationshipVariantId, Style, Walk,
};
pub use implementations::clickhouse::ClickHouse;
pub use implementations::clickhouse::facts::{
    DenormalizedCatalog, DenormalizedDirection, DenormalizedKey, DenormalizedProperty,
    MaterializedJoin, MaterializedNode, MaterializedRelationship, TraversalPathLookup,
};
pub use implementations::duckdb::DuckDb;
pub use implementations::relational::PathColumn;
pub use implementations::relational::RelationshipRoute;
pub use packages::Packages;
pub use types::{
    DataType, EdgeVariantScope, EnumType, FieldSelectivity, RequiredRole, TraversalPathKind,
};
