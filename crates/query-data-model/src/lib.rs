pub mod catalog;
mod error;
#[cfg(feature = "from-ontology")]
pub mod from_ontology;
pub mod generic;
pub mod implementations;
pub mod packages;
pub mod plan;
pub mod spec;
pub mod sql_template;
pub mod surface;

pub use catalog::{Catalog, Model, StorageBackend};
pub use error::DataModelError;
pub use generic::{
    Attribute, AttributeId, Direction, Endpoint, Entity, EntityId, ForeignKey, GraphCatalog, Hop,
    Property, PropertyId, Reach, Relationship, RelationshipId, RelationshipVariant,
    RelationshipVariantId, Walk,
};
pub use implementations::clickhouse::ClickHouse;
pub use implementations::clickhouse::facts::{
    DenormalizedCatalog, DenormalizedDirection, DenormalizedKey, DenormalizedProperty,
    MaterializedJoin, MaterializedNode, MaterializedRelationship, TraversalPathLookup,
};
pub use implementations::duckdb::DuckDb;
pub use implementations::relational::PathColumn;
pub use implementations::relational::RelationshipRoute;
pub use ontology::constants::{
    DEFAULT_PRIMARY_KEY, DELETED_COLUMN, TRAVERSAL_PATH_COLUMN, VERSION_COLUMN,
};
pub use ontology::{DataType, EnumType};
pub use packages::Packages;
