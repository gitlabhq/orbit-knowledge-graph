pub mod catalog;
pub mod constants;
mod deployments;
mod error;
pub mod generic;
pub mod implementations;
pub mod introspection;
pub mod packages;
pub mod plan;
mod spec;
pub mod sql_template;
pub mod surface;
#[cfg(feature = "testkit")]
pub mod testkit;
mod types;

pub use catalog::{Catalog, Model, StorageBackend};
pub use constants::{DEFAULT_PRIMARY_KEY, DELETED_COLUMN, TRAVERSAL_PATH_COLUMN, VERSION_COLUMN};
pub use deployments::Deployments;
pub use error::DataModelError;
pub use generic::{
    Attribute, ConnectionId, Direction, Domain, EdgeField, Endpoint, Entity, EntityId, ForeignKey,
    GraphCatalog, Hop, Property, PropertyId, Relationship, RelationshipId, RelationshipVariant,
    RelationshipVariantId, StoreId,
};
pub use implementations::clickhouse::ClickHouse;
pub use implementations::clickhouse::facts::{
    DenormalizedCatalog, DenormalizedDirection, DenormalizedKey, DenormalizedProperty,
    MaterializedJoin, MaterializedNode, MaterializedRelationship, TraversalPathLookup,
};
pub use implementations::duckdb::DuckDb;
pub use implementations::relational::RelationshipRoute;
pub use packages::{Packages, Tier};
pub use spec::schemas;
pub use types::{Cardinality, DataType, EnumType, FieldSelectivity, TraversalPathKind};
