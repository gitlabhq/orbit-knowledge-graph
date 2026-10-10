mod derive;
mod error;
pub mod generic;
pub mod implementations;
pub mod orbit;
pub mod storage;
pub use storage::{Relational, RelationalBackend, Storage, StorageModel};

pub use error::DataModelError;
pub use generic::{
    DataModel, Entity, EntityId, GraphCatalog, Property, PropertyId, Relationship, RelationshipId,
    RelationshipVariant, RelationshipVariantId,
};
pub use implementations::{
    EntityAuthConfig, GitLabAuthzCatalog, GitLabPolicy, TrustedLocalCatalog,
};

pub use orbit::{
    DenormalizedCatalog, DenormalizedDirection, DenormalizedKey, DenormalizedProperty, Endpoint,
    ForeignKey, PathColumn, PropertyRealization, RelationshipRoute, TraversalPathLookup,
};
pub use orbit::{OrbitQueryModel, RelationalMapping};
pub type ClickHouseDataModel =
    DataModel<Relational<implementations::clickhouse::storage::ClickHouse>, GitLabAuthzCatalog>;
pub type DuckDbDataModel =
    DataModel<Relational<implementations::duckdb::storage::DuckDb>, TrustedLocalCatalog>;
