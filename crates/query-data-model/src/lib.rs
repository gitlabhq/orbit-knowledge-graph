mod derive;
mod error;
pub mod generic;
pub mod implementations;
pub mod ontology_adapter;
pub mod storage;
pub use storage::{Relational, RelationalBackend, Storage, StorageModel};

pub use error::DataModelError;
pub use generic::{
    DataModel, DenormalizedCatalog, DenormalizedDirection, DenormalizedKey, DenormalizedProperty,
    Endpoint, Entity, EntityId, ForeignKey, GraphCatalog, PathColumn, Property, PropertyId,
    PropertyRealization, Relationship, RelationshipId, RelationshipRoute, RelationshipVariant,
    RelationshipVariantId, TraversalPathLookup,
};
pub use implementations::{
    EntityAuthConfig, GitLabAuthzCatalog, GitLabPolicy, TrustedLocalCatalog,
};

pub use generic::relational::{OrbitQueryModel, RelationalMapping};
pub use generic::relational::{
    OrbitQueryModel as QueryDataModel, RelationalMapping as QueryBackendCatalog,
};
pub use implementations::GitLabPolicy as QueryAuthorizationCatalog;
pub type ClickHouseDataModel = ontology_adapter::OntologyModel<
    implementations::clickhouse::storage::ClickHouse,
    GitLabAuthzCatalog,
>;
pub type DuckDbDataModel =
    ontology_adapter::OntologyModel<implementations::duckdb::storage::DuckDb, TrustedLocalCatalog>;
