pub mod authz;
mod derive;
mod error;
pub mod generic;
pub mod implementations;
pub mod models;
pub mod storage;
pub use storage::{Relational, RelationalBackend, Storage, StorageModel};

pub use authz::gitlab::{EntityAuthConfig, GitLabAuthzCatalog, GitLabPolicy};
pub use authz::trusted::TrustedLocalCatalog;
pub use error::DataModelError;
pub use generic::{
    DataModel, Entity, EntityId, GraphCatalog, Property, PropertyId, Relationship, RelationshipId,
    RelationshipVariant, RelationshipVariantId,
};

pub use models::orbit::{ClickHouseDataModel, DuckDbDataModel, OrbitQueryModel};
pub use storage::relational::RelationalMapping;
pub use storage::relational::{
    DenormalizedCatalog, DenormalizedDirection, DenormalizedKey, DenormalizedProperty, Endpoint,
    ForeignKey, PathColumn, PropertyRealization, RelationshipRoute, TraversalPathLookup,
};
