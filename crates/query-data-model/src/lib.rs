pub mod authz;
mod derive;
mod error;
pub mod generic;
pub mod implementations;
pub mod layout;
pub mod models;
pub use layout::{Layout, LayoutModel, Relational, RelationalBackend};

pub use authz::gitlab::{EntityAuthConfig, GitLabAuthzCatalog, GitLabPolicy};
pub use authz::trusted::TrustedLocalCatalog;
pub use error::DataModelError;
pub use generic::{
    DataModel, Entity, EntityId, GraphCatalog, Property, PropertyId, Relationship, RelationshipId,
    RelationshipVariant, RelationshipVariantId,
};

pub use layout::relational::RelationalMapping;
pub use layout::relational::{
    DenormalizedCatalog, DenormalizedDirection, DenormalizedKey, DenormalizedProperty, Endpoint,
    ForeignKey, PathColumn, PropertyRealization, RelationshipRoute, TraversalPathLookup,
};
pub use models::orbit::{ClickHouseDataModel, DuckDbDataModel, OrbitQueryModel};
