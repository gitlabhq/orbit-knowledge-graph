pub mod authz;
pub mod catalog;
mod derive;
mod error;
pub mod generic;
pub mod implementations;
pub mod layout;
pub mod models;
pub mod spec;
pub use layout::{Layout, Mapping, Relational, RelationalBackend};

pub use authz::gitlab::{EntityAuthConfig, GitLabAuthzCatalog, GitLabPolicy};
pub use authz::trusted::TrustedLocalCatalog;
pub use catalog::Catalog;
pub use error::DataModelError;
pub use generic::{
    DataModel, Direction, Entity, EntityId, GraphCatalog, Hop, Property, PropertyId, Reach,
    Relationship, RelationshipId, RelationshipVariant, RelationshipVariantId, Walk,
};
pub use layout::relational::{
    MaterializedJoin, MaterializedNode, MaterializedRelationship, VariantRoute,
};

pub use layout::relational::RelationalMapping;
pub use layout::relational::{
    DenormalizedCatalog, DenormalizedDirection, DenormalizedKey, DenormalizedProperty, Endpoint,
    ForeignKey, PathColumn, PropertyRealization, RelationshipRoute, TraversalPathLookup,
};
pub use models::orbit::{ClickHouseDataModel, DuckDbDataModel, OrbitQueryModel};
