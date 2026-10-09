mod derive;
mod error;
pub mod generic;
pub mod implementations;

pub use error::DataModelError;
pub use generic::{
    DataModel, DenormalizedCatalog, DenormalizedDirection, DenormalizedKey, DenormalizedProperty,
    Endpoint, Entity, EntityId, ForeignKey, GraphCatalog, MaterializedJoin, MaterializedNode,
    MaterializedRelationship, PathColumn, Property, PropertyId, PropertyRealization,
    QueryAuthorizationCatalog, QueryBackendCatalog, QueryDataModel, Relationship, RelationshipId,
    RelationshipRoute, RelationshipVariant, RelationshipVariantId, TraversalPathLookup,
    VariantRoute,
};
pub use implementations::{EntityAuthConfig, GitLabAuthzCatalog, TrustedLocalCatalog};

pub type ClickHouseDataModel = DataModel<implementations::ClickHouseCatalog, GitLabAuthzCatalog>;
pub type DuckDbDataModel = DataModel<implementations::DuckDbCatalog, TrustedLocalCatalog>;
