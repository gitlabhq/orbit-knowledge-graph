mod derive;
mod error;
pub mod generic;
pub mod implementations;

pub use error::DataModelError;
pub use generic::{
    Authz, Backend, DataModel, DenormalizedCatalog, DenormalizedColumns, DenormalizedKey,
    DenormalizedRelationships, Entity, EntityId, ForeignKey, GraphCatalog, PathColumn, Property,
    PropertyId, PropertyRealization, QueryAuthorizationCatalog, QueryBackendCatalog,
    QueryDataModel, Relationship, RelationshipId, RelationshipRoute, RelationshipVariant,
    RelationshipVariantId, TraversalPathLookup,
};
pub use implementations::{ClickHouse, DuckDb, EntityAuthConfig, GitLabAuthz, TrustedLocal};

pub type ClickHouseDataModel = DataModel<ClickHouse, GitLabAuthz>;
pub type DuckDbDataModel = DataModel<DuckDb, TrustedLocal>;
