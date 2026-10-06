pub mod bindings;
mod derive;
mod error;
pub mod generic;
pub mod implementations;
pub mod storage;

pub use error::DataModelError;
pub use generic::{
    ColumnId, DataModel, DenormalizedCatalog, DenormalizedDirection, DenormalizedKey,
    DenormalizedProperty, Endpoint, Entity, EntityId, ForeignKey, GraphCatalog, PathColumn,
    Property, PropertyId, PropertyRealization, QueryAuthorizationCatalog, QueryBackendCatalog,
    QueryDataModel, Relationship, RelationshipId, RelationshipRoute, RelationshipVariant,
    RelationshipVariantId, TableId, TraversalPathLookup,
};
pub use implementations::{EntityAuthConfig, GitLabAuthzCatalog, TrustedLocalCatalog};

pub type ClickHouseDataModel = DataModel<implementations::ClickHouseCatalog, GitLabAuthzCatalog>;
pub type DuckDbDataModel = DataModel<implementations::DuckDbCatalog, TrustedLocalCatalog>;
