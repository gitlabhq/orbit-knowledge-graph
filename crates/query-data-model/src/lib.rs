mod derive;
mod error;
pub mod generic;
pub mod implementations;

pub use error::DataModelError;
pub use generic::{
    Authz, Backend, DataModel, Entity, EntityId, GraphCatalog, Property, PropertyId,
    PropertyRealization, Relationship, RelationshipId, RelationshipVariant, RelationshipVariantId,
};
pub use implementations::{ClickHouse, DuckDb, GitLabAuthz, TrustedLocal};

pub type ClickHouseDataModel = DataModel<ClickHouse, GitLabAuthz>;
pub type DuckDbDataModel = DataModel<DuckDb, TrustedLocal>;
