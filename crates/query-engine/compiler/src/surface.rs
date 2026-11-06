pub mod gql;

use query_data_model::plan::logical::LogicalCatalog;
use query_data_model::surface::{Statement, json};
use query_data_model::{Catalog, StorageBackend};

use crate::Result;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, strum::EnumString, strum::VariantNames, strum::IntoStaticStr,
)]
pub enum Frontend {
    #[strum(serialize = "json")]
    JsonDsl,
    #[strum(serialize = "gql")]
    Gql,
}

pub fn catalog<B: StorageBackend>(catalog: &Catalog<B>, admin: bool) -> LogicalCatalog<'_> {
    let logical = catalog.model().logical().on(catalog.connection());
    match admin {
        true => logical.as_admin(),
        false => logical,
    }
}

pub fn parse(raw: &str, frontend: Frontend, catalog: LogicalCatalog<'_>) -> Result<Statement> {
    let statement = match frontend {
        Frontend::JsonDsl => json::parse(raw, catalog),
        Frontend::Gql => query_data_model::surface::gql::parse(raw, catalog),
    };
    Ok(statement?)
}
