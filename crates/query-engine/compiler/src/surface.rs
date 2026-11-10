pub mod gql;

use query_data_model::plan::logical::LogicalCatalog;
use query_data_model::surface::Statement;
use query_data_model::{Catalog, StorageBackend};

use crate::Result;

pub fn catalog<B: StorageBackend>(catalog: &Catalog<B>, admin: bool) -> LogicalCatalog<'_> {
    let logical = catalog.model().logical().on(catalog.connection());
    match admin {
        true => logical.as_admin(),
        false => logical,
    }
}

pub fn parse(raw: &str, catalog: LogicalCatalog<'_>) -> Result<Statement> {
    Ok(query_data_model::surface::gql::parse(raw, catalog)?)
}
