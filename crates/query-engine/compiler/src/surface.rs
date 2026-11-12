pub mod gql;

use orbit_server_config::QueryLimits;
use query_data_model::plan::logical::LogicalCatalog;
use query_data_model::surface::Statement;
use query_data_model::{Catalog, StorageBackend};

use crate::Result;

pub fn catalog<'m, B: StorageBackend>(
    catalog: &'m Catalog<B>,
    connections: &'m crate::dag::Connections,
    limits: &'m QueryLimits,
    admin: bool,
) -> LogicalCatalog<'m> {
    let logical = catalog.model().logical(limits).serving(connections.names());
    match admin {
        true => logical.as_admin(),
        false => logical,
    }
}

pub fn parse(raw: &str, catalog: LogicalCatalog<'_>) -> Result<Statement> {
    Ok(query_data_model::surface::gql::parse(raw, catalog)?)
}
