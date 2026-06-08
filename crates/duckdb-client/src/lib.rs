mod client;
#[cfg(feature = "code-indexing")]
mod converter;
mod error;
mod params;
mod schema;

pub use client::DuckDbClient;
#[cfg(feature = "code-indexing")]
pub use converter::{DuckDbConverter, DuckDbSink, LocalGraphData, convert_v2_graph};
pub use error::{DuckDbError, Result};
pub use params::to_sql_params;
pub use schema::MANIFEST_DDL;
