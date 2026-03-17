pub mod client;
pub mod error;
pub mod schema;

pub use client::DuckDbClient;
pub use duckdb;
pub use error::{DuckDbError, Result};
