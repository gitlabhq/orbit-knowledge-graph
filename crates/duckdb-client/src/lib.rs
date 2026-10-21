mod client;
mod error;
mod helpers;
mod params;
pub mod search;

pub use client::DuckDbClient;
pub use error::{DuckDbError, Result};
pub use helpers::{bool_column, f64_column, i64_column, scalar_i64, sql_lit, string_column};
pub use params::to_sql_params;
