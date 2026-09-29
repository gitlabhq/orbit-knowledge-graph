//! Split into DML (queries) and DDL (schema definitions):
//! - [`dml`]: SELECT, INSERT, JOIN, UNION ALL, CTEs — used by the query compiler.
//! - [`ddl`]: CREATE TABLE, column definitions, engines, projections — used by
//!   the migration orchestrator and schema generator.

pub mod ddl;
pub mod dml;
mod identifier;
mod value_type;
pub mod visit;
pub use value_type::ValueType;

pub use dml::*;
pub use identifier::{Identifier, Symbol};
