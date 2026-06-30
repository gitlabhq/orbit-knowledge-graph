//! Split into DML (queries) and DDL (schema definitions):
//! - [`dml`]: SELECT, INSERT, JOIN, UNION ALL, CTEs — used by the query compiler.
//! - [`ddl`]: CREATE TABLE, column definitions, engines, projections — used by
//!   the migration orchestrator and schema generator.

pub mod ddl;
pub mod dml;

pub use dml::*;
