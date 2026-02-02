//! Dynamic schema generation for plugin nodes.

mod arrow_schema;
mod ddl_generator;

pub use arrow_schema::build_arrow_schema;
pub use ddl_generator::{generate_alter_table_ddl, generate_create_table_ddl};
