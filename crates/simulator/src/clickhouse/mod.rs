//! ClickHouse integration for the simulator.

mod schema;
mod writer;

pub use schema::SchemaGenerator;
pub use writer::ClickHouseWriter;
