//! ClickHouse destination for the ETL engine.
//!
//! Provides a [`ClickHouseDestination`] that writes Arrow RecordBatches to ClickHouse
//! using the native protocol via `clickhouse-arrow`.
//!
//! # Example
//!
//! ```ignore
//! use etl_engine::clickhouse::{ClickHouseConfiguration, ClickHouseDestination};
//! use etl_engine::destination::Destination;
//!
//! let config = ClickHouseConfiguration {
//!     database: "analytics".to_string(),
//!     url: "127.0.0.1:9000".to_string(),
//!     username: "default".to_string(),
//!     password: None,
//! };
//!
//! let destination = ClickHouseDestination::new(config)?;
//! let writer = destination.new_batch_writer("my_table").await?;
//! writer.write_batch(&batches).await?;
//! ```

mod batch_writer;
mod configuration;
mod destination;
mod error;

pub use configuration::ClickHouseConfiguration;
pub use destination::ClickHouseDestination;
