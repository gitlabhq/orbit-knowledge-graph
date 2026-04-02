mod batch_writer;
mod destination;
mod error;

pub use clickhouse_client::{
    ArrowClickHouseClient, ArrowQuery, ClickHouseConfiguration, ClickHouseConfigurationExt,
    ClickHouseError,
};
pub use destination::ClickHouseDestination;

/// ClickHouse microsecond timestamp format used across watermark stores and data cleaners.
pub const TIMESTAMP_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.6f";
