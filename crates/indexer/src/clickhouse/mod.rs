mod destination;
mod error;

pub use clickhouse_client::{
    ArrowClickHouseClient, ArrowQuery, ClickHouseConfigurationExt, ClickHouseError, QuerySummary,
};
pub use destination::ClickHouseWriter;

/// ClickHouse microsecond timestamp format used across watermark stores and data cleaners.
pub const TIMESTAMP_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.6f";
