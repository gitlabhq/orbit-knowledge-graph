mod error;
mod writer;

pub use clickhouse_client::{
    ArrowClickHouseClient, ArrowQuery, ClickHouseConfigurationExt, ClickHouseError, QuerySummary,
};
pub(crate) use writer::insert_overrides;
pub use writer::{
    BufferedWriter, BufferedWriterConfig, ClickHouseWriter, FlushToken, WriteError, WriteReport,
};

/// ClickHouse microsecond timestamp format used across watermark stores and data cleaners.
pub const TIMESTAMP_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.6f";
