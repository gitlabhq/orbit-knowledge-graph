mod error;
mod writer;

pub use clickhouse_client::{
    ArrowClickHouseClient, ArrowQuery, ClickHouseConfigurationExt, ClickHouseError, QuerySummary,
};
pub use writer::{
    BufferedWriter, BufferedWriterConfig, ClickHouseWriter, FlushToken, WriteError, WriteReport,
};
pub(crate) use writer::{
    QUORUM_RETRY_MAX_ATTEMPTS, insert_overrides, is_unsatisfied_quorum, quorum_retry_backoff,
};

/// ClickHouse microsecond timestamp format used across watermark stores and data cleaners.
pub const TIMESTAMP_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.6f";
