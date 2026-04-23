use crate::locking::LockError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SchemaVersionError {
    #[error("ClickHouse error: {0}")]
    ClickHouse(#[from] clickhouse_client::ClickHouseError),

    #[error("unexpected query result: {0}")]
    UnexpectedResult(String),
}

#[derive(Debug, Error)]
pub enum MigrationError {
    #[error("schema version error: {0}")]
    SchemaVersion(#[from] SchemaVersionError),

    #[error("lock error: {0}")]
    Lock(#[from] LockError),

    #[error("ClickHouse DDL error for table '{table}': {reason}")]
    Ddl { table: String, reason: String },

    #[error("migration lock held by another pod after {seconds}s; giving up")]
    LockTimeout { seconds: u64 },
}

#[derive(Debug, Error)]
#[error("schema reconciliation failed: {0}")]
pub struct ReconcileError(pub String);
