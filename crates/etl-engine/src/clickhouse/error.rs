//! Error types for ClickHouse operations.

use thiserror::Error;

use crate::destination::DestinationError;

#[derive(Debug, Error)]
pub(crate) enum ClickHouseError {
    #[error("connection error: {0}")]
    Connection(#[source] clickhouse_arrow::Error),

    #[error("insert error: {0}")]
    Insert(#[source] clickhouse_arrow::Error),
}

impl From<ClickHouseError> for DestinationError {
    fn from(error: ClickHouseError) -> Self {
        match error {
            ClickHouseError::Connection(source) => {
                DestinationError::Connection(source.to_string(), Some(Box::new(source)))
            }
            ClickHouseError::Insert(source) => {
                DestinationError::Write(source.to_string(), Some(Box::new(source)))
            }
        }
    }
}
