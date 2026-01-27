//! Error types for ClickHouse operations.

use thiserror::Error;

use crate::destination::DestinationError;

#[derive(Debug, Error)]
pub enum ClickHouseError {
    #[error("query error: {0}")]
    Query(#[source] clickhouse::error::Error),

    #[error("insert error: {0}")]
    Insert(#[source] clickhouse::error::Error),

    #[error("arrow decode error: {0}")]
    ArrowDecode(#[source] arrow::error::ArrowError),

    #[error("arrow encode error: {0}")]
    ArrowEncode(#[source] arrow::error::ArrowError),
}

impl From<ClickHouseError> for DestinationError {
    fn from(error: ClickHouseError) -> Self {
        match error {
            ClickHouseError::Query(source) => {
                DestinationError::Write(format!("query error: {source}"), Some(Box::new(source)))
            }
            ClickHouseError::Insert(source) => {
                DestinationError::Write(source.to_string(), Some(Box::new(source)))
            }
            ClickHouseError::ArrowDecode(source) => DestinationError::Write(
                format!("arrow decode error: {source}"),
                Some(Box::new(source)),
            ),
            ClickHouseError::ArrowEncode(source) => DestinationError::Write(
                format!("arrow encode error: {source}"),
                Some(Box::new(source)),
            ),
        }
    }
}
