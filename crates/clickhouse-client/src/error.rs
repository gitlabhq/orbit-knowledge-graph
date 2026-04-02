use thiserror::Error;

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

    #[error("http error: {0}")]
    Http(#[source] reqwest::Error),

    #[error("bad response ({status}): {body}")]
    BadResponse { status: u16, body: String },
}
