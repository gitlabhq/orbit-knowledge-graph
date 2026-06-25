use clickhouse_client::ClickHouseError;

use crate::destination::WriteError;

impl From<ClickHouseError> for WriteError {
    fn from(error: ClickHouseError) -> Self {
        match error {
            ClickHouseError::Query(source) => {
                WriteError::Write(format!("query error: {source}"), Some(Box::new(source)))
            }
            ClickHouseError::Insert(source) => {
                WriteError::Write(source.to_string(), Some(Box::new(source)))
            }
            ClickHouseError::ArrowDecode(source) => WriteError::Write(
                format!("arrow decode error: {source}"),
                Some(Box::new(source)),
            ),
            ClickHouseError::ArrowEncode(source) => WriteError::Write(
                format!("arrow encode error: {source}"),
                Some(Box::new(source)),
            ),
            ClickHouseError::BadResponse { status, body } => {
                WriteError::Write(format!("bad response ({status}): {body}"), None)
            }
            ClickHouseError::CircuitOpen { service } => {
                WriteError::Connection(format!("circuit open for {service}"), None)
            }
        }
    }
}
