use clickhouse_client::ClickHouseError;

use crate::destination::DestinationError;

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
            ClickHouseError::BadResponse { status, body } => {
                DestinationError::Write(format!("bad response ({status}): {body}"), None)
            }
            ClickHouseError::CircuitOpen { service } => {
                DestinationError::Connection(format!("circuit open for {service}"), None)
            }
        }
    }
}
