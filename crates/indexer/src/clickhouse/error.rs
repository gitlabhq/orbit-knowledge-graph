use clickhouse_client::ClickHouseError;

use crate::clickhouse::WriteError;
use crate::handler::HandlerError;

impl From<ClickHouseError> for WriteError {
    fn from(error: ClickHouseError) -> Self {
        match error {
            ClickHouseError::CircuitOpen { .. } => WriteError::Unavailable(error.to_string()),
            _ if error.is_server_overload() => WriteError::Throttled(error.to_string()),
            _ => WriteError::Write(error.to_string()),
        }
    }
}

impl From<WriteError> for HandlerError {
    fn from(error: WriteError) -> Self {
        match error {
            WriteError::Throttled(_) | WriteError::Unavailable(_) => {
                HandlerError::Backpressure(error.to_string())
            }
            WriteError::Write(_) => HandlerError::Processing(error.to_string()),
        }
    }
}
