use clickhouse_client::ClickHouseError;

use crate::clickhouse::WriteError;

impl From<ClickHouseError> for WriteError {
    fn from(error: ClickHouseError) -> Self {
        match error {
            ClickHouseError::CircuitOpen { .. } => WriteError::Unavailable(error.to_string()),
            _ => WriteError::Write(error.to_string()),
        }
    }
}
