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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_open_circuit_makes_the_graph_store_unavailable() {
        let error = WriteError::from(ClickHouseError::CircuitOpen { service: "graph" });
        assert!(matches!(error, WriteError::Unavailable(_)), "got {error:?}");
    }

    #[test]
    fn a_server_error_is_a_failed_write() {
        let error = WriteError::from(ClickHouseError::BadResponse {
            status: 500,
            body: "MEMORY_LIMIT_EXCEEDED".to_string(),
        });
        assert!(matches!(error, WriteError::Write(_)), "got {error:?}");
    }
}
