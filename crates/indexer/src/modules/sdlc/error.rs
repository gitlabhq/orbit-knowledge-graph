use crate::checkpoint::CheckpointError;
use crate::clickhouse::WriteError;
use crate::handler::HandlerError;

use super::datalake::DatalakeError;

impl From<DatalakeError> for HandlerError {
    fn from(error: DatalakeError) -> Self {
        match error {
            DatalakeError::Unavailable(_) => HandlerError::Backpressure(error.to_string()),
            _ => HandlerError::Processing(error.to_string()),
        }
    }
}

impl From<CheckpointError> for HandlerError {
    fn from(error: CheckpointError) -> Self {
        match error {
            CheckpointError::Unavailable(_) => HandlerError::Backpressure(error.to_string()),
            CheckpointError::Store(_) => HandlerError::Processing(error.to_string()),
        }
    }
}

impl From<WriteError> for HandlerError {
    fn from(error: WriteError) -> Self {
        match error {
            WriteError::Unavailable(_) => HandlerError::Backpressure(error.to_string()),
            WriteError::Write(_) => HandlerError::Processing(error.to_string()),
        }
    }
}
