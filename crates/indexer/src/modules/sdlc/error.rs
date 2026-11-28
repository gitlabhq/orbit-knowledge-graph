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
