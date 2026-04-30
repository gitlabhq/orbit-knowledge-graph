//! Where processed data goes. Implement [`Destination`] to write to your storage.

use std::error::Error as StdError;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use circuit_breaker::CircuitBreaker;
use thiserror::Error;

/// Underlying error from implementations.
pub type UnderlyingError = Box<dyn StdError + Send + Sync>;

/// Errors that can occur during destination operations.
#[derive(Debug, Error)]
pub enum DestinationError {
    #[error("failed to write batch: {0}")]
    Write(String, #[source] Option<UnderlyingError>),

    #[error("connection error: {0}")]
    Connection(String, #[source] Option<UnderlyingError>),

    #[error("invalid configuration: {0}")]
    InvalidConfiguration(String),
}

/// Writes record batches to a destination.
#[async_trait]
pub trait BatchWriter: Send + Sync {
    async fn write_batch(&self, batch: &[RecordBatch]) -> Result<(), DestinationError>;
}

/// Creates writers for a storage backend.
#[async_trait]
pub trait Destination: Send + Sync {
    async fn new_batch_writer(&self, table: &str)
    -> Result<Box<dyn BatchWriter>, DestinationError>;
}

pub struct CircuitBreakingDestination {
    inner: Arc<dyn Destination>,
    breaker: CircuitBreaker,
}

impl CircuitBreakingDestination {
    pub fn new(inner: Arc<dyn Destination>, breaker: CircuitBreaker) -> Self {
        Self { inner, breaker }
    }
}

fn is_service_error(error: &DestinationError) -> bool {
    matches!(
        error,
        DestinationError::Write(..) | DestinationError::Connection(..)
    )
}

#[async_trait]
impl Destination for CircuitBreakingDestination {
    async fn new_batch_writer(
        &self,
        table: &str,
    ) -> Result<Box<dyn BatchWriter>, DestinationError> {
        self.breaker
            .call_with_filter(|| self.inner.new_batch_writer(table), is_service_error)
            .await
            .map_err(|e| match e {
                circuit_breaker::CircuitBreakerError::Open { service } => {
                    DestinationError::Connection(
                        format!("circuit breaker open for {service}"),
                        None,
                    )
                }
                circuit_breaker::CircuitBreakerError::Inner(inner) => inner,
            })
    }
}
