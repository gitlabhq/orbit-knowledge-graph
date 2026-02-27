use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use chrono::Utc;
use tracing::info;

use super::metrics::DispatchMetrics;
use crate::dispatcher::{DispatchError, Dispatcher};
use crate::locking::LockService;
use crate::modules::sdlc::locking::{LOCK_TTL, global_lock_key};
use crate::nats::NatsServices;
use crate::topic::GlobalIndexingRequest;
use crate::types::{Envelope, Event};

pub struct GlobalDispatcher {
    nats: Arc<dyn NatsServices>,
    lock_service: Arc<dyn LockService>,
    metrics: DispatchMetrics,
}

impl GlobalDispatcher {
    pub fn new(
        nats: Arc<dyn NatsServices>,
        lock_service: Arc<dyn LockService>,
        metrics: DispatchMetrics,
    ) -> Self {
        Self {
            nats,
            lock_service,
            metrics,
        }
    }
}

#[async_trait]
impl Dispatcher for GlobalDispatcher {
    fn name(&self) -> &str {
        "sdlc.global"
    }

    async fn dispatch(&self) -> Result<(), DispatchError> {
        let start = Instant::now();

        let result = self.dispatch_inner().await;

        let duration = start.elapsed().as_secs_f64();
        let outcome = if result.is_ok() { "success" } else { "error" };
        self.metrics.record_run(self.name(), outcome, duration);

        result
    }
}

impl GlobalDispatcher {
    async fn dispatch_inner(&self) -> Result<(), DispatchError> {
        let acquired = self
            .lock_service
            .try_acquire(global_lock_key(), LOCK_TTL)
            .await
            .map_err(|error| {
                self.metrics.record_error(self.name(), "lock");
                DispatchError::new(error)
            })?;

        if !acquired {
            info!("skipping global indexing request, lock already held");
            self.metrics.record_requests_skipped(self.name(), 1);
            return Ok(());
        }

        let envelope = Envelope::new(&GlobalIndexingRequest {
            watermark: Utc::now(),
        })
        .map_err(|error| {
            self.metrics.record_error(self.name(), "publish");
            DispatchError::new(error)
        })?;

        self.nats
            .publish(&GlobalIndexingRequest::topic(), &envelope)
            .await
            .map_err(|error| {
                self.metrics.record_error(self.name(), "publish");
                DispatchError::new(error)
            })?;

        self.metrics.record_requests_published(self.name(), 1);
        info!("dispatched global indexing request");
        Ok(())
    }
}
