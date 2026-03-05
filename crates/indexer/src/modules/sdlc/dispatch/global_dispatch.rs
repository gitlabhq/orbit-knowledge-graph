use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use tracing::info;

use super::metrics::DispatchMetrics;
use crate::configuration::DispatcherConfiguration;
use crate::dispatcher::{DispatchError, Dispatcher};
use crate::nats::NatsServices;
use crate::topic::GlobalIndexingRequest;
use crate::types::{Envelope, Event};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GlobalDispatcherConfig {
    #[serde(flatten)]
    pub dispatcher: DispatcherConfiguration,
}

pub struct GlobalDispatcher {
    nats: Arc<dyn NatsServices>,
    metrics: DispatchMetrics,
    config: GlobalDispatcherConfig,
}

impl GlobalDispatcher {
    pub fn new(
        nats: Arc<dyn NatsServices>,
        metrics: DispatchMetrics,
        config: GlobalDispatcherConfig,
    ) -> Self {
        Self {
            nats,
            metrics,
            config,
        }
    }
}

#[async_trait]
impl Dispatcher for GlobalDispatcher {
    fn name(&self) -> &str {
        "sdlc.global"
    }

    fn dispatcher_config(&self) -> &DispatcherConfiguration {
        &self.config.dispatcher
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
        let envelope = Envelope::new(&GlobalIndexingRequest {
            watermark: Utc::now(),
        })
        .map_err(|error| {
            self.metrics.record_error(self.name(), "publish");
            DispatchError::new(error)
        })?;

        match self
            .nats
            .publish(&GlobalIndexingRequest::topic(), &envelope)
            .await
        {
            Ok(()) => {
                self.metrics.record_requests_published(self.name(), 1);
                info!("dispatched global indexing request");
            }
            Err(crate::nats::NatsError::PublishDuplicate) => {
                self.metrics.record_requests_skipped(self.name(), 1);
                info!("skipping global indexing request, already in-flight");
            }
            Err(error) => {
                self.metrics.record_error(self.name(), "publish");
                return Err(DispatchError::new(error));
            }
        }

        Ok(())
    }
}
