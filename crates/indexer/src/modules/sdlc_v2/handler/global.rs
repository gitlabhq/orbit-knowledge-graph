use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::{error, info};

use crate::configuration::HandlerConfiguration;
use crate::handler::{Handler, HandlerContext, HandlerError};
use crate::topic::GlobalIndexingRequest;
use crate::types::{Envelope, Event, SerializationError, Topic};

use crate::modules::sdlc_v2::locking::global_lock_key;
use crate::modules::sdlc_v2::metrics::SdlcMetrics;
use crate::modules::sdlc_v2::pipeline::{Pipeline, PipelineContext};
use crate::modules::sdlc_v2::plan::PipelinePlan;

fn default_datalake_batch_size() -> u64 {
    1_000_000
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GlobalHandlerConfig {
    #[serde(flatten)]
    pub engine: HandlerConfiguration,

    #[serde(default = "default_datalake_batch_size")]
    pub datalake_batch_size: u64,
}

impl Default for GlobalHandlerConfig {
    fn default() -> Self {
        Self {
            engine: HandlerConfiguration::default(),
            datalake_batch_size: default_datalake_batch_size(),
        }
    }
}

pub struct GlobalHandler {
    plans: Vec<PipelinePlan>,
    pipeline: Arc<Pipeline>,
    metrics: SdlcMetrics,
    config: GlobalHandlerConfig,
}

impl GlobalHandler {
    pub fn new(
        plans: Vec<PipelinePlan>,
        pipeline: Arc<Pipeline>,
        metrics: SdlcMetrics,
        config: GlobalHandlerConfig,
    ) -> Self {
        Self {
            plans,
            pipeline,
            metrics,
            config,
        }
    }
}

#[async_trait]
impl Handler for GlobalHandler {
    fn name(&self) -> &str {
        "sdlc_v2_global_handler"
    }

    fn topic(&self) -> Topic {
        GlobalIndexingRequest::topic()
    }

    fn engine_config(&self) -> &HandlerConfiguration {
        &self.config.engine
    }

    async fn handle(&self, context: HandlerContext, message: Envelope) -> Result<(), HandlerError> {
        let payload: GlobalIndexingRequest = message.to_event().map_err(|error| match error {
            SerializationError::Json(err) => HandlerError::Deserialization(err),
        })?;

        let started_at = Instant::now();
        info!(
            pipeline_count = self.plans.len(),
            "starting global indexing"
        );

        let pipeline_context = PipelineContext {
            watermark: payload.watermark,
            position_key: "global".to_string(),
            base_conditions: BTreeMap::new(),
        };

        let result = self.pipeline.run(&self.plans, &pipeline_context).await;

        let elapsed = started_at.elapsed();
        self.metrics
            .record_handler_duration("sdlc_v2_global_handler", elapsed.as_secs_f64());

        if result.is_ok() {
            if let Err(err) = context.lock_service.release(global_lock_key()).await {
                error!(%err, "failed to release global lock, will expire via TTL");
            }
            info!(
                elapsed_ms = elapsed.as_millis() as u64,
                "global indexing completed"
            );
        }

        result
    }
}
