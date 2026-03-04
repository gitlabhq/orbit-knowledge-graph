use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::{error, info};

use crate::configuration::HandlerConfiguration;
use crate::handler::{Handler, HandlerContext, HandlerError};
use crate::topic::NamespaceIndexingRequest;
use crate::types::{Envelope, Event, SerializationError, Topic};

use crate::modules::sdlc_v2::locking::namespace_lock_key;
use crate::modules::sdlc_v2::metrics::SdlcMetrics;
use crate::modules::sdlc_v2::pipeline::{Pipeline, PipelineContext};
use crate::modules::sdlc_v2::plan::PipelinePlan;

fn default_datalake_batch_size() -> u64 {
    1_000_000
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct NamespaceHandlerConfig {
    #[serde(flatten)]
    pub engine: HandlerConfiguration,

    #[serde(default = "default_datalake_batch_size")]
    pub datalake_batch_size: u64,
}

impl Default for NamespaceHandlerConfig {
    fn default() -> Self {
        Self {
            engine: HandlerConfiguration::default(),
            datalake_batch_size: default_datalake_batch_size(),
        }
    }
}

pub struct NamespaceHandler {
    plans: Vec<PipelinePlan>,
    pipeline: Arc<Pipeline>,
    metrics: SdlcMetrics,
    config: NamespaceHandlerConfig,
}

impl NamespaceHandler {
    pub fn new(
        plans: Vec<PipelinePlan>,
        pipeline: Arc<Pipeline>,
        metrics: SdlcMetrics,
        config: NamespaceHandlerConfig,
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
impl Handler for NamespaceHandler {
    fn name(&self) -> &str {
        "sdlc_v2_namespace_handler"
    }

    fn topic(&self) -> Topic {
        NamespaceIndexingRequest::topic()
    }

    fn engine_config(&self) -> &HandlerConfiguration {
        &self.config.engine
    }

    async fn handle(&self, context: HandlerContext, message: Envelope) -> Result<(), HandlerError> {
        let payload: NamespaceIndexingRequest =
            message.to_event().map_err(|error| match error {
                SerializationError::Json(err) => HandlerError::Deserialization(err),
            })?;

        let started_at = Instant::now();
        info!(
            namespace_id = payload.namespace,
            organization_id = payload.organization,
            pipeline_count = self.plans.len(),
            "starting namespace indexing"
        );

        let traversal_path = format!("{}/{}/", payload.organization, payload.namespace);

        let pipeline_context = PipelineContext {
            watermark: payload.watermark,
            position_key: format!("ns.{}", payload.namespace),
            base_conditions: BTreeMap::from([("traversal_path".to_string(), traversal_path)]),
        };

        let result = self.pipeline.run(&self.plans, &pipeline_context).await;

        let elapsed = started_at.elapsed();
        self.metrics
            .record_handler_duration("sdlc_v2_namespace_handler", elapsed.as_secs_f64());

        if result.is_ok() {
            let lock_key = namespace_lock_key(payload.organization, payload.namespace);
            if let Err(err) = context.lock_service.release(&lock_key).await {
                error!(
                    namespace_id = payload.namespace,
                    %err,
                    "failed to release namespace lock, will expire via TTL"
                );
            }
            info!(
                namespace_id = payload.namespace,
                elapsed_ms = elapsed.as_millis() as u64,
                "namespace indexing completed"
            );
        }

        result
    }
}
