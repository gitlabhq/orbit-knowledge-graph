use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use etl_engine::module::{Handler, HandlerContext, HandlerError};
use etl_engine::types::{Envelope, Event, SerializationError, Topic};
use serde::{Deserialize, Serialize};
use tracing::warn;

use super::pipeline::OntologyEntityPipeline;
use super::watermark_store::{TIMESTAMP_FORMAT, WatermarkError, WatermarkStore};
use crate::indexer::modules::INDEXER_TOPIC;

const SUBJECT: &str = "sdlc.namespace.indexing.requested";

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct NamespaceHandlerPayload {
    pub organization: i64,
    pub namespace: i64,
    pub watermark: DateTime<Utc>,
}

impl Event for NamespaceHandlerPayload {
    fn topic() -> Topic {
        Topic::new(INDEXER_TOPIC, SUBJECT)
    }
}

#[derive(Clone, Serialize)]
struct NamespacedQueryParams {
    traversal_path: String,
    last_watermark: String,
    watermark: String,
}

pub struct NamespaceHandler {
    watermark_store: Arc<dyn WatermarkStore>,
    pipelines: Vec<OntologyEntityPipeline>,
}

impl NamespaceHandler {
    pub fn new(
        pipelines: Vec<OntologyEntityPipeline>,
        watermark_store: Arc<dyn WatermarkStore>,
    ) -> Self {
        Self {
            watermark_store,
            pipelines,
        }
    }
}

#[async_trait]
impl Handler for NamespaceHandler {
    fn name(&self) -> &str {
        "namespace-handler"
    }

    fn topic(&self) -> Topic {
        Topic::new(INDEXER_TOPIC, SUBJECT)
    }

    async fn handle(
        &self,
        handler_context: HandlerContext,
        message: Envelope,
    ) -> Result<(), HandlerError> {
        let payload: NamespaceHandlerPayload = message.to_event().map_err(|error| match error {
            SerializationError::Json(e) => HandlerError::Deserialization(e),
        })?;

        let last_watermark = match self
            .watermark_store
            .get_namespace_watermark(payload.namespace)
            .await
        {
            Ok(w) => w,
            Err(WatermarkError::NoData) => DateTime::<Utc>::UNIX_EPOCH,
            Err(error) => {
                warn!(%error, "failed to fetch watermark, using epoch");
                DateTime::<Utc>::UNIX_EPOCH
            }
        };

        let params = NamespacedQueryParams {
            traversal_path: format!("{}/{}/", payload.organization, payload.namespace),
            last_watermark: last_watermark.format(TIMESTAMP_FORMAT).to_string(),
            watermark: payload.watermark.format(TIMESTAMP_FORMAT).to_string(),
        };

        let mut errors = Vec::new();

        for pipeline in &self.pipelines {
            if let Err(error) = pipeline
                .run(params.clone(), handler_context.destination.clone())
                .await
            {
                warn!(pipeline = %pipeline.entity_name, %error, "pipeline failed");
                errors.push((pipeline.entity_name.as_str(), error));
            }
        }

        if errors.is_empty() {
            self.watermark_store
                .set_namespace_watermark(payload.namespace, &payload.watermark)
                .await
                .map_err(|e| {
                    HandlerError::Processing(format!("failed to update watermark: {e}"))
                })?;
        }

        if !errors.is_empty() {
            let error_details: Vec<_> = errors
                .iter()
                .map(|(name, err)| format!("{name}: {err}"))
                .collect();
            return Err(HandlerError::Processing(format!(
                "pipelines failed: {}",
                error_details.join("; ")
            )));
        }

        Ok(())
    }
}
