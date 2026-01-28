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

const SUBJECT: &str = "sdlc.global.indexing.requested";

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GlobalHandlerPayload {
    pub watermark: DateTime<Utc>,
}

impl Event for GlobalHandlerPayload {
    fn topic() -> Topic {
        Topic::new(INDEXER_TOPIC, SUBJECT)
    }
}

#[derive(Clone, Serialize)]
struct GlobalQueryParams {
    last_watermark: String,
    watermark: String,
}

pub struct GlobalHandler {
    watermark_store: Arc<dyn WatermarkStore>,
    pipelines: Vec<OntologyEntityPipeline>,
}

impl GlobalHandler {
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
impl Handler for GlobalHandler {
    fn name(&self) -> &str {
        "global-handler"
    }

    fn topic(&self) -> Topic {
        Topic::new(INDEXER_TOPIC, SUBJECT)
    }

    async fn handle(
        &self,
        handler_context: HandlerContext,
        message: Envelope,
    ) -> Result<(), HandlerError> {
        let payload: GlobalHandlerPayload = message.to_event().map_err(|error| match error {
            SerializationError::Json(e) => HandlerError::Deserialization(e),
        })?;

        let last_watermark = match self.watermark_store.get_global_watermark().await {
            Ok(w) => w,
            Err(WatermarkError::NoData) => DateTime::<Utc>::UNIX_EPOCH,
            Err(error) => {
                warn!(%error, "failed to fetch global watermark, using epoch");
                DateTime::<Utc>::UNIX_EPOCH
            }
        };

        let params = GlobalQueryParams {
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
                .set_global_watermark(&payload.watermark)
                .await
                .map_err(|e| {
                    HandlerError::Processing(format!("failed to update global watermark: {e}"))
                })?;
            Ok(())
        } else {
            let error_details: Vec<_> = errors
                .iter()
                .map(|(name, err)| format!("{name}: {err}"))
                .collect();
            Err(HandlerError::Processing(format!(
                "pipelines failed: {}",
                error_details.join("; ")
            )))
        }
    }
}
