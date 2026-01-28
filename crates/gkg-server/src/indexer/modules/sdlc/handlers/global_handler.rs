//! Global handler orchestrator for global-scoped entities.
//!
//! This handler implements the etl-engine Handler trait and orchestrates
//! multiple GlobalEntityHandler implementations.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use etl_engine::module::{Handler, HandlerContext, HandlerError};
use etl_engine::types::{Envelope, Event, SerializationError, Topic};
use serde::{Deserialize, Serialize};
use tracing::warn;

use super::global_entity::{GlobalEntityContext, GlobalEntityHandler};
use crate::indexer::modules::INDEXER_TOPIC;
use crate::indexer::modules::sdlc::watermark_store::{WatermarkError, WatermarkStore};

const SUBJECT: &str = "sdlc.global.indexing.requested";

/// Payload for global indexing requests.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GlobalHandlerPayload {
    /// The watermark to process up to.
    pub watermark: DateTime<Utc>,
}

impl Event for GlobalHandlerPayload {
    fn topic() -> Topic {
        Topic::new(INDEXER_TOPIC, SUBJECT)
    }
}

/// Orchestrator for global-scoped entity handlers.
///
/// This handler:
/// 1. Subscribes to the global indexing topic
/// 2. Queries the global watermark once
/// 3. Creates a GlobalEntityContext with the watermark range
/// 4. Runs all global entity handlers
/// 5. Updates the global watermark only if all handlers succeed
pub struct GlobalHandler {
    watermark_store: Arc<dyn WatermarkStore>,
    entity_handlers: Vec<Box<dyn GlobalEntityHandler>>,
}

impl GlobalHandler {
    /// Create a new global handler with the given entity handlers.
    pub fn new(
        entity_handlers: Vec<Box<dyn GlobalEntityHandler>>,
        watermark_store: Arc<dyn WatermarkStore>,
    ) -> Self {
        Self {
            watermark_store,
            entity_handlers,
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

        let context = GlobalEntityContext {
            handler_context,
            last_watermark,
            watermark: payload.watermark,
        };

        let mut errors = Vec::new();

        for handler in &self.entity_handlers {
            if let Err(error) = handler.handle(&context).await {
                warn!(handler = handler.name(), %error, "global entity handler failed");
                errors.push((handler.name(), error));
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
                "global entity handlers failed: {}",
                error_details.join("; ")
            )))
        }
    }
}
