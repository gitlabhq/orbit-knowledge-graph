//! Handler for caching push events from the events table.

use async_trait::async_trait;
use bytes::Bytes;
use etl_engine::module::{Handler, HandlerContext, HandlerError};
use etl_engine::nats::KvPutOptions;
use etl_engine::types::{Envelope, Topic};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use super::config::{CodeIndexingConfig, EVENTS_CACHE_TTL, buckets, siphon_actions, subjects};
use super::siphon_decoder::{ColumnExtractor, decode_logical_replication_events};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedEventInfo {
    pub project_id: i64,
    pub author_id: i64,
    pub created_at: String,
}

pub struct EventCacheHandler {
    config: CodeIndexingConfig,
}

impl EventCacheHandler {
    pub fn new(config: CodeIndexingConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl Handler for EventCacheHandler {
    fn name(&self) -> &str {
        "event-cache"
    }

    fn topic(&self) -> Topic {
        Topic::new(
            self.config.events_stream_name.clone(),
            format!("{}.{}", self.config.events_stream_name, subjects::EVENTS),
        )
    }

    async fn handle(&self, context: HandlerContext, message: Envelope) -> Result<(), HandlerError> {
        debug!(message_id = %message.id.0, "received event");

        let replication_events = decode_logical_replication_events(&message.payload)
            .map_err(HandlerError::Processing)?;

        let extractor = ColumnExtractor::new(&replication_events);
        for event in &replication_events.events {
            let action = match extractor.get_i32(event, "action") {
                Some(a) => a,
                None => {
                    debug!("event missing action column, skipping");
                    continue;
                }
            };

            if action != siphon_actions::PUSH_EVENT {
                continue;
            }

            let id = match extractor.get_i64(event, "id") {
                Some(id) => id,
                None => {
                    debug!("event missing id column, skipping");
                    continue;
                }
            };

            let project_id = match extractor.get_i64(event, "project_id") {
                Some(id) => id,
                None => {
                    debug!(event_id = id, "event missing project_id column, skipping");
                    continue;
                }
            };

            let author_id = match extractor.get_i64(event, "author_id") {
                Some(id) => id,
                None => {
                    debug!(event_id = id, "event missing author_id column, skipping");
                    continue;
                }
            };

            let created_at = extractor
                .get_timestamp_string(event, "created_at")
                .unwrap_or_default();

            let cached_info = CachedEventInfo {
                project_id,
                author_id,
                created_at,
            };

            let value = serde_json::to_vec(&cached_info).map_err(|e| {
                HandlerError::Processing(format!("failed to serialize cached event: {}", e))
            })?;

            let options = KvPutOptions::with_ttl(EVENTS_CACHE_TTL);
            let key = id.to_string();

            match context
                .nats
                .kv_put(buckets::EVENTS_CACHE, &key, Bytes::from(value), options)
                .await
            {
                Ok(_) => {
                    debug!(event_id = id, project_id = project_id, "cached push event");
                }
                Err(e) => {
                    warn!(
                        event_id = id,
                        error = %e,
                        "failed to cache push event, continuing"
                    );
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexer::modules::code::test_helpers::{
        EventBuilder, build_replication_events, push_event_columns,
    };
    use etl_engine::testkit::{MockDestination, MockNatsServices, TestEnvelopeFactory};
    use std::sync::Arc;

    fn create_test_context() -> (HandlerContext, Arc<MockNatsServices>) {
        let mock_nats = Arc::new(MockNatsServices::new());
        let ctx = HandlerContext::new(Arc::new(MockDestination::new()), mock_nats.clone());
        (ctx, mock_nats)
    }

    #[tokio::test]
    async fn caches_push_events() {
        let handler = EventCacheHandler::new(CodeIndexingConfig::default());
        let (ctx, mock_nats) = create_test_context();

        let payload = build_replication_events(vec![push_event_columns(123, 456, 789).build()]);
        let envelope = TestEnvelopeFactory::with_bytes(payload);

        handler.handle(ctx, envelope).await.unwrap();

        let cached = mock_nats.get_kv(buckets::EVENTS_CACHE, "123");
        assert!(cached.is_some());

        let cached_info: CachedEventInfo = serde_json::from_slice(&cached.unwrap()).unwrap();
        assert_eq!(cached_info.project_id, 456);
        assert_eq!(cached_info.author_id, 789);
    }

    #[tokio::test]
    async fn skips_non_push_events() {
        let handler = EventCacheHandler::new(CodeIndexingConfig::default());
        let (ctx, mock_nats) = create_test_context();

        let event = EventBuilder::new()
            .with_i64("id", 123)
            .with_i32("action", 1) // Not PUSH_EVENT
            .with_i64("project_id", 456)
            .with_i64("author_id", 789)
            .build();

        let payload = build_replication_events(vec![event]);
        let envelope = TestEnvelopeFactory::with_bytes(payload);

        handler.handle(ctx, envelope).await.unwrap();

        assert!(mock_nats.get_kv(buckets::EVENTS_CACHE, "123").is_none());
    }

    #[tokio::test]
    async fn skips_events_missing_required_columns() {
        let handler = EventCacheHandler::new(CodeIndexingConfig::default());
        let (ctx, mock_nats) = create_test_context();

        let event_missing_id = EventBuilder::new()
            .with_i32("action", siphon_actions::PUSH_EVENT)
            .with_i64("project_id", 456)
            .build();

        let payload = build_replication_events(vec![event_missing_id]);
        let envelope = TestEnvelopeFactory::with_bytes(payload);

        handler.handle(ctx, envelope).await.unwrap();

        let keys = mock_nats.get_kv(buckets::EVENTS_CACHE, "456");
        assert!(keys.is_none());
    }
}
