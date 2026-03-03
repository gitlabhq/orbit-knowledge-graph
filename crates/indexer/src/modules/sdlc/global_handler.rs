//! Handler for global-scoped entities.
//!
//! Processes entities with `EtlScope::Global` using ontology-driven pipelines.

use std::sync::Arc;
use std::time::Instant;

use crate::configuration::HandlerConfiguration;
use crate::handler::{Handler, HandlerContext, HandlerError};
use crate::types::{Envelope, Event, SerializationError, Topic};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info};

use super::cursor_paginator::{CursorValue, serialize_cursor};
use super::locking::global_lock_key;
use super::metrics::SdlcMetrics;
use super::pipeline::OntologyEntityPipeline;
use super::watermark_store::{
    CursorReporter, InProgressCursor, WatermarkError, WatermarkState, WatermarkStore,
};
use crate::clickhouse::TIMESTAMP_FORMAT;
use crate::topic::GlobalIndexingRequest;

#[derive(Clone, Serialize)]
pub struct GlobalQueryParams {
    pub last_watermark: String,
    pub watermark: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub __starting_cursor: Option<String>,
}

impl GlobalQueryParams {
    pub fn new(last_watermark: &DateTime<Utc>, watermark: &DateTime<Utc>) -> Self {
        Self {
            last_watermark: last_watermark.format(TIMESTAMP_FORMAT).to_string(),
            watermark: watermark.format(TIMESTAMP_FORMAT).to_string(),
            __starting_cursor: None,
        }
    }

    pub fn with_starting_cursor(mut self, cursor_values: &[CursorValue]) -> Self {
        if !cursor_values.is_empty() {
            self.__starting_cursor = Some(serialize_cursor(cursor_values));
        }
        self
    }

    pub fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("params serialization failed")
    }
}

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

/// Handles entities without namespace ownership.
///
/// These are instance-wide records like User or Organization that exist outside
/// the namespace hierarchy. Queries filter by watermark time range only.
pub struct GlobalHandler {
    watermark_store: Arc<dyn WatermarkStore>,
    pipelines: Vec<OntologyEntityPipeline>,
    metrics: SdlcMetrics,
    config: GlobalHandlerConfig,
}

impl GlobalHandler {
    pub fn new(
        watermark_store: Arc<dyn WatermarkStore>,
        pipelines: Vec<OntologyEntityPipeline>,
        metrics: SdlcMetrics,
        config: GlobalHandlerConfig,
    ) -> Self {
        Self {
            watermark_store,
            pipelines,
            metrics,
            config,
        }
    }
}

#[async_trait]
impl Handler for GlobalHandler {
    fn name(&self) -> &str {
        "global_handler"
    }

    fn topic(&self) -> Topic {
        GlobalIndexingRequest::topic()
    }

    fn engine_config(&self) -> &HandlerConfiguration {
        &self.config.engine
    }

    async fn handle(&self, context: HandlerContext, message: Envelope) -> Result<(), HandlerError> {
        let payload: GlobalIndexingRequest = message.to_event().map_err(|error| match error {
            SerializationError::Json(e) => HandlerError::Deserialization(e),
        })?;

        let state = match self.watermark_store.get_global_state().await {
            Ok(s) => {
                debug!(
                    watermark = %s.watermark.format(TIMESTAMP_FORMAT),
                    has_cursor = s.in_progress.is_some(),
                    "retrieved global state"
                );
                s
            }
            Err(WatermarkError::NoData) => {
                info!("no global watermark found, starting from epoch");
                WatermarkState {
                    watermark: DateTime::<Utc>::UNIX_EPOCH,
                    in_progress: None,
                }
            }
            Err(error) => {
                error!(%error, "global watermark fetch failed, reprocessing from epoch");
                WatermarkState {
                    watermark: DateTime::<Utc>::UNIX_EPOCH,
                    in_progress: None,
                }
            }
        };

        let is_resuming = state.in_progress.is_some();
        let (params, target_watermark) = match &state.in_progress {
            Some(cursor) => {
                let params = GlobalQueryParams::new(&state.watermark, &cursor.upper_watermark)
                    .with_starting_cursor(&cursor.cursor_values);
                (params, cursor.upper_watermark)
            }
            None => {
                let params = GlobalQueryParams::new(&state.watermark, &payload.watermark);
                (params, payload.watermark)
            }
        };

        if !is_resuming {
            let initial_cursor = InProgressCursor {
                cursor_values: vec![],
                upper_watermark: target_watermark,
            };
            if let Err(error) = self
                .watermark_store
                .save_global_cursor(&initial_cursor)
                .await
            {
                error!(%error, "failed to save initial global cursor");
            }
        }

        let started_at = Instant::now();

        info!(
            from_watermark = %params.last_watermark,
            to_watermark = %params.watermark,
            pipeline_count = self.pipelines.len(),
            "starting global indexing"
        );

        let cursor_reporter = GlobalCursorReporter {
            watermark_store: Arc::clone(&self.watermark_store),
            upper_watermark: target_watermark,
        };

        let mut errors = Vec::new();
        let mut successful_pipelines = 0;
        let mut total_rows_indexed: u64 = 0;
        for pipeline in &self.pipelines {
            match pipeline
                .process(
                    params.to_json(),
                    context.destination.as_ref(),
                    &cursor_reporter,
                )
                .await
            {
                Ok(rows) => {
                    successful_pipelines += 1;
                    total_rows_indexed += rows;
                }
                Err(error) => {
                    error!(entity = pipeline.entity_name(), %error, "pipeline processing failed");
                    self.metrics
                        .record_pipeline_error(pipeline.entity_name(), error.error_kind());
                    errors.push((pipeline.entity_name().to_string(), error));
                }
            }
        }

        let elapsed = started_at.elapsed();

        if errors.is_empty() && total_rows_indexed > 0 {
            self.watermark_store
                .complete_global_watermark(&target_watermark)
                .await
                .map_err(|e| {
                    HandlerError::Processing(format!("failed to complete global watermark: {e}"))
                })?;

            self.metrics
                .record_watermark_lag("global", &target_watermark);

            info!(
                watermark = %target_watermark.format(TIMESTAMP_FORMAT),
                "global watermark updated"
            );
        } else if errors.is_empty() {
            // Clear cursor even when no rows indexed
            if let Err(error) = self
                .watermark_store
                .complete_global_watermark(&state.watermark)
                .await
            {
                error!(%error, "failed to clear global cursor");
            }
        }

        if errors.is_empty() {
            if let Err(error) = context.lock_service.release(global_lock_key()).await {
                error!(%error, "failed to release global lock, will expire via TTL");
            }

            info!(
                successful_pipelines,
                elapsed_ms = elapsed.as_millis() as u64,
                "global indexing completed"
            );
        }

        self.metrics
            .record_handler_duration("global_handler", elapsed.as_secs_f64());

        if !errors.is_empty() {
            let failed_count = errors.len();
            error!(
                failed_count,
                successful_pipelines,
                elapsed_ms = elapsed.as_millis() as u64,
                "global indexing finished with failures"
            );

            let error_details: Vec<_> = errors
                .iter()
                .map(|(name, err)| format!("{name}: {err}"))
                .collect();
            return Err(HandlerError::Processing(format!(
                "entity pipelines failed: {}",
                error_details.join("; ")
            )));
        }

        Ok(())
    }
}

struct GlobalCursorReporter {
    watermark_store: Arc<dyn WatermarkStore>,
    upper_watermark: DateTime<Utc>,
}

#[async_trait]
impl CursorReporter for GlobalCursorReporter {
    async fn on_page_complete(&self, cursor_values: &[CursorValue]) -> Result<(), HandlerError> {
        let cursor = InProgressCursor {
            cursor_values: cursor_values.to_vec(),
            upper_watermark: self.upper_watermark,
        };
        self.watermark_store
            .save_global_cursor(&cursor)
            .await
            .map_err(|error| {
                HandlerError::Processing(format!("failed to save global cursor: {error}"))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::modules::sdlc::test_fixtures::{
        EmptyDatalake, NonEmptyDatalake,
    };
    use crate::testkit::{MockDestination, MockLockService, MockNatsServices, TestEnvelopeFactory};
    use ontology::{DataType, EtlConfig, EtlScope, Field, NodeEntity, Ontology};
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    fn test_metrics() -> SdlcMetrics {
        SdlcMetrics::with_meter(&crate::testkit::test_meter())
    }

    struct RecordingGlobalWatermarkStore {
        watermark: Mutex<Option<DateTime<Utc>>>,
    }

    impl RecordingGlobalWatermarkStore {
        fn new() -> Self {
            Self {
                watermark: Mutex::new(None),
            }
        }

        fn stored_watermark(&self) -> Option<DateTime<Utc>> {
            *self.watermark.lock().unwrap()
        }
    }

    #[async_trait]
    impl WatermarkStore for RecordingGlobalWatermarkStore {
        async fn get_namespace_state(
            &self,
            _: i64,
            _: &str,
        ) -> Result<WatermarkState, WatermarkError> {
            Ok(WatermarkState {
                watermark: DateTime::<Utc>::UNIX_EPOCH,
                in_progress: None,
            })
        }

        async fn save_namespace_cursor(
            &self,
            _: i64,
            _: &str,
            _: &InProgressCursor,
        ) -> Result<(), WatermarkError> {
            Ok(())
        }

        async fn complete_namespace_watermark(
            &self,
            _: i64,
            _: &str,
            _: &DateTime<Utc>,
        ) -> Result<(), WatermarkError> {
            Ok(())
        }

        async fn get_global_state(&self) -> Result<WatermarkState, WatermarkError> {
            Ok(WatermarkState {
                watermark: DateTime::<Utc>::UNIX_EPOCH,
                in_progress: None,
            })
        }

        async fn save_global_cursor(
            &self,
            _: &InProgressCursor,
        ) -> Result<(), WatermarkError> {
            Ok(())
        }

        async fn complete_global_watermark(
            &self,
            watermark: &DateTime<Utc>,
        ) -> Result<(), WatermarkError> {
            *self.watermark.lock().unwrap() = Some(*watermark);
            Ok(())
        }
    }

    fn create_test_node(name: &str, source: &str) -> NodeEntity {
        NodeEntity {
            name: name.to_string(),
            fields: vec![Field {
                name: "id".to_string(),
                source: "id".to_string(),
                data_type: DataType::Int,
                nullable: false,
                enum_values: None,
                enum_type: ontology::EnumType::default(),
            }],
            destination_table: format!(
                "{}{}",
                ontology::constants::GL_TABLE_PREFIX,
                name.to_lowercase()
            ),
            etl: Some(EtlConfig::Table {
                scope: EtlScope::Global,
                source: source.to_string(),
                watermark: "_siphon_replicated_at".to_string(),
                deleted: "_siphon_deleted".to_string(),
                edges: BTreeMap::new(),
            }),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn handle_processes_pipelines() {
        let datalake = Arc::new(EmptyDatalake);
        let ontology = Ontology::new();
        let user_node = create_test_node("User", "siphon_users");
        let project_node = create_test_node("Project", "siphon_projects");

        let pipelines = vec![
            OntologyEntityPipeline::from_node(
                &user_node,
                &ontology,
                datalake.clone(),
                test_metrics(),
            )
            .unwrap(),
            OntologyEntityPipeline::from_node(&project_node, &ontology, datalake, test_metrics())
                .unwrap(),
        ];

        let handler = GlobalHandler::new(
            Arc::new(RecordingGlobalWatermarkStore::new()),
            pipelines,
            test_metrics(),
            GlobalHandlerConfig::default(),
        );

        let payload = serde_json::json!({
            "watermark": "2024-01-21T00:00:00Z"
        })
        .to_string();
        let envelope = TestEnvelopeFactory::simple(&payload);

        let destination = Arc::new(MockDestination::new());
        let context = HandlerContext::new(
            destination,
            Arc::new(MockNatsServices::new()),
            Arc::new(MockLockService::new()),
        );

        let result = handler.handle(context, envelope).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn handler_releases_lock_on_success() {
        let datalake = Arc::new(EmptyDatalake);
        let ontology = Ontology::new();
        let user_node = create_test_node("User", "siphon_users");

        let pipelines = vec![
            OntologyEntityPipeline::from_node(&user_node, &ontology, datalake, test_metrics())
                .unwrap(),
        ];

        let handler = GlobalHandler::new(
            Arc::new(RecordingGlobalWatermarkStore::new()),
            pipelines,
            test_metrics(),
            GlobalHandlerConfig::default(),
        );

        let payload = serde_json::json!({
            "watermark": "2024-01-21T00:00:00Z"
        })
        .to_string();
        let envelope = TestEnvelopeFactory::simple(&payload);

        let mock_locks = Arc::new(MockLockService::new());
        mock_locks.set_lock(global_lock_key());

        let destination = Arc::new(MockDestination::new());
        let context = HandlerContext::new(
            destination,
            Arc::new(MockNatsServices::new()),
            mock_locks.clone(),
        );

        let result = handler.handle(context, envelope).await;

        assert!(result.is_ok());
        assert!(
            !mock_locks.is_held(global_lock_key()),
            "global lock should be released after successful processing"
        );
    }

    #[tokio::test]
    async fn watermark_updated_when_rows_indexed() {
        let datalake = Arc::new(NonEmptyDatalake);
        let ontology = Ontology::new();
        let user_node = create_test_node("User", "siphon_users");

        let pipelines = vec![
            OntologyEntityPipeline::from_node(&user_node, &ontology, datalake, test_metrics())
                .unwrap(),
        ];

        let store = Arc::new(RecordingGlobalWatermarkStore::new());
        let handler = GlobalHandler::new(
            store.clone(),
            pipelines,
            test_metrics(),
            GlobalHandlerConfig::default(),
        );

        let payload = serde_json::json!({
            "watermark": "2024-06-15T12:00:00Z"
        })
        .to_string();
        let envelope = TestEnvelopeFactory::simple(&payload);

        let destination = Arc::new(MockDestination::new());
        let context = HandlerContext::new(
            destination,
            Arc::new(MockNatsServices::new()),
            Arc::new(MockLockService::new()),
        );

        handler.handle(context, envelope).await.unwrap();

        let expected = "2024-06-15T12:00:00Z".parse::<DateTime<Utc>>().unwrap();
        assert_eq!(
            store.stored_watermark(),
            Some(expected),
            "global watermark should be updated when rows were indexed"
        );
    }

    #[tokio::test]
    async fn watermark_not_updated_when_no_rows_indexed() {
        let datalake = Arc::new(EmptyDatalake);
        let ontology = Ontology::new();
        let user_node = create_test_node("User", "siphon_users");

        let pipelines = vec![
            OntologyEntityPipeline::from_node(&user_node, &ontology, datalake, test_metrics())
                .unwrap(),
        ];

        let store = Arc::new(RecordingGlobalWatermarkStore::new());
        let handler = GlobalHandler::new(
            store.clone(),
            pipelines,
            test_metrics(),
            GlobalHandlerConfig::default(),
        );

        let payload = serde_json::json!({
            "watermark": "2024-06-15T12:00:00Z"
        })
        .to_string();
        let envelope = TestEnvelopeFactory::simple(&payload);

        let destination = Arc::new(MockDestination::new());
        let context = HandlerContext::new(
            destination,
            Arc::new(MockNatsServices::new()),
            Arc::new(MockLockService::new()),
        );

        handler.handle(context, envelope).await.unwrap();

        // When no rows indexed, complete_global_watermark is called with the old watermark (epoch)
        // to clear the cursor, but the watermark itself doesn't advance
        assert_eq!(
            store.stored_watermark(),
            Some(DateTime::<Utc>::UNIX_EPOCH),
            "global watermark should remain at epoch when no rows were indexed"
        );
    }
}
