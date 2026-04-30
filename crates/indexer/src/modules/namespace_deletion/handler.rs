use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use tracing::{error, info, warn};

use crate::handler::{Handler, HandlerContext, HandlerError};
use crate::topic::NamespaceDeletionRequest;
use crate::types::{Envelope, Event, SerializationError, Subscription};
use gkg_server_config::{HandlerConfiguration, NamespaceDeletionHandlerConfig};

use super::metrics::DeletionMetrics;
use super::store::NamespaceDeletionStore;

pub struct NamespaceDeletionHandler {
    store: Arc<dyn NamespaceDeletionStore>,
    config: NamespaceDeletionHandlerConfig,
    metrics: DeletionMetrics,
}

impl NamespaceDeletionHandler {
    pub fn new(
        store: Arc<dyn NamespaceDeletionStore>,
        config: NamespaceDeletionHandlerConfig,
    ) -> Self {
        Self {
            store,
            config,
            metrics: DeletionMetrics::new(),
        }
    }
}

#[async_trait]
impl Handler for NamespaceDeletionHandler {
    fn name(&self) -> &str {
        "namespace_deletion_handler"
    }

    fn subscription(&self) -> Subscription {
        NamespaceDeletionRequest::subscription()
    }

    fn engine_config(&self) -> &HandlerConfiguration {
        &self.config.engine
    }

    async fn handle(
        &self,
        _context: HandlerContext,
        message: Envelope,
    ) -> Result<(), HandlerError> {
        let payload: NamespaceDeletionRequest =
            message.to_event().map_err(|error| match error {
                SerializationError::Json(err) => HandlerError::Deserialization(err),
            })?;

        if !gkg_utils::traversal_path::is_valid(&payload.traversal_path) {
            error!(
                namespace_id = payload.namespace_id,
                traversal_path = %payload.traversal_path,
                "invalid traversal_path: must match <org_id>/<namespace_id>/"
            );
            return Err(HandlerError::Processing(format!(
                "invalid traversal_path '{}': must match <org_id>/<namespace_id>/",
                payload.traversal_path
            )));
        }

        let still_deleted = self
            .store
            .is_namespace_still_deleted(payload.namespace_id)
            .await
            .map_err(|error| HandlerError::Processing(error.to_string()))?;

        if !still_deleted {
            info!(
                namespace_id = payload.namespace_id,
                "namespace was re-enabled, skipping deletion and clearing schedule"
            );

            self.store
                .mark_deletion_complete(payload.namespace_id, &payload.traversal_path)
                .await
                .map_err(|error| HandlerError::Processing(error.to_string()))?;

            return Ok(());
        }

        let started_at = Instant::now();
        info!(
            namespace_id = payload.namespace_id,
            traversal_path = %payload.traversal_path,
            "starting namespace deletion"
        );

        let outcomes = self
            .store
            .delete_namespace_data(&payload.traversal_path)
            .await;

        let mut failed_tables = Vec::new();
        for outcome in &outcomes {
            if let Some(ref error) = outcome.error {
                self.metrics.record_table_error(&outcome.table);
                warn!(
                    namespace_id = payload.namespace_id,
                    table = %outcome.table,
                    error,
                    "failed to delete namespace data from table"
                );
                failed_tables.push(outcome.table.as_str());
            } else {
                self.metrics
                    .record_table_deleted(&outcome.table, outcome.duration_seconds);
                info!(
                    namespace_id = payload.namespace_id,
                    table = %outcome.table,
                    duration_ms = (outcome.duration_seconds * 1000.0) as u64,
                    "deleted namespace data from table"
                );
            }
        }

        if !failed_tables.is_empty() {
            return Err(HandlerError::Processing(format!(
                "failed to delete from tables: {}",
                failed_tables.join(", ")
            )));
        }

        self.store
            .delete_namespace_checkpoints(&payload.traversal_path, payload.namespace_id)
            .await
            .map_err(|error| HandlerError::Processing(error.to_string()))?;

        info!(
            namespace_id = payload.namespace_id,
            traversal_path = %payload.traversal_path,
            "deleted namespace checkpoints"
        );

        self.store
            .mark_deletion_complete(payload.namespace_id, &payload.traversal_path)
            .await
            .map_err(|error| HandlerError::Processing(error.to_string()))?;

        let elapsed = started_at.elapsed();
        info!(
            namespace_id = payload.namespace_id,
            elapsed_ms = elapsed.as_millis() as u64,
            "namespace deletion completed"
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::destination::Destination;
    use crate::handler::HandlerContext;
    use crate::locking::LockService;
    use crate::nats::ProgressNotifier;
    use crate::testkit::mocks::{MockDestination, MockLockService, MockNatsServices};
    use crate::types::Envelope;

    use super::super::store::test_utils::{MockNamespaceDeletionStore, failed_outcome, ok_outcome};

    fn handler_context() -> HandlerContext {
        let mock_nats = Arc::new(MockNatsServices::new());
        HandlerContext::new(
            Arc::new(MockDestination::new()) as Arc<dyn Destination>,
            mock_nats.clone(),
            Arc::new(MockLockService::new()) as Arc<dyn LockService>,
            ProgressNotifier::noop(),
            Arc::new(crate::indexing_status::IndexingStatusStore::new(mock_nats)),
        )
    }

    fn make_handler(store: Arc<MockNamespaceDeletionStore>) -> NamespaceDeletionHandler {
        NamespaceDeletionHandler::new(store, NamespaceDeletionHandlerConfig::default())
    }

    fn envelope_for(namespace_id: i64, traversal_path: &str) -> Envelope {
        let request = NamespaceDeletionRequest {
            namespace_id,
            traversal_path: traversal_path.to_string(),
        };
        Envelope::new(&request).unwrap()
    }

    #[tokio::test]
    async fn deletes_then_marks_complete_with_correct_arguments() {
        let store = Arc::new(MockNamespaceDeletionStore::new());
        let handler = make_handler(store.clone());

        handler
            .handle(handler_context(), envelope_for(100, "1/100/"))
            .await
            .unwrap();

        assert_eq!(store.delete_calls(), vec!["1/100/"]);
        assert_eq!(store.delete_checkpoint_calls(), vec![100]);
        assert_eq!(
            store.mark_complete_calls(),
            vec![(100, "1/100/".to_string())]
        );
    }

    #[tokio::test]
    async fn rejects_invalid_traversal_paths_before_touching_store() {
        let store = Arc::new(MockNamespaceDeletionStore::new());
        let handler = make_handler(store.clone());

        let invalid = [
            "",
            "/",
            "1/",
            "1/100",
            "abc/100/",
            "1/abc/",
            "../etc/passwd/",
            "1/2/3/",
            "1/100/extra/",
        ];
        for path in invalid {
            let result = handler
                .handle(handler_context(), envelope_for(100, path))
                .await;
            assert!(result.is_err(), "should reject '{path}'");
        }

        assert!(
            store.delete_calls().is_empty(),
            "no invalid path should reach the store"
        );
    }

    #[tokio::test]
    async fn returns_error_on_malformed_envelope() {
        let store = Arc::new(MockNamespaceDeletionStore::new());
        let handler = make_handler(store.clone());

        let bad_envelope = Envelope {
            id: crate::types::MessageId::unique(),
            subject: std::sync::Arc::from(""),
            payload: bytes::Bytes::from_static(b"not json"),
            timestamp: chrono::Utc::now(),
            attempt: 1,
        };
        let result = handler.handle(handler_context(), bad_envelope).await;

        assert!(result.is_err());
        assert!(store.delete_calls().is_empty());
    }

    #[tokio::test]
    async fn does_not_mark_complete_when_all_deletions_fail() {
        let store = Arc::new(
            MockNamespaceDeletionStore::new().with_deletion_outcomes(vec![
                failed_outcome("gl_project"),
                failed_outcome("gl_issue"),
            ]),
        );
        let handler = make_handler(store.clone());

        let result = handler
            .handle(handler_context(), envelope_for(100, "1/100/"))
            .await;

        assert!(result.is_err());
        assert!(
            store.delete_checkpoint_calls().is_empty(),
            "should not delete checkpoints when data deletion fails"
        );
        assert!(store.mark_complete_calls().is_empty());
    }

    #[tokio::test]
    async fn does_not_mark_complete_on_partial_failure() {
        let store = Arc::new(
            MockNamespaceDeletionStore::new()
                .with_deletion_outcomes(vec![ok_outcome("gl_project"), failed_outcome("gl_issue")]),
        );
        let handler = make_handler(store.clone());

        let result = handler
            .handle(handler_context(), envelope_for(100, "1/100/"))
            .await;

        assert!(result.is_err());
        assert!(
            store.delete_checkpoint_calls().is_empty(),
            "should not delete checkpoints when any table fails"
        );
        assert!(
            store.mark_complete_calls().is_empty(),
            "should not mark complete when any table fails"
        );
    }

    #[tokio::test]
    async fn skips_deletion_when_namespace_was_re_enabled() {
        let store = Arc::new(MockNamespaceDeletionStore::new().namespace_re_enabled());
        let handler = make_handler(store.clone());

        handler
            .handle(handler_context(), envelope_for(100, "1/100/"))
            .await
            .unwrap();

        assert!(
            store.delete_calls().is_empty(),
            "should not delete data for a re-enabled namespace"
        );
        assert!(
            store.delete_checkpoint_calls().is_empty(),
            "should not delete checkpoints for a re-enabled namespace"
        );
        assert_eq!(
            store.mark_complete_calls(),
            vec![(100, "1/100/".to_string())],
            "should clear the schedule entry"
        );
    }

    #[tokio::test]
    async fn surfaces_mark_complete_failure_after_successful_deletion() {
        let store = Arc::new(MockNamespaceDeletionStore::new().failing_mark_complete());
        let handler = make_handler(store.clone());

        let result = handler
            .handle(handler_context(), envelope_for(100, "1/100/"))
            .await;

        assert!(result.is_err());
        assert_eq!(
            store.delete_calls(),
            vec!["1/100/"],
            "deletion should have succeeded before mark_complete failed"
        );
    }
}
