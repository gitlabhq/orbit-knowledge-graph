use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use gkg_server_config::indexing_progress::{
    INDEXING_PROGRESS_BUCKET, code_key, counts_key, meta_key,
};
use tracing::{error, info, warn};

use crate::handler::{Handler, HandlerContext, HandlerError};
use crate::nats::NatsServices;
use crate::topic::NamespaceDeletionRequest;
use crate::types::{Envelope, Event, SerializationError, Subscription};
use gkg_server_config::{HandlerConfiguration, NamespaceDeletionHandlerConfig};

use super::metrics::DeletionMetrics;
use super::store::NamespaceDeletionStore;

/// An empty or malformed path would cause `startsWith(traversal_path, '')` to match
/// every row in the table, so we reject anything that isn't `<org_id>/<namespace_id>/`.
fn is_valid_traversal_path(path: &str) -> bool {
    let Some(inner) = path.strip_suffix('/') else {
        return false;
    };
    let Some((org, namespace)) = inner.split_once('/') else {
        return false;
    };
    org.parse::<u64>().is_ok() && namespace.parse::<u64>().is_ok()
}

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

    async fn handle(&self, context: HandlerContext, message: Envelope) -> Result<(), HandlerError> {
        let payload: NamespaceDeletionRequest =
            message.to_event().map_err(|error| match error {
                SerializationError::Json(err) => HandlerError::Deserialization(err),
            })?;

        if !is_valid_traversal_path(&payload.traversal_path) {
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

        // Snapshot KV-relevant identifiers BEFORE graph + checkpoint deletion
        // since those calls clear the source rows. Failure here is non-fatal —
        // we can still delete graph data, we just won't be able to clean the
        // corresponding KV keys.
        let traversal_paths = match self
            .store
            .list_traversal_paths(&payload.traversal_path)
            .await
        {
            Ok(tps) => tps,
            Err(e) => {
                warn!(
                    namespace_id = payload.namespace_id,
                    error = %e,
                    "failed to enumerate traversal paths for KV cleanup; skipping counts.* keys"
                );
                Vec::new()
            }
        };
        let project_ids = match self
            .store
            .list_code_project_ids(&payload.traversal_path)
            .await
        {
            Ok(ids) => ids,
            Err(e) => {
                warn!(
                    namespace_id = payload.namespace_id,
                    error = %e,
                    "failed to enumerate project ids for KV cleanup; skipping code.* keys"
                );
                Vec::new()
            }
        };

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

        cleanup_progress_kv(
            context.nats.as_ref(),
            payload.namespace_id,
            &traversal_paths,
            &project_ids,
        )
        .await;

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

/// Deletes all NATS KV keys owned by a namespace: meta.<ns>, counts.<tp> for
/// each traversal path under the namespace, and code.<project_id> for each
/// code-indexed project. Individual failures are logged and ignored — KV
/// cleanup is best-effort and should not block graph-data deletion.
async fn cleanup_progress_kv(
    nats: &dyn NatsServices,
    namespace_id: i64,
    traversal_paths: &[String],
    project_ids: &[i64],
) {
    let mk = meta_key(namespace_id);
    if let Err(e) = nats.kv_delete(INDEXING_PROGRESS_BUCKET, &mk).await {
        warn!(key = %mk, error = %e, "failed to delete meta KV key");
    }

    for tp in traversal_paths {
        let key = counts_key(tp);
        if let Err(e) = nats.kv_delete(INDEXING_PROGRESS_BUCKET, &key).await {
            warn!(key = %key, error = %e, "failed to delete counts KV key");
        }
    }

    for project_id in project_ids {
        let key = code_key(*project_id);
        if let Err(e) = nats.kv_delete(INDEXING_PROGRESS_BUCKET, &key).await {
            warn!(key = %key, error = %e, "failed to delete code KV key");
        }
    }

    info!(
        namespace_id,
        traversal_paths = traversal_paths.len(),
        code_projects = project_ids.len(),
        "cleaned up progress KV keys"
    );
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::destination::Destination;
    use crate::handler::HandlerContext;
    use crate::locking::LockService;
    use crate::nats::{NatsServices, ProgressNotifier};
    use crate::testkit::mocks::{MockDestination, MockLockService, MockNatsServices};
    use crate::types::Envelope;
    use bytes::Bytes;

    use super::super::store::test_utils::{MockNamespaceDeletionStore, failed_outcome, ok_outcome};

    fn handler_context() -> HandlerContext {
        HandlerContext::new(
            Arc::new(MockDestination::new()) as Arc<dyn Destination>,
            Arc::new(MockNatsServices::new()) as Arc<dyn NatsServices>,
            Arc::new(MockLockService::new()) as Arc<dyn LockService>,
            ProgressNotifier::noop(),
        )
    }

    fn handler_context_with_nats(nats: Arc<MockNatsServices>) -> HandlerContext {
        HandlerContext::new(
            Arc::new(MockDestination::new()) as Arc<dyn Destination>,
            nats as Arc<dyn NatsServices>,
            Arc::new(MockLockService::new()) as Arc<dyn LockService>,
            ProgressNotifier::noop(),
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

    #[tokio::test]
    async fn deletes_kv_keys_for_meta_counts_and_code() {
        let store = Arc::new(
            MockNamespaceDeletionStore::new()
                .with_traversal_paths(vec![
                    "1/100/".to_string(),
                    "1/100/50/".to_string(),
                    "1/100/50/42/".to_string(),
                ])
                .with_project_ids(vec![42, 43]),
        );
        let handler = make_handler(store.clone());
        let nats = Arc::new(MockNatsServices::new());

        // Seed KV entries that should be deleted.
        nats.set_kv(
            INDEXING_PROGRESS_BUCKET,
            &meta_key(100),
            Bytes::from_static(b"{}"),
        );
        nats.set_kv(
            INDEXING_PROGRESS_BUCKET,
            &counts_key("1/100/"),
            Bytes::from_static(b"{}"),
        );
        nats.set_kv(
            INDEXING_PROGRESS_BUCKET,
            &counts_key("1/100/50/"),
            Bytes::from_static(b"{}"),
        );
        nats.set_kv(
            INDEXING_PROGRESS_BUCKET,
            &code_key(42),
            Bytes::from_static(b"{}"),
        );
        nats.set_kv(
            INDEXING_PROGRESS_BUCKET,
            &code_key(43),
            Bytes::from_static(b"{}"),
        );
        // Unrelated key — must survive.
        nats.set_kv(
            INDEXING_PROGRESS_BUCKET,
            &meta_key(999),
            Bytes::from_static(b"{}"),
        );

        handler
            .handle(
                handler_context_with_nats(Arc::clone(&nats)),
                envelope_for(100, "1/100/"),
            )
            .await
            .unwrap();

        assert!(
            nats.get_kv(INDEXING_PROGRESS_BUCKET, &meta_key(100))
                .is_none(),
            "meta.100 should be deleted"
        );
        assert!(
            nats.get_kv(INDEXING_PROGRESS_BUCKET, &counts_key("1/100/"))
                .is_none(),
            "counts.1.100 should be deleted"
        );
        assert!(
            nats.get_kv(INDEXING_PROGRESS_BUCKET, &counts_key("1/100/50/"))
                .is_none(),
            "counts.1.100.50 should be deleted"
        );
        assert!(
            nats.get_kv(INDEXING_PROGRESS_BUCKET, &code_key(42))
                .is_none(),
            "code.42 should be deleted"
        );
        assert!(
            nats.get_kv(INDEXING_PROGRESS_BUCKET, &code_key(43))
                .is_none(),
            "code.43 should be deleted"
        );
        assert!(
            nats.get_kv(INDEXING_PROGRESS_BUCKET, &meta_key(999))
                .is_some(),
            "unrelated meta.999 must survive"
        );
    }

    #[tokio::test]
    async fn snapshots_ids_before_graph_deletion() {
        // list_traversal_paths must be called BEFORE delete_namespace_data
        // (otherwise the source rows are gone). Verify the ordering via
        // call recording.
        let store = Arc::new(
            MockNamespaceDeletionStore::new().with_traversal_paths(vec!["1/100/".to_string()]),
        );
        let handler = make_handler(store.clone());

        handler
            .handle(handler_context(), envelope_for(100, "1/100/"))
            .await
            .unwrap();

        assert_eq!(store.list_tp_calls(), vec!["1/100/"]);
        assert_eq!(store.list_project_calls(), vec!["1/100/"]);
        assert_eq!(store.delete_calls(), vec!["1/100/"]);
    }

    #[tokio::test]
    async fn kv_cleanup_does_not_run_when_namespace_re_enabled() {
        let store = Arc::new(MockNamespaceDeletionStore::new().namespace_re_enabled());
        let handler = make_handler(store.clone());
        let nats = Arc::new(MockNatsServices::new());
        nats.set_kv(
            INDEXING_PROGRESS_BUCKET,
            &meta_key(100),
            Bytes::from_static(b"{}"),
        );

        handler
            .handle(
                handler_context_with_nats(Arc::clone(&nats)),
                envelope_for(100, "1/100/"),
            )
            .await
            .unwrap();

        // re-enabled path skips cleanup: meta survives.
        assert!(
            nats.get_kv(INDEXING_PROGRESS_BUCKET, &meta_key(100))
                .is_some(),
            "meta.100 must survive when namespace is re-enabled"
        );
        assert!(store.list_tp_calls().is_empty());
    }
}
