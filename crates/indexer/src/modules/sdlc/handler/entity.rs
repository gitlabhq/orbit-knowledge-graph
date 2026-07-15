use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use ontology::EtlScope;
use tracing::{Instrument, debug, info_span};
use uuid::Uuid;

use crate::analytics::IndexingAnalytics;
use crate::checkpoint::namespace_position_key;

use crate::handler::{Handler, HandlerContext, HandlerError};
use crate::modules::sdlc::extract::{ExtractRunContext, Extractor};
use crate::modules::sdlc::metrics::SdlcMetrics;
use crate::modules::sdlc::observer::SdlcOtelObserver;
use crate::modules::sdlc::pipeline::{Pipeline, PipelineContext};
use crate::modules::sdlc::plan::Plan;
use crate::observer::{self, IndexingObserver, PipelineType};
use crate::topic::{GlobalIndexingRequest, NamespaceIndexingRequest};
use crate::types::{Envelope, SerializationError, Subscription};

pub struct EntityHandler {
    handler_name: String,
    plan: Plan,
    scope: EtlScope,
    pipeline: Arc<Pipeline>,
    extractor: Arc<dyn Extractor>,
    writer: Arc<crate::clickhouse::ClickHouseWriter>,
    metrics: SdlcMetrics,
    subscription: Subscription,
    analytics: IndexingAnalytics,
}

struct IndexingRequest {
    watermark: DateTime<Utc>,
    scope_key: String,
    traversal_path: Option<String>,
    namespace_id: Option<i64>,
    dispatch_id: Uuid,
    campaign_id: Option<String>,
    targets: Vec<String>,
}

impl IndexingRequest {
    fn indexing_requested(&self, target: &str) -> bool {
        self.targets.is_empty() || self.targets.iter().any(|requested| requested == target)
    }
}

impl EntityHandler {
    #[allow(
        clippy::too_many_arguments,
        reason = "handler constructor wires all collaborators explicitly; grouping into a struct would just move the arity"
    )]
    pub(in crate::modules::sdlc) fn new(
        plan: Plan,
        scope: EtlScope,
        pipeline: Arc<Pipeline>,
        extractor: Arc<dyn Extractor>,
        writer: Arc<crate::clickhouse::ClickHouseWriter>,
        metrics: SdlcMetrics,
        subscription: Subscription,
        analytics: IndexingAnalytics,
    ) -> Self {
        let handler_name = format!("entity.{}", plan.name.to_lowercase());
        Self {
            handler_name,
            plan,
            scope,
            pipeline,
            extractor,
            writer,
            metrics,
            subscription,
            analytics,
        }
    }

    fn deserialize(&self, message: Envelope) -> Result<IndexingRequest, HandlerError> {
        match self.scope {
            EtlScope::Global => {
                let payload: GlobalIndexingRequest =
                    message.to_event().map_err(serialization_error)?;
                Ok(IndexingRequest {
                    watermark: payload.watermark,
                    scope_key: "global".to_string(),
                    traversal_path: None,
                    namespace_id: None,
                    dispatch_id: payload.dispatch_id,
                    campaign_id: payload.campaign_id,
                    targets: payload.targets,
                })
            }
            EtlScope::Namespaced => {
                let payload: NamespaceIndexingRequest =
                    message.to_event().map_err(serialization_error)?;
                Ok(IndexingRequest {
                    watermark: payload.watermark,
                    scope_key: namespace_position_key(payload.namespace),
                    traversal_path: Some(payload.traversal_path),
                    namespace_id: Some(payload.namespace),
                    dispatch_id: payload.dispatch_id,
                    campaign_id: payload.campaign_id,
                    targets: payload.targets,
                })
            }
        }
    }

    async fn execute(
        &self,
        context: HandlerContext,
        request: IndexingRequest,
    ) -> Result<(), HandlerError> {
        let mut observers: Vec<Box<dyn IndexingObserver>> =
            vec![Box::new(SdlcOtelObserver::new(self.metrics.clone()))];
        observers.extend(self.analytics.observer());
        let mut observer: observer::MultiObserver = observer::MultiObserver::new(observers);
        observer.set_dispatch_id(request.dispatch_id);
        observer.set_campaign_id(request.campaign_id.clone());
        observer.set_pipeline_type(PipelineType::Sdlc);
        observer.set_entity_type(&self.plan.name);
        observer.set_traversal_path(request.traversal_path.as_deref());
        observer.set_namespace(request.namespace_id);

        let checkpoint_key = format!("{}.{}", request.scope_key, self.plan.name);

        let observer: Arc<Mutex<dyn IndexingObserver>> = Arc::new(Mutex::new(observer));
        let pipeline_context = PipelineContext {
            writer: Arc::clone(&self.writer),
            progress: context.progress.clone(),
            observer: Arc::clone(&observer),
        };

        let result = self
            .pipeline
            .run_plan(
                &pipeline_context,
                &self.plan,
                self.extractor.as_ref(),
                ExtractRunContext {
                    position_key: checkpoint_key,
                    requested_watermark: request.watermark,
                    traversal_path: request.traversal_path.clone(),
                },
            )
            .await;

        match &result {
            Ok(stats) => {
                debug!(
                    entity = %self.plan.name,
                    read_rows = stats.read_rows,
                    read_bytes = stats.read_bytes,
                    written_rows = stats.written_rows,
                    written_bytes = stats.written_bytes,
                    duration_ms = stats.duration_ms,
                    "indexing resource stats"
                );
                observer.lock().unwrap().finish()
            }
            Err(e) => {
                let mut obs = observer.lock().unwrap();
                obs.record_error(&e.to_string());
                obs.finish();
            }
        }

        result.map(|_| ())
    }
}

fn serialization_error(error: SerializationError) -> HandlerError {
    match error {
        SerializationError::Json(err) => HandlerError::Deserialization(err),
    }
}

#[async_trait]
impl Handler for EntityHandler {
    fn name(&self) -> &str {
        &self.handler_name
    }

    fn subscription(&self) -> Subscription {
        self.subscription.clone()
    }

    async fn handle(&self, context: HandlerContext, message: Envelope) -> Result<(), HandlerError> {
        let request = self.deserialize(message)?;

        if !request.indexing_requested(&self.plan.target) {
            debug!(
                entity = %self.plan.name,
                target = %self.plan.target,
                targets = ?request.targets,
                "skipping request: target not selected"
            );
            return Ok(());
        }

        let started_at = Utc::now();
        let span = match &request.namespace_id {
            Some(id) => info_span!(
                "entity_indexing",
                entity = %self.plan.name,
                namespace_id = id,
                dispatch_id = %request.dispatch_id,
                campaign_id = request.campaign_id.as_deref().unwrap_or("none"),
            ),
            None => info_span!(
                "entity_indexing",
                entity = %self.plan.name,
                dispatch_id = %request.dispatch_id,
                campaign_id = request.campaign_id.as_deref().unwrap_or("none"),
            ),
        };
        let traversal_path = request.traversal_path.clone();

        async {
            if let Some(path) = traversal_path.as_deref() {
                context
                    .indexing_status
                    .record_entity_start(path, &self.plan.name, started_at)
                    .await;
            }

            let result = self.execute(context.clone(), request).await;
            let completed_at = Utc::now();
            let elapsed = completed_at
                .signed_duration_since(started_at)
                .to_std()
                .unwrap_or_default();
            self.metrics
                .record_handler_duration(&self.handler_name, elapsed.as_secs_f64());
            if let Err(err) = &result {
                self.metrics
                    .record_pipeline_error(&self.plan.name, err.error_kind());
            }

            if let Some(path) = traversal_path.as_deref() {
                context
                    .indexing_status
                    .record_entity_completion(
                        path,
                        &self.plan.name,
                        started_at,
                        completed_at,
                        result.as_ref().err().map(ToString::to_string),
                    )
                    .await;
            }

            result
        }
        .instrument(span)
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::modules::sdlc::extract::MemoryExtractor;
    use crate::modules::sdlc::plan::build_plans;
    use crate::modules::sdlc::test_helpers::test_metrics;
    use crate::nats::ProgressNotifier;
    use crate::testkit::{MockLockService, MockNatsServices, TestEnvelopeFactory};
    use crate::types::Event;
    use ontology::Ontology;

    fn handler_context() -> HandlerContext {
        let mock_nats = Arc::new(MockNatsServices::new());
        HandlerContext::new(
            mock_nats.clone(),
            Arc::new(MockLockService::new()),
            ProgressNotifier::noop(),
            Arc::new(crate::indexing_status::IndexingStatusStore::new(mock_nats)),
        )
    }

    fn build_handler(entity_name: &str, scope: EtlScope) -> EntityHandler {
        let ontology = Ontology::load_embedded().expect("should load ontology");
        let plans = build_plans(
            &ontology,
            crate::modules::sdlc::plan::Sizing {
                global_batch_size: 1000,
                namespaced_batch_size: 1000,
                overrides: &Default::default(),
            },
        )
        .expect("plans should build");
        let scope_plans = match scope {
            EtlScope::Global => plans.global,
            EtlScope::Namespaced => plans.namespaced,
        };
        let plan = scope_plans
            .into_iter()
            .find(|plan| plan.name == entity_name)
            .unwrap_or_else(|| panic!("entity plan not found: {entity_name}"));
        let subscription = match scope {
            EtlScope::Global => GlobalIndexingRequest::subscription(),
            EtlScope::Namespaced => NamespaceIndexingRequest::subscription(),
        };

        EntityHandler::new(
            plan,
            scope,
            Arc::new(Pipeline::new(test_metrics())),
            Arc::new(MemoryExtractor::new(Vec::new())),
            crate::testkit::test_writer(),
            test_metrics(),
            subscription,
            IndexingAnalytics::disabled(),
        )
    }

    #[tokio::test]
    async fn global_entity_handler_processes_request() {
        let handler = build_handler("User", EtlScope::Global);
        let envelope = TestEnvelopeFactory::simple(
            &serde_json::json!({ "watermark": "2024-01-21T00:00:00Z" }).to_string(),
        );

        assert_eq!(handler.name(), "entity.user");
        assert!(handler.handle(handler_context(), envelope).await.is_ok());
    }

    #[tokio::test]
    async fn namespaced_entity_handler_processes_request() {
        let handler = build_handler("MergeRequest", EtlScope::Namespaced);
        let envelope = TestEnvelopeFactory::simple(
            &serde_json::json!({
                "namespace": 100,
                "traversal_path": "42/100/",
                "watermark": "2024-01-21T00:00:00Z"
            })
            .to_string(),
        );

        assert_eq!(handler.name(), "entity.mergerequest");
        assert!(handler.handle(handler_context(), envelope).await.is_ok());
    }

    #[test]
    fn indexing_requested_matches_empty_or_matching_target() {
        assert!(indexing_request_with_targets([]).indexing_requested("MergeRequest"));
        assert!(indexing_request_with_targets(["MergeRequest"]).indexing_requested("MergeRequest"));
        assert!(!indexing_request_with_targets(["Job"]).indexing_requested("MergeRequest"));
    }

    fn indexing_request_with_targets<const N: usize>(targets: [&str; N]) -> IndexingRequest {
        IndexingRequest {
            watermark: "2024-01-21T00:00:00Z".parse().unwrap(),
            scope_key: "global".to_string(),
            traversal_path: None,
            namespace_id: None,
            dispatch_id: Uuid::nil(),
            campaign_id: None,
            targets: targets.iter().map(|target| target.to_string()).collect(),
        }
    }
}
