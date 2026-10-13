use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use ontology::EtlScope;
use tokio::task::JoinSet;
use tracing::{Instrument, debug, info, info_span};
use uuid::Uuid;

use crate::analytics::IndexingAnalytics;
use crate::checkpoint::{Checkpoint, CheckpointStore, WindowBounds, namespace_position_key};

use crate::durability::RunDurability;
use crate::handler::{Handler, HandlerContext, HandlerError};
use crate::modules::sdlc::datalake::DatalakeQuery;
use crate::modules::sdlc::metrics::SdlcMetrics;
use crate::modules::sdlc::observer::SdlcOtelObserver;
use crate::modules::sdlc::partitioning::{PartitionAssignment, PartitionStrategy};
use crate::modules::sdlc::pipeline::{Pipeline, PipelineContext, PipelineStats};
use crate::modules::sdlc::plan::{
    DeletedFilter, Plan, PreparedQuery, TraversalPathFilter, WatermarkFilter,
};
use crate::observer::{self, IndexingMode, IndexingObserver, PipelineType};
use crate::topic::{GlobalIndexingRequest, NamespaceIndexingRequest};
use crate::types::{Envelope, SerializationError, Subscription};
use orbit_utils::traversal_path::TraversalPath;

pub struct EntityHandler {
    handler_name: String,
    plan: Plan,
    scope: EtlScope,
    pipeline: Arc<Pipeline>,
    writer: Arc<crate::clickhouse::ClickHouseWriter>,
    datalake: Arc<dyn DatalakeQuery>,
    checkpoint_store: Arc<dyn CheckpointStore>,
    metrics: SdlcMetrics,
    subscription: Subscription,
    partition_strategy: Option<PartitionStrategy>,
    analytics: IndexingAnalytics,
}

struct IndexingRequest {
    watermark: DateTime<Utc>,
    scope_key: String,
    traversal_path: Option<TraversalPath>,
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
        writer: Arc<crate::clickhouse::ClickHouseWriter>,
        datalake: Arc<dyn DatalakeQuery>,
        checkpoint_store: Arc<dyn CheckpointStore>,
        metrics: SdlcMetrics,
        subscription: Subscription,
        partition_strategy: Option<PartitionStrategy>,
        analytics: IndexingAnalytics,
    ) -> Self {
        let handler_name = format!("entity.{}", plan.name.to_lowercase());
        Self {
            handler_name,
            plan,
            scope,
            pipeline,
            writer,
            datalake,
            checkpoint_store,
            metrics,
            subscription,
            partition_strategy,
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
    ) -> Result<PipelineStats, HandlerError> {
        let mut observers: Vec<Box<dyn IndexingObserver>> =
            vec![Box::new(SdlcOtelObserver::new(self.metrics.clone()))];
        observers.extend(self.analytics.observer());
        let mut observer: observer::MultiObserver = observer::MultiObserver::new(observers);
        observer.set_dispatch_id(request.dispatch_id);
        observer.set_campaign_id(request.campaign_id.clone());
        observer.set_pipeline_type(PipelineType::Sdlc);
        observer.set_entity_type(&self.plan.name);
        observer.set_traversal_path(request.traversal_path.as_ref());
        observer.set_namespace(request.namespace_id);

        let checkpoint_key = format!("{}.{}", request.scope_key, self.plan.name);
        let mut checkpoint = self
            .checkpoint_store
            .load(&checkpoint_key)
            .await
            .map_err(|err| HandlerError::Processing(err.to_string()))?
            .unwrap_or_else(|| Checkpoint::new(request.watermark));
        let window = checkpoint.pull_window(request.watermark);
        let mode = window.indexing_mode();
        observer.set_indexing_mode(mode);

        checkpoint.start_attempt();
        self.checkpoint_store
            .save(
                &checkpoint_key,
                &checkpoint,
                RunDurability::for_mode(mode).attempt_start,
            )
            .await
            .map_err(|err| HandlerError::Processing(err.to_string()))?;

        let observer: Arc<Mutex<dyn IndexingObserver>> = Arc::new(Mutex::new(observer));
        let pipeline_context = PipelineContext {
            writer: Arc::clone(&self.writer),
            progress: context.progress.clone(),
            observer: Arc::clone(&observer),
        };

        let base_query = self
            .plan
            .prepare()
            .with(WatermarkFilter {
                column: &self.plan.watermark_column,
                last: window.floor.unwrap_or(DateTime::<Utc>::UNIX_EPOCH),
                current: window.target,
                sources: self.plan.watermark_sources.as_ref(),
            })
            .with(
                request
                    .traversal_path
                    .as_ref()
                    .map(|path| TraversalPathFilter { path }),
            )
            .with((mode == IndexingMode::Full).then_some(DeletedFilter {
                column: &self.plan.deleted_column,
            }));

        let should_partition =
            self.partition_strategy.is_some() && checkpoint.is_first_pass_before_paging();
        let ranges = if should_partition {
            self.partition_strategy
                .as_ref()
                .unwrap()
                .compute_ranges(self.datalake.as_ref(), request.traversal_path.as_ref())
                .await?
        } else {
            Vec::new()
        };

        let result = if ranges.is_empty() {
            self.pipeline
                .run_plan(
                    &pipeline_context,
                    &self.plan,
                    base_query,
                    &checkpoint_key,
                    checkpoint,
                    window,
                )
                .await
        } else {
            info!(
                entity = %self.plan.name,
                partitions = ranges.len(),
                "running partitioned initial load"
            );

            let partition_result = self
                .run_partitions(
                    base_query.into_partitions(ranges),
                    &checkpoint_key,
                    window,
                    &context,
                    &pipeline_context,
                )
                .await;

            match partition_result {
                Ok(stats) => {
                    let partition_checkpoints = self
                        .checkpoint_store
                        .load_by_prefix(&format!(
                            "{checkpoint_key}{}",
                            PartitionAssignment::CHECKPOINT_PREFIX
                        ))
                        .await
                        .map_err(|err| HandlerError::Processing(err.to_string()))?;

                    match consolidated_watermark(&partition_checkpoints, request.watermark) {
                        Ok(watermark) => self
                            .checkpoint_store
                            .consolidate(&checkpoint_key, &watermark)
                            .await
                            .map(|()| stats)
                            .map_err(|err| HandlerError::Processing(err.to_string())),
                        // A parent still at its first-pass start re-triggers partitioning next dispatch; Ok keeps this expected mid-load state out of pipeline-error metrics.
                        Err(incomplete) => {
                            info!(
                                entity = %self.plan.name,
                                checkpoint = %checkpoint_key,
                                incomplete = incomplete.len(),
                                partitions = %incomplete.join(", "),
                                "partitions still in progress; deferring consolidation to next dispatch"
                            );
                            Ok(stats)
                        }
                    }
                }
                Err(e) => Err(e),
            }
        };

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

        result
    }

    async fn run_partitions(
        &self,
        partitions: Vec<(
            crate::modules::sdlc::partitioning::PartitionAssignment,
            PreparedQuery,
        )>,
        checkpoint_key: &str,
        window: WindowBounds,
        context: &HandlerContext,
        parent_pipeline_context: &PipelineContext,
    ) -> Result<PipelineStats, HandlerError> {
        let mut set: JoinSet<Result<PipelineStats, HandlerError>> = JoinSet::new();
        for (assignment, query) in partitions {
            let position_key = format!("{checkpoint_key}{}", assignment.position_suffix());

            let existing = self
                .checkpoint_store
                .load(&position_key)
                .await
                .map_err(|err| HandlerError::Processing(err.to_string()))?;
            let checkpoint = match existing {
                Some(cp) if cp.is_completed() => {
                    info!(partition = %position_key, "skipping already-completed partition");
                    continue;
                }
                Some(cp) => cp,
                None => Checkpoint::new(window.target),
            };

            let plan = self.plan.clone();
            let pipeline = Arc::clone(&self.pipeline);
            let partition_context = PipelineContext {
                writer: Arc::clone(&self.writer),
                progress: context.progress.clone(),
                observer: Arc::clone(&parent_pipeline_context.observer),
            };

            set.spawn(async move {
                pipeline
                    .run_plan(
                        &partition_context,
                        &plan,
                        query,
                        &position_key,
                        checkpoint,
                        window,
                    )
                    .await
            });
        }

        let mut errors = Vec::new();
        let mut total = PipelineStats::default();
        while let Some(result) = set.join_next().await {
            match result {
                Ok(Ok(stats)) => total.merge(stats),
                Ok(Err(err)) => errors.push(err.to_string()),
                Err(join_err) => errors.push(format!("partition task panicked: {join_err}")),
            }
        }

        if errors.is_empty() {
            Ok(total)
        } else {
            Err(HandlerError::Processing(format!(
                "partition failures: {}",
                errors.join("; ")
            )))
        }
    }
}

/// Parent watermark for a finished partitioned load, or the partitions still
/// mid-pull: consolidating past a cursored partition silently drops its id range.
fn consolidated_watermark(
    partition_checkpoints: &[(String, Checkpoint)],
    fallback: DateTime<Utc>,
) -> Result<DateTime<Utc>, Vec<String>> {
    let incomplete: Vec<String> = partition_checkpoints
        .iter()
        .filter(|(_, checkpoint)| !checkpoint.is_completed())
        .map(|(key, _)| key.clone())
        .collect();
    if !incomplete.is_empty() {
        return Err(incomplete);
    }

    Ok(partition_checkpoints
        .iter()
        .map(|(_, checkpoint)| checkpoint.watermark)
        .min()
        .unwrap_or(fallback))
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

        async {
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

            result.map(|_| ())
        }
        .instrument(span)
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::modules::sdlc::plan::build_plans;
    use crate::modules::sdlc::test_helpers::{EmptyDatalake, MockCheckpointStore, test_metrics};
    use crate::nats::ProgressNotifier;
    use crate::testkit::{MockLockService, MockNatsServices, TestEnvelopeFactory};
    use crate::types::Event;
    use ontology::Ontology;
    use orbit_server_config::AppConfig;

    fn handler_context() -> HandlerContext {
        let mock_nats = Arc::new(MockNatsServices::new());
        HandlerContext::new(
            mock_nats.clone(),
            Arc::new(MockLockService::new()),
            ProgressNotifier::noop(),
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
            .find(|p| p.name == entity_name)
            .unwrap_or_else(|| panic!("entity plan not found: {entity_name}"));

        let datalake: Arc<dyn DatalakeQuery> = Arc::new(EmptyDatalake);
        let checkpoint_store: Arc<dyn CheckpointStore> = Arc::new(MockCheckpointStore);
        let pipeline = Arc::new(Pipeline::new(
            Arc::clone(&datalake),
            Arc::clone(&checkpoint_store),
            test_metrics(),
            AppConfig::embedded_defaults().engine.datalake_retry,
        ));
        let subscription = match scope {
            EtlScope::Global => GlobalIndexingRequest::subscription(),
            EtlScope::Namespaced => NamespaceIndexingRequest::subscription(),
        };

        let writer = crate::testkit::test_writer();
        EntityHandler::new(
            plan,
            scope,
            pipeline,
            writer,
            datalake,
            checkpoint_store,
            test_metrics(),
            subscription,
            None,
            IndexingAnalytics::disabled(),
        )
    }

    #[tokio::test]
    async fn global_entity_handler_processes_request() {
        let handler = build_handler("User", EtlScope::Global);
        assert_eq!(handler.name(), "entity.user");

        let envelope = TestEnvelopeFactory::simple(
            &serde_json::json!({ "watermark": "2024-01-21T00:00:00Z" }).to_string(),
        );

        let result = handler.handle(handler_context(), envelope).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn namespaced_entity_handler_processes_request() {
        let handler = build_handler("MergeRequest", EtlScope::Namespaced);
        assert_eq!(handler.name(), "entity.mergerequest");

        let envelope = TestEnvelopeFactory::simple(
            &serde_json::json!({
                "namespace": 100,
                "traversal_path": "42/100/",
                "watermark": "2024-01-21T00:00:00Z"
            })
            .to_string(),
        );

        let result = handler.handle(handler_context(), envelope).await;
        assert!(result.is_ok());
    }

    #[test]
    fn indexing_requested_matches_empty_or_matching_target() {
        assert!(indexing_request_with_targets([]).indexing_requested("MergeRequest"));
        assert!(indexing_request_with_targets(["MergeRequest"]).indexing_requested("MergeRequest"));
        assert!(!indexing_request_with_targets(["Job"]).indexing_requested("MergeRequest"));
    }

    fn indexing_request_with_targets<const N: usize>(targets: [&str; N]) -> IndexingRequest {
        IndexingRequest {
            watermark: ts("2024-01-21T00:00:00Z"),
            scope_key: "global".to_string(),
            traversal_path: None,
            namespace_id: None,
            dispatch_id: Uuid::nil(),
            campaign_id: None,
            targets: targets.iter().map(|target| target.to_string()).collect(),
        }
    }

    fn ts(s: &str) -> DateTime<Utc> {
        s.parse().unwrap()
    }

    fn completed_partition(key: &str, watermark: &str) -> (String, Checkpoint) {
        let mut checkpoint = Checkpoint::new(ts(watermark));
        checkpoint.complete(ts(watermark));
        (key.to_string(), checkpoint)
    }

    fn cursored_partition(key: &str, watermark: &str) -> (String, Checkpoint) {
        let mut checkpoint = Checkpoint::new(ts(watermark));
        checkpoint.record_page(ts(watermark), Some(ts(watermark)), vec!["42".to_string()]);
        (key.to_string(), checkpoint)
    }

    fn started_partition(key: &str, watermark: &str) -> (String, Checkpoint) {
        let mut checkpoint = Checkpoint::new(ts(watermark));
        checkpoint.start_attempt();
        (key.to_string(), checkpoint)
    }

    #[test]
    fn consolidated_watermark_all_complete_returns_min() {
        let partitions = vec![
            completed_partition("ns.7.Job.p1of3", "2026-06-07T22:00:00Z"),
            completed_partition("ns.7.Job.p2of3", "2026-06-07T21:30:00Z"),
            completed_partition("ns.7.Job.p3of3", "2026-06-07T22:15:00Z"),
        ];
        assert_eq!(
            consolidated_watermark(&partitions, ts("2026-06-07T23:00:00Z")),
            Ok(ts("2026-06-07T21:30:00Z"))
        );
    }

    #[test]
    fn consolidated_watermark_any_cursored_returns_incomplete_keys() {
        let partitions = vec![
            completed_partition("ns.7.Job.p1of3", "2026-06-07T22:00:00Z"),
            cursored_partition("ns.7.Job.p2of3", "2026-06-07T21:30:00Z"),
            cursored_partition("ns.7.Job.p3of3", "2026-06-07T22:15:00Z"),
        ];
        assert_eq!(
            consolidated_watermark(&partitions, ts("2026-06-07T23:00:00Z")),
            Err(vec![
                "ns.7.Job.p2of3".to_string(),
                "ns.7.Job.p3of3".to_string()
            ])
        );
    }

    #[test]
    fn consolidated_watermark_treats_a_started_partition_as_incomplete() {
        let partitions = vec![
            completed_partition("ns.7.Job.p1of2", "2026-06-07T22:00:00Z"),
            started_partition("ns.7.Job.p2of2", "2026-06-07T21:30:00Z"),
        ];
        assert_eq!(
            consolidated_watermark(&partitions, ts("2026-06-07T23:00:00Z")),
            Err(vec!["ns.7.Job.p2of2".to_string()])
        );
    }

    #[test]
    fn consolidated_watermark_empty_returns_fallback() {
        let fallback = ts("2026-06-07T23:00:00Z");
        assert_eq!(consolidated_watermark(&[], fallback), Ok(fallback));
    }

    #[tokio::test]
    async fn subscriptions_match_scope() {
        let global = build_handler("User", EtlScope::Global);
        assert_eq!(global.subscription(), GlobalIndexingRequest::subscription());

        let namespaced = build_handler("MergeRequest", EtlScope::Namespaced);
        assert_eq!(
            namespaced.subscription(),
            NamespaceIndexingRequest::subscription()
        );
    }
}
