use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use ontology::EtlScope;
use tokio::task::JoinSet;
use tracing::{Instrument, debug, info, info_span};
use uuid::Uuid;

use crate::analytics::IndexingAnalytics;
use crate::checkpoint::{Checkpoint, CheckpointStore, namespace_position_key};
use crate::handler::{Handler, HandlerContext, HandlerError};
use crate::modules::sdlc::datalake::DatalakeQuery;
use crate::modules::sdlc::metrics::SdlcMetrics;
use crate::modules::sdlc::observer::SdlcOtelObserver;
use crate::modules::sdlc::partitioning::{PartitionAssignment, PartitionStrategy};
use crate::modules::sdlc::pipeline::{
    Pipeline, PipelineContext, PipelineStats, RunDurability, WindowBounds,
};
use crate::modules::sdlc::plan::{
    Plan, PreparedQuery, TransformSpec, TraversalPathFilter, WatermarkFilter,
};
use crate::modules::sdlc::transform::system_notes;
use crate::observer::{self, IndexingMode, IndexingObserver, PipelineType};
use crate::topic::{GlobalIndexingRequest, NamespaceIndexingRequest};
use crate::types::{Envelope, SerializationError, Subscription};

const NAMESPACE_GATED_TRANSFORMS: &[(&str, gkg_server_config::Feature)] = &[(
    system_notes::TRANSFORM_NAME,
    gkg_server_config::Feature::SystemNotes,
)];

pub struct EntityHandler {
    handler_name: String,
    plan: Plan,
    scope: EtlScope,
    pipeline: Arc<Pipeline>,
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
    traversal_path: Option<String>,
    namespace_id: Option<i64>,
    dispatch_id: Uuid,
    campaign_id: Option<String>,
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
            datalake,
            checkpoint_store,
            metrics,
            subscription,
            partition_strategy,
            analytics,
        }
    }

    fn enabled_for_namespace(&self, namespace_id: Option<i64>) -> bool {
        let TransformSpec::Rust(name) = &self.plan.transform else {
            return true;
        };
        match NAMESPACE_GATED_TRANSFORMS
            .iter()
            .find(|(transform, _)| transform == name)
        {
            Some((_, feature)) => gkg_server_config::features::enabled_for(*feature, namespace_id),
            None => true,
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
        let parent_checkpoint = self
            .checkpoint_store
            .load(&checkpoint_key)
            .await
            .map_err(|err| HandlerError::Processing(err.to_string()))?;
        let window = pull_window(parent_checkpoint.as_ref(), request.watermark);

        let mode = indexing_mode(window.floor);
        observer.set_indexing_mode(mode);

        let observer: Arc<Mutex<dyn IndexingObserver>> = Arc::new(Mutex::new(observer));
        let pipeline_context = PipelineContext {
            destination: Arc::clone(&context.destination),
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
            })
            .with(
                request
                    .traversal_path
                    .as_deref()
                    .map(|path| TraversalPathFilter { path }),
            );

        let should_partition = self.partition_strategy.is_some() && parent_checkpoint.is_none();
        let ranges = if should_partition {
            self.partition_strategy
                .as_ref()
                .unwrap()
                .compute_ranges(self.datalake.as_ref(), request.traversal_path.as_deref())
                .await?
        } else {
            Vec::new()
        };

        let durability = RunDurability::for_mode(mode);

        let result = if ranges.is_empty() {
            self.pipeline
                .run_plan(
                    &pipeline_context,
                    &self.plan,
                    base_query,
                    &checkpoint_key,
                    window,
                    durability,
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
                    durability,
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
                        // Leaving the parent absent re-triggers partitioning next dispatch; Ok keeps this expected mid-load state out of pipeline-error metrics.
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

        result.map(|_| ())
    }

    async fn run_partitions(
        &self,
        partitions: Vec<(
            crate::modules::sdlc::partitioning::PartitionAssignment,
            PreparedQuery,
        )>,
        checkpoint_key: &str,
        window: WindowBounds,
        durability: RunDurability,
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
            if let Some(cp) = existing.as_ref()
                && cp.cursor_values.is_none()
            {
                info!(partition = %position_key, "skipping already-completed partition");
                continue;
            }

            let plan = self.plan.clone();
            let pipeline = Arc::clone(&self.pipeline);
            let partition_context = PipelineContext {
                destination: Arc::clone(&context.destination),
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
                        window,
                        durability,
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

/// The window floor is the single source of truth for the run's mode (feeding analytics and
/// write durability): a `None` floor starts from the beginning of time — a backfill — while any
/// `Some` floor is an incremental pull.
fn indexing_mode(floor: Option<DateTime<Utc>>) -> IndexingMode {
    match floor {
        None => IndexingMode::Full,
        Some(_) => IndexingMode::Incremental,
    }
}

/// The `(floor, target]` window for a pull. A cursored checkpoint resumes its original window;
/// a completed one starts after its watermark; a missing one starts from the beginning.
fn pull_window(
    parent_checkpoint: Option<&Checkpoint>,
    request_watermark: DateTime<Utc>,
) -> WindowBounds {
    match parent_checkpoint {
        Some(checkpoint) if checkpoint.cursor_values.is_some() => WindowBounds {
            target: checkpoint.watermark,
            floor: checkpoint.resume_floor,
        },
        Some(checkpoint) => WindowBounds {
            target: request_watermark,
            floor: Some(checkpoint.watermark),
        },
        None => WindowBounds {
            target: request_watermark,
            floor: None,
        },
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
        .filter(|(_, checkpoint)| checkpoint.cursor_values.is_some())
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

        if !self.enabled_for_namespace(request.namespace_id) {
            debug!(
                entity = %self.plan.name,
                namespace_id = ?request.namespace_id,
                "skipping request: feature not enabled for namespace"
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

    use crate::modules::sdlc::plan::build_plans;
    use crate::modules::sdlc::test_helpers::{EmptyDatalake, MockCheckpointStore, test_metrics};
    use crate::nats::ProgressNotifier;
    use crate::testkit::{MockDestination, MockLockService, MockNatsServices, TestEnvelopeFactory};
    use crate::types::Event;
    use ontology::Ontology;

    fn handler_context() -> HandlerContext {
        let destination = Arc::new(MockDestination::new());
        let mock_nats = Arc::new(MockNatsServices::new());
        HandlerContext::new(
            destination,
            mock_nats.clone(),
            Arc::new(MockLockService::new()),
            ProgressNotifier::noop(),
            Arc::new(crate::indexing_status::IndexingStatusStore::new(mock_nats)),
        )
    }

    fn build_handler(entity_name: &str, scope: EtlScope) -> EntityHandler {
        let ontology = Ontology::load_embedded().expect("should load ontology");
        let plans = build_plans(&ontology, 1000, 1000, &Default::default());
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
            Default::default(),
        ));
        let subscription = match scope {
            EtlScope::Global => GlobalIndexingRequest::subscription(),
            EtlScope::Namespaced => NamespaceIndexingRequest::subscription(),
        };

        EntityHandler::new(
            plan,
            scope,
            pipeline,
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

    fn ts(s: &str) -> DateTime<Utc> {
        s.parse().unwrap()
    }

    #[test]
    fn indexing_mode_no_floor_is_full() {
        assert_eq!(indexing_mode(None), IndexingMode::Full);
    }

    #[test]
    fn indexing_mode_some_floor_is_incremental() {
        assert_eq!(
            indexing_mode(Some(ts("2026-06-07T21:59:30Z"))),
            IndexingMode::Incremental
        );
    }

    #[test]
    fn pull_window_missing_checkpoint_starts_from_beginning() {
        let now = ts("2026-06-07T22:00:00Z");
        assert_eq!(
            pull_window(None, now),
            WindowBounds {
                target: now,
                floor: None
            }
        );
    }

    #[test]
    fn pull_window_completed_advances_to_now() {
        let now = ts("2026-06-07T22:00:00Z");
        let completed = Checkpoint {
            watermark: ts("2026-06-07T21:59:30Z"),
            cursor_values: None,
            resume_floor: None,
        };
        assert_eq!(
            pull_window(Some(&completed), now),
            WindowBounds {
                target: now,
                floor: Some(ts("2026-06-07T21:59:30Z")),
            }
        );
    }

    #[test]
    fn pull_window_resume_keeps_original_window() {
        let now = ts("2026-06-07T22:05:00Z");
        let in_progress = Checkpoint {
            watermark: ts("2026-06-07T22:00:00Z"),
            cursor_values: Some(vec!["1/65957873/".to_string(), "42".to_string()]),
            resume_floor: Some(ts("2026-06-07T21:59:30Z")),
        };
        assert_eq!(
            pull_window(Some(&in_progress), now),
            WindowBounds {
                target: ts("2026-06-07T22:00:00Z"),
                floor: Some(ts("2026-06-07T21:59:30Z")),
            }
        );
    }

    #[test]
    fn pull_window_resume_without_floor_starts_from_beginning() {
        let now = ts("2026-06-07T22:05:00Z");
        let legacy = Checkpoint {
            watermark: ts("2026-06-07T22:00:00Z"),
            cursor_values: Some(vec!["42".to_string()]),
            resume_floor: None,
        };
        assert_eq!(
            pull_window(Some(&legacy), now),
            WindowBounds {
                target: ts("2026-06-07T22:00:00Z"),
                floor: None,
            }
        );
    }

    fn completed_partition(key: &str, watermark: &str) -> (String, Checkpoint) {
        (
            key.to_string(),
            Checkpoint {
                watermark: ts(watermark),
                cursor_values: None,
                resume_floor: None,
            },
        )
    }

    fn cursored_partition(key: &str, watermark: &str) -> (String, Checkpoint) {
        (
            key.to_string(),
            Checkpoint {
                watermark: ts(watermark),
                cursor_values: Some(vec!["42".to_string()]),
                resume_floor: Some(ts(watermark)),
            },
        )
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
