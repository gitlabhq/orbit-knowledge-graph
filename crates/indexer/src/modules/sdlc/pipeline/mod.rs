//! SDLC indexing pipeline: extract a whole page, transform it, and bulk-write
//! each destination table, overlapping the next page's read with the writes.
//!
//! [`Pipeline::run_plan`] reads top to bottom as the trace; the payload types
//! in [`page`] enforce the stage ordering at compile time.
//!
//! Two invariants keep throughput identical to the pre-refactor monolith: the
//! read-ahead overlap (`tokio::join!` of drain ‖ extract) lives only in the
//! runner, and pages move by value between stages (Arrow batches are
//! `Arc`-backed, so a move copies pointers, never buffers).

mod page;
mod stage;
mod stages;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use tracing::{debug, info};

use crate::checkpoint::{Checkpoint, CheckpointStore};
use crate::destination::Destination;
use crate::handler::HandlerError;
use crate::nats::ProgressNotifier;
use crate::observer::IndexingObserver;
use gkg_server_config::DatalakeRetryConfig;

use super::datalake::DatalakeQuery;
use super::metrics::SdlcMetrics;
use super::plan::{Cursor, CursorFilter, Plan, PreparedQuery};
use super::transform::TransformRegistry;

use page::{ExtractedPage, RunState, StagedWrites};
use stage::PageStage;
use stages::{Extractor, Transform, write};

pub(in crate::modules::sdlc) use page::{PipelineStats, WindowBounds};

pub(in crate::modules::sdlc) struct PipelineContext {
    pub destination: Arc<dyn Destination>,
    pub progress: ProgressNotifier,
    pub observer: Arc<Mutex<dyn IndexingObserver>>,
}

pub(in crate::modules::sdlc) struct Pipeline {
    extractor: Extractor,
    checkpoint_store: Arc<dyn CheckpointStore>,
    metrics: SdlcMetrics,
    registry: Arc<TransformRegistry>,
}

impl Pipeline {
    pub fn new(
        datalake: Arc<dyn DatalakeQuery>,
        checkpoint_store: Arc<dyn CheckpointStore>,
        metrics: SdlcMetrics,
        retry_config: DatalakeRetryConfig,
    ) -> Self {
        Self {
            extractor: Extractor::new(datalake, retry_config, metrics.clone()),
            checkpoint_store,
            metrics,
            registry: Arc::new(TransformRegistry::default()),
        }
    }

    /// Override the default (empty) registry with one carrying custom
    /// transforms. `data_fusion` plans build inline and need no registry entry.
    pub fn with_registry(mut self, registry: Arc<TransformRegistry>) -> Self {
        self.registry = registry;
        self
    }

    pub async fn run_plan(
        &self,
        context: &PipelineContext,
        plan: &Plan,
        base_query: PreparedQuery,
        position_key: &str,
        window: WindowBounds,
    ) -> Result<PipelineStats, HandlerError> {
        let started_at = Instant::now();
        let transform = Transform::new(self.registry.build(plan)?, self.metrics.clone());

        let mut state = self.begin(position_key, window).await;
        let mut page = self
            .extract_page(&transform, &base_query, plan, &mut state)
            .await?;

        while !page.is_empty() {
            let rows = page.rows();
            let bytes = page.bytes();
            let scanned_rows = page.scan_stats.scanned_rows;
            let extract_ms = page.extract_elapsed.as_millis() as u64;
            state.page_number += 1;
            state.stats.read_rows += rows;
            state.stats.read_bytes += bytes;
            state.stats.scanned_rows += scanned_rows;
            state.stats.scanned_bytes += page.scan_stats.scanned_bytes;
            state.cursor = state.cursor.advance(page.last_block(), &plan.sort_key)?;
            let has_more = rows >= plan.batch_size;

            let transformed = transform.run(page).await?;
            let transform_ms = transformed.transform_elapsed.as_millis() as u64;
            state.stats.transform_ms += transform_ms;

            let staged = write::stage(
                context.destination.as_ref(),
                transform.outputs(),
                transformed,
            )
            .await?;
            self.observe_page(context, &transform, rows, bytes, &staged, &mut state);

            let (write_elapsed, next_page) = self
                .commit_and_prefetch(staged, has_more, &transform, &base_query, plan, &mut state)
                .await?;
            let write_ms = write_elapsed.as_millis() as u64;
            state.stats.write_ms += write_ms;

            info!(
                page = state.page_number,
                rows, scanned_rows, extract_ms, transform_ms, write_ms, "page indexed"
            );

            self.checkpoint(position_key, &state, &context.progress)
                .await?;

            let Some(next) = next_page else {
                break;
            };
            page = next;
        }

        self.finish(position_key, &state.window, plan).await?;
        Ok(self.report(context, plan, state, started_at))
    }

    /// Extract cost is charged where a page is produced (here and in the
    /// prefetch), so the final empty probe still counts its datalake time.
    async fn extract_page(
        &self,
        transform: &Transform,
        base_query: &PreparedQuery,
        plan: &Plan,
        state: &mut RunState,
    ) -> Result<ExtractedPage, HandlerError> {
        let sql = self.page_sql(base_query, &plan.sort_key, &state.cursor);
        let page = self
            .extractor
            .extract(transform.block_name(), &sql, base_query.params())
            .await?;
        state.stats.extract_ms += page.extract_elapsed.as_millis() as u64;
        Ok(page)
    }

    /// Drains this page's writes while the next page extracts, so the next
    /// query-open latency hides behind the writes; peak memory is roughly two
    /// pages. The read-ahead overlap lives only here — it is the
    /// throughput-critical seam.
    async fn commit_and_prefetch(
        &self,
        staged: StagedWrites,
        has_more: bool,
        transform: &Transform,
        base_query: &PreparedQuery,
        plan: &Plan,
        state: &mut RunState,
    ) -> Result<(Duration, Option<ExtractedPage>), HandlerError> {
        if !has_more {
            return Ok((write::drain(staged).await?, None));
        }
        let next_sql = self.page_sql(base_query, &plan.sort_key, &state.cursor);
        let (write_elapsed, next) = tokio::join!(
            write::drain(staged),
            self.extractor
                .extract(transform.block_name(), &next_sql, base_query.params()),
        );
        let write_elapsed = write_elapsed?;
        let next = next?;
        state.stats.extract_ms += next.extract_elapsed.as_millis() as u64;
        Ok((write_elapsed, Some(next)))
    }

    /// One observer lock per page.
    fn observe_page(
        &self,
        context: &PipelineContext,
        transform: &Transform,
        extracted_rows: u64,
        extracted_bytes: u64,
        staged: &StagedWrites,
        state: &mut RunState,
    ) {
        let mut observer = context.observer.lock().unwrap();
        observer.extracted(extracted_rows, extracted_bytes);
        for (index, rows, bytes) in &staged.per_table {
            observer.record_graph_write(&transform.outputs()[*index], *rows, *bytes);
            state.stats.written_rows += rows;
            state.stats.written_bytes += bytes;
        }
    }

    fn report(
        &self,
        context: &PipelineContext,
        plan: &Plan,
        mut state: RunState,
        started_at: Instant,
    ) -> PipelineStats {
        let elapsed = started_at.elapsed();
        state.stats.duration_ms = elapsed.as_millis() as u64;
        self.metrics
            .record_pipeline_completion(&plan.name, elapsed.as_secs_f64());
        self.metrics.record_watermark_lag(&state.window.target);

        {
            let mut observer = context.observer.lock().unwrap();
            observer.record_datalake_read(state.stats.read_rows, state.stats.read_bytes);
            observer.record_datalake_scan(state.stats.scanned_rows, state.stats.scanned_bytes);
            observer.record_duration(state.stats.duration_ms);
        }

        if state.stats.written_rows > 0 || state.stats.read_rows > 0 {
            info!(
                read_rows = state.stats.read_rows,
                scanned_rows = state.stats.scanned_rows,
                written_rows = state.stats.written_rows,
                duration_ms = state.stats.duration_ms,
                extract_ms = state.stats.extract_ms,
                transform_ms = state.stats.transform_ms,
                write_ms = state.stats.write_ms,
                "pipeline completed"
            );
        } else {
            debug!(
                duration_ms = state.stats.duration_ms,
                "pipeline completed with no data"
            );
        }

        state.stats
    }

    async fn begin(&self, position_key: &str, window: WindowBounds) -> RunState {
        let checkpoint = self.load_checkpoint(position_key).await;
        let cursor = Cursor::from_checkpoint(&checkpoint);
        if !cursor.is_first_page() {
            info!("resuming from saved cursor");
        }
        RunState::new(cursor, window)
    }

    /// Saved only after the page's writes are durably committed, so a crash
    /// resumes from the next page rather than rescanning the window;
    /// re-running a page is idempotent (graph tables are `ReplacingMergeTree`).
    async fn checkpoint(
        &self,
        position_key: &str,
        state: &RunState,
        progress: &ProgressNotifier,
    ) -> Result<(), HandlerError> {
        self.checkpoint_store
            .save_progress(
                position_key,
                &Checkpoint {
                    watermark: state.window.target,
                    cursor_values: state.cursor.to_checkpoint_values(),
                    resume_floor: state.window.floor,
                },
            )
            .await
            .map_err(|err| {
                HandlerError::Processing(format!("failed to save cursor for {position_key}: {err}"))
            })?;
        progress.notify_in_progress().await;
        Ok(())
    }

    /// `save_completed` clears the saved cursor as well as advancing the watermark.
    async fn finish(
        &self,
        position_key: &str,
        window: &WindowBounds,
        plan: &Plan,
    ) -> Result<(), HandlerError> {
        self.checkpoint_store
            .save_completed(position_key, &window.target)
            .await
            .map_err(|err| {
                HandlerError::Processing(format!(
                    "failed to mark {} as completed: {err}",
                    plan.name
                ))
            })
    }

    fn page_sql(&self, base_query: &PreparedQuery, sort_key: &[String], cursor: &Cursor) -> String {
        base_query
            .clone()
            .with(CursorFilter {
                sort_key,
                values: cursor.values(),
            })
            .to_sql()
    }

    async fn load_checkpoint(&self, position_key: &str) -> Checkpoint {
        match self.checkpoint_store.load(position_key).await {
            Ok(Some(checkpoint)) => checkpoint,
            Ok(None) => Checkpoint {
                watermark: DateTime::<Utc>::UNIX_EPOCH,
                cursor_values: None,
                resume_floor: None,
            },
            Err(err) => {
                tracing::warn!(
                    position_key,
                    %err,
                    "failed to load checkpoint, starting from epoch"
                );
                Checkpoint {
                    watermark: DateTime::<Utc>::UNIX_EPOCH,
                    cursor_values: None,
                    resume_floor: None,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::plan::{TransformSpec, Transformation};
    use super::*;
    use crate::checkpoint::CheckpointError;
    use crate::modules::sdlc::datalake::{DatalakeError, RecordBatchStream, ScanStats};
    use crate::modules::sdlc::test_helpers::test_metrics;
    use crate::observer::NoOpObserver;
    use crate::testkit::MockDestination;
    use arrow::array::{BooleanArray, Int64Array, StringArray};
    use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema};
    use arrow::record_batch::RecordBatch;
    use async_trait::async_trait;
    use serde_json::Value;
    use std::collections::HashSet;

    fn simple_plan(name: &str) -> Plan {
        simple_plan_with_batch_size(name, 1000)
    }

    fn simple_plan_with_batch_size(name: &str, batch_size: u64) -> Plan {
        Plan {
            name: name.to_string(),
            extract_template: "SELECT id, name, _siphon_replicated_at AS _version, \
                 _siphon_deleted AS _deleted \
                 FROM source_table \
                 WHERE 1=1 {{filters}} \
                 ORDER BY id LIMIT {{batch_size}}"
                .to_string(),
            watermark_column: "_siphon_replicated_at".to_string(),
            sort_key: vec!["id".to_string()],
            batch_size,
            transform: TransformSpec::DataFusion(vec![Transformation {
                sql: format!(
                    "SELECT id, name FROM {}",
                    crate::modules::sdlc::plan::SOURCE_DATA_TABLE
                ),
                destination_table: format!("gl_{}", name.to_lowercase()),
                dict_encode_columns: HashSet::new(),
            }]),
        }
    }

    fn test_watermark() -> DateTime<Utc> {
        "2024-06-15T12:00:00Z".parse().unwrap()
    }

    fn test_window() -> WindowBounds {
        WindowBounds {
            target: test_watermark(),
            floor: None,
        }
    }

    fn base_query(plan: &Plan) -> PreparedQuery {
        plan.prepare()
            .with(crate::modules::sdlc::plan::WatermarkFilter {
                column: &plan.watermark_column,
                last: DateTime::<Utc>::UNIX_EPOCH,
                current: test_watermark(),
            })
    }

    fn position_key(plan: &Plan) -> String {
        format!("test.{}", plan.name)
    }

    fn test_batch(rows: usize) -> RecordBatch {
        test_batch_range(1, rows)
    }

    /// A batch of `count` rows with `id` running `start_id..start_id+count`.
    fn test_batch_range(start_id: i64, count: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            ArrowField::new("id", ArrowDataType::Int64, false),
            ArrowField::new("name", ArrowDataType::Utf8, true),
            ArrowField::new("_version", ArrowDataType::Int64, false),
            ArrowField::new("_deleted", ArrowDataType::Boolean, false),
        ]));

        let ids: Vec<i64> = (start_id..start_id + count as i64).collect();
        let names: Vec<Option<&str>> = (0..count).map(|_| Some("test")).collect();
        let versions: Vec<i64> = ids.clone();
        let deleted: Vec<bool> = vec![false; count];

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names)),
                Arc::new(Int64Array::from(versions)),
                Arc::new(BooleanArray::from(deleted)),
            ],
        )
        .unwrap()
    }

    use crate::modules::sdlc::test_helpers::EmptyDatalake;
    use crate::modules::sdlc::test_helpers::FailingDatalake;
    use crate::nats::ProgressNotifier;

    fn noop_context() -> PipelineContext {
        PipelineContext {
            destination: Arc::new(MockDestination::new()),
            progress: ProgressNotifier::noop(),
            observer: Arc::new(Mutex::new(NoOpObserver)),
        }
    }

    struct MultiBatchDatalake {
        call_count: Mutex<u32>,
        batch_size: usize,
    }

    #[async_trait]
    impl DatalakeQuery for MultiBatchDatalake {
        async fn query_arrow(
            &self,
            _sql: &str,
            _params: Value,
            _max_block_size: Option<u64>,
        ) -> Result<RecordBatchStream<'_>, DatalakeError> {
            Ok(Box::pin(futures::stream::empty()))
        }

        async fn query_batches(
            &self,
            _sql: &str,
            _params: Value,
            _max_block_size: Option<u64>,
        ) -> Result<Vec<RecordBatch>, DatalakeError> {
            let mut count = self.call_count.lock().unwrap();
            *count += 1;

            let rows = if *count == 1 {
                self.batch_size
            } else {
                self.batch_size / 2
            };

            // Two blocks per page so the cursor must come from the last block's
            // last row, not from row order.
            let first = rows / 2;
            let second = rows - first;
            Ok(vec![
                test_batch_range(1, first),
                test_batch_range(first as i64 + 1, second),
            ])
        }
    }

    struct RecordingCheckpointStore {
        state: Mutex<Option<Checkpoint>>,
        progress_history: Mutex<Vec<Checkpoint>>,
    }

    impl RecordingCheckpointStore {
        fn new() -> Self {
            Self {
                state: Mutex::new(None),
                progress_history: Mutex::new(Vec::new()),
            }
        }

        fn current_state(&self) -> Option<Checkpoint> {
            self.state.lock().unwrap().clone()
        }

        fn progress_history(&self) -> Vec<Checkpoint> {
            self.progress_history.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl CheckpointStore for RecordingCheckpointStore {
        async fn load(&self, _key: &str) -> Result<Option<Checkpoint>, CheckpointError> {
            Ok(self.state.lock().unwrap().clone())
        }

        async fn save_progress(
            &self,
            _key: &str,
            checkpoint: &Checkpoint,
        ) -> Result<(), CheckpointError> {
            self.progress_history
                .lock()
                .unwrap()
                .push(checkpoint.clone());
            *self.state.lock().unwrap() = Some(checkpoint.clone());
            Ok(())
        }

        async fn save_completed(
            &self,
            _key: &str,
            watermark: &DateTime<Utc>,
        ) -> Result<(), CheckpointError> {
            *self.state.lock().unwrap() = Some(Checkpoint {
                watermark: *watermark,
                cursor_values: None,
                resume_floor: None,
            });
            Ok(())
        }

        async fn load_by_prefix(
            &self,
            _prefix: &str,
        ) -> Result<Vec<(String, Checkpoint)>, CheckpointError> {
            Ok(Vec::new())
        }

        async fn consolidate(
            &self,
            _parent_key: &str,
            _watermark: &DateTime<Utc>,
        ) -> Result<(), CheckpointError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn empty_datalake_completes_without_error() {
        let pipeline = Pipeline::new(
            Arc::new(EmptyDatalake),
            Arc::new(RecordingCheckpointStore::new()),
            test_metrics(),
            Default::default(),
        );
        let plan = simple_plan("Test");

        let result = pipeline
            .run_plan(
                &noop_context(),
                &plan,
                base_query(&plan),
                &position_key(&plan),
                test_window(),
            )
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn multi_batch_paginates_and_completes() {
        let store = Arc::new(RecordingCheckpointStore::new());
        let plan = simple_plan_with_batch_size("Test", 10);

        let pipeline = Pipeline::new(
            Arc::new(MultiBatchDatalake {
                call_count: Mutex::new(0),
                batch_size: 10,
            }),
            store.clone(),
            test_metrics(),
            Default::default(),
        );
        let result = pipeline
            .run_plan(
                &noop_context(),
                &plan,
                base_query(&plan),
                &position_key(&plan),
                test_window(),
            )
            .await;

        assert!(result.is_ok());

        let history = store.progress_history();
        assert_eq!(history.len(), 2, "should checkpoint after each batch");
        assert_eq!(
            history[0].cursor_values.as_deref(),
            Some(vec!["10".to_string()].as_slice()),
            "first checkpoint should record cursor from batch 1 (last id=10)"
        );
        assert_eq!(
            history[1].cursor_values.as_deref(),
            Some(vec!["5".to_string()].as_slice()),
            "second checkpoint should record cursor from batch 2 (last id=5)"
        );

        let final_state = store.current_state().unwrap();
        assert!(final_state.cursor_values.is_none(), "should be completed");
    }

    #[tokio::test]
    async fn datalake_failure_surfaces_as_error() {
        let pipeline = Pipeline::new(
            Arc::new(FailingDatalake),
            Arc::new(RecordingCheckpointStore::new()),
            test_metrics(),
            Default::default(),
        );
        let plan = simple_plan("Failing");

        let result = pipeline
            .run_plan(
                &noop_context(),
                &plan,
                base_query(&plan),
                &position_key(&plan),
                test_window(),
            )
            .await;

        assert!(result.is_err());
    }

    /// Records every `max_block_size` it sees and replays a configurable
    /// success/failure pattern. Lets us assert that the per-plan override
    /// flows through and that the adaptive halving on retry halves on each
    /// attempt.
    struct RecordingDatalake {
        calls: Mutex<Vec<Option<u64>>>,
        responses: Mutex<Vec<Result<Vec<RecordBatch>, &'static str>>>,
    }

    impl RecordingDatalake {
        fn with_responses(responses: Vec<Result<Vec<RecordBatch>, &'static str>>) -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                responses: Mutex::new(responses),
            }
        }

        fn observed_block_sizes(&self) -> Vec<Option<u64>> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl DatalakeQuery for RecordingDatalake {
        async fn query_arrow(
            &self,
            _sql: &str,
            _params: Value,
            _max_block_size: Option<u64>,
        ) -> Result<RecordBatchStream<'_>, DatalakeError> {
            Ok(Box::pin(futures::stream::empty()))
        }

        async fn query_batches(
            &self,
            _sql: &str,
            _params: Value,
            max_block_size: Option<u64>,
        ) -> Result<Vec<RecordBatch>, DatalakeError> {
            self.calls.lock().unwrap().push(max_block_size);
            let mut responses = self.responses.lock().unwrap();
            if responses.is_empty() {
                return Ok(vec![]);
            }
            match responses.remove(0) {
                Ok(batches) => Ok(batches),
                Err(msg) => Err(DatalakeError::Query(msg.to_string())),
            }
        }
    }

    #[tokio::test]
    async fn extract_first_attempt_uses_datalake_default() {
        let datalake = Arc::new(RecordingDatalake::with_responses(vec![Ok(vec![
            test_batch(1),
        ])]));
        let pipeline = Pipeline::new(
            datalake.clone(),
            Arc::new(RecordingCheckpointStore::new()),
            test_metrics(),
            Default::default(),
        );

        let plan = simple_plan("Test");
        let _ = pipeline
            .run_plan(
                &noop_context(),
                &plan,
                base_query(&plan),
                &position_key(&plan),
                test_window(),
            )
            .await;

        let observed = datalake.observed_block_sizes();
        assert_eq!(
            observed.first().copied(),
            Some(None),
            "first attempt should pass None so the datalake default applies; observed: {observed:?}"
        );
    }

    #[tokio::test]
    async fn extract_halves_max_block_size_on_each_retry() {
        // Three failures then a success on the fourth attempt. The first
        // attempt uses the datalake default (None). Subsequent attempts seed
        // at retry_config.halving_initial_block_size (8000 here) and halve
        // from there: 8000 → 4000 → 2000.
        let datalake = Arc::new(RecordingDatalake::with_responses(vec![
            Err("simulated arrow capacity overflow"),
            Err("simulated arrow capacity overflow"),
            Err("simulated arrow capacity overflow"),
            Ok(vec![test_batch(1)]),
        ]));
        let pipeline = Pipeline::new(
            datalake.clone(),
            Arc::new(RecordingCheckpointStore::new()),
            test_metrics(),
            DatalakeRetryConfig {
                halving_initial_block_size: 8_000,
                halving_min_block_size: 1024,
            },
        );

        let plan = simple_plan("Test");
        let result = pipeline
            .run_plan(
                &noop_context(),
                &plan,
                base_query(&plan),
                &position_key(&plan),
                test_window(),
            )
            .await;
        assert!(result.is_ok(), "should recover after halving: {result:?}");

        let observed = datalake.observed_block_sizes();
        assert_eq!(
            observed,
            vec![None, Some(8_000), Some(4_000), Some(2_000)],
            "block size should halve on each retry; observed: {observed:?}"
        );
    }

    #[tokio::test]
    async fn extract_halving_respects_min_block_size_floor() {
        // Halving stops at halving_min_block_size and stays there for
        // subsequent retries.
        let datalake = Arc::new(RecordingDatalake::with_responses(vec![
            Err("simulated arrow capacity overflow"),
            Err("simulated arrow capacity overflow"),
            Err("simulated arrow capacity overflow"),
            Ok(vec![test_batch(1)]),
        ]));
        let pipeline = Pipeline::new(
            datalake.clone(),
            Arc::new(RecordingCheckpointStore::new()),
            test_metrics(),
            DatalakeRetryConfig {
                halving_initial_block_size: 4_096,
                halving_min_block_size: 2_048,
            },
        );

        let plan = simple_plan("Test");
        let _ = pipeline
            .run_plan(
                &noop_context(),
                &plan,
                base_query(&plan),
                &position_key(&plan),
                test_window(),
            )
            .await;

        let observed = datalake.observed_block_sizes();
        assert_eq!(
            observed,
            vec![None, Some(4_096), Some(2_048), Some(2_048)],
            "halving should clamp at the configured floor; observed: {observed:?}"
        );
    }

    struct OverflowingDatalake {
        calls: Mutex<Vec<Option<u64>>>,
        recover_at_block_size: u64,
    }

    impl OverflowingDatalake {
        fn observed_block_sizes(&self) -> Vec<Option<u64>> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl DatalakeQuery for OverflowingDatalake {
        async fn query_arrow(
            &self,
            _sql: &str,
            _params: Value,
            _max_block_size: Option<u64>,
        ) -> Result<RecordBatchStream<'_>, DatalakeError> {
            Ok(Box::pin(futures::stream::empty()))
        }

        async fn query_batches(
            &self,
            _sql: &str,
            _params: Value,
            max_block_size: Option<u64>,
        ) -> Result<Vec<RecordBatch>, DatalakeError> {
            self.calls.lock().unwrap().push(max_block_size);

            let small_enough =
                matches!(max_block_size, Some(size) if size <= self.recover_at_block_size);
            if small_enough {
                Ok(vec![test_batch_range(1, 5)])
            } else {
                Err(DatalakeError::Query(
                    "Code: 1002. DB::Exception: Error with a Arrow column \"String\": \
                     Capacity error: array cannot contain more than 2147483646 bytes"
                        .to_string(),
                ))
            }
        }
    }

    #[tokio::test]
    async fn arrow_overflow_recovers_via_block_size_halving() {
        let datalake = Arc::new(OverflowingDatalake {
            calls: Mutex::new(Vec::new()),
            recover_at_block_size: 8_000,
        });
        let pipeline = Pipeline::new(
            datalake.clone(),
            Arc::new(RecordingCheckpointStore::new()),
            test_metrics(),
            DatalakeRetryConfig {
                halving_initial_block_size: 8_000,
                halving_min_block_size: 1_024,
            },
        );

        let plan = simple_plan("Test");
        let result = pipeline
            .run_plan(
                &noop_context(),
                &plan,
                base_query(&plan),
                &position_key(&plan),
                test_window(),
            )
            .await;

        assert!(
            result.is_ok(),
            "arrow overflow should self-correct: {result:?}"
        );
        assert_eq!(
            datalake.observed_block_sizes(),
            vec![None, Some(8_000)],
            "first attempt reads at the default and overflows; the retry must \
             shrink the block rather than abort"
        );
    }

    #[tokio::test]
    async fn resumes_from_stored_cursor() {
        let store = Arc::new(RecordingCheckpointStore {
            state: Mutex::new(Some(Checkpoint {
                watermark: "2024-06-15T12:00:00Z".parse().unwrap(),
                cursor_values: Some(vec!["5".to_string()]),
                resume_floor: None,
            })),
            progress_history: Mutex::new(Vec::new()),
        });

        let pipeline = Pipeline::new(
            Arc::new(EmptyDatalake),
            store,
            test_metrics(),
            Default::default(),
        );
        let plan = simple_plan("Test");

        let result = pipeline
            .run_plan(
                &noop_context(),
                &plan,
                base_query(&plan),
                &position_key(&plan),
                test_window(),
            )
            .await;

        assert!(result.is_ok());
    }

    /// Returns one page of `rows` plus a fixed `ScanStats` from its summary,
    /// then nothing.
    struct StatsDatalake {
        rows: usize,
        scan_stats: ScanStats,
        calls: Mutex<u32>,
    }

    #[async_trait]
    impl DatalakeQuery for StatsDatalake {
        async fn query_arrow(
            &self,
            _sql: &str,
            _params: Value,
            _max_block_size: Option<u64>,
        ) -> Result<RecordBatchStream<'_>, DatalakeError> {
            Ok(Box::pin(futures::stream::empty()))
        }

        async fn query_batches(
            &self,
            _sql: &str,
            _params: Value,
            _max_block_size: Option<u64>,
        ) -> Result<Vec<RecordBatch>, DatalakeError> {
            Ok(vec![])
        }

        async fn query_batches_with_summary(
            &self,
            _sql: &str,
            _params: Value,
            _max_block_size: Option<u64>,
        ) -> Result<(Vec<RecordBatch>, ScanStats), DatalakeError> {
            let mut calls = self.calls.lock().unwrap();
            *calls += 1;
            if *calls == 1 {
                Ok((vec![test_batch(self.rows)], self.scan_stats))
            } else {
                Ok((vec![], ScanStats::default()))
            }
        }
    }

    #[tokio::test]
    async fn run_plan_reports_resource_stats() {
        let datalake = Arc::new(StatsDatalake {
            rows: 5,
            // ClickHouse scanned far more rows than the page returns; read_rows
            // must track the 5 returned while scanned_rows carries the cost.
            scan_stats: ScanStats {
                scanned_rows: 1000,
                scanned_bytes: 64_000,
            },
            calls: Mutex::new(0),
        });
        let pipeline = Pipeline::new(
            datalake,
            Arc::new(RecordingCheckpointStore::new()),
            test_metrics(),
            Default::default(),
        );
        let plan = simple_plan("Test");

        let stats = pipeline
            .run_plan(
                &noop_context(),
                &plan,
                base_query(&plan),
                &position_key(&plan),
                test_window(),
            )
            .await
            .expect("run should succeed");

        assert_eq!(
            stats.read_rows, 5,
            "read rows count the rows actually returned"
        );
        assert!(
            stats.read_bytes > 0,
            "read bytes reflect the data actually returned"
        );
        assert_eq!(
            stats.scanned_rows, 1000,
            "scanned rows come from the summary"
        );
        assert_eq!(
            stats.scanned_bytes, 64_000,
            "scanned bytes come from the summary"
        );
        assert_eq!(
            stats.written_rows, 5,
            "transform emits the 5 extracted rows"
        );
        assert!(
            stats.written_bytes > 0,
            "written bytes reflect the batch size"
        );
    }
}
