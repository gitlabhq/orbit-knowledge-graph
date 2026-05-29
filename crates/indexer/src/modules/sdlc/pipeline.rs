use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use gkg_utils::arrow::prepare_batches;
use serde_json::Value;
use tracing::{debug, info, warn};

use crate::destination::Destination;
use crate::handler::HandlerError;
use crate::nats::ProgressNotifier;
use crate::observer::IndexingObserver;

use super::datalake::{DatalakeQuery, ReadStats};
use super::metrics::SdlcMetrics;
use super::plan::{Cursor, CursorFilter, Plan, PreparedQuery, SOURCE_DATA_TABLE, Transformation};
use crate::checkpoint::{Checkpoint, CheckpointStore};
use gkg_server_config::DatalakeRetryConfig;

type WriteFutures = FuturesUnordered<BoxFuture<'static, Result<(), HandlerError>>>;

const MAX_RETRIES: u32 = 3;

/// Per-run resource stats for a pipeline execution, collected for indexing
/// cost attribution. Read stats come from the datalake's `X-ClickHouse-Summary`;
/// write stats are the rows and in-memory size of the batches sent to the graph
/// (the `clickhouse` crate does not expose an insert summary).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(in crate::modules::sdlc) struct PipelineStats {
    pub read_rows: u64,
    pub read_bytes: u64,
    pub written_rows: u64,
    pub written_bytes: u64,
    pub duration_ms: u64,
}

impl PipelineStats {
    /// Folds another run's stats into this one. Used to total partitioned
    /// initial loads, where each partition runs its own plan.
    pub(in crate::modules::sdlc) fn merge(&mut self, other: PipelineStats) {
        self.read_rows += other.read_rows;
        self.read_bytes += other.read_bytes;
        self.written_rows += other.written_rows;
        self.written_bytes += other.written_bytes;
        self.duration_ms = self.duration_ms.max(other.duration_ms);
    }
}

/// Rows and bytes handed to the graph destination by a single `transform` call.
#[derive(Debug, Clone, Copy, Default)]
struct WriteCounts {
    rows: u64,
    bytes: u64,
}

pub(in crate::modules::sdlc) struct PipelineContext {
    pub destination: Arc<dyn Destination>,
    pub progress: ProgressNotifier,
    pub observer: Arc<std::sync::Mutex<dyn IndexingObserver>>,
}

pub(in crate::modules::sdlc) struct Pipeline {
    datalake: Arc<dyn DatalakeQuery>,
    checkpoint_store: Arc<dyn CheckpointStore>,
    metrics: SdlcMetrics,
    retry_config: DatalakeRetryConfig,
}

impl Pipeline {
    pub fn new(
        datalake: Arc<dyn DatalakeQuery>,
        checkpoint_store: Arc<dyn CheckpointStore>,
        metrics: SdlcMetrics,
        retry_config: DatalakeRetryConfig,
    ) -> Self {
        Self {
            datalake,
            checkpoint_store,
            metrics,
            retry_config,
        }
    }

    pub async fn run_plan(
        &self,
        context: &PipelineContext,
        plan: &Plan,
        base_query: PreparedQuery,
        position_key: &str,
        target_watermark: DateTime<Utc>,
    ) -> Result<PipelineStats, HandlerError> {
        let started_at = Instant::now();
        let checkpoint = self.load_checkpoint(position_key).await;
        let mut cursor = Cursor::from_checkpoint(&checkpoint);

        if !cursor.is_first_page() {
            info!("resuming from saved cursor");
        }

        let params = base_query.params();

        let mut total_rows: u64 = 0;
        let mut read_rows: u64 = 0;
        let mut read_bytes: u64 = 0;
        let mut written_rows: u64 = 0;
        let mut written_bytes: u64 = 0;
        let session = SessionContext::new();

        let extract_start = Instant::now();
        let (mut batches, read_stats) = self
            .extract_batch(
                &plan.name,
                &base_query
                    .clone()
                    .with(CursorFilter {
                        sort_key: &plan.sort_key,
                        values: cursor.values(),
                    })
                    .to_sql(),
                params.clone(),
            )
            .await?;
        read_rows += read_stats.read_rows;
        read_bytes += read_stats.read_bytes;
        let mut extract_elapsed = extract_start.elapsed();

        loop {
            if batches.is_empty() {
                break;
            }

            let rows_in_batch: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
            let bytes_in_batch: u64 = batches
                .iter()
                .map(|b| b.get_array_memory_size() as u64)
                .sum();
            total_rows += rows_in_batch;
            context
                .observer
                .lock()
                .unwrap()
                .extracted(rows_in_batch, bytes_in_batch);

            info!(
                rows = rows_in_batch,
                duration_ms = extract_elapsed.as_millis() as u64,
                "batch extracted"
            );

            cursor = cursor.advance(batches.last().unwrap(), &plan.sort_key)?;
            let has_more = rows_in_batch >= plan.batch_size;

            let (write_futures, write_counts) = self
                .transform(
                    &session,
                    &plan.name,
                    batches,
                    &plan.transforms,
                    context.destination.as_ref(),
                )
                .await?;
            written_rows += write_counts.rows;
            written_bytes += write_counts.bytes;

            if has_more {
                let next_sql = base_query
                    .clone()
                    .with(CursorFilter {
                        sort_key: &plan.sort_key,
                        values: cursor.values(),
                    })
                    .to_sql();
                let (write_result, extract_result) = tokio::join!(
                    self.drain_writes(write_futures),
                    self.timed_extract_batch(&plan.name, &next_sql, params.clone()),
                );
                write_result?;
                let (next_batches, next_read_stats, next_elapsed) = extract_result?;
                read_rows += next_read_stats.read_rows;
                read_bytes += next_read_stats.read_bytes;

                self.save_batch_progress(
                    position_key,
                    target_watermark,
                    &cursor,
                    &context.progress,
                )
                .await?;

                batches = next_batches;
                extract_elapsed = next_elapsed;
            } else {
                self.drain_writes(write_futures).await?;

                self.save_batch_progress(
                    position_key,
                    target_watermark,
                    &cursor,
                    &context.progress,
                )
                .await?;

                break;
            }
        }

        self.checkpoint_store
            .save_completed(position_key, &target_watermark)
            .await
            .map_err(|err| {
                HandlerError::Processing(format!(
                    "failed to mark {} as completed: {err}",
                    plan.name
                ))
            })?;

        let elapsed = started_at.elapsed();
        self.metrics
            .record_pipeline_completion(&plan.name, elapsed.as_secs_f64());
        self.metrics.record_watermark_lag(&target_watermark);

        let stats = PipelineStats {
            read_rows,
            read_bytes,
            written_rows,
            written_bytes,
            duration_ms: elapsed.as_millis() as u64,
        };

        if total_rows > 0 {
            info!(
                total_rows,
                duration_ms = stats.duration_ms,
                read_rows = stats.read_rows,
                read_bytes = stats.read_bytes,
                written_rows = stats.written_rows,
                written_bytes = stats.written_bytes,
                "pipeline completed"
            );
        } else {
            debug!(
                duration_ms = stats.duration_ms,
                "pipeline completed with no data"
            );
        }

        Ok(stats)
    }

    async fn extract_batch(
        &self,
        pipeline_name: &str,
        sql: &str,
        params: Value,
    ) -> Result<(Vec<RecordBatch>, ReadStats), HandlerError> {
        let mut last_error = None;
        // First attempt uses the datalake's default `max_block_size`.
        // Subsequent attempts seed and then halve a per-call override so a
        // ClickHouse-side Arrow String offset overflow self-corrects without
        // bouncing the message to the dead letter stream.
        let mut current_block_size: Option<u64> = None;

        for attempt in 0..=MAX_RETRIES {
            if attempt > 0 {
                let backoff = Duration::from_millis(100 * 2u64.pow(attempt - 1));
                tokio::time::sleep(backoff).await;
            }

            let query_start = Instant::now();
            match self
                .datalake
                .query_batches_with_summary(sql, params.clone(), current_block_size)
                .await
            {
                Ok((batches, read_stats)) => {
                    let bytes: u64 = batches
                        .iter()
                        .map(|b| b.get_array_memory_size() as u64)
                        .sum();
                    self.metrics.record_datalake_query(
                        pipeline_name,
                        query_start.elapsed().as_secs_f64(),
                        bytes,
                    );
                    return Ok((batches, read_stats));
                }
                Err(err) => {
                    warn!(
                        attempt,
                        max_retries = MAX_RETRIES,
                        max_block_size = ?current_block_size,
                        %err,
                        "datalake query failed, retrying"
                    );
                    last_error = Some(HandlerError::Processing(format!(
                        "datalake query failed: {err}"
                    )));
                    // Adaptive halving: shrink the next attempt's block size
                    // so an Arrow String offset overflow on a too-large block
                    // can self-correct without bouncing the message to the
                    // dead letter stream. Bounds come from configuration so
                    // operators can tune the seed and floor without a
                    // release.
                    current_block_size = Some(match current_block_size {
                        Some(size) => (size / 2).max(self.retry_config.halving_min_block_size),
                        None => self.retry_config.halving_initial_block_size,
                    });
                }
            }
        }

        Err(last_error.unwrap())
    }

    async fn timed_extract_batch(
        &self,
        pipeline_name: &str,
        sql: &str,
        params: Value,
    ) -> Result<(Vec<RecordBatch>, ReadStats, Duration), HandlerError> {
        let start = Instant::now();
        let (batches, read_stats) = self.extract_batch(pipeline_name, sql, params).await?;
        Ok((batches, read_stats, start.elapsed()))
    }

    async fn transform(
        &self,
        session: &SessionContext,
        pipeline_name: &str,
        batches: Vec<RecordBatch>,
        transforms: &[Transformation],
        destination: &dyn Destination,
    ) -> Result<(WriteFutures, WriteCounts), HandlerError> {
        let schema = batches[0].schema();

        let _ = session.deregister_table(SOURCE_DATA_TABLE);
        let mem_table = MemTable::try_new(schema, vec![batches]).map_err(|err| {
            HandlerError::Processing(format!(
                "failed to create mem table for {pipeline_name}: {err}"
            ))
        })?;

        session
            .register_table(SOURCE_DATA_TABLE, Arc::new(mem_table))
            .map_err(|err| {
                HandlerError::Processing(format!(
                    "failed to register table for {pipeline_name}: {err}"
                ))
            })?;

        let mut total_transform_duration = Duration::ZERO;
        let mut write_futures: WriteFutures = FuturesUnordered::new();
        let mut write_counts = WriteCounts::default();

        for transform in transforms {
            let transform_start = Instant::now();
            let mut result_batches = self
                .execute_transform(session, &transform.sql)
                .await
                .map_err(|err| {
                    HandlerError::Processing(format!(
                        "failed to transform {pipeline_name} for {}: {err}",
                        transform.destination_table
                    ))
                })?;
            let transform_elapsed = transform_start.elapsed();
            total_transform_duration += transform_elapsed;

            info!(
                table = %transform.destination_table,
                duration_ms = transform_elapsed.as_millis() as u64,
                "transform executed"
            );

            prepare_batches(&mut result_batches, &transform.dict_encode_columns);

            let row_count: usize = result_batches.iter().map(|b| b.num_rows()).sum();
            if row_count == 0 {
                continue;
            }

            write_counts.rows += row_count as u64;
            write_counts.bytes += result_batches
                .iter()
                .map(|b| b.get_array_memory_size() as u64)
                .sum::<u64>();

            let destination_table = transform.destination_table.clone();
            let writer = destination
                .new_batch_writer(&destination_table)
                .await
                .map_err(|err| {
                    HandlerError::Processing(format!(
                        "failed to create writer for {destination_table}: {err}"
                    ))
                })?;

            write_futures.push(
                async move {
                    let write_start = Instant::now();
                    writer.write_batch(&result_batches).await.map_err(|err| {
                        HandlerError::Processing(format!(
                            "failed to write to {destination_table}: {err}"
                        ))
                    })?;
                    let write_elapsed = write_start.elapsed();

                    info!(
                        table = %destination_table,
                        rows = row_count,
                        duration_ms = write_elapsed.as_millis() as u64,
                        "transform written"
                    );

                    Ok(())
                }
                .boxed(),
            );

            while let Some(Some(result)) = write_futures.next().now_or_never() {
                result?;
            }
        }

        self.metrics
            .record_transform_duration(total_transform_duration.as_secs_f64());

        let _ = session.deregister_table(SOURCE_DATA_TABLE);

        Ok((write_futures, write_counts))
    }

    async fn drain_writes(&self, mut futures: WriteFutures) -> Result<(), HandlerError> {
        while let Some(result) = futures.next().await {
            result?;
        }
        Ok(())
    }

    async fn save_batch_progress(
        &self,
        position_key: &str,
        target_watermark: DateTime<Utc>,
        cursor: &Cursor,
        progress: &ProgressNotifier,
    ) -> Result<(), HandlerError> {
        self.checkpoint_store
            .save_progress(
                position_key,
                &Checkpoint {
                    watermark: target_watermark,
                    cursor_values: cursor.to_checkpoint_values(),
                },
            )
            .await
            .map_err(|err| {
                HandlerError::Processing(format!("failed to save cursor for {position_key}: {err}"))
            })?;
        progress.notify_in_progress().await;
        Ok(())
    }

    async fn execute_transform(
        &self,
        session: &SessionContext,
        sql: &str,
    ) -> Result<Vec<RecordBatch>, datafusion::error::DataFusionError> {
        let dataframe = session.sql(sql).await?;
        dataframe.collect().await
    }

    async fn load_checkpoint(&self, position_key: &str) -> Checkpoint {
        match self.checkpoint_store.load(position_key).await {
            Ok(Some(checkpoint)) => checkpoint,
            Ok(None) => Checkpoint {
                watermark: DateTime::<Utc>::UNIX_EPOCH,
                cursor_values: None,
            },
            Err(err) => {
                warn!(
                    position_key,
                    %err,
                    "failed to load checkpoint, starting from epoch"
                );
                Checkpoint {
                    watermark: DateTime::<Utc>::UNIX_EPOCH,
                    cursor_values: None,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::CheckpointError;
    use crate::modules::sdlc::datalake::DatalakeError;
    use crate::modules::sdlc::test_helpers::test_metrics;
    use crate::observer::NoOpObserver;
    use crate::testkit::MockDestination;
    use arrow::array::{BooleanArray, Int64Array, StringArray};
    use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema};
    use async_trait::async_trait;
    use std::collections::HashSet;
    use std::sync::Mutex;

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
            transforms: vec![Transformation {
                sql: format!("SELECT id, name FROM {SOURCE_DATA_TABLE}"),
                destination_table: format!("gl_{}", name.to_lowercase()),
                dict_encode_columns: HashSet::new(),
            }],
        }
    }

    fn test_watermark() -> DateTime<Utc> {
        "2024-06-15T12:00:00Z".parse().unwrap()
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
        let schema = Arc::new(Schema::new(vec![
            ArrowField::new("id", ArrowDataType::Int64, false),
            ArrowField::new("name", ArrowDataType::Utf8, true),
            ArrowField::new("_version", ArrowDataType::Int64, false),
            ArrowField::new("_deleted", ArrowDataType::Boolean, false),
        ]));

        let ids: Vec<i64> = (1..=rows as i64).collect();
        let names: Vec<Option<&str>> = (0..rows).map(|_| Some("test")).collect();
        let versions: Vec<i64> = (1..=rows as i64).collect();
        let deleted: Vec<bool> = vec![false; rows];

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
        ) -> Result<crate::modules::sdlc::datalake::RecordBatchStream<'_>, DatalakeError> {
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

            Ok(vec![test_batch(rows)])
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
                test_watermark(),
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
                test_watermark(),
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
                test_watermark(),
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
        ) -> Result<crate::modules::sdlc::datalake::RecordBatchStream<'_>, DatalakeError> {
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
                test_watermark(),
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
                test_watermark(),
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
                test_watermark(),
            )
            .await;

        let observed = datalake.observed_block_sizes();
        assert_eq!(
            observed,
            vec![None, Some(4_096), Some(2_048), Some(2_048)],
            "halving should clamp at the configured floor; observed: {observed:?}"
        );
    }

    #[tokio::test]
    async fn resumes_from_stored_cursor() {
        let store = Arc::new(RecordingCheckpointStore {
            state: Mutex::new(Some(Checkpoint {
                watermark: "2024-06-15T12:00:00Z".parse().unwrap(),
                cursor_values: Some(vec!["5".to_string()]),
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
                test_watermark(),
            )
            .await;

        assert!(result.is_ok());
    }

    /// Returns one page of rows plus a fixed `ReadStats` on the first extract,
    /// then nothing — so a single-page run exercises the stats accumulation.
    struct StatsDatalake {
        rows: usize,
        read_stats: ReadStats,
        calls: Mutex<u32>,
    }

    #[async_trait]
    impl DatalakeQuery for StatsDatalake {
        async fn query_arrow(
            &self,
            _sql: &str,
            _params: Value,
            _max_block_size: Option<u64>,
        ) -> Result<crate::modules::sdlc::datalake::RecordBatchStream<'_>, DatalakeError> {
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
        ) -> Result<(Vec<RecordBatch>, ReadStats), DatalakeError> {
            let mut calls = self.calls.lock().unwrap();
            *calls += 1;
            if *calls == 1 {
                Ok((vec![test_batch(self.rows)], self.read_stats))
            } else {
                Ok((vec![], ReadStats::default()))
            }
        }
    }

    #[tokio::test]
    async fn run_plan_reports_resource_stats() {
        let datalake = Arc::new(StatsDatalake {
            rows: 5,
            read_stats: ReadStats {
                read_rows: 5,
                read_bytes: 4096,
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
                test_watermark(),
            )
            .await
            .expect("run should succeed");

        assert_eq!(
            stats.read_rows, 5,
            "read rows come from the datalake summary"
        );
        assert_eq!(stats.read_bytes, 4096, "read bytes come from the summary");
        assert_eq!(
            stats.written_rows, 5,
            "transform emits the 5 extracted rows to the graph"
        );
        assert!(
            stats.written_bytes > 0,
            "written bytes reflect the transformed batch size"
        );
    }
}
