//! SDLC indexing pipeline: extract a whole page, transform it, and bulk-write
//! each destination table, overlapping the next page's read with the writes.

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use futures::future::OptionFuture;
use futures::stream::FuturesUnordered;
use orbit_log_fields::{debug, info, warn};
use serde_json::Value;
use thiserror::Error;

use super::edge_diff::{EdgeDiffClient, page_rows};
use crate::handler::HandlerError;
use crate::nats::ProgressNotifier;
use crate::observer::IndexingObserver;
use crate::retry::{Backoff, LocalRetry, RetryExhausted, Step, drive_with};

use super::datalake::{
    DatalakeError, DatalakeQuery, FetchedPage, ScanStats, is_arrow_string_overflow,
};
use super::metrics::SdlcMetrics;
use super::paging::{block_size_for, first_page_limit, next_page_limit};
use super::plan::{Cursor, CursorFilter, Plan, PreparedQuery};
use super::transform::{BlockTransform, TransformRegistry};
use crate::checkpoint::{Checkpoint, CheckpointStore, WindowBounds};
use crate::durability::{RunDurability, WriteDurability};
use orbit_arrow::batch_slice_bytes;
use orbit_server_config::DatalakeRetryConfig;

const MAX_RETRIES: u32 = 3;

/// Retry a datalake page read, shrinking the block size each time so an oversized page self-corrects.
const DATALAKE_EXTRACT_RETRY: LocalRetry = LocalRetry {
    backoff: Backoff::Fixed(&[
        Duration::from_millis(100),
        Duration::from_millis(200),
        Duration::from_millis(400),
    ]),
    max_attempts: MAX_RETRIES + 1,
};

#[derive(Debug, Error)]
pub(in crate::modules::sdlc) enum PipelineError {
    #[error("source unavailable: {0}")]
    SourceUnavailable(String),
    #[error(transparent)]
    Failed(#[from] HandlerError),
}

impl From<DatalakeError> for PipelineError {
    fn from(err: DatalakeError) -> Self {
        match err {
            DatalakeError::SourceUnavailable(reason) => Self::SourceUnavailable(reason),
            other => HandlerError::from(other).into(),
        }
    }
}

impl From<RetryExhausted> for PipelineError {
    fn from(err: RetryExhausted) -> Self {
        HandlerError::from(err).into()
    }
}

impl From<PipelineError> for HandlerError {
    fn from(err: PipelineError) -> Self {
        match err {
            PipelineError::Failed(err) => err,
            PipelineError::SourceUnavailable(reason) => {
                HandlerError::Processing(format!("source unavailable: {reason}"))
            }
        }
    }
}

/// `read_*` count the rows/bytes actually returned from the datalake; `scanned_*`
/// ClickHouse's storage-scan cost from the summary; `written_*` the transformed
/// rows/bytes inserted.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(in crate::modules::sdlc) struct PipelineStats {
    pub read_rows: u64,
    pub read_bytes: u64,
    pub scanned_rows: u64,
    pub scanned_bytes: u64,
    pub written_rows: u64,
    pub written_bytes: u64,
    pub duration_ms: u64,
    // Per-phase time can exceed duration_ms: extract and write overlap via tokio::join!.
    pub extract_ms: u64,
    pub transform_ms: u64,
    pub write_ms: u64,
}

impl PipelineStats {
    pub(in crate::modules::sdlc) fn merge(&mut self, other: PipelineStats) {
        self.read_rows += other.read_rows;
        self.read_bytes += other.read_bytes;
        self.scanned_rows += other.scanned_rows;
        self.scanned_bytes += other.scanned_bytes;
        self.written_rows += other.written_rows;
        self.written_bytes += other.written_bytes;
        self.duration_ms = self.duration_ms.max(other.duration_ms);
        self.extract_ms += other.extract_ms;
        self.transform_ms += other.transform_ms;
        self.write_ms += other.write_ms;
    }
}

/// One extracted page and the cost of fetching it. Carried across loop
/// iterations because the next page is read while the current one is written.
struct Page {
    batches: Vec<RecordBatch>,
    scan_stats: ScanStats,
    extract_elapsed: Duration,
    truncated: bool,
}

impl Page {
    fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }

    fn rows(&self) -> u64 {
        self.batches.iter().map(|b| b.num_rows() as u64).sum()
    }

    fn bytes(&self) -> u64 {
        self.batches.iter().map(batch_slice_bytes).sum()
    }
}

#[derive(Debug)]
struct PageWrite {
    table: String,
    batches: Vec<RecordBatch>,
    durability: Option<WriteDurability>,
}

fn write_phases(
    tombstones: Vec<(String, RecordBatch)>,
    grouped: Vec<Vec<RecordBatch>>,
    outputs: &[String],
    data_durability: Option<WriteDurability>,
) -> Vec<Vec<PageWrite>> {
    let tombstones: Vec<PageWrite> = tombstones
        .into_iter()
        .map(|(table, batch)| PageWrite {
            table,
            batches: vec![batch],
            durability: Some(WriteDurability::Durable),
        })
        .collect();
    let data: Vec<PageWrite> = grouped
        .into_iter()
        .enumerate()
        .filter(|(_, batches)| !batches.is_empty())
        .map(|(index, batches)| PageWrite {
            table: outputs[index].clone(),
            batches,
            durability: data_durability,
        })
        .collect();
    [tombstones, data]
        .into_iter()
        .filter(|phase| !phase.is_empty())
        .collect()
}

pub(in crate::modules::sdlc) struct PipelineContext {
    pub writer: Arc<crate::clickhouse::ClickHouseWriter>,
    pub progress: ProgressNotifier,
    pub observer: Arc<std::sync::Mutex<dyn IndexingObserver>>,
}

pub(in crate::modules::sdlc) struct Pipeline {
    datalake: Arc<dyn DatalakeQuery>,
    checkpoint_store: Arc<dyn CheckpointStore>,
    metrics: SdlcMetrics,
    retry_config: DatalakeRetryConfig,
    registry: Arc<TransformRegistry>,
    edge_diff: EdgeDiffClient,
}

impl Pipeline {
    pub fn new(
        datalake: Arc<dyn DatalakeQuery>,
        checkpoint_store: Arc<dyn CheckpointStore>,
        metrics: SdlcMetrics,
        retry_config: DatalakeRetryConfig,
        registry: Arc<TransformRegistry>,
        edge_diff: EdgeDiffClient,
    ) -> Self {
        Self {
            datalake,
            checkpoint_store,
            metrics,
            retry_config,
            registry,
            edge_diff,
        }
    }

    pub async fn run_plan(
        &self,
        context: &PipelineContext,
        plan: &Plan,
        mut base_query: PreparedQuery,
        position_key: &str,
        mut checkpoint: Checkpoint,
        window: WindowBounds,
    ) -> Result<PipelineStats, PipelineError> {
        let started_at = Instant::now();
        let durability = RunDurability::for_mode(window.indexing_mode());
        let mut cursor = Cursor::from_checkpoint(&checkpoint);

        if !cursor.is_first_page() && cursor.values().len() != plan.sort_key.len() {
            warn!(
                safe(CURSOR_LENGTH) = cursor.values().len(),
                safe(SORT_KEY_LENGTH) = plan.sort_key.len(),
                "saved cursor does not match the plan sort key, restarting from the first page"
            );
            cursor = Cursor::default();
        } else if !cursor.is_first_page() {
            info!("resuming from saved cursor");
        }

        let transform = self.registry.build(plan)?;
        let outputs = transform.outputs().to_vec();
        let node_diff = plan.edge_diff.as_ref().map(|diff| {
            let previous_edges = self.registry.sql(
                format!("{} edge diff", plan.name),
                diff.key_transforms().to_vec(),
            );
            (diff, previous_edges)
        });
        let params = base_query.params();
        let plan_limit = base_query.batch_size();
        base_query.set_batch_size(first_page_limit(plan_limit));
        let mut stats = PipelineStats::default();
        let mut block_size = None;

        let mut page = self
            .extract_batch(
                transform.name(),
                &self.page_sql(&base_query, &plan.sort_key, &cursor)?,
                params.clone(),
                block_size,
            )
            .await?;
        stats.extract_ms += page.extract_elapsed.as_millis() as u64;

        let mut page_number: u64 = 0;
        while !page.is_empty() {
            page_number += 1;

            let rows_in_page = page.rows();
            let bytes_in_page = page.bytes();
            stats.read_rows += rows_in_page;
            stats.read_bytes += bytes_in_page;
            stats.scanned_rows += page.scan_stats.scanned_rows;
            stats.scanned_bytes += page.scan_stats.scanned_bytes;

            #[expect(
                clippy::expect_used,
                reason = "a page that reached this point is non-empty, so it has a last block"
            )]
            let last_block = page
                .batches
                .last()
                .expect("non-empty page has a last block");
            cursor = cursor.advance(last_block, &plan.sort_key)?;
            let has_more = rows_in_page >= base_query.batch_size() || page.truncated;
            base_query.set_batch_size(next_page_limit(
                base_query.batch_size(),
                plan_limit,
                rows_in_page,
                bytes_in_page,
                page.truncated,
            ));
            block_size = block_size_for(
                rows_in_page,
                bytes_in_page,
                self.retry_config.halving_min_block_size,
            );

            let read_previous: OptionFuture<_> = node_diff
                .as_ref()
                .map(|(diff, previous_edges)| {
                    page_rows(&page.batches).map(|rows| {
                        diff.previous_edges(&self.edge_diff, previous_edges.as_ref(), rows)
                    })
                })
                .transpose()?
                .into();

            let transform_start = Instant::now();
            let (grouped, previous) = tokio::join!(
                self.transform_page(transform.as_ref(), std::mem::take(&mut page.batches)),
                read_previous,
            );
            let (grouped, previous) = (grouped?, previous.transpose()?);
            let transform_elapsed = transform_start.elapsed();
            self.metrics
                .record_transform_duration(transform_elapsed.as_secs_f64());
            stats.transform_ms += transform_elapsed.as_millis() as u64;

            let tombstones = match (&node_diff, previous) {
                (Some((diff, _)), Some(previous)) => {
                    diff.tombstones(&previous, &outputs, &grouped)?
                }
                _ => Vec::new(),
            };
            let phases = write_phases(tombstones, grouped, &outputs, durability.data_writes);

            {
                #[expect(
                    clippy::unwrap_used,
                    reason = "mutex poisoning means another thread already panicked; propagating is intended"
                )]
                let mut observer = context.observer.lock().unwrap();
                observer.extracted(rows_in_page, bytes_in_page);
            }

            let write_start = Instant::now();
            let drain_writes = async {
                for phase in phases {
                    let mut write_futures: FuturesUnordered<_> = phase
                        .into_iter()
                        .map(|write| {
                            let w = Arc::clone(&context.writer);
                            async move {
                                w.write(&write.table, write.batches, write.durability)
                                    .await
                                    .map_err(HandlerError::from)
                            }
                        })
                        .collect();
                    while let Some(report) = write_futures.next().await {
                        let report = report?;
                        #[expect(
                            clippy::unwrap_used,
                            reason = "mutex poisoning means another thread already panicked; propagating is intended"
                        )]
                        let mut observer = context.observer.lock().unwrap();
                        observer.record_graph_write(&report.table, report.rows, report.bytes);
                        stats.written_rows += report.rows;
                        stats.written_bytes += report.bytes;
                    }
                }
                Ok::<_, HandlerError>(write_start.elapsed())
            };

            // Overlap the next page's read with this page's writes; peak memory is roughly two pages.
            let (write_elapsed, next_page) = if has_more {
                let next_sql = self.page_sql(&base_query, &plan.sort_key, &cursor)?;
                let (write_result, extract_result) = tokio::join!(
                    drain_writes,
                    self.extract_batch(transform.name(), &next_sql, params.clone(), block_size),
                );
                (write_result?, Some(extract_result?))
            } else {
                (drain_writes.await?, None)
            };
            stats.write_ms += write_elapsed.as_millis() as u64;

            if page_number > 1 {
                info!(
                    safe(PAGE_NUMBER) = page_number,
                    safe(ROW_COUNT) = rows_in_page,
                    safe(SCANNED_ROWS) = page.scan_stats.scanned_rows,
                    safe(EXTRACT_DURATION_S) = page.extract_elapsed.as_secs_f64(),
                    safe(TRANSFORM_DURATION_S) = transform_elapsed.as_secs_f64(),
                    safe(WRITE_DURATION_S) = write_elapsed.as_secs_f64(),
                    "page indexed"
                );
            } else {
                debug!(
                    safe(PAGE_NUMBER) = page_number,
                    safe(ROW_COUNT) = rows_in_page,
                    safe(SCANNED_ROWS) = page.scan_stats.scanned_rows,
                    safe(EXTRACT_DURATION_S) = page.extract_elapsed.as_secs_f64(),
                    safe(TRANSFORM_DURATION_S) = transform_elapsed.as_secs_f64(),
                    safe(WRITE_DURATION_S) = write_elapsed.as_secs_f64(),
                    "page indexed"
                );
            }

            checkpoint.record_page(window.target, window.floor, cursor.values().to_vec());
            self.save_batch_progress(position_key, &checkpoint, &context.progress)
                .await?;

            let Some(next) = next_page else {
                break;
            };
            stats.extract_ms += next.extract_elapsed.as_millis() as u64;
            page = next;
        }

        checkpoint.complete(window.target);
        self.checkpoint_store
            .save(position_key, &checkpoint, durability.completion)
            .await
            .map_err(|err| {
                HandlerError::Processing(format!(
                    "failed to mark {} as completed: {err}",
                    plan.name
                ))
            })?;

        let elapsed = started_at.elapsed();
        stats.duration_ms = elapsed.as_millis() as u64;
        self.metrics
            .record_pipeline_completion(&plan.name, elapsed.as_secs_f64());
        self.metrics
            .record_watermark_lag(&plan.name, &window.target);

        {
            #[expect(
                clippy::unwrap_used,
                reason = "mutex poisoning means another thread already panicked; propagating is intended"
            )]
            let mut observer = context.observer.lock().unwrap();
            observer.record_datalake_read(stats.read_rows, stats.read_bytes);
            observer.record_datalake_scan(stats.scanned_rows, stats.scanned_bytes);
            observer.record_duration(stats.duration_ms);
        }

        if stats.written_rows > 0 || stats.read_rows > 0 {
            info!(
                safe(READ_ROWS) = stats.read_rows,
                safe(SCANNED_ROWS) = stats.scanned_rows,
                safe(WRITTEN_ROWS) = stats.written_rows,
                safe(DURATION_S) = elapsed.as_secs_f64(),
                safe(EXTRACT_DURATION_S) = stats.extract_ms as f64 / 1000.0,
                safe(TRANSFORM_DURATION_S) = stats.transform_ms as f64 / 1000.0,
                safe(WRITE_DURATION_S) = stats.write_ms as f64 / 1000.0,
                "pipeline completed"
            );
        } else {
            debug!(
                safe(DURATION_S) = elapsed.as_secs_f64(),
                "pipeline completed with no data"
            );
        }

        Ok(stats)
    }

    fn page_sql(
        &self,
        base_query: &PreparedQuery,
        sort_key: &[String],
        cursor: &Cursor,
    ) -> Result<String, HandlerError> {
        base_query
            .clone()
            .with(CursorFilter {
                sort_key,
                values: cursor.values(),
            })
            .to_sql()
    }

    /// Reads a page, retrying with a smaller block size so a datalake failure
    /// self-corrects instead of bouncing the message to the dead letter stream.
    async fn extract_batch(
        &self,
        transform_name: &str,
        sql: &str,
        params: Value,
        block_size: Option<u64>,
    ) -> Result<Page, PipelineError> {
        drive_with(
            &DATALAKE_EXTRACT_RETRY,
            block_size,
            |block_size, attempt| {
                let query_start = Instant::now();
                let fut = self
                    .datalake
                    .query_batches_with_summary(sql, params.clone(), block_size);
                let metrics = &self.metrics;
                let retry_config = &self.retry_config;
                async move {
                    match fut.await {
                        Ok(FetchedPage {
                            batches,
                            scan_stats,
                            truncated,
                        }) => {
                            let extract_elapsed = query_start.elapsed();
                            let bytes: u64 = batches.iter().map(batch_slice_bytes).sum();
                            metrics.record_datalake_query(
                                transform_name,
                                extract_elapsed.as_secs_f64(),
                                bytes,
                            );
                            Step::Done(Page {
                                batches,
                                scan_stats,
                                extract_elapsed,
                                truncated,
                            })
                        }
                        Err(
                            err @ (DatalakeError::SourceUnavailable(_)
                            | DatalakeError::Unavailable(_)),
                        ) => Step::GiveUp(err.into()),
                        Err(err) => {
                            warn!(
                                safe(ATTEMPT) = attempt + 1,
                                safe(MAX_ATTEMPTS) = DATALAKE_EXTRACT_RETRY.max_attempts,
                                safe(BATCH_SIZE) = block_size,
                                safe(ERROR_TYPE) = <&str>::from(&err),
                                expanded(ERROR_MESSAGE) = %err,
                                "datalake query failed, retrying with smaller block size"
                            );
                            // An overflow only clears once blocks are small, and each halving step
                            // re-scans the page, so jump straight to the floor.
                            let next = if is_arrow_string_overflow(&err) {
                                retry_config.halving_min_block_size
                            } else {
                                match block_size {
                                    Some(size) => {
                                        (size / 2).max(retry_config.halving_min_block_size)
                                    }
                                    None => retry_config.halving_initial_block_size,
                                }
                            };
                            DATALAKE_EXTRACT_RETRY.retry_or_give_up(attempt, Some(next), err.into())
                        }
                    }
                }
            },
        )
        .await
    }

    /// Groups output rows by destination table so each table is written as one bulk insert.
    ///
    /// Consumes the page so none of it survives into the write and read-ahead window.
    async fn transform_page(
        &self,
        transform: &dyn BlockTransform,
        batches: Vec<RecordBatch>,
    ) -> Result<Vec<Vec<RecordBatch>>, HandlerError> {
        let mut grouped: Vec<Vec<RecordBatch>> = vec![Vec::new(); transform.outputs().len()];
        for block in batches {
            for output in transform.transform(&block).await? {
                grouped[output.output_index].push(output.batch);
            }
        }
        Ok(grouped)
    }

    async fn save_batch_progress(
        &self,
        position_key: &str,
        checkpoint: &Checkpoint,
        progress: &ProgressNotifier,
    ) -> Result<(), HandlerError> {
        self.checkpoint_store
            .save(position_key, checkpoint, WriteDurability::FireAndForget)
            .await
            .map_err(|err| {
                HandlerError::from(err)
                    .prefix_message_with(format!("failed to save cursor for {position_key}"))
            })?;
        progress.notify_in_progress().await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::plan::{TransformSpec, Transformation};
    use super::*;
    use crate::checkpoint::CheckpointError;
    use crate::modules::sdlc::datalake::{DatalakeError, RecordBatchStream, ScanStats};
    use crate::modules::sdlc::partitioning::PartitionAssignment;
    use crate::modules::sdlc::test_helpers::{
        passthrough_registry, test_edge_diff_client, test_metrics,
    };
    use crate::observer::NoOpObserver;
    use crate::testkit::test_writer;
    use arrow::array::{BooleanArray, Int64Array, StringArray};
    use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema};
    use async_trait::async_trait;
    use chrono::{DateTime, Utc};
    use orbit_server_config::AppConfig;
    use std::collections::HashSet;
    use std::sync::Mutex;

    fn simple_plan(name: &str) -> Plan {
        simple_plan_with_batch_size(name, 1000)
    }

    fn simple_plan_with_batch_size(name: &str, batch_size: u64) -> Plan {
        Plan {
            name: name.to_string(),
            target: name.to_string(),
            scope: ontology::EtlScope::Namespaced,
            extract_template: crate::modules::sdlc::plan::ExtractTemplate::new(
                "SELECT id, name, _siphon_replicated_at AS _version, \
                 _siphon_deleted AS _deleted \
                 FROM source_table \
                 WHERE 1=1 {{filters}} \
                 ORDER BY id LIMIT {{batch_size}}"
                    .to_string(),
            )
            .expect("valid template"),
            watermark_column: "_siphon_watermark".to_string(),
            deleted_column: "_siphon_deleted".to_string(),
            sort_key: vec!["id".to_string()],
            batch_size,
            edge_diff: None,
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
            writer: test_writer(),
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
        saves: Mutex<Vec<Checkpoint>>,
    }

    impl RecordingCheckpointStore {
        fn new() -> Self {
            Self {
                state: Mutex::new(None),
                saves: Mutex::new(Vec::new()),
            }
        }

        fn current_state(&self) -> Option<Checkpoint> {
            self.state.lock().unwrap().clone()
        }

        fn progress_history(&self) -> Vec<Checkpoint> {
            self.saves
                .lock()
                .unwrap()
                .iter()
                .filter(|checkpoint| checkpoint.is_paging())
                .cloned()
                .collect()
        }
    }

    #[async_trait]
    impl CheckpointStore for RecordingCheckpointStore {
        async fn load(&self, _key: &str) -> Result<Option<Checkpoint>, CheckpointError> {
            Ok(self.state.lock().unwrap().clone())
        }

        async fn save(
            &self,
            _key: &str,
            checkpoint: &Checkpoint,
            _durability: WriteDurability,
        ) -> Result<(), CheckpointError> {
            self.saves.lock().unwrap().push(checkpoint.clone());
            *self.state.lock().unwrap() = Some(checkpoint.clone());
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

    #[test]
    fn edge_tombstones_are_written_durably_before_the_page_rows() {
        let batch = test_batch(1);
        let phases = write_phases(
            vec![("v1_gl_edge".to_string(), batch.clone())],
            vec![vec![batch.clone()], vec![], vec![batch]],
            &[
                "v1_gl_job".to_string(),
                "v1_gl_ci_edge".to_string(),
                "v1_gl_edge".to_string(),
            ],
            Some(WriteDurability::FireAndForget),
        );
        let tables = |phase: &[PageWrite]| -> Vec<String> {
            phase.iter().map(|write| write.table.clone()).collect()
        };
        assert_eq!(phases.len(), 2);
        assert_eq!(tables(&phases[0]), vec!["v1_gl_edge"]);
        assert!(
            phases[0]
                .iter()
                .all(|write| write.durability == Some(WriteDurability::Durable))
        );
        assert_eq!(tables(&phases[1]), vec!["v1_gl_job", "v1_gl_edge"]);
        assert!(
            write_phases(
                Vec::new(),
                vec![vec![test_batch(1)]],
                &["v1_gl_job".to_string()],
                None
            )
            .len()
                == 1
        );
    }

    #[tokio::test]
    async fn empty_datalake_completes_without_error() {
        let pipeline = Pipeline::new(
            Arc::new(EmptyDatalake),
            Arc::new(RecordingCheckpointStore::new()),
            test_metrics(),
            AppConfig::embedded_defaults().engine.datalake_retry,
            passthrough_registry(),
            test_edge_diff_client(),
        );
        let plan = simple_plan("Test");

        let result = pipeline
            .run_plan(
                &noop_context(),
                &plan,
                base_query(&plan),
                &position_key(&plan),
                Checkpoint::new(test_watermark()),
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
            AppConfig::embedded_defaults().engine.datalake_retry,
            passthrough_registry(),
            test_edge_diff_client(),
        );
        let result = pipeline
            .run_plan(
                &noop_context(),
                &plan,
                base_query(&plan),
                &position_key(&plan),
                Checkpoint::new(test_watermark()),
                test_window(),
            )
            .await;

        assert!(result.is_ok());

        let history = store.progress_history();
        assert_eq!(history.len(), 2, "should checkpoint after each batch");
        assert_eq!(
            history[0].resume_cursor(),
            ["10"],
            "first checkpoint should record cursor from batch 1 (last id=10)"
        );
        assert_eq!(
            history[1].resume_cursor(),
            ["5"],
            "second checkpoint should record cursor from batch 2 (last id=5)"
        );

        let final_state = store.current_state().unwrap();
        assert!(final_state.is_completed(), "should be completed");
    }

    #[tokio::test]
    async fn page_writes_keep_the_attempt_count_and_completion_resets_it() {
        let mut checkpoint = Checkpoint::new(test_watermark());
        for _ in 0..3 {
            checkpoint.start_attempt();
        }
        let store = Arc::new(RecordingCheckpointStore::new());
        let plan = simple_plan_with_batch_size("Test", 10);
        let pipeline = Pipeline::new(
            Arc::new(MultiBatchDatalake {
                call_count: Mutex::new(0),
                batch_size: 10,
            }),
            store.clone(),
            test_metrics(),
            AppConfig::embedded_defaults().engine.datalake_retry,
            passthrough_registry(),
            test_edge_diff_client(),
        );

        pipeline
            .run_plan(
                &noop_context(),
                &plan,
                base_query(&plan),
                &position_key(&plan),
                checkpoint,
                test_window(),
            )
            .await
            .unwrap();

        let pages = store.progress_history();
        assert_eq!(pages.len(), 2);
        assert!(
            pages
                .iter()
                .all(|page| page.attempts == 3 && page.indexed_at.is_none())
        );
        let completed = store.current_state().unwrap();
        assert_eq!(completed.attempts, 0);
        assert!(completed.indexed_at.is_some());
    }

    // Comparing `has_more` against the plan's budget rather than the query's share ends
    // a partition after one page and marks it complete with rows unread.
    #[tokio::test]
    async fn partitioned_share_pages_against_the_query_budget() {
        let store = Arc::new(RecordingCheckpointStore::new());
        let plan = simple_plan_with_batch_size("Test", 200_000);
        let mut partitions = base_query(&plan).into_partitions(vec![
            PartitionAssignment {
                index: 0,
                total: 2,
                key_columns: vec!["id".to_string()],
                lower_bound: None,
                upper_bound: Some(vec!["500".to_string()]),
            },
            PartitionAssignment {
                index: 1,
                total: 2,
                key_columns: vec!["id".to_string()],
                lower_bound: Some(vec!["500".to_string()]),
                upper_bound: None,
            },
        ]);
        let (_, query) = partitions.remove(0);
        assert_eq!(query.batch_size(), 100_000);

        let pipeline = Pipeline::new(
            Arc::new(MultiBatchDatalake {
                call_count: Mutex::new(0),
                batch_size: 100_000,
            }),
            store.clone(),
            test_metrics(),
            AppConfig::embedded_defaults().engine.datalake_retry,
            passthrough_registry(),
            test_edge_diff_client(),
        );
        pipeline
            .run_plan(
                &noop_context(),
                &plan,
                query,
                &position_key(&plan),
                Checkpoint::new(test_watermark()),
                test_window(),
            )
            .await
            .expect("partitioned run should succeed");

        assert_eq!(
            store.progress_history().len(),
            2,
            "a full page at the divided share must trigger a second read"
        );
    }

    #[tokio::test]
    async fn datalake_failure_surfaces_as_error() {
        let pipeline = Pipeline::new(
            Arc::new(FailingDatalake),
            Arc::new(RecordingCheckpointStore::new()),
            test_metrics(),
            AppConfig::embedded_defaults().engine.datalake_retry,
            passthrough_registry(),
            test_edge_diff_client(),
        );
        let plan = simple_plan("Failing");

        let result = pipeline
            .run_plan(
                &noop_context(),
                &plan,
                base_query(&plan),
                &position_key(&plan),
                Checkpoint::new(test_watermark()),
                test_window(),
            )
            .await;

        assert!(result.is_err());
    }

    struct RecordingDatalake {
        calls: Mutex<Vec<Option<u64>>>,
        responses: Mutex<Vec<Result<Vec<RecordBatch>, DatalakeError>>>,
    }

    impl RecordingDatalake {
        fn with_responses(responses: Vec<Result<Vec<RecordBatch>, DatalakeError>>) -> Self {
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
            responses.remove(0)
        }
    }

    fn transient_failure() -> DatalakeError {
        DatalakeError::Query("simulated transient failure".to_string())
    }

    #[tokio::test]
    async fn a_missing_source_table_gives_up_without_a_retry() {
        let datalake = Arc::new(RecordingDatalake::with_responses(vec![Err(
            DatalakeError::SourceUnavailable("UNKNOWN_TABLE".to_string()),
        )]));
        let pipeline = Pipeline::new(
            datalake.clone(),
            Arc::new(RecordingCheckpointStore::new()),
            test_metrics(),
            AppConfig::embedded_defaults().engine.datalake_retry,
            passthrough_registry(),
            test_edge_diff_client(),
        );

        let plan = simple_plan("Missing");
        let result = pipeline
            .run_plan(
                &noop_context(),
                &plan,
                base_query(&plan),
                &position_key(&plan),
                Checkpoint::new(test_watermark()),
                test_window(),
            )
            .await;

        assert!(matches!(result, Err(PipelineError::SourceUnavailable(_))));
        assert_eq!(datalake.observed_block_sizes().len(), 1);
    }

    #[tokio::test]
    async fn an_unavailable_datalake_backs_off_without_a_retry() {
        let datalake = Arc::new(RecordingDatalake::with_responses(vec![Err(
            DatalakeError::Unavailable("circuit open for service datalake".to_string()),
        )]));
        let pipeline = Pipeline::new(
            datalake.clone(),
            Arc::new(RecordingCheckpointStore::new()),
            test_metrics(),
            AppConfig::embedded_defaults().engine.datalake_retry,
            passthrough_registry(),
            test_edge_diff_client(),
        );

        let plan = simple_plan("Unavailable");
        let result = pipeline
            .run_plan(
                &noop_context(),
                &plan,
                base_query(&plan),
                &position_key(&plan),
                Checkpoint::new(test_watermark()),
                test_window(),
            )
            .await;

        assert!(
            matches!(
                result,
                Err(PipelineError::Failed(HandlerError::Backpressure(_)))
            ),
            "got {result:?}"
        );
        assert_eq!(datalake.observed_block_sizes().len(), 1);
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
            AppConfig::embedded_defaults().engine.datalake_retry,
            passthrough_registry(),
            test_edge_diff_client(),
        );

        let plan = simple_plan("Test");
        let _ = pipeline
            .run_plan(
                &noop_context(),
                &plan,
                base_query(&plan),
                &position_key(&plan),
                Checkpoint::new(test_watermark()),
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
        let datalake = Arc::new(RecordingDatalake::with_responses(vec![
            Err(transient_failure()),
            Err(transient_failure()),
            Err(transient_failure()),
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
            passthrough_registry(),
            test_edge_diff_client(),
        );

        let plan = simple_plan("Test");
        let result = pipeline
            .run_plan(
                &noop_context(),
                &plan,
                base_query(&plan),
                &position_key(&plan),
                Checkpoint::new(test_watermark()),
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
        let datalake = Arc::new(RecordingDatalake::with_responses(vec![
            Err(transient_failure()),
            Err(transient_failure()),
            Err(transient_failure()),
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
            passthrough_registry(),
            test_edge_diff_client(),
        );

        let plan = simple_plan("Test");
        let _ = pipeline
            .run_plan(
                &noop_context(),
                &plan,
                base_query(&plan),
                &position_key(&plan),
                Checkpoint::new(test_watermark()),
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
    async fn arrow_overflow_drops_straight_to_floor_block_size() {
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
            passthrough_registry(),
            test_edge_diff_client(),
        );

        let plan = simple_plan("Test");
        let result = pipeline
            .run_plan(
                &noop_context(),
                &plan,
                base_query(&plan),
                &position_key(&plan),
                Checkpoint::new(test_watermark()),
                test_window(),
            )
            .await;

        assert!(
            result.is_ok(),
            "arrow overflow should self-correct: {result:?}"
        );
        assert_eq!(
            datalake.observed_block_sizes(),
            vec![None, Some(1_024)],
            "an Arrow overflow skips gradual halving and drops to the floor block size"
        );
    }

    #[tokio::test]
    async fn resumes_from_stored_cursor() {
        let mut stored = Checkpoint::new(test_watermark());
        stored.record_page(test_watermark(), None, vec!["5".to_string()]);

        let pipeline = Pipeline::new(
            Arc::new(EmptyDatalake),
            Arc::new(RecordingCheckpointStore::new()),
            test_metrics(),
            AppConfig::embedded_defaults().engine.datalake_retry,
            passthrough_registry(),
            test_edge_diff_client(),
        );
        let plan = simple_plan("Test");

        let result = pipeline
            .run_plan(
                &noop_context(),
                &plan,
                base_query(&plan),
                &position_key(&plan),
                stored,
                test_window(),
            )
            .await;

        assert!(result.is_ok());
    }

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
        ) -> Result<FetchedPage, DatalakeError> {
            let mut calls = self.calls.lock().unwrap();
            *calls += 1;
            if *calls == 1 {
                Ok(FetchedPage {
                    batches: vec![test_batch(self.rows)],
                    scan_stats: self.scan_stats,
                    truncated: false,
                })
            } else {
                Ok(FetchedPage::default())
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
            AppConfig::embedded_defaults().engine.datalake_retry,
            passthrough_registry(),
            test_edge_diff_client(),
        );
        let plan = simple_plan("Test");

        let stats = pipeline
            .run_plan(
                &noop_context(),
                &plan,
                base_query(&plan),
                &position_key(&plan),
                Checkpoint::new(test_watermark()),
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
