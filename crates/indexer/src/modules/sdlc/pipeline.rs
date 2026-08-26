//! SDLC indexing pipeline: extract a whole page, transform it, and bulk-write
//! each destination table, overlapping the next page's read with the writes.

use std::sync::Arc;
use std::time::{Duration, Instant};

use std::collections::HashMap;

use arrow::array::BooleanArray;
use arrow::compute;
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use serde_json::Value;
use tracing::{debug, info, warn};

use crate::handler::HandlerError;
use crate::nats::ProgressNotifier;
use crate::observer::{IndexingMode, IndexingObserver};
use crate::retry::{Backoff, LocalRetry, Step, drive_with};

use super::datalake::{DatalakeQuery, ScanStats, is_arrow_string_overflow};
use super::metrics::SdlcMetrics;
use super::plan::{Cursor, CursorFilter, Plan, PreparedQuery};
use super::transform::{BlockTransform, TransformRegistry};
use crate::checkpoint::{Checkpoint, CheckpointStore};
use crate::durability::RunDurability;
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
}

impl Page {
    fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }

    fn rows(&self) -> u64 {
        self.batches.iter().map(|b| b.num_rows() as u64).sum()
    }

    fn bytes(&self) -> u64 {
        self.batches
            .iter()
            .map(|b| b.get_array_memory_size() as u64)
            .sum()
    }
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
    graph_sort_keys: HashMap<String, Vec<String>>,
}

impl Pipeline {
    pub fn new(
        datalake: Arc<dyn DatalakeQuery>,
        checkpoint_store: Arc<dyn CheckpointStore>,
        metrics: SdlcMetrics,
        retry_config: DatalakeRetryConfig,
        ontology: &ontology::Ontology,
    ) -> Self {
        Self {
            datalake,
            checkpoint_store,
            metrics,
            retry_config,
            registry: Arc::new(TransformRegistry::default()),
            graph_sort_keys: build_graph_sort_key_map(ontology),
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
        durability: RunDurability,
    ) -> Result<PipelineStats, HandlerError> {
        let started_at = Instant::now();
        let checkpoint = self.load_checkpoint(position_key).await;
        let mut cursor = Cursor::from_checkpoint(&checkpoint);

        if !cursor.is_first_page() {
            info!("resuming from saved cursor");
        }

        let transform = self.registry.build(plan)?;
        let outputs = transform.outputs().to_vec();
        let params = base_query.params();
        let mut stats = PipelineStats::default();

        let mut page = self
            .extract_batch(
                transform.name(),
                &self.page_sql(&base_query, &plan.sort_key, &cursor)?,
                params.clone(),
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

            cursor = cursor.advance(
                page.batches
                    .last()
                    .expect("non-empty page has a last block"),
                &plan.sort_key,
            )?;
            let has_more = rows_in_page >= plan.batch_size;

            let transform_start = Instant::now();
            let grouped = self
                .transform_page(transform.as_ref(), &page.batches)
                .await?;
            let transform_elapsed = transform_start.elapsed();
            self.metrics
                .record_transform_duration(transform_elapsed.as_secs_f64());
            stats.transform_ms += transform_elapsed.as_millis() as u64;

            type WriteFuture = std::pin::Pin<
                Box<
                    dyn std::future::Future<
                            Output = Result<crate::clickhouse::WriteReport, HandlerError>,
                        > + Send,
                >,
            >;
            let mut write_futures: FuturesUnordered<WriteFuture> = FuturesUnordered::new();
            for (index, batches) in grouped.into_iter().enumerate() {
                if batches.is_empty() {
                    continue;
                }
                let table = outputs[index].clone();
                let w = Arc::clone(&context.writer);
                let d = durability.data_writes;

                let sort_key = self
                    .graph_sort_keys
                    .get(&table)
                    .map(|v| v.as_slice())
                    .unwrap_or(&[]);
                let (live, delete_sql) = partition_deleted_rows(&table, batches, sort_key);

                if !live.is_empty() {
                    let w2 = Arc::clone(&w);
                    let t = table.clone();
                    write_futures.push(Box::pin(async move {
                        w2.write(&t, live, d)
                            .await
                            .map_err(|e| HandlerError::Processing(e.to_string()))
                    }));
                }

                if let Some(sql) = delete_sql {
                    let w3 = Arc::clone(&w);
                    let t = table.clone();
                    write_futures.push(Box::pin(async move {
                        w3.lightweight_delete(&sql)
                            .await
                            .map_err(|e| HandlerError::Processing(e.to_string()))?;
                        Ok(crate::clickhouse::WriteReport {
                            table: t,
                            rows: 0,
                            bytes: 0,
                        })
                    }));
                }
            }

            {
                let mut observer = context.observer.lock().unwrap();
                observer.extracted(rows_in_page, bytes_in_page);
            }

            let write_start = Instant::now();
            let drain_writes = async {
                while let Some(report) = write_futures.next().await {
                    let report = report?;
                    let mut observer = context.observer.lock().unwrap();
                    observer.record_graph_write(&report.table, report.rows, report.bytes);
                    stats.written_rows += report.rows;
                    stats.written_bytes += report.bytes;
                }
                Ok::<_, HandlerError>(write_start.elapsed())
            };

            // Overlap the next page's read with this page's writes; peak memory is roughly two pages.
            let (write_elapsed, next_page) = if has_more {
                let next_sql = self.page_sql(&base_query, &plan.sort_key, &cursor)?;
                let (write_result, extract_result) = tokio::join!(
                    drain_writes,
                    self.extract_batch(transform.name(), &next_sql, params.clone()),
                );
                (write_result?, Some(extract_result?))
            } else {
                (drain_writes.await?, None)
            };
            stats.write_ms += write_elapsed.as_millis() as u64;

            if page_number > 1 {
                info!(
                    page = page_number,
                    rows = rows_in_page,
                    scanned_rows = page.scan_stats.scanned_rows,
                    extract_ms = page.extract_elapsed.as_millis() as u64,
                    transform_ms = transform_elapsed.as_millis() as u64,
                    write_ms = write_elapsed.as_millis() as u64,
                    "page indexed"
                );
            } else {
                debug!(
                    page = page_number,
                    rows = rows_in_page,
                    scanned_rows = page.scan_stats.scanned_rows,
                    extract_ms = page.extract_elapsed.as_millis() as u64,
                    transform_ms = transform_elapsed.as_millis() as u64,
                    write_ms = write_elapsed.as_millis() as u64,
                    "page indexed"
                );
            }

            self.save_batch_progress(position_key, window, &cursor, &context.progress)
                .await?;

            let Some(next) = next_page else {
                break;
            };
            stats.extract_ms += next.extract_elapsed.as_millis() as u64;
            page = next;
        }

        self.checkpoint_store
            .save_completed(position_key, &window.target, durability.completion)
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
        self.metrics.record_watermark_lag(&window.target);

        {
            let mut observer = context.observer.lock().unwrap();
            observer.record_datalake_read(stats.read_rows, stats.read_bytes);
            observer.record_datalake_scan(stats.scanned_rows, stats.scanned_bytes);
            observer.record_duration(stats.duration_ms);
        }

        if stats.written_rows > 0 || stats.read_rows > 0 {
            info!(
                read_rows = stats.read_rows,
                scanned_rows = stats.scanned_rows,
                written_rows = stats.written_rows,
                duration_ms = stats.duration_ms,
                extract_ms = stats.extract_ms,
                transform_ms = stats.transform_ms,
                write_ms = stats.write_ms,
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
    ) -> Result<Page, HandlerError> {
        drive_with(
            &DATALAKE_EXTRACT_RETRY,
            None::<u64>,
            |block_size, attempt| {
                let query_start = Instant::now();
                let fut = self
                    .datalake
                    .query_batches_with_summary(sql, params.clone(), block_size);
                let metrics = &self.metrics;
                let retry_config = &self.retry_config;
                async move {
                    match fut.await {
                        Ok((batches, scan_stats)) => {
                            let extract_elapsed = query_start.elapsed();
                            let bytes: u64 = batches
                                .iter()
                                .map(|b| b.get_array_memory_size() as u64)
                                .sum();
                            metrics.record_datalake_query(
                                transform_name,
                                extract_elapsed.as_secs_f64(),
                                bytes,
                            );
                            Step::Done(Page {
                                batches,
                                scan_stats,
                                extract_elapsed,
                            })
                        }
                        Err(err) => {
                            warn!(
                                attempt,
                                max_retries = MAX_RETRIES,
                                max_block_size = ?block_size,
                                %err,
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
                            DATALAKE_EXTRACT_RETRY.retry_or_give_up(
                                attempt,
                                Some(next),
                                HandlerError::Processing(format!("datalake query failed: {err}")),
                            )
                        }
                    }
                }
            },
        )
        .await
    }

    /// Groups output rows by destination table so each table is written as one bulk insert.
    async fn transform_page(
        &self,
        transform: &dyn BlockTransform,
        batches: &[RecordBatch],
    ) -> Result<Vec<Vec<RecordBatch>>, HandlerError> {
        let mut grouped: Vec<Vec<RecordBatch>> = vec![Vec::new(); transform.outputs().len()];
        for block in batches {
            for output in transform.transform(block).await? {
                grouped[output.output_index].push(output.batch);
            }
        }
        Ok(grouped)
    }

    async fn save_batch_progress(
        &self,
        position_key: &str,
        window: WindowBounds,
        cursor: &Cursor,
        progress: &ProgressNotifier,
    ) -> Result<(), HandlerError> {
        self.checkpoint_store
            .save_progress(
                position_key,
                &Checkpoint {
                    watermark: window.target,
                    cursor_values: cursor.to_checkpoint_values(),
                    resume_floor: window.floor,
                },
            )
            .await
            .map_err(|err| {
                HandlerError::Processing(format!("failed to save cursor for {position_key}: {err}"))
            })?;
        progress.notify_in_progress().await;
        Ok(())
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
                warn!(
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

/// `floor` is `None` for a backfill (start of time); it is persisted so a resume rebuilds the window.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::modules::sdlc) struct WindowBounds {
    pub target: DateTime<Utc>,
    pub floor: Option<DateTime<Utc>>,
}

impl WindowBounds {
    pub fn indexing_mode(&self) -> IndexingMode {
        match self.floor {
            Some(_) => IndexingMode::Incremental,
            None => IndexingMode::Full,
        }
    }
}

fn build_graph_sort_key_map(ontology: &ontology::Ontology) -> HashMap<String, Vec<String>> {
    use crate::schema::version::{SCHEMA_VERSION, prefixed_table_name};
    let mut map = HashMap::new();
    for node in ontology.nodes() {
        let prefixed = prefixed_table_name(&node.destination_table, *SCHEMA_VERSION);
        let sort_key = ontology
            .sort_key_for_table(&node.destination_table)
            .unwrap_or(&node.sort_key)
            .to_vec();
        map.insert(prefixed, sort_key);
    }
    for edge_table in ontology.edge_tables() {
        if let Some(config) = ontology.edge_table_config(edge_table) {
            let prefixed = prefixed_table_name(edge_table, *SCHEMA_VERSION);
            map.insert(prefixed, config.sort_key.clone());
        }
    }
    map
}

/// Splits transformed batches into live rows (for INSERT) and a lightweight
/// `DELETE FROM` statement (for deleted rows).
///
/// Returns `(live_batches, optional_delete_sql)`. When no rows are deleted,
/// `live_batches` is the original vec unchanged and the SQL is `None`.
fn partition_deleted_rows(
    table: &str,
    batches: Vec<RecordBatch>,
    sort_key: &[String],
) -> (Vec<RecordBatch>, Option<String>) {
    let mut live = Vec::with_capacity(batches.len());
    let mut has_deletions = false;
    let mut deleted = Vec::new();

    for batch in batches {
        let deleted_col = batch
            .column_by_name(ontology::DELETED_COLUMN)
            .and_then(|col| col.as_any().downcast_ref::<BooleanArray>());

        let Some(deleted_array) = deleted_col else {
            live.push(batch);
            continue;
        };

        let any_deleted = (0..deleted_array.len()).any(|i| deleted_array.value(i));
        if !any_deleted {
            live.push(batch);
            continue;
        }

        has_deletions = true;

        let live_mask = compute::not(deleted_array).expect("boolean NOT cannot fail");
        let live_batch = compute::filter_record_batch(&batch, &live_mask)
            .expect("filter_record_batch with same-length mask cannot fail");
        if live_batch.num_rows() > 0 {
            live.push(live_batch);
        }

        let deleted_batch = compute::filter_record_batch(&batch, deleted_array)
            .expect("filter_record_batch with same-length mask cannot fail");
        if deleted_batch.num_rows() > 0 {
            deleted.push(deleted_batch);
        }
    }

    if !has_deletions {
        return (live, None);
    }

    let delete_sql = build_delete_sql_from_batches(table, &deleted, sort_key);
    (live, delete_sql)
}

/// Builds `DELETE FROM {table} WHERE (sort_key_cols) IN ((v1, v2, ...), ...)`
/// from the sort key column values in the deleted RecordBatches.
fn build_delete_sql_from_batches(
    table: &str,
    batches: &[RecordBatch],
    sort_key: &[String],
) -> Option<String> {
    if batches.is_empty() || sort_key.is_empty() {
        return None;
    }

    let mut tuples = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let values: Vec<String> = sort_key
                .iter()
                .map(|col_name| extract_sort_key_value(batch, col_name, row))
                .collect();
            tuples.push(format!("({})", values.join(", ")));
        }
    }

    if tuples.is_empty() {
        return None;
    }

    let cols = sort_key.join(", ");
    Some(format!(
        "DELETE FROM {table} WHERE ({cols}) IN ({})",
        tuples.join(", ")
    ))
}

/// Extracts a single cell value from a RecordBatch as an SQL literal.
fn extract_sort_key_value(batch: &RecordBatch, column: &str, row: usize) -> String {
    use arrow::array::{AsArray, Int64Array, StringArray};
    use arrow::datatypes::DataType;

    let col = batch
        .column_by_name(column)
        .unwrap_or_else(|| panic!("sort key column '{column}' not found in RecordBatch"));

    match col.data_type() {
        DataType::Utf8 => {
            let array = col.as_any().downcast_ref::<StringArray>().unwrap();
            format!("'{}'", array.value(row).replace('\'', "\\'"))
        }
        DataType::LargeUtf8 => {
            let array: &arrow::array::LargeStringArray = col.as_string();
            format!("'{}'", array.value(row).replace('\'', "\\'"))
        }
        DataType::Int64 => {
            let array = col.as_any().downcast_ref::<Int64Array>().unwrap();
            array.value(row).to_string()
        }
        dt => panic!("unsupported sort key column type {dt:?} for column '{column}'"),
    }
}

#[cfg(test)]
mod tests {
    use super::super::plan::{TransformSpec, Transformation};
    use super::*;
    use crate::checkpoint::CheckpointError;
    use crate::durability::WriteDurability;
    use crate::modules::sdlc::datalake::{DatalakeError, RecordBatchStream, ScanStats};
    use crate::modules::sdlc::test_helpers::test_metrics;
    use crate::observer::NoOpObserver;
    use crate::testkit::test_writer;
    use arrow::array::{BooleanArray, Int64Array, StringArray};
    use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema};
    use async_trait::async_trait;
    use std::collections::HashSet;
    use std::sync::Mutex;

    fn test_ontology() -> ontology::Ontology {
        ontology::Ontology::load_embedded().expect("ontology must load")
    }

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
            _durability: WriteDurability,
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
            &test_ontology(),
        );
        let plan = simple_plan("Test");

        let result = pipeline
            .run_plan(
                &noop_context(),
                &plan,
                base_query(&plan),
                &position_key(&plan),
                test_window(),
                RunDurability::for_mode(IndexingMode::Incremental),
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
            &test_ontology(),
        );
        let result = pipeline
            .run_plan(
                &noop_context(),
                &plan,
                base_query(&plan),
                &position_key(&plan),
                test_window(),
                RunDurability::for_mode(IndexingMode::Incremental),
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
            &test_ontology(),
        );
        let plan = simple_plan("Test");

        let result = pipeline
            .run_plan(
                &noop_context(),
                &plan,
                base_query(&plan),
                &position_key(&plan),
                test_window(),
                RunDurability::for_mode(IndexingMode::Incremental),
            )
            .await;

        assert!(result.is_err());
    }

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
            &test_ontology(),
        );

        let plan = simple_plan("Test");
        let _ = pipeline
            .run_plan(
                &noop_context(),
                &plan,
                base_query(&plan),
                &position_key(&plan),
                test_window(),
                RunDurability::for_mode(IndexingMode::Incremental),
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
            Err("simulated transient failure"),
            Err("simulated transient failure"),
            Err("simulated transient failure"),
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
            &test_ontology(),
        );

        let plan = simple_plan("Test");
        let result = pipeline
            .run_plan(
                &noop_context(),
                &plan,
                base_query(&plan),
                &position_key(&plan),
                test_window(),
                RunDurability::for_mode(IndexingMode::Incremental),
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
            Err("simulated transient failure"),
            Err("simulated transient failure"),
            Err("simulated transient failure"),
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
            &test_ontology(),
        );

        let plan = simple_plan("Test");
        let _ = pipeline
            .run_plan(
                &noop_context(),
                &plan,
                base_query(&plan),
                &position_key(&plan),
                test_window(),
                RunDurability::for_mode(IndexingMode::Incremental),
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
            &test_ontology(),
        );

        let plan = simple_plan("Test");
        let result = pipeline
            .run_plan(
                &noop_context(),
                &plan,
                base_query(&plan),
                &position_key(&plan),
                test_window(),
                RunDurability::for_mode(IndexingMode::Incremental),
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
            &test_ontology(),
        );
        let plan = simple_plan("Test");

        let result = pipeline
            .run_plan(
                &noop_context(),
                &plan,
                base_query(&plan),
                &position_key(&plan),
                test_window(),
                RunDurability::for_mode(IndexingMode::Incremental),
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
            &test_ontology(),
        );
        let plan = simple_plan("Test");

        let stats = pipeline
            .run_plan(
                &noop_context(),
                &plan,
                base_query(&plan),
                &position_key(&plan),
                test_window(),
                RunDurability::for_mode(IndexingMode::Incremental),
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
