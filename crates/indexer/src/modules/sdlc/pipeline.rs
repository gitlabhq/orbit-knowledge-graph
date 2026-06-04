//! SDLC indexing pipeline: [`Extractor`] -> [`Transformer`] -> [`Loader`].

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use serde_json::Value;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::destination::{Destination, StreamingWriter};
use crate::handler::HandlerError;
use crate::nats::ProgressNotifier;
use crate::observer::IndexingObserver;

use super::datalake::{
    DatalakeError, DatalakeQuery, ReadStats, RecordBatchStream, ScanStats, ScanStatsFuture,
};
use super::metrics::SdlcMetrics;
use super::plan::{Cursor, CursorFilter, Plan, PreparedQuery};
use super::transform::{BlockTransform, TableBatch, TransformRegistry};
use crate::checkpoint::{Checkpoint, CheckpointStore};
use gkg_server_config::DatalakeRetryConfig;

const MAX_RETRIES: u32 = 3;

/// Default read-ahead window when not overridden via
/// `EntityHandlerConfig::write_channel_capacity`. Higher values keep the writer
/// fed (more throughput) at the cost of peak memory.
const DEFAULT_WRITE_CHANNEL_CAPACITY: usize = 8;

// A page's `Batch`es always precede its `PageComplete`.
enum WriteCommand {
    Batch(TableBatch),
    PageComplete {
        cursor: Cursor,
        extracted_rows: u64,
        extracted_bytes: u64,
        read_stats: ReadStats,
        scan_stats: ScanStats,
    },
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
    }
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
    write_channel_capacity: usize,
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
            datalake,
            checkpoint_store,
            metrics,
            retry_config,
            write_channel_capacity: DEFAULT_WRITE_CHANNEL_CAPACITY,
            registry: Arc::new(TransformRegistry::default()),
        }
    }

    /// Override the read-ahead window (see `EntityHandlerConfig::write_channel_capacity`).
    pub fn with_write_channel_capacity(mut self, capacity: usize) -> Self {
        self.write_channel_capacity = capacity.max(1);
        self
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
        target_watermark: DateTime<Utc>,
    ) -> Result<PipelineStats, HandlerError> {
        let started_at = Instant::now();
        let checkpoint = self.load_checkpoint(position_key).await;
        let cursor = Cursor::from_checkpoint(&checkpoint);

        if !cursor.is_first_page() {
            info!("resuming from saved cursor");
        }

        let mut stats = self
            .run(
                context,
                plan,
                base_query,
                cursor,
                position_key,
                target_watermark,
            )
            .await?;

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
        stats.duration_ms = elapsed.as_millis() as u64;
        self.metrics
            .record_pipeline_completion(&plan.name, elapsed.as_secs_f64());
        self.metrics.record_watermark_lag(&target_watermark);

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

    /// The only concurrent step: producer (extract + transform) streams to the
    /// consumer (load) so the next page's read overlaps the current page's writes.
    async fn run(
        &self,
        context: &PipelineContext,
        plan: &Plan,
        base_query: PreparedQuery,
        cursor: Cursor,
        position_key: &str,
        target_watermark: DateTime<Utc>,
    ) -> Result<PipelineStats, HandlerError> {
        let transform = self.registry.build(plan)?;

        let (tx, rx) = mpsc::channel::<WriteCommand>(self.write_channel_capacity);
        let producer = tokio::spawn(
            Producer {
                extractor: Extractor {
                    datalake: Arc::clone(&self.datalake),
                    params: base_query.params(),
                },
                transform: Arc::clone(&transform),
                metrics: self.metrics.clone(),
                retry_config: self.retry_config.clone(),
                base_query,
                sort_key: plan.sort_key.clone(),
                batch_size: plan.batch_size,
            }
            .run(cursor, tx),
        );

        let consumed = self
            .consume(
                rx,
                context,
                transform.as_ref(),
                position_key,
                target_watermark,
            )
            .await;

        // Producer error wins: a write failure usually means it failed first and
        // dropped `tx`. `rx` (owned by `consume`) is already dropped, so no hang.
        producer.await.map_err(|err| {
            HandlerError::Processing(format!("extract task failed to join: {err}"))
        })??;
        consumed
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

    /// Checkpoints only on `PageComplete`, after the page's inserts are closed, so
    /// the cursor never advances past rows ClickHouse hasn't durably accepted.
    async fn consume(
        &self,
        mut rx: mpsc::Receiver<WriteCommand>,
        context: &PipelineContext,
        transform: &dyn BlockTransform,
        position_key: &str,
        target_watermark: DateTime<Utc>,
    ) -> Result<PipelineStats, HandlerError> {
        let outputs = transform.outputs();
        let mut loader = Loader::new(context.destination.as_ref(), outputs);
        let mut stats = PipelineStats::default();

        while let Some(command) = rx.recv().await {
            match command {
                WriteCommand::Batch(output) => loader.write(output).await?,
                WriteCommand::PageComplete {
                    cursor,
                    extracted_rows,
                    extracted_bytes,
                    read_stats,
                    scan_stats,
                } => {
                    let written = loader.flush_page().await?;
                    stats.read_rows += read_stats.read_rows;
                    stats.read_bytes += read_stats.read_bytes;
                    stats.scanned_rows += scan_stats.scanned_rows;
                    stats.scanned_bytes += scan_stats.scanned_bytes;

                    {
                        let mut observer = context.observer.lock().unwrap();
                        for (index, rows, bytes) in &written {
                            observer.record_graph_write(&outputs[*index], *rows, *bytes);
                            stats.written_rows += rows;
                            stats.written_bytes += bytes;
                        }
                        if extracted_rows > 0 {
                            observer.extracted(extracted_rows, extracted_bytes);
                        }
                    }

                    if extracted_rows > 0 {
                        self.save_batch_progress(
                            position_key,
                            target_watermark,
                            &cursor,
                            &context.progress,
                        )
                        .await?;
                    }
                }
            }
        }

        Ok(stats)
    }
}

struct Extractor {
    datalake: Arc<dyn DatalakeQuery>,
    params: Value,
}

impl Extractor {
    async fn open(
        &self,
        sql: &str,
        max_block_size: Option<u64>,
    ) -> Result<(RecordBatchStream<'_>, ScanStatsFuture), DatalakeError> {
        self.datalake
            .query_arrow_with_scan(sql, self.params.clone(), max_block_size)
            .await
    }
}

struct PageStats {
    cursor: Cursor,
    rows: u64,
    bytes: u64,
    read_stats: ReadStats,
    scan_stats: ScanStats,
    extract_elapsed: Duration,
    transform_duration: Duration,
}

enum PageError {
    Datalake(DatalakeError),
    Fatal(HandlerError),
    ReceiverGone,
}

/// Owned by the spawned task, so all fields are owned rather than borrowed.
struct Producer {
    extractor: Extractor,
    transform: Arc<dyn BlockTransform>,
    metrics: SdlcMetrics,
    retry_config: DatalakeRetryConfig,
    base_query: PreparedQuery,
    sort_key: Vec<String>,
    batch_size: u64,
}

impl Producer {
    async fn run(
        self,
        start_cursor: Cursor,
        tx: mpsc::Sender<WriteCommand>,
    ) -> Result<(), HandlerError> {
        let mut cursor = start_cursor;

        loop {
            let page_sql = self
                .base_query
                .clone()
                .with(CursorFilter {
                    sort_key: &self.sort_key,
                    values: cursor.values(),
                })
                .to_sql();

            let page = match self.extract_page(&page_sql, &cursor, &tx).await? {
                Some(page) => page,
                None => return Ok(()),
            };

            if page.rows > 0 {
                self.metrics.record_datalake_query(
                    self.transform.name(),
                    page.extract_elapsed.as_secs_f64(),
                    page.bytes,
                );
                self.metrics
                    .record_transform_duration(page.transform_duration.as_secs_f64());
                info!(
                    rows = page.rows,
                    duration_ms = page.extract_elapsed.as_millis() as u64,
                    "page extracted"
                );
            }

            if tx
                .send(WriteCommand::PageComplete {
                    cursor: page.cursor.clone(),
                    extracted_rows: page.rows,
                    extracted_bytes: page.bytes,
                    read_stats: page.read_stats,
                    scan_stats: page.scan_stats,
                })
                .await
                .is_err()
            {
                return Ok(());
            }

            // A short page is the last; an empty page means nothing past the cursor.
            if page.rows < self.batch_size {
                break;
            }
            cursor = page.cursor;
        }

        Ok(())
    }

    /// Streams one page, shrinking the block size on a datalake failure (the
    /// Arrow 2GB overflow surfaces mid-stream, so the retry wraps the drain, not
    /// just the open). `None` means the consumer went away.
    async fn extract_page(
        &self,
        page_sql: &str,
        start_cursor: &Cursor,
        tx: &mpsc::Sender<WriteCommand>,
    ) -> Result<Option<PageStats>, HandlerError> {
        let mut last_error: Option<HandlerError> = None;
        let mut max_block_size: Option<u64> = None;

        for attempt in 0..=MAX_RETRIES {
            if attempt > 0 {
                let backoff = Duration::from_millis(100 * 2u64.pow(attempt - 1));
                tokio::time::sleep(backoff).await;
            }

            match self
                .stream_page(page_sql, start_cursor, tx, max_block_size)
                .await
            {
                Ok(Some(page)) => return Ok(Some(page)),
                Ok(None) => return Ok(None),
                Err(PageError::Fatal(err)) => return Err(err),
                Err(PageError::ReceiverGone) => return Ok(None),
                Err(PageError::Datalake(err)) => {
                    warn!(
                        attempt,
                        max_retries = MAX_RETRIES,
                        max_block_size = ?max_block_size,
                        %err,
                        "datalake page failed, retrying with smaller block size"
                    );
                    last_error = Some(HandlerError::Processing(format!(
                        "datalake stream failed: {err}"
                    )));
                    max_block_size = Some(match max_block_size {
                        Some(size) => (size / 2).max(self.retry_config.halving_min_block_size),
                        None => self.retry_config.halving_initial_block_size,
                    });
                }
            }
        }

        Err(last_error.expect("loop runs once and only exits here after a failure"))
    }

    async fn stream_page(
        &self,
        page_sql: &str,
        start_cursor: &Cursor,
        tx: &mpsc::Sender<WriteCommand>,
        max_block_size: Option<u64>,
    ) -> Result<Option<PageStats>, PageError> {
        let extract_start = Instant::now();
        let (mut stream, scan_stats) = self
            .extractor
            .open(page_sql, max_block_size)
            .await
            .map_err(PageError::Datalake)?;

        let mut page_cursor = start_cursor.clone();
        let mut page_rows: u64 = 0;
        let mut page_bytes: u64 = 0;
        let mut transform_duration = Duration::ZERO;

        while let Some(block) = stream.next().await {
            let block = block.map_err(PageError::Datalake)?;
            if block.num_rows() == 0 {
                continue;
            }

            page_rows += block.num_rows() as u64;
            page_bytes += block.get_array_memory_size() as u64;
            page_cursor = page_cursor
                .advance(&block, &self.sort_key)
                .map_err(PageError::Fatal)?;

            let transform_start = Instant::now();
            let outputs = self
                .transform
                .transform(&block)
                .await
                .map_err(PageError::Fatal)?;
            transform_duration += transform_start.elapsed();

            for output in outputs {
                if tx.send(WriteCommand::Batch(output)).await.is_err() {
                    return Err(PageError::ReceiverGone);
                }
            }
        }

        // read_* count what the page returned; the scan summary arrives only
        // after the body is fully drained.
        let read_stats = ReadStats {
            read_rows: page_rows,
            read_bytes: page_bytes,
        };
        let scan_stats = scan_stats.await;

        Ok(Some(PageStats {
            cursor: page_cursor,
            rows: page_rows,
            bytes: page_bytes,
            read_stats,
            scan_stats,
            extract_elapsed: extract_start.elapsed(),
            transform_duration,
        }))
    }
}

struct Loader<'a> {
    writers: Vec<PageWriter<'a>>,
}

impl<'a> Loader<'a> {
    fn new(destination: &'a dyn Destination, outputs: &'a [String]) -> Self {
        let writers = outputs
            .iter()
            .map(|table| PageWriter::new(destination, table))
            .collect();
        Self { writers }
    }

    async fn write(&mut self, output: TableBatch) -> Result<(), HandlerError> {
        self.writers[output.output_index]
            .write_batch(&output.batch)
            .await
    }

    /// Returns `(transform index, rows, bytes)` for each table that wrote this page.
    async fn flush_page(&mut self) -> Result<Vec<(usize, u64, u64)>, HandlerError> {
        let mut written = Vec::new();
        for (index, writer) in self.writers.iter_mut().enumerate() {
            let (rows, bytes) = writer.finish_page().await?;
            if rows > 0 {
                written.push((index, rows, bytes));
            }
        }
        Ok(written)
    }
}

/// Opens its insert lazily, so a transform with no rows for a page never opens one.
struct PageWriter<'a> {
    destination: &'a dyn Destination,
    table: &'a str,
    writer: Option<Box<dyn StreamingWriter>>,
    rows: u64,
    bytes: u64,
}

impl<'a> PageWriter<'a> {
    fn new(destination: &'a dyn Destination, table: &'a str) -> Self {
        Self {
            destination,
            table,
            writer: None,
            rows: 0,
            bytes: 0,
        }
    }

    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), HandlerError> {
        if self.writer.is_none() {
            self.writer = Some(
                self.destination
                    .open_streaming_writer(self.table)
                    .await
                    .map_err(|err| {
                        HandlerError::Processing(format!(
                            "failed to create writer for {}: {err}",
                            self.table
                        ))
                    })?,
            );
        }

        self.writer
            .as_mut()
            .expect("writer opened above")
            .write_batch(batch)
            .await
            .map_err(|err| {
                HandlerError::Processing(format!("failed to write to {}: {err}", self.table))
            })?;
        self.rows += batch.num_rows() as u64;
        self.bytes += batch.get_array_memory_size() as u64;
        Ok(())
    }

    async fn finish_page(&mut self) -> Result<(u64, u64), HandlerError> {
        if let Some(writer) = self.writer.take() {
            writer.finish().await.map_err(|err| {
                HandlerError::Processing(format!(
                    "failed to finish write for {}: {err}",
                    self.table
                ))
            })?;
        }
        let counts = (self.rows, self.bytes);
        self.rows = 0;
        self.bytes = 0;
        Ok(counts)
    }
}

#[cfg(test)]
mod tests {
    use super::super::plan::{TransformSpec, Transformation};
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
        ) -> Result<crate::modules::sdlc::datalake::RecordBatchStream<'_>, DatalakeError> {
            let mut count = self.call_count.lock().unwrap();
            *count += 1;

            let rows = if *count == 1 {
                self.batch_size
            } else {
                self.batch_size / 2
            };

            // Split each page into two blocks to exercise multi-block streaming
            // within a page; the last block's last row drives the cursor.
            let first = rows / 2;
            let second = rows - first;
            let blocks = vec![
                Ok(test_batch_range(1, first)),
                Ok(test_batch_range(first as i64 + 1, second)),
            ];
            Ok(Box::pin(futures::stream::iter(blocks)))
        }

        async fn query_batches(
            &self,
            _sql: &str,
            _params: Value,
            _max_block_size: Option<u64>,
        ) -> Result<Vec<RecordBatch>, DatalakeError> {
            Ok(vec![])
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
            max_block_size: Option<u64>,
        ) -> Result<crate::modules::sdlc::datalake::RecordBatchStream<'_>, DatalakeError> {
            self.calls.lock().unwrap().push(max_block_size);
            let mut responses = self.responses.lock().unwrap();
            if responses.is_empty() {
                return Ok(Box::pin(futures::stream::empty()));
            }
            match responses.remove(0) {
                Ok(batches) => {
                    let items: Vec<Result<RecordBatch, DatalakeError>> =
                        batches.into_iter().map(Ok).collect();
                    Ok(Box::pin(futures::stream::iter(items)))
                }
                Err(msg) => Err(DatalakeError::Query(msg.to_string())),
            }
        }

        async fn query_batches(
            &self,
            _sql: &str,
            _params: Value,
            _max_block_size: Option<u64>,
        ) -> Result<Vec<RecordBatch>, DatalakeError> {
            Ok(vec![])
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

    struct StreamFailingDatalake {
        calls: Mutex<Vec<Option<u64>>>,
        recover_at_block_size: u64,
    }

    impl StreamFailingDatalake {
        fn observed_block_sizes(&self) -> Vec<Option<u64>> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl DatalakeQuery for StreamFailingDatalake {
        async fn query_arrow(
            &self,
            _sql: &str,
            _params: Value,
            max_block_size: Option<u64>,
        ) -> Result<crate::modules::sdlc::datalake::RecordBatchStream<'_>, DatalakeError> {
            self.calls.lock().unwrap().push(max_block_size);

            let small_enough =
                matches!(max_block_size, Some(size) if size <= self.recover_at_block_size);
            let blocks: Vec<Result<RecordBatch, DatalakeError>> = if small_enough {
                vec![Ok(test_batch_range(1, 5))]
            } else {
                vec![Err(DatalakeError::Query(
                    "Code: 1002. DB::Exception: Error with a Arrow column \"String\": \
                     Capacity error: array cannot contain more than 2147483646 bytes"
                        .to_string(),
                ))]
            };
            Ok(Box::pin(futures::stream::iter(blocks)))
        }

        async fn query_batches(
            &self,
            _sql: &str,
            _params: Value,
            _max_block_size: Option<u64>,
        ) -> Result<Vec<RecordBatch>, DatalakeError> {
            Ok(vec![])
        }
    }

    #[tokio::test]
    async fn mid_stream_overflow_recovers_via_block_size_halving() {
        let datalake = Arc::new(StreamFailingDatalake {
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
                test_watermark(),
            )
            .await;

        assert!(
            result.is_ok(),
            "mid-stream overflow should self-correct: {result:?}"
        );
        assert_eq!(
            datalake.observed_block_sizes(),
            vec![None, Some(8_000)],
            "first attempt streams at the default and fails mid-drain; the retry \
             must shrink the block rather than abort"
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

        async fn query_arrow_with_scan(
            &self,
            _sql: &str,
            _params: Value,
            _max_block_size: Option<u64>,
        ) -> Result<(RecordBatchStream<'_>, ScanStatsFuture), DatalakeError> {
            let mut calls = self.calls.lock().unwrap();
            *calls += 1;
            let (blocks, stats): (Vec<Result<RecordBatch, DatalakeError>>, ScanStats) =
                if *calls == 1 {
                    (vec![Ok(test_batch(self.rows))], self.scan_stats)
                } else {
                    (vec![], ScanStats::default())
                };
            Ok((
                Box::pin(futures::stream::iter(blocks)),
                Box::pin(async move { stats }),
            ))
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
                test_watermark(),
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
