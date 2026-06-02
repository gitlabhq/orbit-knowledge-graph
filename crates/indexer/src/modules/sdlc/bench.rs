//! In-crate benchmark harness for the SDLC indexing pipeline.
//!
//! Feature-gated behind `bench-sdlc`. Lives inside `crate::modules::sdlc` so it
//! can construct the otherwise module-private `Pipeline`, `Plan`, and
//! `Transformation` and drive `run_plan` against a real ClickHouse instance.
//!
//! It measures end-to-end throughput and ClickHouse ingestion time. Peak heap is
//! measured by the binary that hosts the global allocator (`bin/bench_sdlc.rs`).

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use clickhouse_client::{ArrowClickHouseClient, ClickHouseConfigurationExt};
use gkg_server_config::ClickHouseConfiguration;
use std::sync::Mutex;

use super::datalake::{Datalake, DatalakeQuery};
use super::metrics::SdlcMetrics;
use super::pipeline::{Pipeline, PipelineContext};
use super::plan::{Plan, PreparedQuery, Transformation, WatermarkFilter};
use crate::checkpoint::{Checkpoint, CheckpointError, CheckpointStore};
use crate::destination::{BatchWriter, Destination, DestinationError, StreamingWriter};
use crate::engine::metrics::EngineMetrics;
use crate::nats::ProgressNotifier;
use crate::observer::NoOpObserver;

/// Knobs for one benchmark run. Defaults model a wide SDLC entity (work items
/// with a long `description`) at a volume that exercises multi-page pagination.
#[derive(Debug, Clone)]
pub struct BenchConfig {
    pub url: String,
    pub database: String,
    pub username: String,
    pub password: Option<String>,
    /// Total rows seeded into the source table.
    pub rows: u64,
    /// Pagination unit (SQL `LIMIT`).
    pub batch_size: u64,
    /// Datalake streaming `max_block_size`, in rows.
    pub stream_block_size: u64,
    /// Extract→write read-ahead window, in blocks.
    pub channel_capacity: usize,
    /// Length of the synthetic `description` column, in bytes.
    pub description_len: u64,
}

impl BenchConfig {
    pub fn from_env() -> Self {
        fn var_u64(key: &str, default: u64) -> u64 {
            std::env::var(key)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(default)
        }
        Self {
            url: std::env::var("BENCH_CH_URL")
                .unwrap_or_else(|_| "http://localhost:18123".to_string()),
            database: std::env::var("BENCH_CH_DATABASE").unwrap_or_else(|_| "default".to_string()),
            username: std::env::var("BENCH_CH_USER").unwrap_or_else(|_| "default".to_string()),
            password: std::env::var("BENCH_CH_PASSWORD")
                .ok()
                .or(Some("testpass".to_string())),
            rows: var_u64("BENCH_ROWS", 1_000_000),
            batch_size: var_u64("BENCH_BATCH_SIZE", 500_000),
            stream_block_size: var_u64(
                "BENCH_BLOCK_SIZE",
                super::datalake::DEFAULT_STREAM_BLOCK_SIZE,
            ),
            channel_capacity: var_u64("BENCH_CHANNEL_CAP", 8) as usize,
            description_len: var_u64("BENCH_DESC_LEN", 400),
        }
    }

    fn clickhouse_configuration(&self) -> ClickHouseConfiguration {
        // Mirror production graph insert settings when BENCH_ASYNC_INSERT=1 so the
        // insert round-trip cost matches reality (async inserts ack before the
        // part is written, making extra inserts much cheaper than sync inserts).
        let mut insert_settings = std::collections::HashMap::new();
        if std::env::var("BENCH_ASYNC_INSERT").is_ok() {
            insert_settings.insert("async_insert".to_string(), "1".to_string());
            insert_settings.insert("wait_for_async_insert".to_string(), "1".to_string());
            insert_settings.insert("async_insert_deduplicate".to_string(), "0".to_string());
        }
        // Match production graph settings: without this, ClickHouse runs a merge
        // on every insert into the ReplacingMergeTree destination, which makes
        // many-small-insert workloads look throttled.
        let mut query_settings = std::collections::HashMap::new();
        query_settings.insert("optimize_on_insert".to_string(), "0".to_string());
        ClickHouseConfiguration {
            database: self.database.clone(),
            url: self.url.clone(),
            username: self.username.clone(),
            password: self.password.clone(),
            query_settings,
            insert_settings,
            profiling: Default::default(),
        }
    }
}

/// What one run produced.
#[derive(Debug, Clone)]
pub struct RunStats {
    pub rows_written: u64,
    pub total_elapsed: Duration,
    pub ingestion_elapsed: Duration,
    pub bytes_written: u64,
}

impl RunStats {
    pub fn throughput_rows_per_sec(&self) -> f64 {
        self.rows_written as f64 / self.total_elapsed.as_secs_f64().max(f64::EPSILON)
    }

    pub fn ingestion_rows_per_sec(&self) -> f64 {
        self.rows_written as f64 / self.ingestion_elapsed.as_secs_f64().max(f64::EPSILON)
    }
}

const SOURCE_TABLE: &str = "bench_source";
const DEST_TABLE: &str = "bench_dest";

/// A prepared benchmark: source data seeded, tables created, pipeline wired.
/// Split from `run` so the allocation-heavy setup is excluded from the measured
/// section.
pub struct Benchmark {
    pipeline: Pipeline,
    context: PipelineContext,
    plan: Plan,
    base_query: PreparedQuery,
    position_key: String,
    watermark: DateTime<Utc>,
    timing: Arc<TimingDestination>,
}

impl Benchmark {
    pub async fn setup(config: &BenchConfig) -> Self {
        let ch_config = config.clickhouse_configuration();
        let client = Arc::new(ch_config.build_client());

        Self::seed_source(&client, config).await;
        Self::create_destination(&client).await;

        let datalake: Arc<dyn DatalakeQuery> =
            Arc::new(Datalake::new(Arc::clone(&client), config.stream_block_size));

        let checkpoint_store: Arc<dyn CheckpointStore> =
            Arc::new(InMemoryCheckpointStore::default());

        let inner_destination = crate::clickhouse::ClickHouseDestination::new(
            ch_config,
            Arc::new(EngineMetrics::new()),
        )
        .expect("destination");
        let counters = Arc::new(Counters::default());
        let timing = Arc::new(TimingDestination {
            inner: Box::new(inner_destination),
            counters: Arc::clone(&counters),
        });

        let pipeline = Pipeline::new(
            Arc::clone(&datalake),
            Arc::clone(&checkpoint_store),
            SdlcMetrics::new(),
            Default::default(),
        )
        .with_write_channel_capacity(config.channel_capacity);

        let context = PipelineContext {
            destination: timing.clone(),
            progress: ProgressNotifier::noop(),
            observer: Arc::new(Mutex::new(NoOpObserver)),
        };

        let plan = bench_plan(config.batch_size);
        let watermark: DateTime<Utc> = Utc::now() + chrono::Duration::days(1);
        let base_query = plan.prepare().with(WatermarkFilter {
            column: &plan.watermark_column,
            last: DateTime::<Utc>::UNIX_EPOCH,
            current: watermark,
        });

        Self {
            pipeline,
            context,
            plan,
            base_query,
            position_key: "bench.WorkItem".to_string(),
            watermark,
            timing,
        }
    }

    pub async fn run(&self) -> RunStats {
        let counters = &self.timing.counters;
        counters.reset();
        let started = Instant::now();
        self.pipeline
            .run_plan(
                &self.context,
                &self.plan,
                self.base_query.clone(),
                &self.position_key,
                self.watermark,
            )
            .await
            .expect("run_plan failed");
        let total_elapsed = started.elapsed();

        RunStats {
            rows_written: counters.rows.load(Ordering::Relaxed),
            total_elapsed,
            ingestion_elapsed: Duration::from_nanos(counters.write_nanos.load(Ordering::Relaxed)),
            bytes_written: counters.bytes.load(Ordering::Relaxed),
        }
    }

    async fn seed_source(client: &ArrowClickHouseClient, config: &BenchConfig) {
        client
            .execute(&format!("DROP TABLE IF EXISTS {SOURCE_TABLE}"))
            .await
            .expect("drop source");
        client
            .execute(&format!(
                "CREATE TABLE {SOURCE_TABLE} (
                    id Int64,
                    title String,
                    description String,
                    author_id Int64,
                    state_id Int8,
                    namespace_id Int64,
                    traversal_path String,
                    created_at DateTime64(3),
                    _siphon_replicated_at DateTime64(3),
                    _siphon_deleted Bool
                ) ENGINE = MergeTree ORDER BY id"
            ))
            .await
            .expect("create source");

        let rows = config.rows;
        let desc_len = config.description_len;
        client
            .execute(&format!(
                "INSERT INTO {SOURCE_TABLE}
                 SELECT
                    number AS id,
                    concat('Work item ', toString(number)) AS title,
                    randomPrintableASCII({desc_len}) AS description,
                    (number % 1000) AS author_id,
                    toInt8(number % 5) AS state_id,
                    (number % 50) AS namespace_id,
                    concat('1/', toString(number % 50), '/') AS traversal_path,
                    now64(3) AS created_at,
                    now64(3) AS _siphon_replicated_at,
                    false AS _siphon_deleted
                 FROM numbers({rows})"
            ))
            .await
            .expect("seed source");
    }

    async fn create_destination(client: &ArrowClickHouseClient) {
        client
            .execute(&format!("DROP TABLE IF EXISTS {DEST_TABLE}"))
            .await
            .expect("drop dest");
        client
            .execute(&format!(
                "CREATE TABLE {DEST_TABLE} (
                    id Int64,
                    title String,
                    description String,
                    author_id Int64,
                    state LowCardinality(String),
                    namespace_id Int64,
                    traversal_path String,
                    _version DateTime64(3)
                ) ENGINE = ReplacingMergeTree(_version) ORDER BY id"
            ))
            .await
            .expect("create dest");
    }
}

fn bench_plan(batch_size: u64) -> Plan {
    Plan {
        name: "WorkItem".to_string(),
        extract_template: format!(
            "SELECT id, title, description, author_id, state_id, namespace_id, traversal_path, \
             _siphon_replicated_at AS _version, _siphon_deleted AS _deleted \
             FROM {SOURCE_TABLE} \
             WHERE 1=1 {{{{filters}}}} \
             ORDER BY id \
             LIMIT {{{{batch_size}}}}"
        ),
        watermark_column: "_siphon_replicated_at".to_string(),
        sort_key: vec!["id".to_string()],
        batch_size,
        transforms: vec![Transformation {
            sql: format!(
                "SELECT id, title, description, author_id, \
                 CAST(state_id AS VARCHAR) AS state, namespace_id, traversal_path, _version \
                 FROM {table}",
                table = super::plan::SOURCE_DATA_TABLE
            ),
            destination_table: DEST_TABLE.to_string(),
            // Exercise the dict-encoding path (LowCardinality) across blocks.
            dict_encode_columns: HashSet::from(["state".to_string()]),
        }],
    }
}

#[derive(Default)]
struct Counters {
    write_nanos: AtomicU64,
    rows: AtomicU64,
    bytes: AtomicU64,
}

impl Counters {
    fn reset(&self) {
        self.write_nanos.store(0, Ordering::Relaxed);
        self.rows.store(0, Ordering::Relaxed);
        self.bytes.store(0, Ordering::Relaxed);
    }
}

/// Wraps a real `Destination`, summing per-write wall-clock, rows, and bytes so
/// the benchmark can report ClickHouse ingestion speed separately from
/// end-to-end throughput.
struct TimingDestination {
    inner: Box<dyn Destination>,
    counters: Arc<Counters>,
}

#[async_trait]
impl Destination for TimingDestination {
    async fn new_batch_writer(
        &self,
        table: &str,
    ) -> Result<Box<dyn BatchWriter>, DestinationError> {
        let inner = self.inner.new_batch_writer(table).await?;
        Ok(Box::new(TimingWriter {
            inner,
            counters: Arc::clone(&self.counters),
        }))
    }

    // Forward to the inner destination's real streaming writer (ClickHouse's
    // true per-batch streaming insert). Without this override the default
    // buffering writer would accumulate the whole page, which is NOT what the
    // production pipeline does.
    async fn open_streaming_writer(
        &self,
        table: &str,
    ) -> Result<Box<dyn StreamingWriter>, DestinationError> {
        let inner = self.inner.open_streaming_writer(table).await?;
        Ok(Box::new(TimingStreamingWriter {
            inner,
            counters: Arc::clone(&self.counters),
        }))
    }
}

struct TimingStreamingWriter {
    inner: Box<dyn StreamingWriter>,
    counters: Arc<Counters>,
}

#[async_trait]
impl StreamingWriter for TimingStreamingWriter {
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), DestinationError> {
        let rows = batch.num_rows() as u64;
        let bytes = batch.get_array_memory_size() as u64;
        let start = Instant::now();
        let result = self.inner.write_batch(batch).await;
        self.counters
            .write_nanos
            .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        self.counters.rows.fetch_add(rows, Ordering::Relaxed);
        self.counters.bytes.fetch_add(bytes, Ordering::Relaxed);
        result
    }

    async fn finish(self: Box<Self>) -> Result<(), DestinationError> {
        let start = Instant::now();
        let result = self.inner.finish().await;
        self.counters
            .write_nanos
            .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        result
    }
}

struct TimingWriter {
    inner: Box<dyn BatchWriter>,
    counters: Arc<Counters>,
}

#[async_trait]
impl BatchWriter for TimingWriter {
    async fn write_batch(&self, batches: &[RecordBatch]) -> Result<(), DestinationError> {
        let row_count: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        let byte_count: u64 = batches
            .iter()
            .map(|b| b.get_array_memory_size() as u64)
            .sum();
        let start = Instant::now();
        let result = self.inner.write_batch(batches).await;
        let elapsed = start.elapsed();
        self.counters
            .write_nanos
            .fetch_add(elapsed.as_nanos() as u64, Ordering::Relaxed);
        self.counters.rows.fetch_add(row_count, Ordering::Relaxed);
        self.counters.bytes.fetch_add(byte_count, Ordering::Relaxed);
        result
    }
}

#[derive(Default)]
struct InMemoryCheckpointStore {
    state: Mutex<Option<Checkpoint>>,
}

#[async_trait]
impl CheckpointStore for InMemoryCheckpointStore {
    async fn load(&self, _key: &str) -> Result<Option<Checkpoint>, CheckpointError> {
        Ok(self.state.lock().unwrap().clone())
    }

    async fn save_progress(
        &self,
        _key: &str,
        checkpoint: &Checkpoint,
    ) -> Result<(), CheckpointError> {
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
