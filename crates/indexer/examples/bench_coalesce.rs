//! Iteration 4: production validation at scale.
//!
//! Mirrors the exact production code path from pipeline.rs:
//!   - Coalesce all transform outputs (no dict encoding during transform)
//!   - Sort via DataFusion by table ORDER BY key
//!   - Dict-encode after sort
//!   - split_into_chunks(batches, WRITE_PARALLELISM=14) — row-count based
//!   - Write all chunks in parallel via FuturesUnordered
//!
//! Extracts source data ONCE up front, reuses across tiers.
//!
//! Usage:
//!   CH_URL=https://... CH_USER=... CH_PASS=... cargo run --release --example bench_coalesce -p indexer

use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use std::time::Instant;

use arrow::record_batch::RecordBatch;
use clickhouse_client::ArrowClickHouseClient;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use gkg_utils::arrow::prepare_batches;

const WRITE_PARALLELISM: usize = 14;

const SORT_ORDER: &str =
    "traversal_path, source_id, relationship_kind, target_id, source_kind, target_kind";

const CI_EDGE_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS {name} (
    traversal_path String DEFAULT '0/' CODEC(ZSTD(1)),
    source_id Int64 CODEC(Delta(8), LZ4),
    source_kind LowCardinality(String) CODEC(LZ4),
    relationship_kind LowCardinality(String) CODEC(LZ4),
    target_id Int64 CODEC(Delta(8), LZ4),
    target_kind LowCardinality(String) CODEC(LZ4),
    source_tags Array(LowCardinality(String)) CODEC(LZ4),
    target_tags Array(LowCardinality(String)) CODEC(LZ4),
    _version DateTime64(6, 'UTC') DEFAULT now64(6) CODEC(ZSTD(1)),
    _deleted Bool DEFAULT false,
    INDEX idx_relationship relationship_kind TYPE set(50) GRANULARITY 2,
    INDEX source_tags_idx source_tags TYPE text(tokenizer = 'array') GRANULARITY 64,
    INDEX target_tags_idx target_tags TYPE text(tokenizer = 'array') GRANULARITY 64,
    PROJECTION by_source (SELECT * ORDER BY (source_id, relationship_kind, target_id, traversal_path, source_kind, target_kind)),
    PROJECTION by_target (SELECT * ORDER BY (target_id, relationship_kind, source_id, traversal_path, source_kind, target_kind)),
    PROJECTION by_rel_source_kind (SELECT * ORDER BY (relationship_kind, source_kind, source_id, target_id, traversal_path, target_kind)),
    PROJECTION by_rel_target_kind (SELECT * ORDER BY (relationship_kind, target_kind, target_id, source_id, traversal_path, source_kind)),
    PROJECTION agg_counts_by_source (
      SELECT relationship_kind, target_kind, source_id, traversal_path, count()
      GROUP BY relationship_kind, target_kind, source_id, traversal_path
    ),
    PROJECTION agg_counts_by_target (
      SELECT relationship_kind, source_kind, target_id, traversal_path, count()
      GROUP BY relationship_kind, source_kind, target_id, traversal_path
    )
) ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY (traversal_path, source_id, relationship_kind, target_id, source_kind, target_kind)
PRIMARY KEY (traversal_path, source_id, relationship_kind)
SETTINGS index_granularity = 1024,
    deduplicate_merge_projection_mode = 'rebuild',
    allow_experimental_replacing_merge_with_cleanup = 1
"#;

struct EdgeTransform {
    relationship_kind: &'static str,
    target_expr: &'static str,
    target_kind: &'static str,
}

const EDGE_TRANSFORMS: &[EdgeTransform] = &[
    EdgeTransform {
        relationship_kind: "has_pipeline",
        target_expr: "coalesce(upstream_pipeline_id, 0)",
        target_kind: "Pipeline",
    },
    EdgeTransform {
        relationship_kind: "ran_in_stage",
        target_expr: "coalesce(stage_id, 0)",
        target_kind: "CiStage",
    },
    EdgeTransform {
        relationship_kind: "in_pipeline",
        target_expr: "coalesce(partition_id, 0)",
        target_kind: "Pipeline",
    },
    EdgeTransform {
        relationship_kind: "built_by",
        target_expr: "coalesce(user_id, 0)",
        target_kind: "User",
    },
    EdgeTransform {
        relationship_kind: "runs_on",
        target_expr: "coalesce(runner_id, 0)",
        target_kind: "Runner",
    },
    EdgeTransform {
        relationship_kind: "auto_canceled_by",
        target_expr: "coalesce(auto_canceled_by_id, 0)",
        target_kind: "Pipeline",
    },
    EdgeTransform {
        relationship_kind: "has_project",
        target_expr: "project_id",
        target_kind: "Project",
    },
];

const DICT_ENCODE_COLS: &[&str] = &["source_kind", "relationship_kind", "target_kind"];

fn transform_sql(et: &EdgeTransform) -> String {
    format!(
        "SELECT traversal_path, id as source_id, 'Job' as source_kind, \
         '{}' as relationship_kind, \
         {} as target_id, '{}' as target_kind, \
         [] as source_tags, [] as target_tags, \
         now() as _version, false as _deleted \
         FROM source_data",
        et.relationship_kind, et.target_expr, et.target_kind
    )
}

fn dict_columns() -> std::collections::HashSet<String> {
    DICT_ENCODE_COLS.iter().map(|s| s.to_string()).collect()
}

fn batch_memory_bytes(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.get_array_memory_size()).sum()
}

fn batch_row_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

fn make_client() -> ArrowClickHouseClient {
    let url = std::env::var("CH_URL").expect("CH_URL required");
    let user = std::env::var("CH_USER").expect("CH_USER required");
    let pass = std::env::var("CH_PASS").ok();
    let mut settings = HashMap::new();
    settings.insert("optimize_on_insert".to_string(), "0".to_string());
    settings.insert("insert_deduplicate".to_string(), "0".to_string());
    ArrowClickHouseClient::new(&url, "default", &user, pass.as_deref(), &settings)
}

async fn exec_ddl(client: &ArrowClickHouseClient, sql: &str) {
    client.query(sql).fetch_arrow().await.ok();
}

async fn register_source(session: &SessionContext, batches: &[RecordBatch]) {
    let schema = batches[0].schema();
    let _ = session.deregister_table("source_data");
    let mem_table = MemTable::try_new(schema, vec![batches.to_vec()]).unwrap();
    session
        .register_table("source_data", Arc::new(mem_table))
        .unwrap();
}

async fn run_transform(session: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    let df = session.sql(sql).await.unwrap();
    df.collect().await.unwrap()
}

/// Identical to pipeline.rs split_into_chunks
fn split_into_chunks(batches: Vec<RecordBatch>, target_chunks: usize) -> Vec<Vec<RecordBatch>> {
    if batches.is_empty() || target_chunks <= 1 {
        return vec![batches];
    }

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    let rows_per_chunk = (total_rows / target_chunks).max(1);
    let mut chunks: Vec<Vec<RecordBatch>> = Vec::with_capacity(target_chunks);
    let mut current_chunk = Vec::new();
    let mut current_rows = 0usize;

    for batch in batches {
        current_rows += batch.num_rows();
        current_chunk.push(batch);

        if current_rows >= rows_per_chunk && chunks.len() < target_chunks - 1 {
            chunks.push(std::mem::take(&mut current_chunk));
            current_rows = 0;
        }
    }

    if !current_chunk.is_empty() {
        chunks.push(current_chunk);
    }

    chunks
}

fn median(xs: &[f64]) -> f64 {
    let mut s = xs.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = s.len();
    if n % 2 == 1 {
        s[n / 2]
    } else {
        (s[n / 2 - 1] + s[n / 2]) / 2.0
    }
}

fn fmt_bytes(b: usize) -> String {
    if b > 1_073_741_824 {
        format!("{:.1} GiB", b as f64 / 1_073_741_824.0)
    } else {
        format!("{:.1} MiB", b as f64 / 1_048_576.0)
    }
}

macro_rules! log {
    ($($arg:tt)*) => {{
        println!($($arg)*);
        let _ = std::io::stdout().flush();
    }};
}

fn table_name(experiment_name: &str) -> String {
    format!(
        "bench_{}",
        experiment_name
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
            .collect::<String>()
            .to_lowercase()
    )
}

// ── Strategies ────────────────────────────────────────────────────────────

struct CycleResult {
    total_secs: f64,
    write_secs: f64,
    transform_secs: f64,
    sort_secs: f64,
    peak_memory_bytes: usize,
    total_rows_written: usize,
    insert_count: usize,
}

/// Old production path: per-transform dict-encode + FuturesUnordered drain
async fn old_write_drain(
    client: &ArrowClickHouseClient,
    table: &str,
    session: &SessionContext,
) -> CycleResult {
    let dict_cols = dict_columns();
    let start = Instant::now();

    let mut peak_mem: usize = 0;
    let mut current_mem: usize = 0;
    let mut total_rows: usize = 0;
    let mut write_futures = FuturesUnordered::new();
    let mut transform_elapsed = std::time::Duration::ZERO;

    for et in EDGE_TRANSFORMS {
        let t0 = Instant::now();
        let mut batches = run_transform(session, &transform_sql(et)).await;
        prepare_batches(&mut batches, &dict_cols);
        transform_elapsed += t0.elapsed();

        let mem = batch_memory_bytes(&batches);
        current_mem += mem;
        peak_mem = peak_mem.max(current_mem);
        total_rows += batch_row_count(&batches);

        let client = client.clone();
        let tbl = table.to_string();
        let batch_mem = mem;
        write_futures.push(async move {
            client.insert_arrow_streaming(&tbl, &batches).await.unwrap();
            batch_mem
        });

        while let Some(Some(freed)) = write_futures.next().now_or_never() {
            current_mem = current_mem.saturating_sub(freed);
        }
    }

    let write_start = Instant::now();
    while let Some(freed) = write_futures.next().await {
        current_mem = current_mem.saturating_sub(freed);
    }
    let write_secs = write_start.elapsed().as_secs_f64();

    CycleResult {
        total_secs: start.elapsed().as_secs_f64(),
        write_secs,
        transform_secs: transform_elapsed.as_secs_f64(),
        sort_secs: 0.0,
        peak_memory_bytes: peak_mem,
        total_rows_written: total_rows,
        insert_count: EDGE_TRANSFORMS.len(),
    }
}

/// New production path: coalesce → sort → dict-encode → split(14) → parallel write
async fn production_presorted(
    client: &ArrowClickHouseClient,
    table: &str,
    session: &SessionContext,
) -> CycleResult {
    let dict_cols = dict_columns();
    let start = Instant::now();

    let mut all_batches = Vec::new();
    for et in EDGE_TRANSFORMS {
        let batches = run_transform(session, &transform_sql(et)).await;
        all_batches.extend(batches);
    }
    let total_rows = batch_row_count(&all_batches);
    let transform_secs = start.elapsed().as_secs_f64();

    let sort_start = Instant::now();
    let schema = all_batches[0].schema();
    let _ = session.deregister_table("_presort_staging");
    let mem_table = MemTable::try_new(schema, vec![all_batches]).unwrap();
    session
        .register_table("_presort_staging", Arc::new(mem_table))
        .unwrap();
    let mut sorted_batches = session
        .sql(&format!(
            "SELECT * FROM _presort_staging ORDER BY {SORT_ORDER}"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let _ = session.deregister_table("_presort_staging");
    let sort_secs = sort_start.elapsed().as_secs_f64();

    prepare_batches(&mut sorted_batches, &dict_cols);
    let peak_mem = batch_memory_bytes(&sorted_batches);

    let chunks = split_into_chunks(sorted_batches, WRITE_PARALLELISM);
    let actual_chunks = chunks.len();

    let mut write_futures = FuturesUnordered::new();
    for chunk in chunks {
        let client = client.clone();
        let tbl = table.to_string();
        write_futures.push(async move {
            client.insert_arrow_streaming(&tbl, &chunk).await.unwrap();
        });
    }

    let write_start = Instant::now();
    while write_futures.next().await.is_some() {}
    let write_secs = write_start.elapsed().as_secs_f64();

    CycleResult {
        total_secs: start.elapsed().as_secs_f64(),
        write_secs,
        transform_secs,
        sort_secs,
        peak_memory_bytes: peak_mem,
        total_rows_written: total_rows,
        insert_count: actual_chunks,
    }
}

// ── Harness ──────────────────────────────────────────────────────────────

async fn run_experiment<F>(
    name: &str,
    client: &ArrowClickHouseClient,
    source_data: &[RecordBatch],
    cycles: usize,
    run_fn: F,
) where
    F: for<'a> Fn(
        &'a ArrowClickHouseClient,
        &'a str,
        &'a SessionContext,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = CycleResult> + 'a>>,
{
    let table = table_name(name);
    exec_ddl(client, &format!("DROP TABLE IF EXISTS {table}")).await;
    exec_ddl(client, &CI_EDGE_DDL.replace("{name}", &table)).await;

    log!("── {name} ──");

    let session = SessionContext::new();
    register_source(&session, source_data).await;

    // Warmup
    let warmup = run_fn(client, &table, &session).await;
    log!(
        "  warmup: {:.1}s  rows={} mem={} inserts={}  xform={:.2}s sort={:.2}s",
        warmup.total_secs,
        warmup.total_rows_written,
        fmt_bytes(warmup.peak_memory_bytes),
        warmup.insert_count,
        warmup.transform_secs,
        warmup.sort_secs,
    );

    // Reset table for clean measurement
    exec_ddl(client, &format!("DROP TABLE IF EXISTS {table}")).await;
    exec_ddl(client, &CI_EDGE_DDL.replace("{name}", &table)).await;

    let mut cycle_totals = Vec::new();
    let mut peak_mem: usize = 0;
    let mut total_rows: usize = 0;
    let mut total_inserts: usize = 0;

    for cycle in 0..cycles {
        let result = run_fn(client, &table, &session).await;
        cycle_totals.push(result.total_secs);
        peak_mem = peak_mem.max(result.peak_memory_bytes);
        total_rows += result.total_rows_written;
        total_inserts += result.insert_count;

        log!(
            "  cycle {:2}/{}: {:.1}s  xform={:.2}s sort={:.2}s write={:.1}s  mem={} inserts={} rows={}",
            cycle + 1,
            cycles,
            result.total_secs,
            result.transform_secs,
            result.sort_secs,
            result.write_secs,
            fmt_bytes(result.peak_memory_bytes),
            result.insert_count,
            result.total_rows_written,
        );
    }

    let grand_total: f64 = cycle_totals.iter().sum();
    let rows_per_sec = total_rows as f64 / grand_total;
    log!(
        "  ► TOTAL={:.1}s  median={:.1}s  peak_mem={}  rows={}  inserts={}  throughput={:.0} rows/s\n",
        grand_total,
        median(&cycle_totals),
        fmt_bytes(peak_mem),
        total_rows,
        total_inserts,
        rows_per_sec,
    );

    exec_ddl(client, &format!("DROP TABLE IF EXISTS {table}")).await;
}

#[tokio::main]
async fn main() {
    let client = make_client();

    log!("================================================================");
    log!("ITERATION 4: PRODUCTION VALIDATION AT SCALE");
    log!("================================================================");
    log!("  Code path: pipeline.rs transform_and_write (presorted split)");
    log!("  WRITE_PARALLELISM = {WRITE_PARALLELISM}");
    log!("  split_into_chunks: row-count based (production identical)");
    log!("  All 6 projections enabled (full gl_ci_edge schema)");
    log!();

    // Extract all source data ONCE up front via Arrow streaming.
    // Single query avoids OFFSET scanning overhead on the 100M row table.
    let max_source = 8_000_000usize;
    log!("Extracting {max_source} source rows (single query, Arrow stream)...");

    let extract_start = Instant::now();
    let sql = format!(
        "SELECT * FROM siphon_p_ci_builds LIMIT {max_source}"
    );
    let all_source = client.query_arrow(&sql).await.expect("extract failed");
    let total_source: usize = all_source.iter().map(|b| b.num_rows()).sum();
    let extract_secs = extract_start.elapsed().as_secs_f64();
    log!(
        "  extracted {} source rows in {:.1}s ({:.1} MiB, {} batches)\n",
        total_source,
        extract_secs,
        batch_memory_bytes(&all_source) as f64 / 1_048_576.0,
        all_source.len(),
    );

    // Helper: slice source data to a given row count
    fn slice_source(all: &[RecordBatch], target_rows: usize) -> Vec<RecordBatch> {
        let mut result = Vec::new();
        let mut remaining = target_rows;
        for batch in all {
            if remaining == 0 {
                break;
            }
            if batch.num_rows() <= remaining {
                result.push(batch.clone());
                remaining -= batch.num_rows();
            } else {
                result.push(batch.slice(0, remaining));
                remaining = 0;
            }
        }
        result
    }

    // ── Scale tiers ─────────────────────────────────────────────────────
    // (source_rows, cycles)
    let tiers: &[(usize, usize)] = &[
        (500_000, 5),     // 3.5M edge rows — baseline validation
        (1_000_000, 3),   // 7M edge rows
        (2_000_000, 3),   // 14M edge rows
        (5_000_000, 3),   // 35M edge rows
        (8_000_000, 3),   // 56M edge rows — the target
    ];

    for &(source_rows, cycles) in tiers {
        let edge_rows = source_rows * EDGE_TRANSFORMS.len();
        log!("────────────────────────────────────────────────────────────────");
        log!(
            "SCALE: {} source → {} edge rows/cycle ({:.1}M)  [{} cycles]",
            source_rows,
            edge_rows,
            edge_rows as f64 / 1_000_000.0,
            cycles,
        );
        log!("────────────────────────────────────────────────────────────────");

        if source_rows > total_source {
            log!("  ⚠ not enough source data ({total_source} available), skipping\n");
            continue;
        }

        let source = slice_source(&all_source, source_rows);
        let actual = batch_row_count(&source);
        log!("  source slice: {actual} rows\n");

        run_experiment(
            &format!("old_drain_{edge_rows}"),
            &client,
            &source,
            cycles,
            |c, t, s| Box::pin(old_write_drain(c, t, s)),
        )
        .await;

        run_experiment(
            &format!("presorted_{edge_rows}"),
            &client,
            &source,
            cycles,
            |c, t, s| Box::pin(production_presorted(c, t, s)),
        )
        .await;
    }

    log!("================================================================");
    log!("ALL EXPERIMENTS COMPLETE");
    log!("================================================================");
}
