use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use arrow::array::{Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use clickhouse_client::{ArrowClickHouseClient, IpcCompressionType};
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use gkg_utils::arrow::prepare_batches;
use tokio::sync::Semaphore;

#[derive(Debug, Clone)]
pub struct BenchConfig {
    pub clickhouse_url: String,
    pub database: String,
    pub username: String,
    pub password: Option<String>,
    pub total_rows: u64,
    pub batch_size: u64,
    pub max_concurrent_writes: usize,
    pub seed_batch_size: u64,
    pub lock_file: PathBuf,
    pub max_batches: Option<u64>,
    pub async_insert: bool,
    pub coalesce_edges: bool,
    pub write_chunk_size: Option<usize>,
    pub pipeline: bool,
    pub use_offset: bool,
    pub zstd: bool,
}

impl Default for BenchConfig {
    fn default() -> Self {
        Self {
            clickhouse_url: "http://localhost:8123".into(),
            database: "default".into(),
            username: "default".into(),
            password: None,
            total_rows: 100_000_000,
            batch_size: 500_000,
            max_concurrent_writes: 3,
            seed_batch_size: 1_000_000,
            lock_file: PathBuf::from(".bench-seeded.lock"),
            max_batches: None,
            async_insert: false,
            coalesce_edges: false,
            write_chunk_size: None,
            pipeline: false,
            use_offset: false,
            zstd: false,
        }
    }
}

struct Cursor {
    traversal_path: String,
    id: i64,
    partition_id: i64,
}

struct TransformSpec {
    sql: String,
    destination_table: String,
    dict_encode_columns: HashSet<String>,
}

struct WriteJob {
    destination_table: String,
    batches: Vec<RecordBatch>,
    row_count: usize,
}

#[derive(Default)]
struct RunStats {
    total_rows_extracted: u64,
    total_rows_written: u64,
    total_extract_time: Duration,
    total_transform_time: Duration,
    total_write_time: Duration,
    batch_count: u64,
}

pub async fn run(config: &BenchConfig) -> Result<()> {
    let client = build_client(config);

    println!("=== Write Pipeline Benchmark ===");
    println!(
        "  batch_size: {}  max_concurrent_writes: {}  total_rows: {}",
        config.batch_size, config.max_concurrent_writes, config.total_rows
    );
    if let Some(max) = config.max_batches {
        println!("  max_batches: {max}");
    }
    if config.async_insert {
        println!("  async_insert: enabled");
    }
    if config.coalesce_edges {
        println!("  coalesce_edges: enabled (single write for all edge transforms)");
    }
    if let Some(chunk) = config.write_chunk_size {
        println!("  write_chunk_size: {chunk} rows");
    }
    if config.pipeline {
        println!("  pipeline: enabled (overlap extract and write)");
    }
    if config.use_offset {
        println!("  pagination: OFFSET (legacy)");
    }
    if config.zstd {
        println!("  compression: ZSTD");
    }

    seed_if_needed(&client, config).await?;
    reset_graph_tables(&client).await?;

    let transforms = job_transforms();
    let stats = run_benchmark(&client, config, &transforms).await?;

    print_results(&stats);
    Ok(())
}

fn build_client(config: &BenchConfig) -> ArrowClickHouseClient {
    let mut settings = std::collections::HashMap::new();
    settings.insert("max_execution_time".into(), "3600".into());
    settings.insert("send_timeout".into(), "3600".into());
    settings.insert("receive_timeout".into(), "3600".into());
    if config.async_insert {
        settings.insert("async_insert".into(), "1".into());
        settings.insert("wait_for_async_insert".into(), "1".into());
    }
    ArrowClickHouseClient::new(
        &config.clickhouse_url,
        &config.database,
        &config.username,
        config.password.as_deref(),
        &settings,
    )
}

async fn seed_if_needed(client: &ArrowClickHouseClient, config: &BenchConfig) -> Result<()> {
    if config.lock_file.exists() {
        let meta = std::fs::read_to_string(&config.lock_file)?;
        println!("  seed: skipped (lock file exists: {meta})");
        return Ok(());
    }

    println!("  seed: creating siphon_p_ci_builds table...");
    client
        .execute(include_str!("seed_ci_builds.sql"))
        .await
        .context("failed to create siphon_p_ci_builds")?;

    let row_count: u64 = client
        .inner()
        .query("SELECT count() FROM siphon_p_ci_builds")
        .fetch_one()
        .await
        .context("failed to count rows")?;

    if row_count >= config.total_rows {
        println!("  seed: table already has {row_count} rows, skipping insert");
        let lock_content = format!("seeded={row_count}");
        std::fs::write(&config.lock_file, &lock_content)?;
        return Ok(());
    }

    println!(
        "  seed: inserting {} rows in batches of {}...",
        config.total_rows, config.seed_batch_size
    );
    let seed_start = Instant::now();
    let remaining = config.total_rows - row_count;
    let batches_needed = remaining.div_ceil(config.seed_batch_size);

    for i in 0..batches_needed {
        let offset = row_count + (i * config.seed_batch_size);
        let batch_rows = config
            .seed_batch_size
            .min(remaining - (i * config.seed_batch_size));
        let sql = seed_insert_sql(offset, batch_rows);
        client
            .execute(&sql)
            .await
            .with_context(|| format!("failed to seed batch {}/{batches_needed}", i + 1))?;
        if (i + 1) % 10 == 0 || i + 1 == batches_needed {
            let elapsed = seed_start.elapsed();
            let inserted = (i + 1) * config.seed_batch_size;
            println!(
                "    {}/{} batches ({:.0}M rows, {:.1}s)",
                i + 1,
                batches_needed,
                inserted as f64 / 1e6,
                elapsed.as_secs_f64()
            );
        }
    }

    let elapsed = seed_start.elapsed();
    println!("  seed: done in {:.1}s", elapsed.as_secs_f64());

    let lock_content = format!("seeded={}", config.total_rows);
    std::fs::write(&config.lock_file, &lock_content)?;
    Ok(())
}

fn seed_insert_sql(offset: u64, batch_rows: u64) -> String {
    // Server-side data generation — no network transfer for the bulk data.
    // Produces realistic-ish siphon_p_ci_builds rows with proper distributions.
    format!(
        r#"
INSERT INTO siphon_p_ci_builds (
    id, project_id, commit_id, stage_id, user_id, runner_id, partition_id,
    status, name, ref, type, `when`, allow_failure, retried,
    failure_reason, coverage, timeout, timeout_source, scheduling_type,
    environment, auto_canceled_by_id, upstream_pipeline_id,
    created_at, started_at, finished_at, queued_at,
    traversal_path, _siphon_replicated_at, _siphon_deleted
)
SELECT
    {offset} + number AS id,
    1 + (rand64() % 50000) AS project_id,
    1 + (rand64() % 10000000) AS commit_id,
    1 + (rand64() % 5000000) AS stage_id,
    if(rand() % 10 < 8, toNullable(1 + toInt64(rand64() % 100000)), NULL) AS user_id,
    if(rand() % 10 < 7, toNullable(1 + toInt64(rand64() % 5000)), NULL) AS runner_id,
    100 + (rand64() % 10) AS partition_id,
    arrayElement(
        ['created','pending','running','success','failed','canceled','skipped','manual'],
        1 + (rand() % 8)
    ) AS status,
    concat('job_', toString(rand() % 500)) AS name,
    concat('refs/heads/branch-', toString(rand() % 1000)) AS ref,
    if(rand() % 20 = 0, 'Ci::Bridge', 'Ci::Build') AS type,
    arrayElement(['on_success','on_failure','always','manual','delayed'], 1 + (rand() % 5)) AS `when`,
    rand() % 20 = 0 AS allow_failure,
    if(rand() % 10 < 2, toNullable(true), toNullable(false)) AS retried,
    if(rand() % 5 = 0, toNullable(toInt64(arrayElement([1,2,3,4,5,6,7,8,9,10], 1 + (rand() % 10)))), NULL) AS failure_reason,
    if(rand() % 3 = 0, toNullable(toFloat64(rand() % 10000) / 100.0), NULL) AS coverage,
    if(rand() % 4 = 0, toNullable(toInt64(300 + rand() % 7200)), NULL) AS timeout,
    if(rand() % 4 = 0, toNullable(toInt16(1 + rand() % 4)), NULL) AS timeout_source,
    if(rand() % 3 = 0, toNullable(toInt16(rand() % 2)), NULL) AS scheduling_type,
    if(rand() % 10 = 0, 'production', '') AS environment,
    if(rand() % 50 = 0, toNullable(toInt64(rand64() % 100000000)), NULL) AS auto_canceled_by_id,
    if(rand() % 10 = 0, toNullable(toInt64(rand64() % 10000000)), NULL) AS upstream_pipeline_id,
    now() - toIntervalSecond(rand() % 2592000) AS created_at,
    if(rand() % 10 < 8, toNullable(now() - toIntervalSecond(rand() % 2592000)), NULL) AS started_at,
    if(rand() % 10 < 7, toNullable(now() - toIntervalSecond(rand() % 2592000)), NULL) AS finished_at,
    if(rand() % 10 < 6, toNullable(now() - toIntervalSecond(rand() % 2592000)), NULL) AS queued_at,
    concat(toString(1 + rand64() % 1000), '/', toString(1 + rand64() % 10000), '/') AS traversal_path,
    now() AS _siphon_replicated_at,
    false AS _siphon_deleted
FROM numbers({batch_rows})
"#
    )
}

async fn reset_graph_tables(client: &ArrowClickHouseClient) -> Result<()> {
    println!("  reset: ensuring graph tables exist...");
    client
        .execute(include_str!("create_gl_job.sql"))
        .await
        .context("failed to create gl_job")?;
    client
        .execute(include_str!("create_gl_ci_edge.sql"))
        .await
        .context("failed to create gl_ci_edge")?;

    println!("  reset: truncating graph tables...");
    client
        .execute("TRUNCATE TABLE gl_job")
        .await
        .context("failed to truncate gl_job")?;
    client
        .execute("TRUNCATE TABLE gl_ci_edge")
        .await
        .context("failed to truncate gl_ci_edge")?;
    println!("  reset: done");
    Ok(())
}

fn job_transforms() -> Vec<TransformSpec> {
    vec![
        TransformSpec {
            sql: NODE_TRANSFORM_SQL.into(),
            destination_table: "gl_job".into(),
            dict_encode_columns: HashSet::from([
                "status".into(),
                "type".into(),
                "when".into(),
                "timeout_source".into(),
                "scheduling_type".into(),
            ]),
        },
        fk_edge_transform("project_id", "Job", "IN_PROJECT", "Project", false),
        fk_edge_transform("commit_id", "Pipeline", "IN_PIPELINE", "Job", true),
        fk_edge_transform("stage_id", "Stage", "HAS_JOB", "Job", true),
        fk_edge_transform("commit_id", "Pipeline", "HAS_JOB", "Job", true),
        fk_edge_transform("user_id", "User", "TRIGGERED", "Job", true),
        fk_edge_transform("runner_id", "Job", "RUNS_ON", "Runner", false),
        fk_edge_transform(
            "upstream_pipeline_id",
            "Job",
            "TRIGGERED_BY_PIPELINE",
            "Pipeline",
            false,
        ),
        fk_edge_transform(
            "auto_canceled_by_id",
            "Job",
            "AUTO_CANCELED_BY",
            "Job",
            false,
        ),
    ]
}

fn fk_edge_transform(
    fk_column: &str,
    source_kind: &str,
    relationship_kind: &str,
    target_kind: &str,
    incoming: bool,
) -> TransformSpec {
    let (source_id, target_id) = if incoming {
        (fk_column, "id")
    } else {
        ("id", fk_column)
    };
    let sql = format!(
        r#"SELECT
    {source_id} AS source_id,
    '{source_kind}' AS source_kind,
    '{relationship_kind}' AS relationship_kind,
    {target_id} AS target_id,
    '{target_kind}' AS target_kind,
    CAST(ARRAY[] AS ARRAY<VARCHAR>) AS source_tags,
    CAST(ARRAY[] AS ARRAY<VARCHAR>) AS target_tags,
    traversal_path,
    _version,
    _deleted
FROM source_data
WHERE {fk_column} IS NOT NULL"#
    );
    TransformSpec {
        sql,
        destination_table: "gl_ci_edge".into(),
        dict_encode_columns: HashSet::from([
            "source_kind".into(),
            "relationship_kind".into(),
            "target_kind".into(),
        ]),
    }
}

const NODE_TRANSFORM_SQL: &str = r#"SELECT
    id,
    COALESCE(name, '') AS name,
    status,
    COALESCE(ref, '') AS ref,
    tag,
    allow_failure,
    COALESCE(CAST(coverage AS VARCHAR), '') AS coverage,
    environment,
    "when",
    retried,
    CASE
        WHEN failure_reason IS NULL THEN NULL
        WHEN failure_reason = 1 THEN 'script_failure'
        WHEN failure_reason = 2 THEN 'api_failure'
        WHEN failure_reason = 3 THEN 'stuck_or_timeout_failure'
        WHEN failure_reason = 4 THEN 'runner_system_failure'
        ELSE CAST(failure_reason AS VARCHAR)
    END AS failure_reason,
    created_at,
    started_at,
    finished_at,
    queued_at,
    type,
    runner_id,
    timeout,
    CASE
        WHEN timeout_source IS NULL THEN ''
        WHEN timeout_source = 1 THEN 'unknown_timeout_source'
        WHEN timeout_source = 2 THEN 'project_timeout_source'
        WHEN timeout_source = 3 THEN 'runner_timeout_source'
        WHEN timeout_source = 4 THEN 'no_timeout_source'
        ELSE ''
    END AS timeout_source,
    CAST(NULL AS BIGINT) AS exit_code,
    CASE
        WHEN scheduling_type IS NULL THEN ''
        WHEN scheduling_type = 0 THEN 'stage'
        WHEN scheduling_type = 1 THEN 'dag'
        ELSE ''
    END AS scheduling_type,
    project_id,
    user_id,
    upstream_pipeline_id,
    stage_id,
    commit_id AS pipeline_id,
    auto_canceled_by_id,
    traversal_path,
    _version,
    _deleted
FROM source_data"#;

fn extract_sql_cursor(batch_size: u64, cursor: Option<&Cursor>) -> String {
    let base = "SELECT *, _siphon_replicated_at AS _version, _siphon_deleted AS _deleted \
                FROM siphon_p_ci_builds";
    let where_clause = match cursor {
        Some(c) => format!(
            " WHERE (traversal_path, id, partition_id) > ('{}', {}, {})",
            c.traversal_path.replace('\'', "''"),
            c.id,
            c.partition_id
        ),
        None => String::new(),
    };
    format!("{base}{where_clause} ORDER BY traversal_path, id, partition_id LIMIT {batch_size}")
}

fn extract_sql_offset(batch_size: u64, offset: u64) -> String {
    format!(
        "SELECT *, _siphon_replicated_at AS _version, _siphon_deleted AS _deleted \
         FROM siphon_p_ci_builds ORDER BY traversal_path, id, partition_id LIMIT {batch_size} OFFSET {offset}"
    )
}

fn extract_cursor(batches: &[RecordBatch]) -> Option<Cursor> {
    let last_batch = batches.last()?;
    if last_batch.num_rows() == 0 {
        return None;
    }
    let last_row = last_batch.num_rows() - 1;

    let traversal_path = last_batch
        .column_by_name("traversal_path")?
        .as_any()
        .downcast_ref::<StringArray>()?
        .value(last_row)
        .to_string();

    let id = last_batch
        .column_by_name("id")?
        .as_any()
        .downcast_ref::<Int64Array>()?
        .value(last_row);

    let partition_id = last_batch
        .column_by_name("partition_id")?
        .as_any()
        .downcast_ref::<Int64Array>()?
        .value(last_row);

    Some(Cursor {
        traversal_path,
        id,
        partition_id,
    })
}

fn next_extract_sql(config: &BenchConfig, cursor: Option<&Cursor>, offset: u64) -> String {
    if config.use_offset {
        extract_sql_offset(config.batch_size, offset)
    } else {
        extract_sql_cursor(config.batch_size, cursor)
    }
}

async fn run_benchmark(
    client: &ArrowClickHouseClient,
    config: &BenchConfig,
    transforms: &[TransformSpec],
) -> Result<RunStats> {
    println!("\n=== Running Benchmark ===");
    let overall_start = Instant::now();
    let mut stats = RunStats::default();
    let mut offset: u64 = 0;
    let compression = if config.zstd {
        IpcCompressionType::ZSTD
    } else {
        IpcCompressionType::LZ4_FRAME
    };

    let extract_start = Instant::now();
    let sql = next_extract_sql(config, None, 0);
    let mut source_batches = client
        .query_arrow(&sql)
        .await
        .context("extract query failed")?;
    let mut extract_elapsed = extract_start.elapsed();

    loop {
        if source_batches.is_empty() {
            break;
        }

        let row_count: u64 = source_batches.iter().map(|b| b.num_rows() as u64).sum();
        stats.total_rows_extracted += row_count;
        stats.total_extract_time += extract_elapsed;
        stats.batch_count += 1;
        offset += row_count;

        let cursor = extract_cursor(&source_batches);

        let (transform_time, write_jobs) = run_transforms(
            &source_batches,
            transforms,
            config.coalesce_edges,
            config.write_chunk_size,
        )
        .await
        .with_context(|| format!("batch {} transform failed", stats.batch_count))?;
        stats.total_transform_time += transform_time;

        let should_extract_next = row_count >= config.batch_size
            && config.max_batches.is_none_or(|max| stats.batch_count < max);

        if config.pipeline && should_extract_next {
            let next_sql = next_extract_sql(config, cursor.as_ref(), offset);
            let (write_result, extract_result) = tokio::join!(
                write_jobs_to_clickhouse(
                    client,
                    &write_jobs,
                    config.max_concurrent_writes,
                    compression
                ),
                async {
                    let start = Instant::now();
                    let batches = client
                        .query_arrow(&next_sql)
                        .await
                        .context("pipelined extract failed")?;
                    Ok::<_, anyhow::Error>((batches, start.elapsed()))
                },
            );

            let (write_time, rows_written) = write_result
                .with_context(|| format!("batch {} write failed", stats.batch_count))?;
            let (next_batches, next_extract_elapsed) = extract_result?;

            stats.total_write_time += write_time;
            stats.total_rows_written += rows_written;

            let batch_total = extract_elapsed + transform_time + write_time;
            println!(
                "  batch {}: extracted={} transform={:.1}s write={:.1}s total={:.1}s",
                stats.batch_count,
                row_count,
                transform_time.as_secs_f64(),
                write_time.as_secs_f64(),
                batch_total.as_secs_f64(),
            );

            source_batches = next_batches;
            extract_elapsed = next_extract_elapsed;
        } else {
            let (write_time, rows_written) = write_jobs_to_clickhouse(
                client,
                &write_jobs,
                config.max_concurrent_writes,
                compression,
            )
            .await
            .with_context(|| format!("batch {} write failed", stats.batch_count))?;

            stats.total_write_time += write_time;
            stats.total_rows_written += rows_written;

            let batch_total = extract_elapsed + transform_time + write_time;
            println!(
                "  batch {}: extracted={} transform={:.1}s write={:.1}s total={:.1}s",
                stats.batch_count,
                row_count,
                transform_time.as_secs_f64(),
                write_time.as_secs_f64(),
                batch_total.as_secs_f64(),
            );

            if !should_extract_next {
                break;
            }

            let es = Instant::now();
            let sql = next_extract_sql(config, cursor.as_ref(), offset);
            source_batches = client
                .query_arrow(&sql)
                .await
                .context("extract query failed")?;
            extract_elapsed = es.elapsed();
        }
    }

    let overall_elapsed = overall_start.elapsed();
    println!("\n  total wall time: {:.1}s", overall_elapsed.as_secs_f64());

    Ok(stats)
}

async fn run_transforms(
    source_batches: &[RecordBatch],
    transforms: &[TransformSpec],
    coalesce_edges: bool,
    write_chunk_size: Option<usize>,
) -> Result<(Duration, Vec<WriteJob>)> {
    let session = SessionContext::new();
    let schema = source_batches[0].schema();
    let mem_table =
        MemTable::try_new(schema, vec![source_batches.to_vec()]).context("MemTable creation")?;
    session
        .register_table("source_data", Arc::new(mem_table))
        .context("register table")?;

    let mut total_transform_time = Duration::ZERO;
    let mut write_jobs: Vec<WriteJob> = Vec::new();

    for spec in transforms {
        let t_start = Instant::now();
        let df = session
            .sql(&spec.sql)
            .await
            .with_context(|| format!("transform SQL for {}", spec.destination_table))?;
        let mut result_batches = df
            .collect()
            .await
            .with_context(|| format!("transform collect for {}", spec.destination_table))?;
        total_transform_time += t_start.elapsed();

        prepare_batches(&mut result_batches, &spec.dict_encode_columns);

        let row_count: usize = result_batches.iter().map(|b| b.num_rows()).sum();
        if row_count == 0 {
            continue;
        }

        write_jobs.push(WriteJob {
            destination_table: spec.destination_table.clone(),
            batches: result_batches,
            row_count,
        });
    }

    let write_jobs = if coalesce_edges {
        let edge_table = "gl_ci_edge";
        let (edge_jobs, node_jobs): (Vec<_>, Vec<_>) = write_jobs
            .into_iter()
            .partition(|j| j.destination_table == edge_table);
        let mut merged = node_jobs;
        if !edge_jobs.is_empty() {
            let all_batches: Vec<RecordBatch> =
                edge_jobs.into_iter().flat_map(|j| j.batches).collect();
            let row_count = all_batches.iter().map(|b| b.num_rows()).sum();
            merged.push(WriteJob {
                destination_table: edge_table.into(),
                batches: all_batches,
                row_count,
            });
        }
        merged
    } else {
        write_jobs
    };

    let write_jobs = if let Some(chunk_size) = write_chunk_size {
        write_jobs
            .into_iter()
            .flat_map(|job| chunk_write_job(job, chunk_size))
            .collect()
    } else {
        write_jobs
    };

    Ok((total_transform_time, write_jobs))
}

async fn write_jobs_to_clickhouse(
    client: &ArrowClickHouseClient,
    jobs: &[WriteJob],
    max_concurrent_writes: usize,
    compression: IpcCompressionType,
) -> Result<(Duration, u64)> {
    let write_start = Instant::now();
    let semaphore = Semaphore::new(max_concurrent_writes.max(1));
    let mut futures = FuturesUnordered::new();

    for job in jobs {
        futures.push(async {
            let _permit = semaphore.acquire().await.expect("semaphore is not closed");
            client
                .insert_arrow_streaming_with(&job.destination_table, &job.batches, compression)
                .await
                .with_context(|| format!("write to {}", job.destination_table))?;
            Ok::<usize, anyhow::Error>(job.row_count)
        });
    }

    let mut total_rows_written: u64 = 0;
    while let Some(result) = futures.next().await {
        total_rows_written += result? as u64;
    }

    Ok((write_start.elapsed(), total_rows_written))
}

fn chunk_write_job(job: WriteJob, chunk_size: usize) -> Vec<WriteJob> {
    if job.row_count <= chunk_size {
        return vec![job];
    }

    let mut chunks: Vec<WriteJob> = Vec::new();
    let mut current_batches: Vec<RecordBatch> = Vec::new();
    let mut current_rows: usize = 0;

    for batch in job.batches {
        let batch_rows = batch.num_rows();
        if current_rows + batch_rows > chunk_size && !current_batches.is_empty() {
            chunks.push(WriteJob {
                destination_table: job.destination_table.clone(),
                batches: std::mem::take(&mut current_batches),
                row_count: current_rows,
            });
            current_rows = 0;
        }

        if batch_rows > chunk_size {
            let mut offset = 0;
            while offset < batch_rows {
                let len = (batch_rows - offset).min(chunk_size);
                let slice = batch.slice(offset, len);
                chunks.push(WriteJob {
                    destination_table: job.destination_table.clone(),
                    batches: vec![slice],
                    row_count: len,
                });
                offset += len;
            }
        } else {
            current_rows += batch_rows;
            current_batches.push(batch);
        }
    }

    if !current_batches.is_empty() {
        chunks.push(WriteJob {
            destination_table: job.destination_table.clone(),
            batches: current_batches,
            row_count: current_rows,
        });
    }

    chunks
}

fn print_results(stats: &RunStats) {
    println!("\n=== Results ===");
    println!("  batches:           {}", stats.batch_count);
    println!("  rows extracted:    {}", stats.total_rows_extracted);
    println!(
        "  rows written:      {} (edge fan-out: {:.1}x)",
        stats.total_rows_written,
        stats.total_rows_written as f64 / stats.total_rows_extracted.max(1) as f64
    );
    println!(
        "  extract time:      {:.1}s",
        stats.total_extract_time.as_secs_f64()
    );
    println!(
        "  transform time:    {:.1}s",
        stats.total_transform_time.as_secs_f64()
    );
    println!(
        "  write time:        {:.1}s",
        stats.total_write_time.as_secs_f64()
    );
    let total = stats.total_extract_time + stats.total_transform_time + stats.total_write_time;
    println!("  total pipeline:    {:.1}s", total.as_secs_f64());
    if total.as_secs_f64() > 0.0 {
        println!(
            "  extract throughput: {:.0} rows/s",
            stats.total_rows_extracted as f64 / stats.total_extract_time.as_secs_f64()
        );
        println!(
            "  write throughput:  {:.0} rows/s",
            stats.total_rows_written as f64 / stats.total_write_time.as_secs_f64()
        );
    }
}
