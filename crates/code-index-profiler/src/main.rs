mod clickhouse;
mod corpus;
mod events;
mod harness;
mod memory;

use std::path::PathBuf;
use std::time::Instant;

use clap::Parser;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

#[cfg(all(not(feature = "dhat-heap"), feature = "track-alloc"))]
#[global_allocator]
static GLOBAL: memory::Tracking<mimalloc::MiMalloc> = memory::Tracking(mimalloc::MiMalloc);

#[cfg(all(not(feature = "dhat-heap"), not(feature = "track-alloc")))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static GLOBAL: memory::Tracking<dhat::Alloc> = memory::Tracking(dhat::Alloc);

#[derive(Parser, Debug)]
#[command(
    name = "code-index-profiler",
    about = "Drives the production code-indexing handler (archive download -> extract -> parse -> Arrow -> ClickHouse) under memory instrumentation."
)]
struct Args {
    /// Directory holding `<project_id>.tar.gz` corpus archives.
    #[arg(long, default_value = ".memprofile/corpus")]
    corpus_dir: PathBuf,

    /// Project ID to index; must have a matching archive in the corpus dir.
    /// Repeat the flag to index several projects concurrently, which is what a
    /// production pod does across its indexing lanes.
    #[arg(long, required = true)]
    project_id: Vec<i64>,

    /// Label used for the output directory and report.
    #[arg(long)]
    label: String,

    #[arg(long, default_value = "main")]
    branch: String,

    #[arg(long, default_value = "profilersha")]
    commit_sha: String,

    #[arg(long, default_value = "1/1/")]
    traversal_path: String,

    /// Concurrent indexing lanes, split evenly between the small and big pools.
    #[arg(long, default_value_t = 1)]
    indexing_lanes: usize,

    #[arg(long, default_value = "http://localhost:18123")]
    clickhouse_url: String,

    #[arg(long, default_value = "gkg_memprofile")]
    clickhouse_database: String,

    #[arg(long, default_value = "default")]
    clickhouse_user: String,

    #[arg(long, default_value = "memprofile")]
    clickhouse_password: String,

    /// Skip DROP/CREATE DATABASE + DDL. Use when re-running against a database
    /// that is already at the current schema version.
    #[arg(long)]
    keep_schema: bool,

    #[arg(long, default_value = "50")]
    sample_hz: u32,

    /// Rayon workers per language family. 0 = one per core.
    #[arg(long, default_value = "0")]
    worker_threads: usize,

    /// Language families parsed concurrently. 0 = the pipeline default of 2.
    #[arg(long, default_value = "0")]
    max_concurrent_languages: usize,

    /// Rows per slice submitted to the buffered ClickHouse writer.
    #[arg(long, default_value = "1000000")]
    write_slice_rows: usize,

    /// Disable the per-job wall-clock budget (0) or set it in seconds.
    #[arg(long, default_value = "0")]
    job_timeout_secs: u64,

    /// Per-file CPU budgets in milliseconds, production defaults. Set every one
    /// to 0 under dhat: its ~30x slowdown otherwise trips the budgets and most
    /// files abort mid-parse, so the profile describes a run that never happened.
    #[arg(long, default_value_t = 2000)]
    per_file_timeout_ms: u64,

    #[arg(long, default_value_t = 100)]
    per_file_parse_timeout_ms: u64,

    #[arg(long, default_value_t = 100)]
    per_file_walk_timeout_ms: u64,

    #[arg(long, default_value_t = 100)]
    per_file_ssa_timeout_ms: u64,

    #[arg(long, default_value_t = 180_000)]
    cross_file_resolve_timeout_ms: u64,

    #[arg(long, default_value = ".memprofile/runs")]
    out_dir: PathBuf,

    /// Extra scratch dir for the extracted repository. Defaults to a temp dir.
    #[arg(long)]
    cache_dir: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let run_dir = args.out_dir.join(&args.label);
    std::fs::create_dir_all(&run_dir)?;

    #[cfg(feature = "dhat-heap")]
    let _dhat = dhat::Profiler::builder()
        .file_name(run_dir.join("dhat-heap.json"))
        .trim_backtraces(Some(24))
        .build();

    let start = Instant::now();
    let event_layer = events::EventLayer::new(&run_dir.join("events.jsonl"), start)?;
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(true)
                .with_filter(
                    EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| EnvFilter::new("info,codegraph_mem=debug")),
                ),
        )
        .with(event_layer.with_filter(EnvFilter::new(
            "warn,codegraph_mem=debug,profiler=info,indexer=info,code_graph=info",
        )))
        .init();

    let mut archive_bytes = 0u64;
    for project_id in &args.project_id {
        let archive = corpus::archive_path(&args.corpus_dir, *project_id);
        archive_bytes += std::fs::metadata(&archive)
            .map_err(|e| anyhow::anyhow!("corpus archive {} missing: {e}", archive.display()))?
            .len();
    }

    let sampler = memory::Sampler::start(&run_dir.join("samples.jsonl"), args.sample_hz, start)?;

    memory::set_phase("setup");
    let ch = clickhouse::ClickHouse::new(
        &args.clickhouse_url,
        &args.clickhouse_database,
        &args.clickhouse_user,
        Some(&args.clickhouse_password),
    );
    ch.wait_ready(150).await?;
    if !args.keep_schema {
        let applied = ch.reset_schema().await?;
        tracing::info!(statements = applied, "graph schema applied");
    }

    let corpus_server = corpus::CorpusServer::start(&args.corpus_dir, &args.branch).await?;

    let cache_dir = match &args.cache_dir {
        Some(p) => {
            std::fs::create_dir_all(p)?;
            tempfile::TempDir::new_in(p)?
        }
        None => tempfile::TempDir::new()?,
    };

    let pipeline_config = orbit_server_config::CodeIndexingPipelineConfig {
        worker_threads: args.worker_threads,
        max_concurrent_languages: args.max_concurrent_languages,
        write_slice_rows: args.write_slice_rows,
        job_timeout_secs: args.job_timeout_secs,
        per_file_timeout_ms: args.per_file_timeout_ms,
        per_file_parse_timeout_ms: args.per_file_parse_timeout_ms,
        per_file_walk_timeout_ms: args.per_file_walk_timeout_ms,
        per_file_ssa_timeout_ms: args.per_file_ssa_timeout_ms,
        cross_file_resolve_timeout_ms: args.cross_file_resolve_timeout_ms,
        small_indexing_slots: Some(args.indexing_lanes),
        big_indexing_slots: Some(args.indexing_lanes),
        ..Default::default()
    };

    let harness = harness::Harness::build(
        &corpus_server,
        &ch.config,
        pipeline_config.clone(),
        cache_dir,
    )?;

    memory::set_phase("index");
    let t_index = Instant::now();
    let harness = std::sync::Arc::new(harness);
    let mut jobs = tokio::task::JoinSet::new();
    for (i, project_id) in args.project_id.iter().copied().enumerate() {
        let harness = harness.clone();
        let branch = args.branch.clone();
        let commit_sha = args.commit_sha.clone();
        // Each project needs its own traversal path so the stale sweep and the
        // checkpoint of one do not touch another's rows.
        let traversal_path = format!("{}{}/", args.traversal_path, i + 1);
        jobs.spawn(async move {
            harness
                .index(project_id, &branch, &commit_sha, &traversal_path)
                .await
        });
    }
    let mut outcome = Ok(());
    while let Some(joined) = jobs.join_next().await {
        match joined {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                tracing::error!(error = %e, "indexing failed");
                outcome = Err(e);
            }
            Err(e) => {
                outcome = Err(anyhow::anyhow!("index task panicked: {e}"));
            }
        }
    }
    let index_ms = t_index.elapsed().as_millis() as u64;

    memory::set_phase("flush");
    let t_flush = Instant::now();
    harness.flush().await?;
    let flush_ms = t_flush.elapsed().as_millis() as u64;

    memory::set_phase("done");
    let final_mem = memory::process_memory();
    let final_alloc = memory::alloc_stats();
    let peaks = sampler.stop();

    let row_counts = ch.row_counts(&clickhouse::table_prefix()).await?;
    let total_rows: u64 = row_counts.iter().map(|(_, n)| n).sum();

    let summary = serde_json::json!({
        "label": args.label,
        "project_ids": args.project_id,
        "archive_bytes": archive_bytes,
        "index_ms": index_ms,
        "flush_ms": flush_ms,
        "total_ms": start.elapsed().as_millis() as u64,
        "ok": outcome.is_ok(),
        "error": outcome.as_ref().err().map(|e| e.to_string()),
        "peaks": peaks,
        "final": { "process": final_mem, "alloc": final_alloc },
        "clickhouse_rows": row_counts.iter().map(|(t, n)| serde_json::json!({"table": t, "rows": n})).collect::<Vec<_>>(),
        "clickhouse_total_rows": total_rows,
        "config": {
            "worker_threads": pipeline_config.worker_threads,
            "max_concurrent_languages": pipeline_config.max_concurrent_languages,
            "write_slice_rows": pipeline_config.write_slice_rows,
            "max_file_size_bytes": pipeline_config.max_file_size_bytes,
            "max_total_bytes": pipeline_config.max_total_bytes,
            "job_timeout_secs": pipeline_config.job_timeout_secs,
            "indexing_lanes": args.indexing_lanes,
            "per_file_timeout_ms": pipeline_config.per_file_timeout_ms,
            "per_file_parse_timeout_ms": pipeline_config.per_file_parse_timeout_ms,
            "per_file_walk_timeout_ms": pipeline_config.per_file_walk_timeout_ms,
            "per_file_ssa_timeout_ms": pipeline_config.per_file_ssa_timeout_ms,
            "allocator": if cfg!(feature = "dhat-heap") { "dhat" } else { "mimalloc" },
            "alloc_tracking": cfg!(feature = "track-alloc"),
        },
    });
    std::fs::write(
        run_dir.join("summary.json"),
        serde_json::to_string_pretty(&summary)?,
    )?;

    println!("{}", serde_json::to_string_pretty(&summary)?);
    outcome
}
