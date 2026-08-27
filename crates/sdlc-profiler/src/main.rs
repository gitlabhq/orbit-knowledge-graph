//! Runs the SDLC indexer's real backfill against a synthetic siphon datalake,
//! writing transformed rows to ClickHouse, and samples memory throughout.

#[cfg(feature = "track-alloc")]
#[global_allocator]
static GLOBAL: memprofile::Tracking<mimalloc::MiMalloc> = memprofile::Tracking(mimalloc::MiMalloc);

#[cfg(not(feature = "track-alloc"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use memprofile as memory;

mod harness;
mod seed;

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Instant;

use anyhow::Context;
use clap::Parser;
use indexer::topic::{GlobalIndexingRequest, NamespaceIndexingRequest};
use indexer::types::{Envelope, Event};
use orbit_server_config::{EngineConfiguration, HandlersConfiguration};
use orbit_utils::traversal_path::TraversalPath;

use crate::harness::{HandlerRun, Harness};
use crate::seed::{NAMESPACE_ID_BASE, ORGANIZATION_ID, Seeder, Shape};

#[derive(Parser)]
#[command(about = "Profile SDLC indexer backfill peak memory over a synthetic datalake")]
struct Args {
    #[arg(long, default_value = "http://localhost:18123")]
    url: String,
    #[arg(long, default_value = "default")]
    username: String,
    #[arg(long, env = "CH_PASSWORD", default_value = "memprofile")]
    password: String,
    #[arg(long, default_value = "sdlc_profile_datalake")]
    datalake_database: String,
    /// Recreated every run, so two arms can be fingerprinted against each other.
    #[arg(long, default_value = "sdlc_profile_graph")]
    graph_database: String,

    #[arg(long, default_value_t = 1)]
    namespaces: u64,
    #[arg(long, default_value_t = 50)]
    projects_per_namespace: u64,
    /// Rows per entity table. Needs to be a few multiples of the page size for
    /// the paging loop and its read-ahead to be exercised.
    #[arg(long, default_value_t = 1_600_000)]
    rows_per_table: u64,
    #[arg(long, default_value_t = 3)]
    path_depth: usize,
    #[arg(long, default_value_t = 600)]
    note_bytes: u64,
    #[arg(long, default_value_t = 1200)]
    description_bytes: u64,
    #[arg(long, default_value_t = 64)]
    title_bytes: u64,
    #[arg(long, default_value_t = 32)]
    text_bytes: u64,

    /// Rows per datalake page. Production derives 500k from the 32 GiB sdlc pool.
    #[arg(long, default_value_t = 500_000)]
    datalake_batch_size: u64,
    /// Production `engine.concurrency_groups.sdlc` for the pool is 20.
    #[arg(long, default_value_t = 20)]
    sdlc_concurrency: usize,
    /// Skips the partitioned initial load below this row count, as production does.
    #[arg(long, default_value_t = indexer::modules::sdlc::PARTITION_MIN_ROWS)]
    partition_min_rows: u64,
    /// Runs only these entity handlers, by handler name suffix (e.g. `mergerequest`).
    #[arg(long, value_delimiter = ',')]
    only: Vec<String>,

    /// Reseed only these datalake tables, leaving the rest of the database as is.
    #[arg(long, value_delimiter = ',')]
    seed_tables: Vec<String>,
    /// Reuse whatever is already in the datalake database.
    #[arg(long)]
    skip_seed: bool,
    /// Seed and exit, so a series of runs can share one seeded dataset.
    #[arg(long)]
    seed_only: bool,
    /// Skip the global pipelines (User, Runner) and profile the namespaced sweep alone.
    #[arg(long)]
    skip_global: bool,

    #[arg(long, default_value = "run")]
    label: String,
    #[arg(long, default_value = ".memprofile/sdlc")]
    out_dir: PathBuf,
    #[arg(long, default_value_t = 50)]
    sample_hz: u32,
}

#[derive(serde::Serialize)]
struct Report {
    label: String,
    shape: ReportShape,
    config: ReportConfig,
    namespace_handlers: usize,
    global_handlers: usize,
    seeded_rows: u64,
    seed_ms: u128,
    global_ms: u128,
    namespace_ms: u128,
    total_ms: u128,
    graph_rows: u64,
    graph_tables_written: usize,
    failures: Vec<HandlerRun>,
    slowest: Vec<HandlerRun>,
    peaks: memory::Peaks,
    process: memory::ProcessMemory,
    allocator: memory::AllocatorInfo,
    alloc: memory::AllocStats,
    settled: Settled,
}

#[derive(serde::Serialize)]
struct ReportShape {
    namespaces: u64,
    projects_per_namespace: u64,
    rows_per_table: u64,
    path_depth: usize,
    note_bytes: u64,
    description_bytes: u64,
}

#[derive(serde::Serialize)]
struct ReportConfig {
    datalake_batch_size: u64,
    sdlc_concurrency: usize,
    partition_min_rows: u64,
    alloc_secure: bool,
    alloc_tracked: bool,
}

/// Measured after `mi_collect`, so retention shows up separately from live data.
#[derive(serde::Serialize)]
struct Settled {
    process: memory::ProcessMemory,
    allocator: memory::AllocatorInfo,
    alloc: memory::AllocStats,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let shape = Shape {
        namespaces: args.namespaces,
        projects_per_namespace: args.projects_per_namespace,
        rows_per_table: args.rows_per_table,
        path_depth: args.path_depth,
        note_bytes: args.note_bytes,
        description_bytes: args.description_bytes,
        title_bytes: args.title_bytes,
        text_bytes: args.text_bytes,
    };

    let seeder = Seeder::new(
        &args.url,
        &args.username,
        Some(args.password.as_str()).filter(|password| !password.is_empty()),
        &args.datalake_database,
        &args.graph_database,
    );
    seeder.wait_ready(60).await?;

    let mut seeded_rows = 0;
    let mut seed_ms = 0;
    if !args.seed_tables.is_empty() {
        memory::set_phase("seed");
        let started = Instant::now();
        seeded_rows = seeder.seed_tables(shape, &args.seed_tables).await?;
        seed_ms = started.elapsed().as_millis();
        tracing::info!(ms = seed_ms, rows = seeded_rows, "reseeded tables");
        // Reshaping one table is a maintenance step before a series of runs, so
        // it returns rather than profiling whatever the other flags happen to say.
        return Ok(());
    } else if !args.skip_seed {
        memory::set_phase("seed");
        let started = Instant::now();
        seeded_rows = seeder.seed_datalake(shape).await?;
        seed_ms = started.elapsed().as_millis();
        tracing::info!(ms = seed_ms, rows = seeded_rows, "seeded datalake");
    }
    if args.seed_only {
        return Ok(());
    }
    seeder.reset_graph().await?;

    let config = indexer_config(&args, &seeder);
    let ontology = ontology::Ontology::load_embedded().context("ontology must load")?;
    let harness = Harness::new(&config, &ontology, args.partition_min_rows).await?;

    std::fs::create_dir_all(&args.out_dir)?;
    let samples = args.out_dir.join(format!("{}.samples.jsonl", args.label));
    let start = Instant::now();
    let sampler = memory::Sampler::start(&samples, args.sample_hz, start)?;

    let watermark = chrono::Utc::now();
    let mut runs = Vec::new();

    let global_subscription = GlobalIndexingRequest::subscription();
    let global_handlers = harness.handler_count(&global_subscription);
    let mut global_ms = 0;
    if !args.skip_global {
        memory::set_phase("global");
        let started = Instant::now();
        let envelope = Envelope::new(&GlobalIndexingRequest {
            watermark,
            dispatch_id: uuid::Uuid::new_v4(),
            campaign_id: None,
            targets: args.only.clone(),
        })
        .map_err(|error| anyhow::anyhow!("global envelope: {error}"))?;
        runs.extend(harness.dispatch(&global_subscription, envelope).await);
        global_ms = started.elapsed().as_millis();
    }

    let namespace_subscription = NamespaceIndexingRequest::subscription();
    let namespace_handlers = harness.handler_count(&namespace_subscription);

    memory::set_phase("namespace");
    let started = Instant::now();
    let mut namespace_tasks = Vec::new();
    for index in 0..args.namespaces {
        let namespace = NAMESPACE_ID_BASE + index as i64;
        let request = NamespaceIndexingRequest {
            namespace,
            traversal_path: TraversalPath::new_unchecked(format!("{ORGANIZATION_ID}/{namespace}/")),
            watermark,
            dispatch_id: uuid::Uuid::new_v4(),
            campaign_id: None,
            targets: args.only.clone(),
        };
        let envelope = Envelope::new(&request)
            .map_err(|error| anyhow::anyhow!("namespace envelope: {error}"))?;
        namespace_tasks.push(harness.dispatch(&namespace_subscription, envelope));
    }
    for batch in futures::future::join_all(namespace_tasks).await {
        runs.extend(batch);
    }
    let namespace_ms = started.elapsed().as_millis();

    memory::set_phase("done");
    let total_ms = start.elapsed().as_millis();
    let process = memory::process_memory();
    let allocator = memory::allocator_info();
    let alloc = memory::alloc_stats();

    memory::allocator_collect();
    let settled = Settled {
        process: memory::process_memory(),
        allocator: memory::allocator_info(),
        alloc: memory::alloc_stats(),
    };
    let peaks = sampler.stop();

    let (graph_rows, graph_tables_written) = graph_totals(&seeder).await?;
    let failures: Vec<HandlerRun> = runs
        .iter()
        .filter(|run| run.error.is_some())
        .map(|run| HandlerRun {
            handler: run.handler.clone(),
            ms: run.ms,
            error: run.error.clone(),
        })
        .collect();
    runs.sort_by_key(|run| std::cmp::Reverse(run.ms));
    let slowest: Vec<HandlerRun> = runs.into_iter().take(15).collect();

    let report = Report {
        label: args.label.clone(),
        shape: ReportShape {
            namespaces: args.namespaces,
            projects_per_namespace: args.projects_per_namespace,
            rows_per_table: args.rows_per_table,
            path_depth: args.path_depth,
            note_bytes: args.note_bytes,
            description_bytes: args.description_bytes,
        },
        config: ReportConfig {
            datalake_batch_size: args.datalake_batch_size,
            sdlc_concurrency: args.sdlc_concurrency,
            partition_min_rows: args.partition_min_rows,
            alloc_secure: cfg!(feature = "secure-alloc"),
            alloc_tracked: cfg!(feature = "track-alloc"),
        },
        namespace_handlers,
        global_handlers,
        seeded_rows,
        seed_ms,
        global_ms,
        namespace_ms,
        total_ms,
        graph_rows,
        graph_tables_written,
        failures,
        slowest,
        peaks,
        process,
        allocator,
        alloc,
        settled,
    };

    let json = serde_json::to_string_pretty(&report)?;
    std::fs::write(args.out_dir.join(format!("{}.json", args.label)), &json)?;
    println!("{json}");
    Ok(())
}

/// Production shape: the page size and the sdlc concurrency group are what the
/// pool's 32 GiB limit derives, so both are set explicitly rather than being
/// re-derived from whatever machine the profiler runs on.
fn indexer_config(args: &Args, seeder: &Seeder) -> indexer::IndexerConfig {
    let mut handlers = HandlersConfiguration::default();
    handlers.entity_handler.datalake_batch_size = Some(args.datalake_batch_size);

    indexer::IndexerConfig {
        graph: seeder.configuration(&seeder.graph),
        datalake: seeder.configuration(&seeder.datalake),
        engine: EngineConfiguration {
            max_concurrent_workers: Some(args.sdlc_concurrency),
            concurrency_groups: HashMap::from([("sdlc".to_string(), args.sdlc_concurrency)]),
            handlers,
            ..EngineConfiguration::default()
        },
        ..indexer::IndexerConfig::default()
    }
}

/// Pre-merge counts, so this is a progress signal for the run and not an
/// output-equivalence check; use `scripts/devtools/memprofile-verify-output.py`
/// for that.
async fn graph_totals(seeder: &Seeder) -> anyhow::Result<(u64, usize)> {
    let client = seeder.client(&seeder.graph);
    let sql = format!(
        "SELECT toString(sum(rows)) AS rows, toString(uniqExact(table)) AS tables \
         FROM system.parts WHERE database = '{}' AND active",
        seeder.graph
    );
    let batches = client
        .query_arrow(&sql)
        .await
        .context("reading graph row totals")?;
    let Some(batch) = batches.first() else {
        return Ok((0, 0));
    };
    let scalar = |column: &str| -> u64 {
        orbit_utils::arrow::ArrowUtils::get_column_string(batch, column, 0)
            .and_then(|value| value.parse().ok())
            .unwrap_or(0)
    };
    Ok((scalar("rows"), scalar("tables") as usize))
}
