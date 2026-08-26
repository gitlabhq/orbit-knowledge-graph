//! Peak-memory profiler for the dispatcher's code-backfill enumeration.
//!
//! Seeds a synthetic datalake at production scale, then runs the same
//! `CodeBackfill` the `DispatchIndexing` mode runs, with a counting NATS in
//! place of the broker, while sampling RSS, footprint and requested bytes.

#[cfg(feature = "track-alloc")]
#[global_allocator]
static GLOBAL: memory::Tracking<mimalloc::MiMalloc> = memory::Tracking(mimalloc::MiMalloc);

#[cfg(not(feature = "track-alloc"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod memory;
mod nats_stub;
mod seed;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use clap::Parser;
use indexer::campaign::CampaignState;
use indexer::clickhouse::ClickHouseConfigurationExt;
use indexer::orchestrator::dispatch::CodeBackfill;
use indexer::orchestrator::scheduled::ScheduledTaskMetrics;
use indexer::schema::version::{SCHEMA_VERSION, prefixed_table_name};

use crate::nats_stub::CountingNats;
use crate::seed::{Seeder, Shape};

#[derive(Parser)]
#[command(about = "Profile dispatcher peak memory over a synthetic backfill")]
struct Args {
    #[arg(long, default_value = "http://localhost:18123")]
    url: String,
    #[arg(long, default_value = "default")]
    username: String,
    #[arg(long, env = "CH_PASSWORD", default_value = "memprofile")]
    password: String,
    #[arg(long, default_value = "dispatch_profile_datalake")]
    datalake_database: String,
    #[arg(long, default_value = "dispatch_profile_graph")]
    graph_database: String,

    #[arg(long, default_value_t = 1_000_000)]
    projects: u64,
    #[arg(long, default_value_t = 100)]
    namespaces: u64,
    #[arg(long, default_value_t = 0)]
    checkpointed_pct: u64,
    #[arg(long, default_value_t = 3)]
    path_depth: usize,

    /// Reuse whatever is already in the two databases.
    #[arg(long)]
    skip_seed: bool,
    /// Seed and exit, so a series of runs can share one seeded dataset.
    #[arg(long)]
    seed_only: bool,
    /// Sleep per publish, standing in for the JetStream ack round trip.
    #[arg(long, default_value_t = 0)]
    publish_delay_us: u64,

    #[arg(long, default_value = "run")]
    label: String,
    #[arg(long, default_value = ".memprofile/dispatch")]
    out_dir: PathBuf,
    #[arg(long, default_value_t = 50)]
    sample_hz: u32,
}

#[derive(serde::Serialize)]
struct Report {
    label: String,
    shape: ReportShape,
    dispatched: u64,
    skipped: u64,
    published: u64,
    published_bytes: u64,
    enabled_namespaces: usize,
    enumerate_ms: u128,
    dispatch_ms: u128,
    total_ms: u128,
    peaks: memory::Peaks,
    process: memory::ProcessMemory,
    allocator: memory::AllocatorInfo,
    alloc: memory::AllocStats,
    settled: Settled,
}

#[derive(serde::Serialize)]
struct ReportShape {
    projects: u64,
    namespaces: u64,
    checkpointed_pct: u64,
    path_depth: usize,
}

/// After the dispatch has returned and the allocator has been asked to release
/// what it is holding: the part of the peak that is retention rather than live
/// data.
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
        projects: args.projects,
        checkpointed_pct: args.checkpointed_pct,
        path_depth: args.path_depth,
    };
    let checkpoint_table = prefixed_table_name("code_indexing_checkpoint", *SCHEMA_VERSION);

    let seeder = Seeder::new(
        &args.url,
        &args.username,
        Some(args.password.as_str()).filter(|p| !p.is_empty()),
        &args.datalake_database,
        &args.graph_database,
    );
    seeder.wait_ready(60).await?;

    if !args.skip_seed {
        memory::set_phase("seed");
        let started = Instant::now();
        seeder.seed(shape, &checkpoint_table).await?;
        tracing::info!(ms = started.elapsed().as_millis(), "seeded");
    }
    if args.seed_only {
        return Ok(());
    }

    let nats = Arc::new(CountingNats::new(
        Some(args.publish_delay_us)
            .filter(|d| *d > 0)
            .map(std::time::Duration::from_micros),
    ));
    let backfill = CodeBackfill::new(
        nats.clone(),
        seeder.configuration(&args.graph_database).build_client(),
        seeder.configuration(&args.datalake_database).build_client(),
        ScheduledTaskMetrics::new(),
        Arc::new(CampaignState::new()),
    );

    std::fs::create_dir_all(&args.out_dir)?;
    let samples = args.out_dir.join(format!("{}.samples.jsonl", args.label));
    let start = Instant::now();
    let sampler = memory::Sampler::start(&samples, args.sample_hz, start)?;

    memory::set_phase("enumerate_namespaces");
    let enumerate_start = Instant::now();
    let namespaces = backfill
        .fetch_enabled_namespaces()
        .await
        .map_err(|error| anyhow::anyhow!("fetch_enabled_namespaces: {error}"))?;
    let enumerate_ms = enumerate_start.elapsed().as_millis();

    memory::set_phase("dispatch");
    let dispatch_start = Instant::now();
    let outcome = backfill
        .dispatch_for_namespaces(&namespaces, uuid::Uuid::new_v4())
        .await
        .map_err(|error| anyhow::anyhow!("dispatch_for_namespaces: {error}"))?;
    let dispatch_ms = dispatch_start.elapsed().as_millis();

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

    let report = Report {
        label: args.label.clone(),
        shape: ReportShape {
            projects: args.projects,
            namespaces: args.namespaces,
            checkpointed_pct: args.checkpointed_pct,
            path_depth: args.path_depth,
        },
        dispatched: outcome.dispatched,
        skipped: outcome.skipped,
        published: nats.published(),
        published_bytes: nats.published_bytes(),
        enabled_namespaces: namespaces.len(),
        enumerate_ms,
        dispatch_ms,
        total_ms,
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
