mod config;
mod executor;
mod output;
mod service;

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use clickhouse_client::ArrowClickHouseClient;
use compiler::SecurityContext;
use ontology::Ontology;
use tracing_subscriber::EnvFilter;

use config::ProfilingConfig;
use executor::enrich_output;
use output::build_output;
use service::ProfilerPipelineService;

#[derive(Parser)]
#[command(
    name = "query-profiler",
    about = "Profile GKG queries against ClickHouse"
)]
struct Cli {
    /// JSON query string or @filepath
    query: String,

    /// ClickHouse HTTP URL
    #[arg(long, default_value = "http://127.0.0.1:8123", env = "CLICKHOUSE_URL")]
    ch_url: String,

    /// ClickHouse database
    #[arg(long, default_value = "gkg_graph", env = "CLICKHOUSE_DATABASE")]
    ch_database: String,

    /// ClickHouse user
    #[arg(long, default_value = "default", env = "CLICKHOUSE_USER")]
    ch_user: String,

    /// ClickHouse password
    #[arg(long, env = "CLICKHOUSE_PASSWORD")]
    ch_password: Option<String>,

    /// Traversal paths for security context (repeatable)
    #[arg(short = 't', long, required = true)]
    traversal_paths: Vec<String>,

    /// Ontology config directory
    #[arg(long, default_value = "config/ontology")]
    ontology: PathBuf,

    /// Include EXPLAIN PLAN and EXPLAIN PIPELINE for each query
    #[arg(long)]
    explain: bool,

    /// Deep profile: query system.query_log for ProfileEvents, CPU, memory
    #[arg(long)]
    profile: bool,

    /// Include system.processors_profile_log pipeline breakdown
    #[arg(long)]
    processors: bool,

    /// Show ClickHouse instance health snapshot
    #[arg(long)]
    health: bool,

    /// Output format
    #[arg(long, default_value = "json")]
    format: OutputFormat,

    /// ClickHouse join algorithm (hash, parallel_hash, full_sorting_merge, etc.)
    #[arg(long, default_value = "hash", env = "CLICKHOUSE_JOIN_ALGORITHM")]
    join_algorithm: String,

    /// Extra ClickHouse settings (repeatable, e.g. max_threads=4)
    #[arg(long)]
    settings: Vec<String>,
}

#[derive(Clone, clap::ValueEnum)]
enum OutputFormat {
    Json,
    Pretty,
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        .with_writer(std::io::stderr)
        .init();

    let query_json = if cli.query.starts_with('@') {
        std::fs::read_to_string(&cli.query[1..])
            .context(format!("failed to read query file: {}", &cli.query[1..]))?
    } else {
        cli.query.clone()
    };

    let ontology =
        Arc::new(Ontology::load_from_dir(&cli.ontology).context("failed to load ontology")?);

    let org_id = cli.traversal_paths[0]
        .trim_start_matches('/')
        .split('/')
        .next()
        .and_then(|s| s.parse::<i64>().ok())
        .context("failed to parse org_id from first traversal path")?;

    let security_ctx = SecurityContext::new(org_id, cli.traversal_paths.clone())
        .map_err(|e| anyhow::anyhow!("invalid security context: {e}"))?;

    let mut custom_settings: std::collections::HashMap<String, String> = cli
        .settings
        .iter()
        .filter_map(|s| {
            let (k, v) = s.split_once('=')?;
            Some((k.to_string(), v.to_string()))
        })
        .collect();

    custom_settings
        .entry("join_algorithm".to_string())
        .or_insert_with(|| cli.join_algorithm.clone());

    let client = Arc::new(ArrowClickHouseClient::new(
        &cli.ch_url,
        &cli.ch_database,
        &cli.ch_user,
        cli.ch_password.as_deref(),
        &custom_settings,
    ));

    let profiling_config = ProfilingConfig {
        enabled: true,
        explain: cli.explain,
        query_log: cli.profile,
        processors: cli.processors,
        instance_health: cli.health,
    };

    let service = ProfilerPipelineService::new(ontology, Arc::clone(&client));
    let mut output = service
        .run_query(security_ctx, &query_json)
        .await
        .map_err(|e| anyhow::anyhow!("pipeline failed: {e}"))?;

    enrich_output(&client, &mut output, &profiling_config).await;

    let instance_health = if profiling_config.instance_health {
        match client.profiler().fetch_instance_health().await {
            Ok(health) => Some(serde_json::to_value(&health).unwrap_or_default()),
            Err(e) => {
                tracing::warn!("failed to fetch instance health: {e}");
                None
            }
        }
    } else {
        None
    };

    let profiler_output = build_output(
        &query_json,
        org_id,
        &cli.traversal_paths,
        &output,
        instance_health,
    );

    match cli.format {
        OutputFormat::Json => println!("{}", serde_json::to_string(&profiler_output)?),
        OutputFormat::Pretty => println!("{}", serde_json::to_string_pretty(&profiler_output)?),
    }

    Ok(())
}
