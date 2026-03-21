mod executor;
mod output;

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use clickhouse_client::profiler::QueryProfiler;
use compiler::SecurityContext;
use ontology::Ontology;
use tracing_subscriber::EnvFilter;

use executor::{ProfilerOptions, execute_profiled_query};
use output::build_output;

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

    let custom_settings: std::collections::HashMap<String, String> = cli
        .settings
        .iter()
        .filter_map(|s| {
            let (k, v) = s.split_once('=')?;
            Some((k.to_string(), v.to_string()))
        })
        .collect();

    let profiler = QueryProfiler::new(
        &cli.ch_url,
        &cli.ch_database,
        &cli.ch_user,
        cli.ch_password.as_deref(),
        &custom_settings,
    );

    let opts = ProfilerOptions {
        explain: cli.explain,
        profile: cli.profile,
        processors: cli.processors,
    };

    let result =
        execute_profiled_query(&profiler, &ontology, &security_ctx, &query_json, &opts).await?;

    let instance_health = if cli.health {
        match profiler.fetch_instance_health().await {
            Ok(health) => Some(serde_json::to_value(&health).unwrap_or_default()),
            Err(e) => {
                tracing::warn!("failed to fetch instance health: {e}");
                None
            }
        }
    } else {
        None
    };

    let output = build_output(
        &query_json,
        org_id,
        &cli.traversal_paths,
        &result,
        instance_health,
    );

    match cli.format {
        OutputFormat::Json => {
            println!("{}", serde_json::to_string(&output)?);
        }
        OutputFormat::Pretty => {
            println!("{}", serde_json::to_string_pretty(&output)?);
        }
    }

    Ok(())
}
