mod executor;
mod output;
mod service;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use clickhouse_client::ArrowClickHouseClient;
use compiler::SecurityContext;
use ontology::Ontology;
use tracing_subscriber::EnvFilter;

use executor::enrich_output;
use gkg_server_config::ProfilingConfig;
use output::{ProfilerOutput, build_output};
use service::ProfilerPipelineService;

#[derive(Parser)]
#[command(
    name = "query-profiler",
    about = "Profile GKG queries against ClickHouse"
)]
struct Cli {
    /// JSON query string or @filepath (single query or named query collection)
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

    /// Schema version prefix for table names (e.g. 24 → v24_gl_project).
    /// Defaults to the embedded SCHEMA_VERSION from config/SCHEMA_VERSION.
    #[arg(long)]
    schema_version: Option<u32>,

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

    /// Filter multi-query files by regex pattern (e.g. --filter "Q14|E2|D2")
    #[arg(long)]
    filter: Option<String>,

    /// Write output to a file instead of stdout
    #[arg(short = 'o', long)]
    output: Option<PathBuf>,

    /// Send raw SQL directly to ClickHouse (skip DSL compilation).
    /// Supports @filepath for SQL files.
    #[arg(long)]
    raw_sql: bool,
}

#[derive(Clone, clap::ValueEnum)]
enum OutputFormat {
    Json,
    Pretty,
}

struct RunContext<'a> {
    service: &'a ProfilerPipelineService,
    client: &'a ArrowClickHouseClient,
    security_ctx: &'a SecurityContext,
    profiling_config: &'a ProfilingConfig,
    org_id: i64,
    traversal_paths: &'a [String],
}

async fn run_single(
    ctx: &RunContext<'_>,
    query_json: &str,
    instance_health: Option<serde_json::Value>,
) -> Result<ProfilerOutput> {
    let mut output = ctx
        .service
        .run_query(ctx.security_ctx.clone(), query_json)
        .await
        .map_err(|e| anyhow::anyhow!("pipeline failed: {e}"))?;

    enrich_output(ctx.client, &mut output, ctx.profiling_config).await;

    Ok(build_output(
        query_json,
        ctx.org_id,
        ctx.traversal_paths,
        &output,
        instance_health,
    ))
}

fn emit_output(
    format: &OutputFormat,
    value: &impl serde::Serialize,
    output_path: Option<&PathBuf>,
) -> Result<()> {
    let serialized = match format {
        OutputFormat::Json => serde_json::to_string(value)?,
        OutputFormat::Pretty => serde_json::to_string_pretty(value)?,
    };

    if let Some(path) = output_path {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create directory {}", parent.display()))?;
        }
        std::fs::write(path, &serialized)
            .with_context(|| format!("failed to write output to {}", path.display()))?;
        eprintln!("wrote {}", path.display());
    } else {
        println!("{serialized}");
    }

    Ok(())
}

async fn run_raw_sql(
    client: &ArrowClickHouseClient,
    sql: &str,
    cli: &Cli,
    config: &ProfilingConfig,
) -> Result<()> {
    let start = std::time::Instant::now();
    let (batches, summary) = client
        .query(sql)
        .fetch_arrow_with_summary()
        .await
        .map_err(|e| anyhow::anyhow!("query failed: {e}"))?;

    let elapsed = start.elapsed();
    let result_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>() as u64;

    let summary =
        summary.ok_or_else(|| anyhow::anyhow!("missing X-ClickHouse-Summary header"))?;

    let mut result = serde_json::json!({
        "sql": sql,
        "elapsed_ms": elapsed.as_secs_f64() * 1000.0,
        "stats": {
            "read_rows": summary.read_rows().unwrap_or(0),
            "read_bytes": summary.read_bytes().unwrap_or(0),
            "result_rows": summary.result_rows().unwrap_or(result_rows),
            "memory_usage": summary.memory_usage().unwrap_or(0),
            "elapsed_ns": summary.elapsed_ns().unwrap_or(elapsed.as_nanos() as u64),
        }
    });

    if config.explain {
        result["explain_plan"] = match client.explain_plan(sql).await {
            Ok(plan) => serde_json::Value::String(plan),
            Err(e) => {
                eprintln!("EXPLAIN PLAN failed: {e}");
                serde_json::Value::Null
            }
        };
        result["explain_pipeline"] = match client.explain_pipeline(sql).await {
            Ok(pipeline) => serde_json::Value::String(pipeline),
            Err(e) => {
                eprintln!("EXPLAIN PIPELINE failed: {e}");
                serde_json::Value::Null
            }
        };
    }

    emit_output(&cli.format, &result, cli.output.as_ref())
}

fn embedded_schema_version() -> u32 {
    include_str!("../../../../config/SCHEMA_VERSION")
        .trim()
        .parse()
        .expect("config/SCHEMA_VERSION must contain a valid u32")
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

    let ontology = {
        let ont = Ontology::load_from_dir(&cli.ontology).context("failed to load ontology")?;
        let version = cli.schema_version.unwrap_or(embedded_schema_version());
        if version > 0 {
            let prefix = format!("v{version}_");
            eprintln!("using table prefix: {prefix}");
            Arc::new(ont.with_schema_version_prefix(&prefix))
        } else {
            Arc::new(ont)
        }
    };

    let org_id = gkg_utils::traversal_path::org_id(&cli.traversal_paths[0])
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

    if cli.raw_sql {
        return run_raw_sql(&client, &query_json, &cli, &profiling_config).await;
    }

    let parsed: serde_json::Value =
        serde_json::from_str(&query_json).context("failed to parse query JSON")?;

    let is_single_query = matches!(parsed.get("query_type"), Some(serde_json::Value::String(_)));

    let instance_health = if profiling_config.instance_health {
        match client.fetch_instance_health().await {
            Ok(health) => Some(serde_json::to_value(&health).unwrap_or_default()),
            Err(e) => {
                tracing::warn!("failed to fetch instance health: {e}");
                None
            }
        }
    } else {
        None
    };

    let service = ProfilerPipelineService::new(ontology, Arc::clone(&client));
    let run_ctx = RunContext {
        service: &service,
        client: &client,
        security_ctx: &security_ctx,
        profiling_config: &profiling_config,
        org_id,
        traversal_paths: &cli.traversal_paths,
    };

    if is_single_query {
        if cli.filter.is_some() {
            eprintln!("warning: --filter is ignored for single-query input");
        }

        let profiler_output = run_single(&run_ctx, &query_json, instance_health).await?;

        emit_output(&cli.format, &profiler_output, cli.output.as_ref())?;
    } else {
        let queries = parsed.as_object().context(if parsed.is_array() {
            "input is a JSON array; expected a single query object or a named query collection"
        } else {
            "multi-query file must be a JSON object with named queries"
        })?;

        let entries: Vec<_> = match &cli.filter {
            Some(f) => {
                let re =
                    regex::Regex::new(f).with_context(|| format!("invalid filter regex: {f}"))?;
                queries.iter().filter(|(k, _)| re.is_match(k)).collect()
            }
            None => queries.iter().collect(),
        };

        if entries.is_empty() {
            anyhow::bail!(
                "no queries matched filter {:?}",
                cli.filter.as_deref().unwrap_or("")
            );
        }

        let total = entries.len();
        let mut results: BTreeMap<String, serde_json::Value> = BTreeMap::new();

        for (i, (name, query_value)) in entries.into_iter().enumerate() {
            eprintln!("[{}/{}] {}...", i + 1, total, name);

            let single_json =
                serde_json::to_string(query_value).context("failed to serialize query value")?;

            match run_single(&run_ctx, &single_json, instance_health.clone()).await {
                Ok(output) => {
                    let value = serde_json::to_value(&output)
                        .context("failed to serialize profiler output")?;
                    results.insert(name.clone(), value);
                }
                Err(e) => {
                    eprintln!("  FAILED: {e}");
                    results.insert(
                        name.clone(),
                        serde_json::json!({ "error": format!("{e:#}") }),
                    );
                }
            }
        }

        emit_output(&cli.format, &results, cli.output.as_ref())?;
    }

    Ok(())
}
