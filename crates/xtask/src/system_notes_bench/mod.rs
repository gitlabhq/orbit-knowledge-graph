//! POC benchmark harness for system-notes edge materialization (kg#499).
//!
//! Runs in two modes:
//!
//! * `parser` — pure CPU benchmark of the regex extractor against an
//!   in-memory corpus (the golden set, or a stdin-fed dump). Reports
//!   notes/sec, per-action match counts, and reference density. Does not
//!   require ClickHouse.
//!
//! * `clickhouse` — exercises the two-stage resolver SQL against a real
//!   ClickHouse instance, with synthetic batches drawn from the corpus.
//!   Reports per-stage latency and result-row counts. Connection details
//!   come from `--url`, `--database`, `--user`, `--password` (or the
//!   matching `CLICKHOUSE_*` env vars).
//!
//! The harness does **not** write edges to `gl_edge` and does not touch the
//! NATS indexer wiring. It is throwaway code (see
//! [poc-plan.md](https://gitlab.com/dgruzd/droid-workspace/-/tree/main/task/2685/poc-plan.md))
//! whose only purpose is to produce the numbers that gate the ADR decision.

pub mod golden;
pub mod parser;
pub mod resolver;

use std::path::PathBuf;
use std::time::Instant;

use anyhow::{Context, Result};
use clap::{Args, Subcommand};
use tracing::info;

use parser::{Action, extract};
use resolver::ResolutionPlan;

#[derive(Debug, Args)]
pub struct BenchArgs {
    #[command(subcommand)]
    pub command: BenchCommand,
}

#[derive(Debug, Subcommand)]
pub enum BenchCommand {
    /// CPU-only benchmark of the regex parser against the golden corpus or a
    /// newline-delimited JSON dump (`{"action":"...","body":"..."}`).
    Parser(ParserArgs),
    /// Exercise the resolver SQL against a live ClickHouse. Requires a
    /// running ClickHouse with `siphon_routes`, `merge_requests`, and
    /// `work_items` populated.
    Clickhouse(ClickhouseArgs),
    /// Print the golden corpus with parser output, for hand-inspection.
    Inspect,
}

#[derive(Debug, Args)]
pub struct ParserArgs {
    /// Path to a newline-delimited JSON dump. Each line:
    /// `{"action": "<action>", "body": "<note body>"}`.
    /// When omitted, the golden corpus (`golden::SAMPLES`) is used.
    #[arg(short, long)]
    pub input: Option<PathBuf>,
    /// How many times to loop the corpus through the parser. Each loop
    /// is timed independently; the report prints min/median/max.
    #[arg(long, default_value_t = 100)]
    pub iterations: usize,
}

#[derive(Debug, Args)]
pub struct ClickhouseArgs {
    /// ClickHouse HTTP URL (e.g. `http://localhost:8123`).
    #[arg(long, env = "CLICKHOUSE_URL", default_value = "http://localhost:8123")]
    pub url: String,
    /// ClickHouse database.
    #[arg(long, env = "CLICKHOUSE_DATABASE", default_value = "default")]
    pub database: String,
    /// ClickHouse user.
    #[arg(long, env = "CLICKHOUSE_USER", default_value = "default")]
    pub user: String,
    /// ClickHouse password (optional).
    #[arg(long, env = "CLICKHOUSE_PASSWORD")]
    pub password: Option<String>,
    /// Traversal path scope (e.g. `1/100/` for namespace 100 under root 1).
    /// Used as the `{traversal_path:String}` parameter on both lookups.
    #[arg(long)]
    pub traversal_path: String,
    /// Synthetic batch size for the resolver test. Each batch issues one
    /// routes lookup and two entity lookups.
    #[arg(long, default_value_t = 1000)]
    pub batch_size: usize,
}

pub async fn run(args: BenchArgs) -> Result<()> {
    init_tracing();
    match args.command {
        BenchCommand::Parser(a) => run_parser_bench(a),
        BenchCommand::Clickhouse(a) => run_clickhouse_bench(a).await,
        BenchCommand::Inspect => {
            run_inspect();
            Ok(())
        }
    }
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .try_init();
}

fn run_inspect() {
    println!("# Golden corpus — system note bodies\n");
    for sample in golden::SAMPLES {
        let refs = extract(sample.action, sample.body);
        println!(
            "action={action:<22} description={desc}",
            action = sample.action.as_str(),
            desc = sample.description
        );
        println!("  body: {body}", body = sample.body.replace('\n', "\\n"));
        if refs.is_empty() {
            println!("  refs: (none)");
        } else {
            for r in &refs {
                println!("  ref:  {r:?}");
            }
        }
        println!();
    }
}

#[derive(Debug, Clone)]
struct CorpusEntry {
    action: Action,
    body: String,
}

fn load_corpus(path: Option<PathBuf>) -> Result<Vec<CorpusEntry>> {
    if let Some(p) = path {
        let raw = std::fs::read_to_string(&p)
            .with_context(|| format!("read input dump at {}", p.display()))?;
        let mut out = Vec::new();
        for (lineno, line) in raw.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let v: serde_json::Value = serde_json::from_str(line)
                .with_context(|| format!("parse JSON at line {}", lineno + 1))?;
            let action_str = v
                .get("action")
                .and_then(|x| x.as_str())
                .with_context(|| format!("missing 'action' at line {}", lineno + 1))?;
            let body = v
                .get("body")
                .and_then(|x| x.as_str())
                .with_context(|| format!("missing 'body' at line {}", lineno + 1))?;
            let Some(action) = Action::parse(action_str) else {
                // Unknown action: skip with a log line, don't fail the run.
                tracing::warn!(action = action_str, "unknown action, skipping");
                continue;
            };
            out.push(CorpusEntry {
                action,
                body: body.to_owned(),
            });
        }
        Ok(out)
    } else {
        Ok(golden::SAMPLES
            .iter()
            .map(|s| CorpusEntry {
                action: s.action,
                body: s.body.to_owned(),
            })
            .collect())
    }
}

fn run_parser_bench(args: ParserArgs) -> Result<()> {
    let corpus = load_corpus(args.input)?;
    let n = corpus.len();
    if n == 0 {
        anyhow::bail!("corpus is empty");
    }

    let mut timings_ns: Vec<u128> = Vec::with_capacity(args.iterations);
    let mut total_refs = 0usize;

    for _ in 0..args.iterations {
        let start = Instant::now();
        let mut refs_this_pass = 0usize;
        for entry in &corpus {
            let refs = extract(entry.action, &entry.body);
            refs_this_pass += refs.len();
        }
        let elapsed = start.elapsed().as_nanos();
        timings_ns.push(elapsed);
        total_refs = refs_this_pass;
    }

    timings_ns.sort_unstable();
    let min_ns = timings_ns[0];
    let median_ns = timings_ns[timings_ns.len() / 2];
    let max_ns = *timings_ns.last().unwrap();

    let median_per_note_ns = median_ns / (n as u128);
    let notes_per_sec_median = 1_000_000_000u128
        .checked_div(median_per_note_ns)
        .unwrap_or(u128::MAX);

    info!(
        corpus_size = n,
        iterations = args.iterations,
        min_ns,
        median_ns,
        max_ns,
        median_per_note_ns,
        notes_per_sec_median,
        total_refs_per_pass = total_refs,
        "parser benchmark"
    );

    println!("=== Parser benchmark ===");
    println!("corpus size:           {n}");
    println!("iterations:            {}", args.iterations);
    println!("min   per-pass:        {min_ns} ns");
    println!("median per-pass:       {median_ns} ns");
    println!("max   per-pass:        {max_ns} ns");
    println!("median per-note:       {median_per_note_ns} ns");
    println!("median notes/sec:      {notes_per_sec_median}");
    println!("refs/pass:             {total_refs}");

    Ok(())
}

async fn run_clickhouse_bench(args: ClickhouseArgs) -> Result<()> {
    use clickhouse_client::ArrowClickHouseClient;

    let client = ArrowClickHouseClient::new(
        &args.url,
        &args.database,
        &args.user,
        args.password.as_deref(),
        &std::collections::HashMap::new(),
    );

    // Build a synthetic batch from the golden corpus, repeated to reach
    // `batch_size`. Real benchmarks against staging would replace this with
    // a streamed read from `siphon_notes`.
    let mut all_refs = Vec::new();
    while all_refs.len() < args.batch_size {
        for sample in golden::SAMPLES {
            let refs = extract(sample.action, sample.body);
            for r in refs {
                all_refs.push(("gitlab-org/gitlab".to_string(), r));
            }
            if all_refs.len() >= args.batch_size {
                break;
            }
        }
    }
    all_refs.truncate(args.batch_size);

    let plan = ResolutionPlan::from_refs(all_refs.iter().map(|(p, r)| (p.as_str(), r)));

    info!(
        batch_size = args.batch_size,
        distinct_paths = plan.paths.len(),
        issue_pairs = plan.issue_pairs.len(),
        mr_pairs = plan.mr_pairs.len(),
        commit_refs = plan.commit_ref_count,
        "resolution plan"
    );

    let paths: Vec<String> = plan.paths.into_iter().collect();
    let start = Instant::now();
    let routes_batches = client
        .query(resolver::ROUTES_SQL)
        .param("traversal_path", args.traversal_path.as_str())
        .param("paths", paths.clone())
        .fetch_arrow()
        .await
        .context("routes lookup failed")?;
    let routes_elapsed = start.elapsed();
    let routes_rows: usize = routes_batches.iter().map(|b| b.num_rows()).sum();
    info!(
        elapsed_ms = routes_elapsed.as_millis() as u64,
        rows = routes_rows,
        paths_in = paths.len(),
        "routes lookup"
    );

    let mr_pairs_json: Vec<serde_json::Value> = plan
        .mr_pairs
        .iter()
        .map(|(_, iid)| serde_json::json!([0_i64, *iid]))
        .collect();
    let start = Instant::now();
    let mr_batches = client
        .query(resolver::MERGE_REQUESTS_SQL)
        .param("traversal_path", args.traversal_path.as_str())
        .param("pairs", mr_pairs_json.clone())
        .fetch_arrow()
        .await
        .context("merge_requests lookup failed")?;
    let mr_elapsed = start.elapsed();
    let mr_rows: usize = mr_batches.iter().map(|b| b.num_rows()).sum();
    info!(
        elapsed_ms = mr_elapsed.as_millis() as u64,
        rows = mr_rows,
        pairs_in = mr_pairs_json.len(),
        "merge_requests lookup"
    );

    let issue_pairs_json: Vec<serde_json::Value> = plan
        .issue_pairs
        .iter()
        .map(|(_, iid)| serde_json::json!([0_i64, *iid]))
        .collect();
    let start = Instant::now();
    let wi_batches = client
        .query(resolver::WORK_ITEMS_SQL)
        .param("traversal_path", args.traversal_path.as_str())
        .param("pairs", issue_pairs_json.clone())
        .fetch_arrow()
        .await
        .context("work_items lookup failed")?;
    let wi_elapsed = start.elapsed();
    let wi_rows: usize = wi_batches.iter().map(|b| b.num_rows()).sum();
    info!(
        elapsed_ms = wi_elapsed.as_millis() as u64,
        rows = wi_rows,
        pairs_in = issue_pairs_json.len(),
        "work_items lookup"
    );

    println!("=== ClickHouse resolver benchmark ===");
    println!("batch size:            {}", args.batch_size);
    println!("traversal_path:        {}", args.traversal_path);
    println!(
        "routes:        {:>6} ms  ({} rows from {} paths)",
        routes_elapsed.as_millis(),
        routes_rows,
        paths.len()
    );
    println!(
        "merge_requests:{:>6} ms  ({} rows from {} pairs)",
        mr_elapsed.as_millis(),
        mr_rows,
        mr_pairs_json.len()
    );
    println!(
        "work_items:    {:>6} ms  ({} rows from {} pairs)",
        wi_elapsed.as_millis(),
        wi_rows,
        issue_pairs_json.len()
    );

    Ok(())
}
