use std::path::PathBuf;
use std::process::Stdio;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use tokio::time::interval;

#[derive(clap::Args)]
pub struct Args {
    /// Directory of v1-gkgdsl-*.ndjson mix files.
    #[arg(long)]
    pub mix: PathBuf,

    /// QPS ladder (comma-separated).
    #[arg(long, value_delimiter = ',')]
    pub qps: Vec<f64>,

    /// Seconds per QPS step.
    #[arg(long, default_value = "60")]
    pub duration_per_step_secs: u64,

    /// Path to the query-profiler binary.
    #[arg(long, default_value = "query-profiler")]
    pub profiler_bin: PathBuf,

    /// ClickHouse URL for the profiler.
    #[arg(long, env = "CH_URL", default_value = "http://localhost:8123")]
    pub ch_url: String,

    /// ClickHouse database.
    #[arg(long, env = "CH_DATABASE", default_value = "gkg")]
    pub ch_database: String,

    /// ClickHouse user.
    #[arg(long, env = "CH_USER", default_value = "default")]
    pub ch_user: String,

    /// ClickHouse password.
    #[arg(long, env = "CH_PASSWORD")]
    pub ch_password: Option<String>,

    /// Graph schema version (e.g. 71).
    #[arg(long)]
    pub schema_version: u32,

    /// Traversal paths for security context (comma-separated).
    #[arg(long, value_delimiter = ',', default_value = "1/9970/")]
    pub traversal_paths: Vec<String>,

    /// Write per-step results JSON here.
    #[arg(long)]
    pub results: PathBuf,

    /// Max concurrent profiler invocations.
    #[arg(long, default_value = "16")]
    pub concurrency: usize,
}

/// One record in a v1-gkgdsl-*.ndjson mix file.
#[derive(Deserialize)]
struct MixRecord {
    dsl: serde_json::Value,
    #[serde(default)]
    schema_version: Option<u32>,
    #[serde(default)]
    #[allow(dead_code, reason = "parsed for future suite filtering")]
    tags: Vec<String>,
}

#[derive(Serialize)]
struct StepResult {
    target_qps: f64,
    achieved_qps: f64,
    total: u64,
    ok: u64,
    errors: u64,
    success_rate: f64,
    p50_ms: f64,
    p90_ms: f64,
    p99_ms: f64,
    verdict: String,
}

fn load_mix(dir: &PathBuf, expected_version: u32) -> Result<Vec<MixRecord>> {
    let mut records = Vec::new();
    let mut entries: Vec<_> = std::fs::read_dir(dir)
        .context("reading mix dir")?
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .file_name()
                .is_some_and(|n| n.to_string_lossy().starts_with("v1-gkgdsl-"))
        })
        .collect();
    entries.sort_by_key(|e| e.path());

    for entry in entries {
        let content = std::fs::read_to_string(entry.path())?;
        for line in content.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let rec: MixRecord = serde_json::from_str(line)
                .with_context(|| format!("parsing {}", entry.path().display()))?;
            if let Some(v) = rec.schema_version
                && v != expected_version
            {
                bail!(
                    "mix record schema_version {} != deployed {}",
                    v,
                    expected_version
                );
            }
            records.push(rec);
        }
    }

    if records.is_empty() {
        bail!("no records in mix dir {}", dir.display());
    }
    Ok(records)
}

struct ProfilerCtx {
    bin: PathBuf,
    ch_url: String,
    ch_database: String,
    ch_user: String,
    ch_password: Option<String>,
    schema_version: u32,
    traversal_paths: Vec<String>,
}

async fn run_one_query(ctx: &ProfilerCtx, query_json: &str) -> Result<f64> {
    let tmp = tempfile::NamedTempFile::new()?;
    std::fs::write(tmp.path(), query_json)?;

    let t_paths: Vec<String> = ctx
        .traversal_paths
        .iter()
        .map(|p| format!("-t={p}"))
        .collect();

    let mut cmd = tokio::process::Command::new(&ctx.bin);
    cmd.arg("--ch-url")
        .arg(&ctx.ch_url)
        .arg("--ch-database")
        .arg(&ctx.ch_database)
        .arg("--ch-user")
        .arg(&ctx.ch_user)
        .arg("--schema-version")
        .arg(ctx.schema_version.to_string())
        .arg("--format")
        .arg("json")
        .arg("-o")
        .arg("/dev/stderr")
        .args(&t_paths)
        .arg(format!("@{}", tmp.path().display()))
        .stdout(Stdio::null())
        .stderr(Stdio::piped());

    if let Some(pw) = &ctx.ch_password {
        cmd.arg("--ch-password").arg(pw);
    }

    let output = cmd.output().await.context("running profiler")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("profiler failed: {stderr}");
    }

    // Parse elapsed from the JSON output on stderr.
    let stderr = String::from_utf8_lossy(&output.stderr);
    let parsed: serde_json::Value = serde_json::from_str(&stderr)
        .unwrap_or_else(|_| serde_json::json!({"summary": {"elapsed_ms": 0.0}}));
    let elapsed = parsed
        .pointer("/summary/elapsed_ms")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);

    Ok(elapsed)
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((p / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

pub async fn run(args: Args) -> Result<()> {
    let records = load_mix(&args.mix, args.schema_version)?;
    println!(
        "query-load: {} records, {} QPS steps, {}s each",
        records.len(),
        args.qps.len(),
        args.duration_per_step_secs
    );

    let ctx = std::sync::Arc::new(ProfilerCtx {
        bin: args.profiler_bin.clone(),
        ch_url: args.ch_url.clone(),
        ch_database: args.ch_database.clone(),
        ch_user: args.ch_user.clone(),
        ch_password: args.ch_password.clone(),
        schema_version: args.schema_version,
        traversal_paths: args.traversal_paths.clone(),
    });

    let mut step_results = Vec::new();
    let sem = std::sync::Arc::new(tokio::sync::Semaphore::new(args.concurrency));

    for &target_qps in &args.qps {
        println!("  step: {target_qps} QPS");
        let step_duration = Duration::from_secs(args.duration_per_step_secs);
        let start = Instant::now();
        let mut tick = interval(Duration::from_secs_f64(1.0 / target_qps));
        let mut handles = Vec::new();
        let mut idx = 0usize;

        while start.elapsed() < step_duration {
            tick.tick().await;
            let rec = &records[idx % records.len()];
            idx += 1;

            let query_json = serde_json::to_string(&rec.dsl)?;
            let ctx = ctx.clone();
            let permit = sem.clone().acquire_owned().await?;

            handles.push(tokio::spawn(async move {
                let result = run_one_query(&ctx, &query_json).await;
                drop(permit);
                result
            }));
        }

        let elapsed = start.elapsed().as_secs_f64();
        let mut latencies = Vec::new();
        let mut ok = 0u64;
        let mut errors = 0u64;
        for h in handles {
            match h.await? {
                Ok(ms) => {
                    latencies.push(ms);
                    ok += 1;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "query failed");
                    errors += 1;
                }
            }
        }

        latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let total = ok + errors;
        let achieved = total as f64 / elapsed;
        let success_rate = if total > 0 {
            ok as f64 / total as f64
        } else {
            0.0
        };

        let verdict = if achieved >= 0.8 * target_qps && success_rate >= 0.99 {
            "PASS"
        } else {
            "FAIL"
        };

        println!(
            "    {verdict}: achieved {achieved:.1} QPS, success {:.1}%, p90 {:.0}ms",
            success_rate * 100.0,
            percentile(&latencies, 90.0)
        );

        step_results.push(StepResult {
            target_qps,
            achieved_qps: achieved,
            total,
            ok,
            errors,
            success_rate,
            p50_ms: percentile(&latencies, 50.0),
            p90_ms: percentile(&latencies, 90.0),
            p99_ms: percentile(&latencies, 99.0),
            verdict: verdict.to_string(),
        });
    }

    let json = serde_json::to_string_pretty(&step_results)?;
    std::fs::write(&args.results, &json)?;
    println!("results written to {}", args.results.display());
    Ok(())
}
