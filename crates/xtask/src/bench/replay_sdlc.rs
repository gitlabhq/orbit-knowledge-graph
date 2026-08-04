use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use clickhouse_client::ArrowClickHouseClient;
use serde::Serialize;
use tokio::time::interval;

#[derive(clap::Args)]
pub struct Args {
    /// Source database containing the full dump (read-only).
    #[arg(long, default_value = "staging")]
    pub source: String,

    /// Target database the indexer reads.
    #[arg(long, default_value = "datalake")]
    pub target: String,

    /// Aggregate target rows/s across all tables.
    #[arg(long)]
    pub rate: u64,

    /// Duration in seconds.
    #[arg(long)]
    pub duration_secs: u64,

    /// Comma-separated table list, or "all".
    #[arg(long, default_value = "all")]
    pub tables: String,

    /// Path to manifest.tsv from the dump (table<TAB>rows).
    #[arg(long)]
    pub manifest: PathBuf,

    /// Rows per INSERT batch.
    #[arg(long, default_value = "5000")]
    pub batch_rows: u64,

    /// Write per-table achieved-rate JSON here.
    #[arg(long)]
    pub results: PathBuf,

    /// ClickHouse HTTP URL.
    #[arg(long, env = "CH_URL", default_value = "http://localhost:8123")]
    pub ch_url: String,

    /// ClickHouse user.
    #[arg(long, env = "CH_USER", default_value = "default")]
    pub ch_user: String,

    /// ClickHouse password.
    #[arg(long, env = "CH_PASSWORD")]
    pub ch_password: Option<String>,
}

struct TablePlan {
    name: String,
    weight: f64,
    order_key: String,
    last_key: i64,
}

#[derive(Serialize)]
struct TableResult {
    table: String,
    target_rps: f64,
    achieved_rps: f64,
    total_rows: u64,
    wrapped: bool,
}

fn parse_manifest(path: &Path, filter: &str) -> Result<HashMap<String, u64>> {
    let content = std::fs::read_to_string(path).context("reading manifest")?;
    let mut map = HashMap::new();
    for line in content.lines() {
        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() < 2 {
            continue;
        }
        let table = parts[0];
        if filter != "all" && !filter.split(',').any(|t| t == table) {
            continue;
        }
        let rows: u64 = parts[1].parse().unwrap_or(0);
        if rows > 0 {
            *map.entry(table.to_string()).or_insert(0) += rows;
        }
    }
    Ok(map)
}

async fn detect_order_key(client: &ArrowClickHouseClient, db: &str, table: &str) -> Result<String> {
    let sql = format!(
        "SELECT name FROM system.columns \
         WHERE database = '{db}' AND table = '{table}' AND name = 'id' LIMIT 1"
    );
    let result: Result<String, _> = client.inner().query(&sql).fetch_one().await;
    match result {
        Ok(_) => Ok("id".to_string()),
        Err(_) => {
            // Fall back to the first column of the ORDER BY key.
            let sql = format!(
                "SELECT sorting_key FROM system.tables \
                 WHERE database = '{db}' AND name = '{table}'"
            );
            let key: String = client.inner().query(&sql).fetch_one().await?;
            let first = key
                .split(',')
                .next()
                .unwrap_or("_siphon_replicated_at")
                .trim();
            Ok(first.to_string())
        }
    }
}

pub async fn run(args: Args) -> Result<()> {
    let empty = HashMap::new();
    let client = ArrowClickHouseClient::new(
        &args.ch_url,
        &args.source,
        &args.ch_user,
        args.ch_password.as_deref(),
        &empty,
        &empty,
    );

    let manifest = parse_manifest(&args.manifest, &args.tables)?;
    if manifest.is_empty() {
        bail!("no tables matched in manifest");
    }

    let total_rows: u64 = manifest.values().sum();
    let mut plans: Vec<TablePlan> = Vec::new();
    for (table, rows) in &manifest {
        let weight = *rows as f64 / total_rows as f64;
        let order_key = detect_order_key(&client, &args.source, table).await?;
        plans.push(TablePlan {
            name: table.clone(),
            weight,
            order_key,
            last_key: 0,
        });
    }

    println!(
        "replay-sdlc: {} tables, {total_rows} source rows, target {}/s for {}s",
        plans.len(),
        args.rate,
        args.duration_secs
    );

    let target_client = ArrowClickHouseClient::new(
        &args.ch_url,
        &args.target,
        &args.ch_user,
        args.ch_password.as_deref(),
        &empty,
        &empty,
    );
    let mut results: Vec<TableResult> = plans
        .iter()
        .map(|p| TableResult {
            table: p.name.clone(),
            target_rps: p.weight * args.rate as f64,
            achieved_rps: 0.0,
            total_rows: 0,
            wrapped: false,
        })
        .collect();

    let start = Instant::now();
    let deadline = Duration::from_secs(args.duration_secs);
    let mut tick = interval(Duration::from_secs(1));

    while start.elapsed() < deadline {
        tick.tick().await;

        for (i, plan) in plans.iter_mut().enumerate() {
            let budget = (plan.weight * args.rate as f64).round() as u64;
            if budget == 0 {
                continue;
            }
            let n = budget.min(args.batch_rows);

            // Watermark rewrite: the whole trick (PRD fact 4).
            let sql = format!(
                "INSERT INTO `{target}`.`{table}` \
                 SELECT * REPLACE (now64(6, 'UTC') AS _siphon_watermark) \
                 FROM `{source}`.`{table}` \
                 WHERE `{key}` > {last} ORDER BY `{key}` LIMIT {n}",
                target = args.target,
                source = args.source,
                table = plan.name,
                key = plan.order_key,
                last = plan.last_key,
                n = n,
            );

            match target_client.inner().query(&sql).execute().await {
                Ok(()) => {
                    let max_sql = format!(
                        "SELECT max(`{key}`) FROM `{target}`.`{table}`",
                        key = plan.order_key,
                        target = args.target,
                        table = plan.name,
                    );
                    if let Ok(max_val) = target_client
                        .inner()
                        .query(&max_sql)
                        .fetch_one::<i64>()
                        .await
                    {
                        plan.last_key = max_val;
                    }
                    results[i].total_rows += n;
                }
                Err(e) => {
                    tracing::warn!(table = plan.name, error = %e, "batch failed, wrapping cursor");
                    plan.last_key = 0;
                    results[i].wrapped = true;
                }
            }
        }
    }

    let elapsed = start.elapsed().as_secs_f64();
    for r in &mut results {
        r.achieved_rps = r.total_rows as f64 / elapsed;
    }

    let json = serde_json::to_string_pretty(&results)?;
    std::fs::write(&args.results, &json)?;
    println!("results written to {}", args.results.display());

    let total_achieved: f64 = results.iter().map(|r| r.achieved_rps).sum();
    println!(
        "aggregate: target {}/s, achieved {:.0}/s ({:.0}%)",
        args.rate,
        total_achieved,
        total_achieved / args.rate as f64 * 100.0
    );

    Ok(())
}
