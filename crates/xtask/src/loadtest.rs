//! gRPC load-test subcommand.
//!
//! Replays the performance query corpus (the scenario YAML files under
//! `crates/integration-tests/tests/server/performance/scenarios`) against a
//! running Orbit server over gRPC and reports latency percentiles per query.
//!
//! It reuses the canonical proto and the generated `OrbitServiceClient` from
//! `orbit-server`, and mints JWTs with the server's own `Claims` struct, so
//! there is no vendored proto and no hand-rolled token to drift from the
//! service. The bidirectional `ExecuteQuery` stream is driven end to end,
//! auto-authorizing every resource in the redaction exchange.
//!
//! Each round runs every query once in a seeded shuffled order, sending
//! `concurrency` requests at once, so slow server drift spreads across queries
//! instead of landing on whichever ran last; warm-up rounds are discarded.
//! Every request carries a correlation id that the server prefixes onto its
//! ClickHouse `query_id`s, so `system.query_log` rows can be joined back to
//! scenario and phase.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use clickhouse::Row;
use futures::StreamExt;
use jsonwebtoken::{EncodingKey, Header, encode};
use orbit_server::auth::{Claims, SourceType};
use orbit_server::proto::execute_query_message::Content;
use orbit_server::proto::orbit_service_client::OrbitServiceClient;
use orbit_server::proto::{
    ExecuteQueryError, ExecuteQueryMessage, ExecuteQueryRequest, GetClusterHealthRequest,
    RedactionExchange, RedactionResponse, ResourceAuthorization, redaction_exchange,
};
use rand::SeedableRng;
use rand::rngs::Xoshiro256PlusPlus;
use rand::seq::SliceRandom;
use serde::Deserialize;
use tabled::builder::Builder;
use tabled::settings::Style;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;
use tonic::transport::Channel;

/// Fixed identity for the minted JWT, matching the seeded data.
const USER_ID: u64 = 1;
const USERNAME: &str = "root";
const ORG_ID: u64 = 1;
const MIN_ACCESS_LEVEL: u32 = 20;
/// Token lifetime; generous so a long run never fails auth mid-flight.
const TOKEN_TTL: i64 = 3600;
/// gRPC metadata key the server's labkit correlation layer reads.
const CORRELATION_HEADER: &str = "x-gitlab-correlation-id";
/// Bound on each ClickHouse call so an unreachable host cannot stall the report.
const CLICKHOUSE_TIMEOUT: Duration = Duration::from_secs(30);

pub struct Options {
    pub endpoint: String,
    pub concurrency: usize,
    pub rounds: usize,
    pub warmup_rounds: usize,
    pub seed: u64,
    pub scenarios: PathBuf,
    pub query: Option<String>,
    pub admin: bool,
    pub per_call_timeout: Duration,
    pub run_id: Option<String>,
    pub clickhouse: Option<ClickHouseOptions>,
}

pub struct ClickHouseOptions {
    pub url: String,
    pub user: String,
    pub password: Option<String>,
}

/// Minimal view of a scenario file; unknown fields are ignored on purpose.
#[derive(Debug, Deserialize)]
struct ScenarioFile {
    query: BTreeMap<String, String>,
}

struct LoadQuery {
    label: String,
    body: String,
}

/// Which pass a request belongs to; the code is embedded in the correlation id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Phase {
    Warmup,
    Measured,
}

impl Phase {
    fn code(self) -> &'static str {
        match self {
            Phase::Warmup => "w",
            Phase::Measured => "t",
        }
    }

    fn from_code(code: &str) -> Option<Self> {
        [Phase::Warmup, Phase::Measured]
            .into_iter()
            .find(|p| p.code() == code)
    }

    fn title(self) -> &'static str {
        match self {
            Phase::Warmup => "Warm-up",
            Phase::Measured => "Measured",
        }
    }
}

/// Shared state for every request in a run.
struct Ctx {
    client: OrbitServiceClient<Channel>,
    token: String,
    per_call: Duration,
    run_id: String,
    seed: u64,
    /// Per-run counter so every correlation id is unique.
    seq: AtomicU64,
}

/// One pass: `rounds` rounds of `concurrency` requests per query, all in flight at once.
#[derive(Clone, Copy)]
struct PassSpec {
    phase: Phase,
    rounds: usize,
    concurrency: usize,
}

pub async fn run(opts: Options) -> Result<()> {
    if opts.concurrency == 0 || opts.rounds == 0 {
        bail!("--concurrency and --rounds must both be at least 1");
    }
    let run_id = opts
        .run_id
        .clone()
        .unwrap_or_else(|| to_base36(chrono::Utc::now().timestamp_millis().max(0) as u64));
    if !is_id_safe(&run_id) {
        bail!("--run-id must be non-empty and use only [A-Za-z0-9-], got {run_id:?}");
    }

    let secret = std::env::var("GKG_JWT_SECRET")
        .context("GKG_JWT_SECRET must be set (base64-encoded HMAC key, same as the server)")?;
    let token = mint_token(&secret, opts.admin)?;

    let mut queries = load_scenarios(&opts.scenarios)
        .with_context(|| format!("loading scenarios from {}", opts.scenarios.display()))?;
    if let Some(filter) = &opts.query {
        queries.retain(|q| q.label.contains(filter));
    }
    if queries.is_empty() {
        bail!("no runnable scenarios found (need a `query.json` body)");
    }

    let spec = |phase, rounds| PassSpec {
        phase,
        rounds,
        concurrency: opts.concurrency,
    };
    let warmup = spec(Phase::Warmup, opts.warmup_rounds);
    let measured = spec(Phase::Measured, opts.rounds);

    eprintln!(
        "Load test: endpoint={} run_id={run_id} seed={} queries={}",
        opts.endpoint,
        opts.seed,
        queries.len()
    );
    for p in [warmup, measured] {
        eprintln!(
            "  {}: {} rounds x {} requests/query at concurrency {} => {} requests/query",
            p.phase.title(),
            p.rounds,
            p.concurrency,
            p.concurrency,
            p.rounds * p.concurrency
        );
    }
    eprintln!(
        "JWT: user={USERNAME} org={ORG_ID} admin={} ttl={TOKEN_TTL}s",
        opts.admin
    );

    let mut client = OrbitServiceClient::connect(opts.endpoint.clone())
        .await
        .with_context(|| format!("connecting to {}", opts.endpoint))?;

    // Fail fast if the server is unreachable or the secret is wrong.
    client
        .get_cluster_health(authed(GetClusterHealthRequest::default(), &token, None))
        .await
        .context("health check failed - is the server reachable and GKG_JWT_SECRET correct?")?;

    let ctx = Ctx {
        client,
        token,
        per_call: opts.per_call_timeout,
        run_id: run_id.clone(),
        seed: opts.seed,
        seq: AtomicU64::new(0),
    };

    run_pass(&ctx, &queries, warmup).await;
    let runs = run_pass(&ctx, &queries, measured).await;
    let summaries = queries.iter().zip(runs);
    let summaries = summaries.map(|(q, r)| summarize(&q.label, r)).collect();
    let sent = opts.rounds * opts.concurrency * queries.len();

    let (server, server_note) = match &opts.clickhouse {
        None => (
            None,
            Some("Server-side (ClickHouse) stats not collected: no --clickhouse-url.".to_string()),
        ),
        Some(ch) => match fetch_server_stats(ch, &run_id, sent).await {
            Ok((stats, note)) => (Some(stats), note),
            Err(e) => (
                None,
                Some(format!(
                    "Server-side (ClickHouse) stats unavailable: {}",
                    one_line(&format!("{e:#}"))
                )),
            ),
        },
    };

    let report = Report {
        run_id,
        seed: opts.seed,
        warmup_rounds: opts.warmup_rounds,
        rounds: opts.rounds,
        concurrency: opts.concurrency,
        queries: summaries,
        server,
        server_note,
    };
    print!("{}", render_report(&report));

    // After the report, so CI still publishes it; a failing exit marks the job.
    if let Some(reason) = failure(&report.queries) {
        bail!("{reason}");
    }
    Ok(())
}

/// Why the run failed: any request errored or a query had no successful request.
fn failure(queries: &[QuerySummary]) -> Option<String> {
    let bad: Vec<String> = queries
        .iter()
        .filter(|q| q.err > 0 || q.n == 0)
        .map(|q| format!("{} ({} ok, {} errors)", q.label, q.n, q.err))
        .collect();
    (!bad.is_empty()).then(|| format!("failed requests: {}", bad.join(", ")))
}

fn to_base36(mut n: u64) -> String {
    let mut out = String::new();
    loop {
        out.insert(
            0,
            char::from_digit((n % 36) as u32, 36).expect("digit < 36"),
        );
        n /= 36;
        if n == 0 {
            return out;
        }
    }
}

/// The server only adopts correlation ids made of `[A-Za-z0-9-]`.
fn is_id_safe(id: &str) -> bool {
    !id.is_empty() && id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
}

/// `lt-<run_id>-<phase>-q<NN>-<seq>`.
fn correlation_id(run_id: &str, phase: Phase, query_idx: usize, seq: u64) -> String {
    format!("lt-{run_id}-{}-q{query_idx:02}-{seq}", phase.code())
}

/// Recover `(phase, query index, seq)` from a correlation id or a ClickHouse
/// `query_id` built from one (`<corr>-base`, `<corr>-hydration-...`).
fn parse_correlation(run_id: &str, id: &str) -> Option<(Phase, usize, u64)> {
    let rest = id.strip_prefix(&format!("lt-{run_id}-"))?;
    let mut parts = rest.splitn(4, '-');
    let phase = Phase::from_code(parts.next()?)?;
    let idx = parts.next()?.strip_prefix('q')?.parse().ok()?;
    let seq = parts.next()?.parse().ok()?;
    Some((phase, idx, seq))
}

/// Seeded query order, stable for a given seed, phase and round.
fn round_order(seed: u64, phase: Phase, round: usize, n: usize) -> Vec<usize> {
    let salt = (phase.code().as_bytes()[0] as u64) << 32 | round as u64;
    let mut rng =
        Xoshiro256PlusPlus::seed_from_u64(seed ^ salt.wrapping_mul(0x9E37_79B9_7F4A_7C15));
    let mut order: Vec<usize> = (0..n).collect();
    order.shuffle(&mut rng);
    order
}

/// Per-query outcomes for one pass, with successful latencies kept per round.
#[derive(Default)]
struct QueryRun {
    rounds: Vec<Vec<f64>>,
    errors: BTreeMap<String, usize>,
}

async fn run_pass(ctx: &Ctx, queries: &[LoadQuery], spec: PassSpec) -> Vec<QueryRun> {
    let mut runs: Vec<QueryRun> = queries.iter().map(|_| QueryRun::default()).collect();
    for round in 0..spec.rounds {
        let started = Instant::now();
        let mut round_errors = 0;
        for idx in round_order(ctx.seed, spec.phase, round, queries.len()) {
            let outcomes = run_batch(ctx, idx, &queries[idx], spec).await;
            let run = &mut runs[idx];
            let mut samples = Vec::with_capacity(outcomes.len());
            for outcome in outcomes {
                match outcome {
                    Ok(ms) => samples.push(ms),
                    Err(msg) => {
                        round_errors += 1;
                        *run.errors.entry(truncate(&msg)).or_default() += 1;
                    }
                }
            }
            run.rounds.push(samples);
        }
        eprintln!(
            "{} round {}/{} done in {:.1}s ({})",
            spec.phase.title(),
            round + 1,
            spec.rounds,
            started.elapsed().as_secs_f64(),
            if round_errors == 0 {
                "OK".to_string()
            } else {
                format!("{round_errors} errors")
            }
        );
    }
    runs
}

/// Send `spec.concurrency` requests for one query at once, each with its own correlation id.
async fn run_batch(
    ctx: &Ctx,
    idx: usize,
    query: &LoadQuery,
    spec: PassSpec,
) -> Vec<Result<f64, String>> {
    futures::stream::iter(0..spec.concurrency)
        .map(|_| {
            let mut client = ctx.client.clone();
            let seq = ctx.seq.fetch_add(1, Ordering::Relaxed);
            let corr = correlation_id(&ctx.run_id, spec.phase, idx, seq);
            async move {
                let start = Instant::now();
                let result =
                    execute(&mut client, &ctx.token, &corr, &query.body, ctx.per_call).await;
                let ms = start.elapsed().as_secs_f64() * 1000.0;
                result.map(|()| ms)
            }
        })
        .buffer_unordered(spec.concurrency.max(1))
        .collect()
        .await
}

/// Drive one `ExecuteQuery` stream to completion: send the request, answer any
/// redaction request by authorizing every resource, then return on the result
/// or error. Latency is measured by the caller across this whole exchange.
async fn execute(
    client: &mut OrbitServiceClient<Channel>,
    token: &str,
    correlation_id: &str,
    body: &str,
    per_call: Duration,
) -> Result<(), String> {
    let (sender, receiver) = mpsc::channel(4);
    sender
        .send(ExecuteQueryMessage {
            content: Some(Content::Request(ExecuteQueryRequest {
                query: body.to_string(),
                ..Default::default()
            })),
        })
        .await
        .map_err(|e| e.to_string())?;

    let mut stream = client
        .execute_query(authed(
            ReceiverStream::new(receiver),
            token,
            Some(correlation_id),
        ))
        .await
        .map_err(|e| e.to_string())?
        .into_inner();

    loop {
        let message = timeout(per_call, stream.message())
            .await
            .map_err(|_| "per-call timeout".to_string())?
            .map_err(|e| e.to_string())?;
        let Some(message) = message else {
            return Err("stream closed without a result".to_string());
        };
        match message.content {
            Some(Content::Redaction(RedactionExchange {
                content: Some(redaction_exchange::Content::Required(required)),
            })) => {
                let authorizations = required
                    .resources
                    .into_iter()
                    .map(|resource| ResourceAuthorization {
                        resource_type: resource.resource_type,
                        authorized: resource
                            .resource_ids
                            .into_iter()
                            .map(|id| (id, true))
                            .collect(),
                    })
                    .collect();
                sender
                    .send(ExecuteQueryMessage {
                        content: Some(Content::Redaction(RedactionExchange {
                            content: Some(redaction_exchange::Content::Response(
                                RedactionResponse {
                                    result_id: required.result_id,
                                    authorizations,
                                },
                            )),
                        })),
                    })
                    .await
                    .map_err(|e| e.to_string())?;
            }
            Some(Content::Result(_)) => return Ok(()),
            Some(Content::Error(ExecuteQueryError { message, code })) => {
                return Err(format!("{code}: {message}"));
            }
            other => return Err(format!("unexpected message: {other:?}")),
        }
    }
}

/// Everything the report shows; rendering reads only this.
struct Report {
    run_id: String,
    seed: u64,
    warmup_rounds: usize,
    rounds: usize,
    concurrency: usize,
    /// One entry per scenario, in scenario-index order.
    queries: Vec<QuerySummary>,
    /// ClickHouse stats per scenario index; `None` when not collected.
    server: Option<BTreeMap<usize, ServerStats>>,
    /// One-line note about the ClickHouse columns (not collected, unavailable,
    /// flush failed, or fewer requests matched than sent).
    server_note: Option<String>,
}

/// Client-side latency for one scenario over the measured rounds.
struct QuerySummary {
    label: String,
    n: usize,
    err: usize,
    /// Median of the per-round medians.
    med: f64,
    p90: f64,
    max: f64,
    /// Sorted per-round medians (rounds with no successful request skipped).
    round_medians: Vec<f64>,
    errors: BTreeMap<String, usize>,
}

/// Pooled stats over all rounds, except `med`, which is taken over the
/// per-round medians so one noisy round cannot shift it on its own.
fn summarize(label: &str, run: QueryRun) -> QuerySummary {
    let mut pooled: Vec<f64> = run.rounds.iter().flatten().copied().collect();
    pooled.sort_by(f64::total_cmp);
    let mut round_medians: Vec<f64> = run
        .rounds
        .into_iter()
        .filter(|r| !r.is_empty())
        .map(|mut r| {
            r.sort_by(f64::total_cmp);
            pct(&r, 0.5)
        })
        .collect();
    round_medians.sort_by(f64::total_cmp);
    QuerySummary {
        label: label.to_string(),
        n: pooled.len(),
        err: run.errors.values().sum(),
        med: pct(&round_medians, 0.5),
        p90: pct(&pooled, 0.9),
        max: pooled.last().copied().unwrap_or(0.0),
        round_medians,
        errors: run.errors,
    }
}

/// `min–max (±X%)` of the round medians, where X is half the range as a share of Med.
fn fmt_spread(sorted_meds: &[f64], med: f64) -> String {
    let (Some(lo), Some(hi)) = (sorted_meds.first(), sorted_meds.last()) else {
        return "-".to_string();
    };
    let range = format!("{}–{}", fmt_ms(*lo), fmt_ms(*hi));
    if med > 0.0 {
        format!("{range} (±{:.0}%)", (hi - lo) / 2.0 / med * 100.0)
    } else {
        range
    }
}

/// Markdown report: the run line (kept first; orbit-perf.sh inserts a line after
/// it), one under-load table, errors in a text fence so `|` and backticks cannot
/// break rendering, then the ClickHouse note.
fn render_report(report: &Report) -> String {
    let c = report.concurrency;
    let mut out = format!(
        "Run `{}`: {} queries, seed {}, {} warm-up round(s) discarded, {} measured round(s), \
         each query once per round in seeded shuffled order.\n",
        report.run_id,
        report.queries.len(),
        report.seed,
        report.warmup_rounds,
        report.rounds
    );
    out.push_str(&format!(
        "\n### Under load (concurrency {c})\n\n{} requests per query ({} rounds of {c}, {c} in \
         flight). Client times are end to end, in ms, successful requests only. Med is the \
         median of per-round medians. CH columns are ClickHouse work per request.\n\n{}\n",
        report.rounds * c,
        report.rounds,
        render_table(report)
    ));
    let mut errors = String::new();
    for q in report.queries.iter().filter(|q| !q.errors.is_empty()) {
        errors.push_str(&format!("{}:\n", q.label));
        for (msg, count) in &q.errors {
            errors.push_str(&format!("  [{count}x] {msg}\n"));
        }
    }
    if !errors.is_empty() {
        out.push_str(&format!("\n#### Errors\n\n```text\n{errors}```\n"));
    }
    if let Some(note) = &report.server_note {
        out.push_str(&format!("\n{note}\n"));
    }
    out
}

/// Client and ClickHouse columns side by side; CH cells are `-` without stats.
fn render_table(report: &Report) -> String {
    let mut table = Builder::default();
    table.push_record([
        "Query",
        "N",
        "Err",
        "Med",
        "CH med ms",
        "p90",
        "CH p90 ms",
        "Max",
        "Round med range",
        "Read rows",
        "Peak mem",
    ]);
    for (i, q) in report.queries.iter().enumerate() {
        let ch = report.server.as_ref().and_then(|s| s.get(&i));
        let cell = |f: fn(&ServerStats) -> String| ch.map_or_else(|| "-".to_string(), f);
        // No successful request means no latency, not a latency of 0.
        let ms = |v: f64| if q.n == 0 { "-".to_string() } else { fmt_ms(v) };
        table.push_record([
            q.label.clone(),
            q.n.to_string(),
            q.err.to_string(),
            ms(q.med),
            cell(|s| s.med_ms.to_string()),
            ms(q.p90),
            cell(|s| s.p90_ms.to_string()),
            ms(q.max),
            fmt_spread(&q.round_medians, q.med),
            cell(|s| fmt_thousands(s.med_read_rows)),
            cell(|s| fmt_bytes(s.peak_memory.max(0) as f64)),
        ]);
    }
    table.build().with(Style::markdown()).to_string()
}

/// Per-scenario ClickHouse work, summed across a request's stages.
#[derive(Debug, PartialEq)]
struct ServerStats {
    med_read_rows: u64,
    /// Largest stage memory of any request.
    peak_memory: i64,
    med_ms: u64,
    p90_ms: u64,
}

fn fmt_thousands(n: u64) -> String {
    let digits = n.to_string();
    let mut out = String::new();
    for (i, ch) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(ch);
    }
    out
}

fn fmt_bytes(bytes: f64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{value:.0} {}", UNITS[unit])
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

/// One finished ClickHouse query from `system.query_log`.
#[derive(Debug, Row, Deserialize)]
struct QueryLogRow {
    query_id: String,
    read_rows: u64,
    memory_usage: i64,
    query_duration_ms: u64,
}

/// Finished queries for this run's measured pass, from the local
/// `system.query_log` rather than `clusterAllReplicas`.
fn server_stats_sql(run_id: &str) -> String {
    format!(
        "SELECT query_id, toUInt64(read_rows) AS read_rows, \
         toInt64(memory_usage) AS memory_usage, toUInt64(query_duration_ms) AS query_duration_ms \
         FROM system.query_log \
         WHERE type = 'QueryFinish' AND event_date >= yesterday() AND \
         startsWith(query_id, 'lt-{run_id}-{}-')",
        Phase::Measured.code()
    )
}

/// Per-scenario ClickHouse stats and how many distinct requests they cover.
struct ServerAggregate {
    stats: BTreeMap<usize, ServerStats>,
    matched: usize,
}

/// Fold stage rows into requests, then requests into per-scenario stats.
/// Rows that are not this run's measured pass are ignored.
fn aggregate_server(run_id: &str, rows: &[QueryLogRow]) -> ServerAggregate {
    #[derive(Default)]
    struct Req {
        rows: u64,
        memory: i64,
        ms: u64,
    }
    let mut requests: BTreeMap<(usize, u64), Req> = BTreeMap::new();
    for row in rows {
        let Some((Phase::Measured, idx, seq)) = parse_correlation(run_id, &row.query_id) else {
            continue;
        };
        let req = requests.entry((idx, seq)).or_default();
        req.rows += row.read_rows;
        req.memory = req.memory.max(row.memory_usage);
        req.ms += row.query_duration_ms;
    }

    let matched = requests.len();
    let mut grouped: BTreeMap<usize, Vec<Req>> = BTreeMap::new();
    for ((idx, _), req) in requests {
        grouped.entry(idx).or_default().push(req);
    }
    let stats = grouped
        .into_iter()
        .map(|(idx, reqs)| {
            let sorted = |f: fn(&Req) -> u64| {
                let mut v: Vec<u64> = reqs.iter().map(f).collect();
                v.sort_unstable();
                v
            };
            let ms = sorted(|r| r.ms);
            let stats = ServerStats {
                med_read_rows: pct(&sorted(|r| r.rows), 0.5),
                peak_memory: reqs.iter().map(|r| r.memory).max().unwrap_or(0),
                med_ms: pct(&ms, 0.5),
                p90_ms: pct(&ms, 0.9),
            };
            (idx, stats)
        })
        .collect();
    ServerAggregate { stats, matched }
}

/// Note for the ClickHouse columns when the flush failed or rows are missing.
fn server_note(flush_error: Option<&str>, matched: usize, sent: usize) -> Option<String> {
    let mut parts = Vec::new();
    if let Some(e) = flush_error {
        parts.push(format!(
            "Server-side (ClickHouse): SYSTEM FLUSH LOGS failed ({e}); counts may be incomplete."
        ));
    }
    if matched < sent {
        parts.push(format!(
            "Server-side (ClickHouse): matched {matched} of {sent} requests."
        ));
    }
    (!parts.is_empty()).then(|| parts.join(" "))
}

/// Flush and read `system.query_log`. A failed flush or fewer matched requests
/// than `sent` is reported as a note; a failed connection or read is an error.
async fn fetch_server_stats(
    opts: &ClickHouseOptions,
    run_id: &str,
    sent: usize,
) -> Result<(BTreeMap<usize, ServerStats>, Option<String>)> {
    let mut client = clickhouse::Client::default()
        .with_url(&opts.url)
        .with_user(&opts.user);
    if let Some(password) = &opts.password {
        client = client.with_password(password);
    }

    let flush = timeout(
        CLICKHOUSE_TIMEOUT,
        client.query("SYSTEM FLUSH LOGS").execute(),
    )
    .await;
    let flush_error = match flush {
        Ok(Ok(())) => None,
        Ok(Err(e)) => Some(one_line(&e.to_string())),
        Err(_) => Some("timed out".to_string()),
    };

    let rows: Vec<QueryLogRow> = timeout(
        CLICKHOUSE_TIMEOUT,
        client.query(&server_stats_sql(run_id)).fetch_all(),
    )
    .await
    .context("timed out reading system.query_log")?
    .context("reading system.query_log")?;

    let agg = aggregate_server(run_id, &rows);
    let note = server_note(flush_error.as_deref(), agg.matched, sent);
    Ok((agg.stats, note))
}

/// Index-based percentile over a pre-sorted slice (no interpolation), matching
/// the previous driver's convention so numbers stay comparable.
fn pct<T: Copy + Default>(sorted: &[T], p: f64) -> T {
    let idx = ((sorted.len() as f64) * p) as usize;
    sorted
        .get(idx.min(sorted.len().saturating_sub(1)))
        .copied()
        .unwrap_or_default()
}

fn fmt_ms(ms: f64) -> String {
    format!("{ms:.0}")
}

fn truncate(msg: &str) -> String {
    if msg.chars().count() <= 200 {
        msg.to_string()
    } else {
        let head: String = msg.chars().take(200).collect();
        format!("{head}...")
    }
}

/// Collapse whitespace so a multi-line error fits the one-line report note.
fn one_line(msg: &str) -> String {
    truncate(&msg.split_whitespace().collect::<Vec<_>>().join(" "))
}

fn mint_token(secret_b64: &str, admin: bool) -> Result<String> {
    let now = chrono::Utc::now().timestamp();
    let claims = Claims {
        sub: format!("user:{USER_ID}"),
        iss: "gitlab".to_string(),
        aud: "gitlab-knowledge-graph".to_string(),
        iat: now,
        exp: now + TOKEN_TTL,
        user_id: USER_ID,
        username: USERNAME.to_string(),
        admin,
        organization_id: Some(ORG_ID),
        min_access_level: Some(MIN_ACCESS_LEVEL),
        group_traversal_ids: Vec::new(),
        source_type: SourceType::Core,
        ai_session_id: None,
        request_id: None,
        instance_id: None,
        unique_instance_id: None,
        instance_version: None,
        global_user_id: None,
        host_name: None,
        root_namespace_id: None,
        deployment_type: None,
        realm: None,
        is_gitlab_team_member: None,
        license_checksum: None,
    };
    // Rails base64-decodes the secret before signing; mirror that so the
    // server's JwtValidator accepts the token. Fall back to raw bytes if the
    // value is not valid base64.
    let key = STANDARD
        .decode(secret_b64.trim().as_bytes())
        .unwrap_or_else(|_| secret_b64.as_bytes().to_vec());
    encode(&Header::default(), &claims, &EncodingKey::from_secret(&key)).context("signing JWT")
}

fn authed<T>(message: T, token: &str, correlation_id: Option<&str>) -> Request<T> {
    let mut request = Request::new(message);
    let metadata = request.metadata_mut();
    metadata.insert("authorization", format!("Bearer {token}").parse().unwrap());
    if let Some(id) = correlation_id {
        metadata.insert(CORRELATION_HEADER, id.parse().unwrap());
    }
    request
}

/// Recursively collect scenario files, using each file's path relative to the
/// root (without extension) as its label.
fn load_scenarios(root: &Path) -> Result<Vec<LoadQuery>> {
    let mut queries = Vec::new();
    collect(root, root, &mut queries)?;
    queries.sort_by(|a, b| a.label.cmp(&b.label));
    Ok(queries)
}

fn collect(root: &Path, dir: &Path, out: &mut Vec<LoadQuery>) -> Result<()> {
    for entry in std::fs::read_dir(dir).with_context(|| format!("reading {}", dir.display()))? {
        let path = entry?.path();
        if path.is_dir() {
            collect(root, &path, out)?;
            continue;
        }
        if path.extension().and_then(|e| e.to_str()) != Some("yaml") {
            continue;
        }
        let text = std::fs::read_to_string(&path)?;
        let scenario: ScenarioFile = orbit_utils::yaml::from_str(&text)
            .with_context(|| format!("parsing {}", path.display()))?;
        let Some(body) = scenario.query.get("json") else {
            continue; // gql-only or non-executable scenario
        };
        let label = path
            .strip_prefix(root)
            .unwrap_or(&path)
            .with_extension("")
            .to_string_lossy()
            .into_owned();
        out.push(LoadQuery {
            label,
            body: body.clone(),
        });
    }
    Ok(())
}

/// Minimal view of a query body: the entity and pinned ids of each node.
#[derive(Deserialize)]
struct QueryNodes {
    #[serde(default)]
    nodes: Vec<QueryNode>,
}

#[derive(Deserialize)]
struct QueryNode {
    entity: String,
    #[serde(default)]
    node_ids: Vec<i64>,
}

/// `(entity, id)` for every pinned node id in a query body, deduplicated.
fn node_refs(body: &str) -> Result<Vec<(String, i64)>> {
    let parsed: QueryNodes = serde_json::from_str(body).context("parsing query.json")?;
    let mut refs: Vec<(String, i64)> = parsed
        .nodes
        .into_iter()
        .flat_map(|n| n.node_ids.into_iter().map(move |id| (n.entity.clone(), id)))
        .collect();
    refs.sort();
    refs.dedup();
    Ok(refs)
}

/// Print `<entity>\t<id>\t<label>` per pinned node id, so CI can check the ids exist.
pub fn list_node_ids(scenarios: &Path, filter: Option<&str>) -> Result<()> {
    let queries = load_scenarios(scenarios)
        .with_context(|| format!("loading scenarios from {}", scenarios.display()))?;
    for q in queries
        .iter()
        .filter(|q| filter.is_none_or(|f| q.label.contains(f)))
    {
        for (entity, id) in node_refs(&q.body).with_context(|| q.label.clone())? {
            println!("{entity}\t{id}\t{}", q.label);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run_of(rounds: Vec<Vec<f64>>, errors: &[(&str, usize)]) -> QueryRun {
        QueryRun {
            rounds,
            errors: errors.iter().map(|(m, c)| (m.to_string(), *c)).collect(),
        }
    }

    fn report_with(queries: Vec<QuerySummary>, server_note: Option<String>) -> Report {
        Report {
            run_id: "r1".into(),
            seed: 42,
            warmup_rounds: 1,
            rounds: 2,
            concurrency: 20,
            queries,
            server: None,
            server_note,
        }
    }

    fn row<'a>(out: &'a str, label: &str) -> Vec<&'a str> {
        let line = out
            .lines()
            .find(|l| l.starts_with(&format!("| {label} ")))
            .expect("table row");
        line.trim_matches('|').split('|').map(str::trim).collect()
    }

    #[test]
    fn report_is_one_under_load_table_with_fenced_errors() {
        let q = summarize("q", run_of(vec![vec![10.0, 20.0]], &[("boom | `x`", 2)]));
        let out = render_report(&report_with(vec![q], None));
        assert!(out.starts_with("Run `r1`: 1 queries, seed 42"));
        assert_eq!(out.lines().next().unwrap().len(), out.find('\n').unwrap());
        assert!(out.contains(
            "\n### Under load (concurrency 20)\n\n40 requests per query (2 rounds of 20, 20 in \
             flight). Client times are end to end, in ms, successful requests only. Med is the \
             median of per-round medians. CH columns are ClickHouse work per request.\n\n"
        ));
        assert_eq!(out.matches("\n### ").count(), 1);
        assert!(out.contains("|---"));
        assert!(out.contains("\n#### Errors\n\n```text\nq:\n  [2x] boom | `x`\n```"));
        for gone in ["Min", "Mean", "p99", "bytes", "Latency", "Throughput"] {
            assert!(!out.contains(gone), "{gone} should be gone");
        }
    }

    #[test]
    fn table_columns_are_in_order_and_ch_cells_dash_without_stats() {
        let queries = vec![
            summarize("a", run_of(vec![vec![10.0], vec![30.0]], &[])),
            summarize("b", run_of(vec![vec![1.0]], &[])),
        ];
        let mut report = report_with(queries, None);
        report.server = Some(BTreeMap::from([(
            0,
            ServerStats {
                med_read_rows: 11_816,
                peak_memory: 2 * 1024 * 1024,
                med_ms: 7,
                p90_ms: 12,
            },
        )]));
        let out = render_report(&report);
        assert_eq!(
            row(&out, "Query"),
            [
                "Query",
                "N",
                "Err",
                "Med",
                "CH med ms",
                "p90",
                "CH p90 ms",
                "Max",
                "Round med range",
                "Read rows",
                "Peak mem",
            ]
        );
        assert_eq!(
            row(&out, "a"),
            [
                "a",
                "2",
                "0",
                "30",
                "7",
                "30",
                "12",
                "30",
                "10–30 (±33%)",
                "11,816",
                "2.0 MiB"
            ]
        );
        assert_eq!(
            row(&out, "b"),
            [
                "b",
                "1",
                "0",
                "1",
                "-",
                "1",
                "-",
                "1",
                "1–1 (±0%)",
                "-",
                "-"
            ]
        );
    }

    #[test]
    fn report_without_errors_has_no_errors_section() {
        let q = summarize("q", run_of(vec![vec![5.0]], &[]));
        let out = render_report(&report_with(vec![q], None));
        assert!(!out.contains("Errors"));
        assert!(!out.contains("```"));
    }

    #[test]
    fn report_notes_unavailable_clickhouse_and_still_renders() {
        let q = summarize("q", run_of(vec![vec![5.0]], &[]));
        let note = "Server-side (ClickHouse) stats unavailable: connection refused".to_string();
        let out = render_report(&report_with(vec![q], Some(note)));
        assert_eq!(row(&out, "q")[4], "-");
        assert_eq!(row(&out, "q")[9], "-");
        assert!(
            out.ends_with("\nServer-side (ClickHouse) stats unavailable: connection refused\n")
        );
    }

    #[test]
    fn rows_without_successes_dash_latency_cells() {
        let queries = vec![
            summarize("dead", run_of(vec![vec![], vec![]], &[("boom", 40)])),
            summarize("ok", run_of(vec![vec![5.0]], &[])),
        ];
        let out = render_report(&report_with(queries, None));
        assert_eq!(
            row(&out, "dead"),
            ["dead", "0", "40", "-", "-", "-", "-", "-", "-", "-", "-"]
        );
        assert_eq!(row(&out, "ok")[3], "5");
    }

    #[test]
    fn failure_flags_any_error_or_zero_successes() {
        let ok = summarize("ok", run_of(vec![vec![5.0]], &[]));
        assert_eq!(failure(&[ok]), None);
        assert_eq!(failure(&[]), None);
        let queries = [
            summarize("ok", run_of(vec![vec![5.0]], &[])),
            summarize("flaky", run_of(vec![vec![5.0]], &[("x", 1)])),
            summarize("empty", run_of(vec![vec![]], &[])),
        ];
        assert_eq!(
            failure(&queries).as_deref(),
            Some("failed requests: flaky (1 ok, 1 errors), empty (0 ok, 0 errors)")
        );
    }

    #[test]
    fn server_note_reports_flush_failure_and_partial_matches() {
        assert_eq!(server_note(None, 40, 40), None);
        assert_eq!(
            server_note(None, 0, 40).as_deref(),
            Some("Server-side (ClickHouse): matched 0 of 40 requests.")
        );
        assert_eq!(
            server_note(Some("boom"), 39, 40).as_deref(),
            Some(
                "Server-side (ClickHouse): SYSTEM FLUSH LOGS failed (boom); counts may be \
                 incomplete. Server-side (ClickHouse): matched 39 of 40 requests."
            )
        );
        assert!(
            server_note(Some("boom"), 40, 40)
                .unwrap()
                .contains("FLUSH LOGS failed")
        );
    }

    #[test]
    fn node_refs_lists_pinned_ids_per_entity() {
        let body = r#"{"query_type":"path_finding","nodes":[
            {"id":"u1","entity":"User","node_ids":[2,1]},
            {"id":"u2","entity":"User","node_ids":[2]},
            {"id":"p","entity":"Project"}]}"#;
        assert_eq!(
            node_refs(body).unwrap(),
            [("User".to_string(), 1), ("User".to_string(), 2)]
        );
        assert!(
            node_refs(r#"{"query_type":"traversal"}"#)
                .unwrap()
                .is_empty()
        );
        assert!(node_refs("not json").is_err());
    }

    #[test]
    fn read_rows_use_thousands_separators() {
        assert_eq!(fmt_thousands(0), "0");
        assert_eq!(fmt_thousands(999), "999");
        assert_eq!(fmt_thousands(1000), "1,000");
        assert_eq!(fmt_thousands(11_816), "11,816");
        assert_eq!(fmt_thousands(1_234_567), "1,234,567");
        assert_eq!(fmt_bytes(512.0), "512 B");
        assert_eq!(fmt_bytes(1.5 * 1024.0 * 1024.0 * 1024.0), "1.5 GiB");
    }

    #[test]
    fn round_order_is_a_seeded_permutation() {
        let a = round_order(42, Phase::Measured, 0, 13);
        assert_eq!(a, round_order(42, Phase::Measured, 0, 13));
        let mut sorted = a.clone();
        sorted.sort_unstable();
        assert_eq!(sorted, (0..13).collect::<Vec<_>>());
        assert_ne!(a, round_order(42, Phase::Measured, 1, 13));
        assert_ne!(a, round_order(42, Phase::Warmup, 0, 13));
        assert_ne!(a, round_order(7, Phase::Measured, 0, 13));
        assert!(round_order(42, Phase::Measured, 0, 0).is_empty());
    }

    #[test]
    fn med_is_median_of_round_medians_and_pooled_stats_span_all_rounds() {
        let run = run_of(
            vec![
                vec![100.0, 90.0, 110.0],
                vec![1000.0, 1000.0, 1000.0],
                vec![95.0, 100.0, 105.0],
                vec![],
            ],
            &[("x", 3)],
        );
        let s = summarize("q", run);
        assert_eq!(s.round_medians, vec![100.0, 100.0, 1000.0]);
        assert_eq!(s.med, 100.0);
        assert_eq!(s.n, 9);
        assert_eq!(s.err, 3);
        assert_eq!(s.p90, 1000.0);
        assert_eq!(s.max, 1000.0);
        assert_eq!(fmt_spread(&s.round_medians, s.med), "100–1000 (±450%)");
        assert_eq!(fmt_spread(&[90.0, 100.0, 110.0], 100.0), "90–110 (±10%)");
    }

    #[test]
    fn spread_and_summary_handle_empty_input() {
        let s = summarize("q", run_of(vec![vec![], vec![]], &[("x", 4)]));
        assert!(s.round_medians.is_empty());
        assert_eq!((s.n, s.med, s.p90, s.max), (0, 0.0, 0.0, 0.0));
        assert_eq!(fmt_spread(&s.round_medians, s.med), "-");
        assert_eq!(fmt_spread(&[0.0, 0.0], 0.0), "0–0");
    }

    #[test]
    fn correlation_id_is_server_safe_and_parses_back() {
        let id = correlation_id("123456", Phase::Measured, 7, 42);
        assert_eq!(id, "lt-123456-t-q07-42");
        assert!(is_id_safe(&id));
        assert_eq!(
            parse_correlation("123456", &id),
            Some((Phase::Measured, 7, 42))
        );
        // ClickHouse query_ids append the stage after the correlation id.
        assert_eq!(
            parse_correlation("123456", &format!("{id}-base")),
            Some((Phase::Measured, 7, 42))
        );
        assert_eq!(
            parse_correlation("123456", &format!("{id}-hydration-static-0")),
            Some((Phase::Measured, 7, 42))
        );
        let warm = correlation_id("123456", Phase::Warmup, 12, 0);
        assert_eq!(
            parse_correlation("123456", &warm),
            Some((Phase::Warmup, 12, 0))
        );
        assert_eq!(parse_correlation("other", &id), None);
        assert_eq!(parse_correlation("123456", "lt-123456-l-q01-1"), None);
        assert_eq!(parse_correlation("123456", "lt-123456-t-01-1"), None);
    }

    #[test]
    fn run_ids_are_validated_and_base36_encodes() {
        assert!(is_id_safe("ab-12"));
        assert!(!is_id_safe(""));
        assert!(!is_id_safe("a_b"));
        assert_eq!(to_base36(0), "0");
        assert_eq!(to_base36(35), "z");
        assert_eq!(to_base36(36), "10");
        assert_eq!(to_base36(u64::MAX), "3w5e11264sgsf");
    }

    #[test]
    fn server_sql_filters_run_and_measured_phase_only() {
        let sql = server_stats_sql("987");
        assert!(sql.contains("FROM system.query_log"));
        assert!(!sql.contains("clusterAllReplicas"));
        assert!(!sql.contains("read_bytes"));
        assert!(sql.contains("type = 'QueryFinish'"));
        assert!(sql.ends_with("startsWith(query_id, 'lt-987-t-')"));
        assert!(!sql.contains("-w-") && !sql.contains("-l-"));
        assert!(
            !sql.contains('?'),
            "`?` is a bind placeholder in the clickhouse crate"
        );
    }

    fn log_row(id: &str, rows: u64, mem: i64, ms: u64) -> QueryLogRow {
        QueryLogRow {
            query_id: id.into(),
            read_rows: rows,
            memory_usage: mem,
            query_duration_ms: ms,
        }
    }

    #[test]
    fn server_stats_sum_stages_per_request_and_skip_other_phases_and_foreign_rows() {
        let rows = vec![
            log_row("lt-r1-t-q00-1-base", 10, 500, 5),
            log_row("lt-r1-t-q00-1-hydration-static", 5, 900, 3),
            log_row("lt-r1-t-q00-2-base", 30, 700, 20),
            log_row("lt-r1-t-q00-3-base", 20, 600, 10),
            log_row("lt-r1-t-q01-4-base", 1, 1, 1),
            log_row("lt-r1-w-q02-0-base", 9999, 9999, 9999),
            log_row("lt-r1-l-q02-5-base", 9999, 9999, 9999),
            log_row("lt-r2-t-q00-1-base", 9999, 9999, 9999),
            log_row("01JABCDEF0123456789ABCDEFG-base", 9999, 9999, 9999),
        ];
        let agg = aggregate_server("r1", &rows);
        assert_eq!(agg.matched, 4);
        let stats = agg.stats;
        assert_eq!(stats.len(), 2);
        // Requests: (15 rows, 900 mem, 8 ms), (30, 700, 20), (20, 600, 10).
        assert_eq!(
            stats[&0],
            ServerStats {
                med_read_rows: 20,
                peak_memory: 900,
                med_ms: 10,
                p90_ms: 20,
            }
        );
        assert_eq!(stats[&1].med_read_rows, 1);
        assert!(!stats.contains_key(&2));
    }

    #[test]
    fn percentile_uses_nearest_rank_index() {
        let sorted: Vec<f64> = (1..=100).map(f64::from).collect();
        // idx = (100 * p) as usize, clamped to the last element.
        assert_eq!(pct(&sorted, 0.5), 51.0);
        assert_eq!(pct(&sorted, 0.9), 91.0);
        assert_eq!(pct(&sorted, 0.99), 100.0);
        assert_eq!(pct(&sorted, 1.0), 100.0);
        assert_eq!(pct::<f64>(&[], 0.9), 0.0);
        assert_eq!(pct(&[1, 2, 3], 0.5), 2);
        assert_eq!(pct::<u64>(&[], 0.5), 0);
    }

    #[test]
    fn truncate_caps_long_messages_on_char_boundaries() {
        assert_eq!(truncate("short"), "short");
        let out = truncate(&"x".repeat(250));
        assert!(out.ends_with("..."));
        assert_eq!(out.chars().count(), 203);
        assert_eq!(one_line("a\n  b\tc"), "a b c");
    }

    #[test]
    fn scenarios_missing_json_body_are_skipped() {
        let dir = std::env::temp_dir().join(format!("xtask-loadtest-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("with_json.yaml"),
            "query:\n  json: '{\"query_type\":\"traversal\"}'\n",
        )
        .unwrap();
        std::fs::write(dir.join("gql_only.yaml"), "query:\n  gql: 'MATCH (n)'\n").unwrap();

        let queries = load_scenarios(&dir).unwrap();
        std::fs::remove_dir_all(&dir).ok();

        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0].label, "with_json");
    }
}
