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
//! Measurement: a round runs every query once, in a seeded shuffled order, so
//! slow drift on the server spreads across queries instead of landing on
//! whichever ran last. Warm-up rounds run first and are discarded. Every
//! request carries an `x-gitlab-correlation-id`; the server prefixes its
//! ClickHouse `query_id`s with it, which lets the report join
//! `system.query_log` back to scenario and phase.

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
use tabled::settings::Style;
use tabled::{Table, Tabled};
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;
use tonic::transport::Channel;

/// Fixed identity for the minted JWT. These are stable defaults for a load
/// test against seeded data; expose them as flags later if a run needs to vary
/// the caller.
const USER_ID: u64 = 1;
const USERNAME: &str = "root";
const ORG_ID: u64 = 1;
const MIN_ACCESS_LEVEL: u32 = 20;
/// Token lifetime. Generous so a long run never fails auth mid-flight — the
/// bug that bit the Python driver, which minted a single 5-minute token.
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
    pub latency_concurrency: usize,
    pub latency_requests: usize,
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

/// Minimal view of a scenario file — only the query body matters here. The
/// full schema (config, expect, ...) is owned by the integration-testkit
/// runner; unknown fields are ignored on purpose.
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
    Latency,
    Throughput,
}

impl Phase {
    fn code(self) -> char {
        match self {
            Phase::Warmup => 'w',
            Phase::Latency => 'l',
            Phase::Throughput => 't',
        }
    }

    fn from_code(code: &str) -> Option<Self> {
        match code {
            "w" => Some(Phase::Warmup),
            "l" => Some(Phase::Latency),
            "t" => Some(Phase::Throughput),
            _ => None,
        }
    }

    fn title(self) -> &'static str {
        match self {
            Phase::Warmup => "Warm-up",
            Phase::Latency => "Latency",
            Phase::Throughput => "Throughput",
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
    /// Monotonic per-run counter so every correlation id is unique.
    seq: AtomicU64,
}

/// Shape of one pass: `rounds` rounds, each sending `requests` requests per
/// query with at most `in_flight` outstanding.
#[derive(Clone, Copy)]
struct PassSpec {
    phase: Phase,
    rounds: usize,
    requests: usize,
    in_flight: usize,
}

pub async fn run(opts: Options) -> Result<()> {
    if opts.concurrency == 0 || opts.rounds == 0 {
        bail!("--concurrency and --rounds must both be at least 1");
    }
    if opts.latency_concurrency > 0 && opts.latency_requests == 0 {
        bail!("--latency-requests must be at least 1 when --latency-concurrency is set");
    }
    let run_id = opts.run_id.clone().unwrap_or_else(default_run_id);
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

    let warmup = PassSpec {
        phase: Phase::Warmup,
        rounds: opts.warmup_rounds,
        requests: opts.concurrency,
        in_flight: opts.concurrency,
    };
    let latency = PassSpec {
        phase: Phase::Latency,
        rounds: opts.rounds,
        requests: opts.latency_requests,
        in_flight: opts.latency_concurrency,
    };
    let throughput = PassSpec {
        phase: Phase::Throughput,
        rounds: opts.rounds,
        requests: opts.concurrency,
        in_flight: opts.concurrency,
    };
    let mut passes = vec![warmup];
    if opts.latency_concurrency > 0 {
        passes.push(latency);
    }
    passes.push(throughput);

    eprintln!(
        "Load test: endpoint={} run_id={run_id} seed={} queries={}",
        opts.endpoint,
        opts.seed,
        queries.len()
    );
    for p in &passes {
        eprintln!(
            "  {}: {} rounds x {} requests/query at concurrency {} => {} requests/query",
            p.phase.title(),
            p.rounds,
            p.requests,
            p.in_flight,
            p.rounds * p.requests
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

    let mut pass_reports = Vec::new();
    for spec in passes {
        let runs = run_pass(&ctx, &queries, spec).await;
        if spec.phase == Phase::Warmup {
            continue;
        }
        pass_reports.push(PassReport {
            phase: spec.phase,
            concurrency: spec.in_flight,
            requests_per_round: spec.requests,
            queries: queries
                .iter()
                .zip(&runs)
                .map(|(q, r)| summarize(&q.label, r))
                .collect(),
            server: None,
        });
    }

    let server_note = match &opts.clickhouse {
        None => None,
        Some(ch) => match fetch_server_stats(ch, &run_id).await {
            Ok((stats, note)) => {
                for pass in &mut pass_reports {
                    pass.server = Some(
                        (0..pass.queries.len())
                            .map(|i| stats.get(&(pass.phase, i)).cloned())
                            .collect(),
                    );
                }
                note
            }
            Err(e) => Some(format!(
                "Server-side (ClickHouse) stats unavailable: {}",
                one_line(&format!("{e:#}"))
            )),
        },
    };

    let report = Report {
        run_id,
        seed: opts.seed,
        warmup_rounds: opts.warmup_rounds,
        rounds: opts.rounds,
        passes: pass_reports,
        server_note,
    };
    print!("{}", render_report(&report));

    Ok(())
}

/// Default run id: the current Unix time in milliseconds, base 36.
fn default_run_id() -> String {
    let millis = chrono::Utc::now().timestamp_millis().max(0) as u64;
    to_base36(millis)
}

fn to_base36(mut n: u64) -> String {
    const DIGITS: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut out = Vec::new();
    loop {
        out.push(DIGITS[(n % 36) as usize]);
        n /= 36;
        if n == 0 {
            break;
        }
    }
    out.reverse();
    String::from_utf8(out).expect("ascii digits")
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

/// Seeded query order for one round. Stable for a given seed, phase and round
/// so a run can be replayed in the same order.
fn round_order(seed: u64, phase: Phase, round: usize, n: usize) -> Vec<usize> {
    let salt = (phase.code() as u64) << 32 | round as u64;
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

/// Send `spec.requests` requests for one query with at most `spec.in_flight`
/// outstanding, each tagged with its own correlation id.
async fn run_batch(
    ctx: &Ctx,
    idx: usize,
    query: &LoadQuery,
    spec: PassSpec,
) -> Vec<Result<f64, String>> {
    futures::stream::iter(0..spec.requests)
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
        .buffer_unordered(spec.in_flight.max(1))
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

/// Everything the report shows. Rendering reads only this, so another output
/// format can be added beside the markdown without touching measurement.
struct Report {
    run_id: String,
    seed: u64,
    warmup_rounds: usize,
    rounds: usize,
    passes: Vec<PassReport>,
    /// One-line note about the ClickHouse section (unavailable, partial).
    server_note: Option<String>,
}

struct PassReport {
    phase: Phase,
    concurrency: usize,
    requests_per_round: usize,
    /// One entry per scenario, in scenario-index order.
    queries: Vec<QuerySummary>,
    /// Same order as `queries`; `None` entries are scenarios with no rows.
    /// `None` overall means ClickHouse stats were not collected.
    server: Option<Vec<Option<ServerStats>>>,
}

struct QuerySummary {
    label: String,
    n: usize,
    err: usize,
    min: f64,
    mean: f64,
    /// Median of the per-round medians.
    med: f64,
    p90: f64,
    p99: f64,
    max: f64,
    /// Sorted per-round medians (rounds with no successful request skipped).
    round_medians: Vec<f64>,
    errors: BTreeMap<String, usize>,
}

/// Pooled stats over all rounds, except `med`, which is taken over the
/// per-round medians so one noisy round cannot shift it on its own.
fn summarize(label: &str, run: &QueryRun) -> QuerySummary {
    let mut pooled: Vec<f64> = run.rounds.iter().flatten().copied().collect();
    pooled.sort_by(|a, b| a.total_cmp(b));
    let round_medians = round_medians(&run.rounds);
    let n = pooled.len();
    QuerySummary {
        label: label.to_string(),
        n,
        err: run.errors.values().sum(),
        min: pooled.first().copied().unwrap_or(0.0),
        mean: if n == 0 {
            0.0
        } else {
            pooled.iter().sum::<f64>() / n as f64
        },
        med: pct(&round_medians, 0.5),
        p90: pct(&pooled, 0.9),
        p99: pct(&pooled, 0.99),
        max: pooled.last().copied().unwrap_or(0.0),
        round_medians,
        errors: run.errors.clone(),
    }
}

/// Sorted medians of each non-empty round.
fn round_medians(rounds: &[Vec<f64>]) -> Vec<f64> {
    let mut meds: Vec<f64> = rounds
        .iter()
        .filter(|r| !r.is_empty())
        .map(|r| {
            let mut sorted = r.clone();
            sorted.sort_by(|a, b| a.total_cmp(b));
            pct(&sorted, 0.5)
        })
        .collect();
    meds.sort_by(|a, b| a.total_cmp(b));
    meds
}

/// `min–max (±X%)`, where X is half the range as a share of `med`.
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

#[derive(Tabled)]
struct ReportRow {
    #[tabled(rename = "Query")]
    query: String,
    #[tabled(rename = "N")]
    n: usize,
    #[tabled(rename = "Err")]
    err: usize,
    #[tabled(rename = "Min")]
    min: String,
    #[tabled(rename = "Mean")]
    mean: String,
    #[tabled(rename = "Med")]
    med: String,
    #[tabled(rename = "Round med range")]
    round_med_range: String,
    #[tabled(rename = "p90")]
    p90: String,
    #[tabled(rename = "p99")]
    p99: String,
    #[tabled(rename = "Max")]
    max: String,
}

impl From<&QuerySummary> for ReportRow {
    fn from(s: &QuerySummary) -> Self {
        ReportRow {
            query: s.label.clone(),
            n: s.n,
            err: s.err,
            min: fmt_ms(s.min),
            mean: fmt_ms(s.mean),
            med: fmt_ms(s.med),
            round_med_range: fmt_spread(&s.round_medians, s.med),
            p90: fmt_ms(s.p90),
            p99: fmt_ms(s.p99),
            max: fmt_ms(s.max),
        }
    }
}

/// Markdown report: per pass a latency table, an optional ClickHouse table,
/// then errors in a text fence so `|` and backticks cannot break rendering.
fn render_report(report: &Report) -> String {
    let queries = report.passes.first().map_or(0, |p| p.queries.len());
    let mut out = format!(
        "Run `{}`: {queries} queries, seed {}, {} warm-up round(s) discarded, {} measured round(s), \
         each query once per round in seeded shuffled order.\n",
        report.run_id, report.seed, report.warmup_rounds, report.rounds
    );
    for pass in &report.passes {
        let title = format!("{} (concurrency {})", pass.phase.title(), pass.concurrency);
        out.push_str(&format!(
            "\n### {title}\n\n{} requests per query per round. Latencies in ms, successful \
             requests only. Med is the median of per-round medians; Round med range is the \
             min–max of those round medians (± half the range as % of Med). Min, Mean, p90, \
             p99 and Max are pooled over all rounds.\n\n{}\n",
            pass.requests_per_round,
            Table::new(pass.queries.iter().map(ReportRow::from)).with(Style::markdown())
        ));
        if let Some(server) = &pass.server {
            out.push_str(&format!(
                "\n#### Server-side (ClickHouse), {title}\n\n{}",
                render_server_table(&pass.queries, server)
            ));
        }
        let errored: Vec<&QuerySummary> = pass
            .queries
            .iter()
            .filter(|q| !q.errors.is_empty())
            .collect();
        if !errored.is_empty() {
            out.push_str(&format!("\n#### Errors, {title}\n\n```text\n"));
            for q in errored {
                out.push_str(&format!("{}:\n", q.label));
                for (msg, count) in &q.errors {
                    out.push_str(&format!("  [{count}x] {msg}\n"));
                }
            }
            out.push_str("```\n");
        }
    }
    if let Some(note) = &report.server_note {
        out.push_str(&format!("\n{note}\n"));
    }
    out
}

/// Per-scenario ClickHouse work for one pass, summed across a request's stages.
#[derive(Debug, Clone, PartialEq)]
struct ServerStats {
    requests: usize,
    med_read_rows: u64,
    med_read_bytes: u64,
    peak_memory: i64,
    med_ms: u64,
    p90_ms: u64,
}

#[derive(Tabled)]
struct ServerRow {
    #[tabled(rename = "Query")]
    query: String,
    #[tabled(rename = "Reqs")]
    requests: String,
    #[tabled(rename = "Med read rows")]
    read_rows: String,
    #[tabled(rename = "Med read bytes")]
    read_bytes: String,
    #[tabled(rename = "Peak memory")]
    peak_memory: String,
    #[tabled(rename = "Med CH ms")]
    med_ms: String,
    #[tabled(rename = "p90 CH ms")]
    p90_ms: String,
}

fn render_server_table(queries: &[QuerySummary], server: &[Option<ServerStats>]) -> String {
    let rows = queries.iter().enumerate().map(|(i, q)| {
        let dash = || "-".to_string();
        match server.get(i).and_then(Option::as_ref) {
            Some(s) => ServerRow {
                query: q.label.clone(),
                requests: s.requests.to_string(),
                read_rows: s.med_read_rows.to_string(),
                read_bytes: fmt_bytes(s.med_read_bytes as f64),
                peak_memory: fmt_bytes(s.peak_memory.max(0) as f64),
                med_ms: s.med_ms.to_string(),
                p90_ms: s.p90_ms.to_string(),
            },
            None => ServerRow {
                query: q.label.clone(),
                requests: "0".to_string(),
                read_rows: dash(),
                read_bytes: dash(),
                peak_memory: dash(),
                med_ms: dash(),
                p90_ms: dash(),
            },
        }
    });
    format!(
        "Per request, read rows, read bytes and duration are summed over its \
         ClickHouse queries and memory is the largest of them; Peak memory is the \
         max over requests.\n\n{}\n",
        Table::new(rows).with(Style::markdown())
    )
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
    read_bytes: u64,
    memory_usage: i64,
    query_duration_ms: u64,
}

/// Finished queries for this run's measured passes. Warm-up (`w`) ids are left
/// out; the local `system.query_log` is read, not `clusterAllReplicas`.
fn server_stats_sql(run_id: &str) -> String {
    let measured = [Phase::Latency, Phase::Throughput]
        .iter()
        .map(|p| format!("startsWith(query_id, 'lt-{run_id}-{}-')", p.code()))
        .collect::<Vec<_>>()
        .join(" OR ");
    format!(
        "SELECT query_id, toUInt64(read_rows) AS read_rows, toUInt64(read_bytes) AS read_bytes, \
         toInt64(memory_usage) AS memory_usage, toUInt64(query_duration_ms) AS query_duration_ms \
         FROM system.query_log \
         WHERE type = 'QueryFinish' AND event_date >= yesterday() AND ({measured})"
    )
}

/// Fold stage rows into requests, then requests into per-(phase, scenario)
/// stats. Rows whose id does not parse for this run are ignored.
fn aggregate_server(run_id: &str, rows: &[QueryLogRow]) -> BTreeMap<(Phase, usize), ServerStats> {
    #[derive(Default)]
    struct Req {
        rows: u64,
        bytes: u64,
        memory: i64,
        ms: u64,
    }
    let mut requests: BTreeMap<(Phase, usize, u64), Req> = BTreeMap::new();
    for row in rows {
        let Some((phase, idx, seq)) = parse_correlation(run_id, &row.query_id) else {
            continue;
        };
        if phase == Phase::Warmup {
            continue;
        }
        let req = requests.entry((phase, idx, seq)).or_default();
        req.rows += row.read_rows;
        req.bytes += row.read_bytes;
        req.memory = req.memory.max(row.memory_usage);
        req.ms += row.query_duration_ms;
    }

    let mut grouped: BTreeMap<(Phase, usize), Vec<Req>> = BTreeMap::new();
    for ((phase, idx, _), req) in requests {
        grouped.entry((phase, idx)).or_default().push(req);
    }
    grouped
        .into_iter()
        .map(|(key, reqs)| {
            let sorted = |f: fn(&Req) -> u64| {
                let mut v: Vec<u64> = reqs.iter().map(f).collect();
                v.sort_unstable();
                v
            };
            let ms = sorted(|r| r.ms);
            let stats = ServerStats {
                requests: reqs.len(),
                med_read_rows: pct_u64(&sorted(|r| r.rows), 0.5),
                med_read_bytes: pct_u64(&sorted(|r| r.bytes), 0.5),
                peak_memory: reqs.iter().map(|r| r.memory).max().unwrap_or(0),
                med_ms: pct_u64(&ms, 0.5),
                p90_ms: pct_u64(&ms, 0.9),
            };
            (key, stats)
        })
        .collect()
}

/// Flush and read `system.query_log`. A failed flush is reported as a note
/// (rows may lag) rather than an error; a failed connection or read is an error.
async fn fetch_server_stats(
    opts: &ClickHouseOptions,
    run_id: &str,
) -> Result<(BTreeMap<(Phase, usize), ServerStats>, Option<String>)> {
    let mut client = clickhouse::Client::default()
        .with_url(&opts.url)
        .with_user(&opts.user);
    if let Some(password) = &opts.password {
        client = client.with_password(password);
    }

    let flush_note = match timeout(
        CLICKHOUSE_TIMEOUT,
        client.query("SYSTEM FLUSH LOGS").execute(),
    )
    .await
    {
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

    let note = flush_note.map(|e| {
        format!(
            "Server-side (ClickHouse): SYSTEM FLUSH LOGS failed ({e}); counts may be incomplete."
        )
    });
    Ok((aggregate_server(run_id, &rows), note))
}

/// Index-based percentile over a pre-sorted slice (no interpolation), matching
/// the previous driver's convention so numbers stay comparable.
fn pct(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len() as f64) * p) as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn pct_u64(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64) * p) as usize;
    sorted[idx.min(sorted.len() - 1)]
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

#[cfg(test)]
mod tests {
    use super::*;

    fn run_of(rounds: Vec<Vec<f64>>, errors: &[(&str, usize)]) -> QueryRun {
        QueryRun {
            rounds,
            errors: errors.iter().map(|(m, c)| (m.to_string(), *c)).collect(),
        }
    }

    fn report_with(passes: Vec<PassReport>, server_note: Option<String>) -> Report {
        Report {
            run_id: "r1".into(),
            seed: 42,
            warmup_rounds: 1,
            rounds: 2,
            passes,
            server_note,
        }
    }

    fn pass(phase: Phase, queries: Vec<QuerySummary>) -> PassReport {
        PassReport {
            phase,
            concurrency: 20,
            requests_per_round: 20,
            queries,
            server: None,
        }
    }

    #[test]
    fn report_is_a_markdown_table_with_fenced_errors() {
        let q = summarize("q", &run_of(vec![vec![10.0, 20.0]], &[("boom | `x`", 2)]));
        let out = render_report(&report_with(vec![pass(Phase::Throughput, vec![q])], None));
        assert!(out.starts_with("Run `r1`: 1 queries, seed 42"));
        assert!(out.contains("### Throughput (concurrency 20)"));
        assert!(out.contains("Med is the median of per-round medians"));
        assert!(out.contains("| Query |"));
        assert!(out.contains("| Round med range |"));
        assert!(out.contains("|---"));
        assert!(out.contains("```text\nq:\n  [2x] boom | `x`\n```"));
    }

    #[test]
    fn report_without_errors_has_no_errors_section() {
        let q = summarize("q", &run_of(vec![vec![5.0]], &[]));
        let out = render_report(&report_with(
            vec![
                pass(
                    Phase::Latency,
                    vec![summarize("q", &run_of(vec![vec![5.0]], &[]))],
                ),
                pass(Phase::Throughput, vec![q]),
            ],
            None,
        ));
        assert!(!out.contains("Errors"));
        assert!(!out.contains("Server-side"));
        let lat = out.find("### Latency (concurrency 20)").unwrap();
        let thr = out.find("### Throughput (concurrency 20)").unwrap();
        assert!(lat < thr);
    }

    #[test]
    fn report_notes_unavailable_clickhouse_and_still_renders() {
        let q = summarize("q", &run_of(vec![vec![5.0]], &[]));
        let note = "Server-side (ClickHouse) stats unavailable: connection refused".to_string();
        let out = render_report(&report_with(
            vec![pass(Phase::Throughput, vec![q])],
            Some(note),
        ));
        assert!(out.contains("| Query |"));
        assert!(
            out.ends_with("\nServer-side (ClickHouse) stats unavailable: connection refused\n")
        );
        assert!(!out.contains("#### Server-side"));
    }

    #[test]
    fn round_order_is_a_seeded_permutation() {
        let a = round_order(42, Phase::Throughput, 0, 13);
        assert_eq!(a, round_order(42, Phase::Throughput, 0, 13));
        let mut sorted = a.clone();
        sorted.sort_unstable();
        assert_eq!(sorted, (0..13).collect::<Vec<_>>());
        assert_ne!(a, round_order(42, Phase::Throughput, 1, 13));
        assert_ne!(a, round_order(42, Phase::Latency, 0, 13));
        assert_ne!(a, round_order(7, Phase::Throughput, 0, 13));
        assert!(round_order(42, Phase::Throughput, 0, 0).is_empty());
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
        let s = summarize("q", &run);
        assert_eq!(s.round_medians, vec![100.0, 100.0, 1000.0]);
        assert_eq!(s.med, 100.0);
        assert_eq!(s.n, 9);
        assert_eq!(s.err, 3);
        assert_eq!(s.min, 90.0);
        assert_eq!(s.max, 1000.0);
        assert_eq!(fmt_spread(&s.round_medians, s.med), "100–1000 (±450%)");
        assert_eq!(fmt_spread(&[90.0, 100.0, 110.0], 100.0), "90–110 (±10%)");
    }

    #[test]
    fn spread_and_summary_handle_empty_input() {
        let s = summarize("q", &run_of(vec![vec![], vec![]], &[("x", 4)]));
        assert!(s.round_medians.is_empty());
        assert_eq!((s.n, s.med, s.mean), (0, 0.0, 0.0));
        assert_eq!(fmt_spread(&s.round_medians, s.med), "-");
        assert_eq!(fmt_spread(&[0.0, 0.0], 0.0), "0–0");
    }

    #[test]
    fn correlation_id_is_server_safe_and_parses_back() {
        let id = correlation_id("123456", Phase::Throughput, 7, 42);
        assert_eq!(id, "lt-123456-t-q07-42");
        assert!(is_id_safe(&id));
        assert_eq!(
            parse_correlation("123456", &id),
            Some((Phase::Throughput, 7, 42))
        );
        // ClickHouse query_ids append the stage after the correlation id.
        assert_eq!(
            parse_correlation("123456", &format!("{id}-base")),
            Some((Phase::Throughput, 7, 42))
        );
        assert_eq!(
            parse_correlation("123456", &format!("{id}-hydration-static-0")),
            Some((Phase::Throughput, 7, 42))
        );
        let warm = correlation_id("123456", Phase::Warmup, 12, 0);
        assert_eq!(
            parse_correlation("123456", &warm),
            Some((Phase::Warmup, 12, 0))
        );
        assert_eq!(parse_correlation("other", &id), None);
        assert_eq!(parse_correlation("123456", "lt-123456-x-q01-1"), None);
        assert_eq!(parse_correlation("123456", "lt-123456-t-01-1"), None);
    }

    #[test]
    fn run_ids_are_validated_and_default_is_base36() {
        assert!(is_id_safe("ab-12"));
        assert!(!is_id_safe(""));
        assert!(!is_id_safe("a_b"));
        assert_eq!(to_base36(0), "0");
        assert_eq!(to_base36(35), "z");
        assert_eq!(to_base36(36), "10");
        let id = default_run_id();
        assert!(
            id.chars()
                .all(|c| c.is_ascii_digit() || c.is_ascii_lowercase())
        );
    }

    #[test]
    fn server_sql_filters_run_and_measured_phases_only() {
        let sql = server_stats_sql("987");
        assert!(sql.contains("FROM system.query_log"));
        assert!(!sql.contains("clusterAllReplicas"));
        assert!(sql.contains("type = 'QueryFinish'"));
        assert!(sql.contains("startsWith(query_id, 'lt-987-l-')"));
        assert!(sql.contains("startsWith(query_id, 'lt-987-t-')"));
        assert!(!sql.contains("-w-"));
        assert!(
            !sql.contains('?'),
            "`?` is a bind placeholder in the clickhouse crate"
        );
    }

    fn log_row(id: &str, rows: u64, bytes: u64, mem: i64, ms: u64) -> QueryLogRow {
        QueryLogRow {
            query_id: id.into(),
            read_rows: rows,
            read_bytes: bytes,
            memory_usage: mem,
            query_duration_ms: ms,
        }
    }

    #[test]
    fn server_stats_sum_stages_per_request_and_skip_warmup_and_foreign_rows() {
        let rows = vec![
            log_row("lt-r1-t-q00-1-base", 10, 100, 500, 5),
            log_row("lt-r1-t-q00-1-hydration-static", 5, 50, 900, 3),
            log_row("lt-r1-t-q00-2-base", 30, 300, 700, 20),
            log_row("lt-r1-t-q00-3-base", 20, 200, 600, 10),
            log_row("lt-r1-l-q01-4-base", 1, 1, 1, 1),
            log_row("lt-r1-w-q00-0-base", 9999, 9999, 9999, 9999),
            log_row("lt-r2-t-q00-1-base", 9999, 9999, 9999, 9999),
            log_row("01JABCDEF0123456789ABCDEFG-base", 9999, 9999, 9999, 9999),
        ];
        let stats = aggregate_server("r1", &rows);
        assert_eq!(stats.len(), 2);
        // Requests: (15 rows, 150 B, 900 mem, 8 ms), (30, 300, 700, 20), (20, 200, 600, 10).
        assert_eq!(
            stats[&(Phase::Throughput, 0)],
            ServerStats {
                requests: 3,
                med_read_rows: 20,
                med_read_bytes: 200,
                peak_memory: 900,
                med_ms: 10,
                p90_ms: 20,
            }
        );
        assert_eq!(stats[&(Phase::Latency, 1)].requests, 1);
        assert!(!stats.contains_key(&(Phase::Warmup, 0)));
    }

    #[test]
    fn server_table_shows_dashes_for_missing_scenarios() {
        let queries = vec![
            summarize("a", &run_of(vec![vec![1.0]], &[])),
            summarize("b", &run_of(vec![vec![1.0]], &[])),
        ];
        let server = vec![
            Some(ServerStats {
                requests: 100,
                med_read_rows: 1234,
                med_read_bytes: 2 * 1024 * 1024,
                peak_memory: 512,
                med_ms: 12,
                p90_ms: 30,
            }),
            None,
        ];
        let mut p = pass(Phase::Throughput, queries);
        p.server = Some(server);
        let out = render_report(&report_with(vec![p], None));
        assert!(out.contains("#### Server-side (ClickHouse), Throughput (concurrency 20)"));
        let row_a = out
            .lines()
            .find(|l| l.starts_with("| a ") && l.contains("1234"))
            .unwrap();
        assert!(row_a.contains("| 100 "));
        assert!(row_a.contains("2.0 MiB"));
        assert!(row_a.contains("512 B"));
        let row_b = out
            .lines()
            .filter(|l| l.starts_with("| b "))
            .nth(1)
            .expect("server row for b");
        assert_eq!(row_b.matches("| - ").count(), 5);
        assert!(row_b.contains("| 0 "));
    }

    #[test]
    fn percentile_uses_nearest_rank_index() {
        let sorted: Vec<f64> = (1..=100).map(f64::from).collect();
        // idx = (100 * p) as usize, clamped to the last element.
        assert_eq!(pct(&sorted, 0.5), 51.0);
        assert_eq!(pct(&sorted, 0.9), 91.0);
        assert_eq!(pct(&sorted, 0.99), 100.0);
        assert_eq!(pct(&sorted, 1.0), 100.0);
        assert_eq!(pct(&[], 0.9), 0.0);
        assert_eq!(pct_u64(&[1, 2, 3], 0.5), 2);
        assert_eq!(pct_u64(&[], 0.5), 0);
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
