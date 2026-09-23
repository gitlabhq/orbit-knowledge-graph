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

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use futures::StreamExt;
use jsonwebtoken::{EncodingKey, Header, encode};
use orbit_server::auth::{Claims, SourceType};
use orbit_server::proto::execute_query_message::Content;
use orbit_server::proto::orbit_service_client::OrbitServiceClient;
use orbit_server::proto::{
    ExecuteQueryError, ExecuteQueryMessage, ExecuteQueryRequest, GetClusterHealthRequest,
    RedactionExchange, RedactionResponse, ResourceAuthorization, redaction_exchange,
};
use serde::Deserialize;
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

pub struct Options {
    pub endpoint: String,
    pub concurrency: usize,
    pub rounds: usize,
    pub scenarios: PathBuf,
    pub query: Option<String>,
    pub admin: bool,
    pub per_call_timeout: Duration,
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

pub async fn run(opts: Options) -> Result<()> {
    if opts.concurrency == 0 || opts.rounds == 0 {
        bail!("--concurrency and --rounds must both be at least 1");
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

    let total = opts.concurrency * opts.rounds;
    println!(
        "Load test: endpoint={} concurrency={} rounds={} => {} requests/query, {} queries",
        opts.endpoint,
        opts.concurrency,
        opts.rounds,
        total,
        queries.len()
    );
    println!(
        "JWT: user={USERNAME} org={ORG_ID} admin={} ttl={TOKEN_TTL}s",
        opts.admin
    );

    let mut client = OrbitServiceClient::connect(opts.endpoint.clone())
        .await
        .with_context(|| format!("connecting to {}", opts.endpoint))?;

    // Fail fast if the server is unreachable or the secret is wrong.
    client
        .get_cluster_health(authed(GetClusterHealthRequest::default(), &token))
        .await
        .context("health check failed - is the server reachable and GKG_JWT_SECRET correct?")?;

    let mut rows = Vec::with_capacity(queries.len());
    let mut error_detail: Vec<(String, BTreeMap<String, usize>)> = Vec::new();
    for q in &queries {
        let stats = bench_query(
            &client,
            &token,
            q,
            total,
            opts.concurrency,
            opts.per_call_timeout,
        )
        .await;
        println!(
            "done: {} ({} reqs, {})",
            q.label,
            stats.samples.len() + stats.errors.values().sum::<usize>(),
            if stats.errors.is_empty() {
                "OK".to_string()
            } else {
                format!("{} errors", stats.errors.values().sum::<usize>())
            }
        );
        if !stats.errors.is_empty() {
            error_detail.push((q.label.clone(), stats.errors.clone()));
        }
        rows.push(stats.into_row(q.label.clone()));
    }

    println!("\n{}", Table::new(rows));

    if !error_detail.is_empty() {
        println!("\nErrors:");
        for (label, errors) in error_detail {
            println!("  {label}:");
            for (msg, count) in errors {
                println!("    [{count}x] {msg}");
            }
        }
    }

    Ok(())
}

/// Fan out `total` requests for one query with a bounded `concurrency` of
/// in-flight calls, collecting per-request outcomes.
async fn bench_query(
    client: &OrbitServiceClient<Channel>,
    token: &str,
    query: &LoadQuery,
    total: usize,
    concurrency: usize,
    per_call: Duration,
) -> QueryStats {
    let outcomes: Vec<Result<f64, String>> = futures::stream::iter(0..total)
        .map(|_| {
            let mut client = client.clone();
            async move {
                let start = Instant::now();
                let result = execute(&mut client, token, &query.body, per_call).await;
                let ms = start.elapsed().as_secs_f64() * 1000.0;
                result.map(|()| ms)
            }
        })
        .buffer_unordered(concurrency)
        .collect()
        .await;

    let mut stats = QueryStats::default();
    for outcome in outcomes {
        match outcome {
            Ok(ms) => stats.samples.push(ms),
            Err(msg) => *stats.errors.entry(truncate(&msg)).or_default() += 1,
        }
    }
    stats
}

/// Drive one `ExecuteQuery` stream to completion: send the request, answer any
/// redaction request by authorizing every resource, then return on the result
/// or error. Latency is measured by the caller across this whole exchange.
async fn execute(
    client: &mut OrbitServiceClient<Channel>,
    token: &str,
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
        .execute_query(authed(ReceiverStream::new(receiver), token))
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

#[derive(Default)]
struct QueryStats {
    /// Latencies (ms) of successful requests only. Kept separate from errors so
    /// percentiles are not polluted by fast-failing calls.
    samples: Vec<f64>,
    errors: BTreeMap<String, usize>,
}

impl QueryStats {
    fn into_row(mut self, label: String) -> ReportRow {
        self.samples.sort_by(|a, b| a.total_cmp(b));
        let n = self.samples.len();
        let err = self.errors.values().sum();
        let mean = if n == 0 {
            0.0
        } else {
            self.samples.iter().sum::<f64>() / n as f64
        };
        ReportRow {
            query: label,
            n,
            err,
            min: fmt_ms(self.samples.first().copied().unwrap_or(0.0)),
            mean: fmt_ms(mean),
            med: fmt_ms(pct(&self.samples, 0.5)),
            p90: fmt_ms(pct(&self.samples, 0.9)),
            p99: fmt_ms(pct(&self.samples, 0.99)),
            max: fmt_ms(self.samples.last().copied().unwrap_or(0.0)),
        }
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
    #[tabled(rename = "p90")]
    p90: String,
    #[tabled(rename = "p99")]
    p99: String,
    #[tabled(rename = "Max")]
    max: String,
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
    };
    // Rails base64-decodes the secret before signing; mirror that so the
    // server's JwtValidator accepts the token. Fall back to raw bytes if the
    // value is not valid base64.
    let key = STANDARD
        .decode(secret_b64.trim().as_bytes())
        .unwrap_or_else(|_| secret_b64.as_bytes().to_vec());
    encode(&Header::default(), &claims, &EncodingKey::from_secret(&key)).context("signing JWT")
}

fn authed<T>(message: T, token: &str) -> Request<T> {
    let mut request = Request::new(message);
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
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

    #[test]
    fn percentile_uses_nearest_rank_index() {
        let sorted: Vec<f64> = (1..=100).map(f64::from).collect();
        // idx = (100 * p) as usize, clamped to the last element.
        assert_eq!(pct(&sorted, 0.5), 51.0);
        assert_eq!(pct(&sorted, 0.9), 91.0);
        assert_eq!(pct(&sorted, 0.99), 100.0);
        assert_eq!(pct(&sorted, 1.0), 100.0);
        assert_eq!(pct(&[], 0.9), 0.0);
    }

    #[test]
    fn truncate_caps_long_messages_on_char_boundaries() {
        assert_eq!(truncate("short"), "short");
        let out = truncate(&"x".repeat(250));
        assert!(out.ends_with("..."));
        assert_eq!(out.chars().count(), 203);
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
