use std::collections::HashMap;

use integration_testkit::TestContext;
use orbit_arrow::ColumnValue;
use orbit_server::content::observability::client_pool::TenantClientPool;
use orbit_server::content::observability::host_map::TenantRoute;
use orbit_server::content::observability::traces::{self, TraceLookup};

const SIGNOZ_INDEX_DDL: &str = "
CREATE TABLE IF NOT EXISTS signoz_index_v3 (
    ts_bucket_start UInt64,
    timestamp DateTime64(9),
    trace_id FixedString(32),
    span_id String,
    name LowCardinality(String),
    duration_nano UInt64,
    has_error Bool,
    attributes_string Map(LowCardinality(String), String)
) ENGINE = MergeTree()
ORDER BY (ts_bucket_start, timestamp)
";

/// Relative to now: the resolver drops keys past the tenant's trace
/// retention, so a fixed timestamp would age out of the window.
fn pipeline_started() -> i64 {
    now_unix() - 3_600
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_secs() as i64
}

fn bucket(unix: i64) -> u64 {
    (unix / 1_800 * 1_800) as u64
}

const ORBIT_READER_PASSWORD: &str = "orbit-test-secret";

/// Users and profiles are server-wide while databases are per test, so both
/// are named after the database to keep parallel tests from sharing them.
fn reader_user(ctx: &TestContext) -> String {
    format!("orbit_reader_{}", ctx.config.database)
}

fn reader_profile(ctx: &TestContext) -> String {
    format!("orbit_reader_profile_{}", ctx.config.database)
}

/// The settings profile o11y_provisioner declares in
/// clickhouse_configs/users.xml, and the user and grants
/// ClickhouseMigrationRunner#grant_orbit_reader_script creates against it.
/// Production also restricts the user with HOST IP, which a container test
/// cannot reproduce. What this does reproduce is the readonly profile, which
/// rejects any session setting it does not fix with code 164.
async fn create_orbit_reader(ctx: &TestContext) {
    let user = reader_user(ctx);
    let profile = reader_profile(ctx);
    ctx.execute(&format!("DROP USER IF EXISTS {user}")).await;
    ctx.execute(&format!("DROP SETTINGS PROFILE IF EXISTS {profile}"))
        .await;
    ctx.execute(&format!(
        "CREATE SETTINGS PROFILE {profile} SETTINGS \
         readonly = 2 READONLY, \
         max_execution_time = 30 READONLY, \
         max_result_rows = 10000 READONLY, \
         max_bytes_to_read = 5000000000 READONLY, \
         max_memory_usage = 2000000000 READONLY"
    ))
    .await;
    ctx.execute(&format!(
        "CREATE USER {user} IDENTIFIED BY '{ORBIT_READER_PASSWORD}' \
         SETTINGS PROFILE {profile}"
    ))
    .await;
    ctx.execute(&format!(
        "GRANT SELECT ON {}.* TO {user}",
        ctx.config.database
    ))
    .await;
}

fn route(ctx: &TestContext) -> TenantRoute {
    let (scheme, authority) = ctx
        .config
        .url
        .split_once("://")
        .expect("clickhouse url carries a scheme");
    let (host, port) = authority
        .trim_end_matches('/')
        .split_once(':')
        .expect("clickhouse url carries a port");

    TenantRoute {
        group_id: "12345".to_string(),
        scheme: scheme.to_string(),
        host: host.to_string(),
        port: port.to_string(),
        username: reader_user(ctx),
        password: ORBIT_READER_PASSWORD.to_string(),
        traces_db: ctx.config.database.clone(),
        logs_db: ctx.config.database.clone(),
    }
}

fn key(entity_id: &str) -> traces::TraceKey {
    traces::TraceKey {
        entity_id: entity_id.to_string(),
        from_unix: pipeline_started() - 3_600,
        to_unix: pipeline_started() + 86_400,
    }
}

async fn seed_span(
    ctx: &TestContext,
    unix: i64,
    trace_id: &str,
    span_id: &str,
    duration_nano: u64,
    has_error: bool,
    attributes: &[(&str, &str)],
) {
    let map = attributes
        .iter()
        .map(|(k, v)| format!("'{k}', '{v}'"))
        .collect::<Vec<_>>()
        .join(", ");

    ctx.execute(&format!(
        "INSERT INTO signoz_index_v3 \
         (ts_bucket_start, timestamp, trace_id, span_id, name, duration_nano, has_error, attributes_string) \
         VALUES ({}, toDateTime64({unix}, 9), '{trace_id}', '{span_id}', 'span', {duration_nano}, {}, map({map}))",
        bucket(unix),
        u8::from(has_error),
    ))
    .await;
}

async fn seeded_pipeline() -> TestContext {
    let ctx = TestContext::new(&[SIGNOZ_INDEX_DDL]).await;
    create_orbit_reader(&ctx).await;

    seed_span(
        &ctx,
        pipeline_started(),
        "4bf92f3577b34da6a3ce929d0e0e4736",
        "00f067aa0ba902b7",
        412_000_000,
        false,
        &[("cicd.pipeline.run.id", "3000")],
    )
    .await;
    seed_span(
        &ctx,
        pipeline_started() + 10,
        "4bf92f3577b34da6a3ce929d0e0e4736",
        "00f067aa0ba902b8",
        118_000_000,
        true,
        &[("cicd.pipeline.task.run.id", "4000")],
    )
    .await;

    ctx
}

async fn fetch(
    ctx: &TestContext,
    lookup: TraceLookup,
    keys: &[traces::TraceKey],
) -> HashMap<String, String> {
    traces::fetch(&TenantClientPool::new(), &route(ctx), lookup, keys)
        .await
        .expect("lookup should succeed against a real ClickHouse")
}

#[tokio::test]
async fn resolves_a_pipeline_trace_id_from_clickhouse() {
    let ctx = seeded_pipeline().await;

    let rows = fetch(&ctx, TraceLookup::PipelineTraceId, &[key("3000")]).await;

    assert_eq!(
        rows.get("3000").map(String::as_str),
        Some("4bf92f3577b34da6a3ce929d0e0e4736")
    );
}

#[tokio::test]
async fn aggregates_duration_and_errors_across_a_trace() {
    let ctx = seeded_pipeline().await;

    let durations = fetch(&ctx, TraceLookup::PipelineTraceDuration, &[key("3000")]).await;
    let errors = fetch(&ctx, TraceLookup::PipelineTraceErrorCount, &[key("3000")]).await;

    assert_eq!(durations.get("3000").map(String::as_str), Some("412000000"));
    assert_eq!(errors.get("3000").map(String::as_str), Some("1"));
}

#[tokio::test]
async fn resolves_a_job_span_by_its_own_attribute() {
    let ctx = seeded_pipeline().await;

    let rows = fetch(&ctx, TraceLookup::JobSpanId, &[key("4000")]).await;

    assert_eq!(
        rows.get("4000").map(String::as_str),
        Some("00f067aa0ba902b8")
    );
}

#[tokio::test]
async fn a_pipeline_with_no_spans_returns_no_rows() {
    let ctx = seeded_pipeline().await;

    let rows = fetch(&ctx, TraceLookup::PipelineTraceId, &[key("9999")]).await;

    assert!(rows.is_empty());
}

#[tokio::test]
async fn every_requested_id_is_resolved_in_one_query() {
    let ctx = seeded_pipeline().await;
    seed_span(
        &ctx,
        pipeline_started() + 20,
        "9f8e7d6c5b4a39281706f5e4d3c2b1a0",
        "11a2b3c4d5e6f708",
        90_000_000,
        false,
        &[("cicd.pipeline.run.id", "3001")],
    )
    .await;

    let rows = fetch(
        &ctx,
        TraceLookup::PipelineTraceId,
        &[key("3000"), key("3001")],
    )
    .await;

    assert_eq!(rows.len(), 2);
    assert_ne!(rows.get("3000"), rows.get("3001"));
}

/// The index table is sorted by time bucket, so a lookup whose window does not
/// cover the span must not return it. This is what keeps a lookup from
/// scanning the tenant's whole retention.
#[tokio::test]
async fn a_span_outside_the_time_window_is_not_returned() {
    let ctx = seeded_pipeline().await;
    let stale = traces::TraceKey {
        entity_id: "3000".to_string(),
        from_unix: pipeline_started() - 86_400 * 10,
        to_unix: pipeline_started() - 86_400 * 9,
    };

    let rows = fetch(&ctx, TraceLookup::PipelineTraceId, &[stale]).await;

    assert!(rows.is_empty());
}

/// The reader runs under a READONLY settings profile, so a client that sends
/// any setting outside it is rejected with code 164 and the lookup resolves to
/// null for every row.
#[tokio::test]
async fn a_lookup_succeeds_under_the_readonly_settings_profile() {
    let ctx = seeded_pipeline().await;

    let rows = traces::fetch(
        &TenantClientPool::new(),
        &route(&ctx),
        TraceLookup::PipelineTraceId,
        &[key("3000")],
    )
    .await
    .expect("a query as orbit_reader must not be rejected by its own profile");

    assert!(!rows.is_empty(), "the profile rejected the query");
}

#[tokio::test]
async fn values_resolve_to_their_declared_column_types() {
    let ctx = seeded_pipeline().await;

    let durations = fetch(&ctx, TraceLookup::PipelineTraceDuration, &[key("3000")]).await;
    let raw = durations.get("3000").cloned().expect("a duration");

    assert_eq!(
        TraceLookup::PipelineTraceDuration.column_value(raw),
        Some(ColumnValue::Int64(412_000_000))
    );
}
