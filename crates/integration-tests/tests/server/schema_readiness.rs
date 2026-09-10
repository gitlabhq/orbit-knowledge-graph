use std::time::Duration;

use arrow::datatypes::UInt64Type;
use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use clickhouse_client::{ArrowClickHouseClient, ClickHouseConfigurationExt};
use integration_testkit::TestContext;
use orbit_migrations::version::{
    ensure_version_table, mark_version_active, mark_version_dropped, mark_version_migrating,
    mark_version_retired, prefixed_table_name,
};
use orbit_server::schema_watcher::SchemaWatcher;
use orbit_server::webserver::create_router;
use orbit_utils::arrow::ArrowUtils;
use tokio::time::{Instant, sleep, timeout};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

const POLL: Duration = Duration::from_millis(50);
const WAIT_LIMIT: Duration = Duration::from_secs(5);
const OBSERVATION_WINDOW: Duration = Duration::from_millis(250);
const READER_VERSION: u32 = 1;
const NEWER_VERSION: u32 = READER_VERSION + 1;
const TABLE_NAMES: [&str; 2] = ["readiness_nodes", "readiness_edges"];
const ROW_ID: u64 = 7;

fn reader_table_names() -> Vec<String> {
    TABLE_NAMES
        .iter()
        .map(|name| prefixed_table_name(name, READER_VERSION))
        .collect()
}

async fn create_tables(context: &TestContext, table_names: &[String]) {
    for table_name in table_names {
        context
            .execute(&format!(
                "CREATE TABLE {table_name} (id UInt64) ENGINE = Memory"
            ))
            .await;
        context
            .execute(&format!("INSERT INTO {table_name} VALUES ({ROW_ID})"))
            .await;
    }
}

fn start_reader(client: ArrowClickHouseClient, shutdown: &CancellationToken) -> Router {
    create_router(SchemaWatcher::spawn(
        client,
        READER_VERSION,
        reader_table_names(),
        POLL,
        shutdown.clone(),
    ))
}

async fn health_response(router: &Router, path: &str) -> (StatusCode, serde_json::Value) {
    let request = Request::get(path).body(Body::empty()).unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}

async fn wait_for_readiness(
    router: &Router,
    expected_status: StatusCode,
    expected_body_status: &str,
) {
    timeout(WAIT_LIMIT, async {
        loop {
            let (status, body) = health_response(router, "/ready").await;
            if status == expected_status && body["status"] == expected_body_status {
                return;
            }
            sleep(POLL).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("/ready never returned {expected_status} / {expected_body_status}"));
}

async fn assert_readiness_stays(router: &Router, expected_status: StatusCode) {
    let deadline = Instant::now() + OBSERVATION_WINDOW;
    while Instant::now() < deadline {
        let (status, body) = health_response(router, "/ready").await;
        assert_eq!(status, expected_status, "{body}");
        sleep(POLL).await;
    }
}

async fn assert_reader_is_live(router: &Router, shutdown: &CancellationToken) {
    let (status, body) = health_response(router, "/live").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "ok");
    assert!(!shutdown.is_cancelled());
}

async fn assert_reader_tables_queryable(client: &ArrowClickHouseClient) {
    for table_name in reader_table_names() {
        let batches = client
            .query_arrow(&format!("SELECT id FROM {table_name}"))
            .await
            .unwrap();
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            1
        );
        assert_eq!(
            ArrowUtils::get_column::<UInt64Type>(&batches[0], "id", 0),
            Some(ROW_ID)
        );
    }
}

#[tokio::test]
async fn reader_waits_for_schema_initialization_and_promotion() {
    let context = TestContext::new(&[]).await;
    let client = context.create_client();
    ensure_version_table(&client).await.unwrap();
    create_tables(&context, &reader_table_names()).await;

    let shutdown = CancellationToken::new();
    let router = start_reader(client.clone(), &shutdown);
    assert_readiness_stays(&router, StatusCode::SERVICE_UNAVAILABLE).await;
    assert_reader_is_live(&router, &shutdown).await;

    mark_version_migrating(&client, READER_VERSION)
        .await
        .unwrap();
    wait_for_readiness(&router, StatusCode::SERVICE_UNAVAILABLE, "migrating").await;

    mark_version_active(&client, READER_VERSION).await.unwrap();
    wait_for_readiness(&router, StatusCode::OK, "ok").await;
    assert_reader_tables_queryable(&client).await;
    shutdown.cancel();
}

#[tokio::test]
async fn retained_reader_keeps_serving_and_detects_table_loss_and_recovery() {
    let context = TestContext::new(&[]).await;
    let client = context.create_client();
    let table_names = reader_table_names();
    ensure_version_table(&client).await.unwrap();
    create_tables(&context, &table_names).await;
    mark_version_active(&client, READER_VERSION).await.unwrap();

    let shutdown = CancellationToken::new();
    let router = start_reader(client.clone(), &shutdown);
    wait_for_readiness(&router, StatusCode::OK, "ok").await;
    assert_reader_tables_queryable(&client).await;

    mark_version_retired(&client, READER_VERSION).await.unwrap();
    mark_version_active(&client, NEWER_VERSION).await.unwrap();
    wait_for_readiness(&router, StatusCode::OK, "ok").await;
    assert_readiness_stays(&router, StatusCode::OK).await;
    assert_reader_tables_queryable(&client).await;

    context
        .execute(&format!("DROP TABLE {}", table_names[0]))
        .await;
    wait_for_readiness(&router, StatusCode::SERVICE_UNAVAILABLE, "unhealthy").await;
    assert_reader_is_live(&router, &shutdown).await;

    create_tables(&context, &table_names[..1]).await;
    wait_for_readiness(&router, StatusCode::OK, "ok").await;
    assert_reader_tables_queryable(&client).await;
    shutdown.cancel();
}

#[tokio::test]
async fn cold_reader_serves_retained_schema_with_read_only_client() {
    let context = TestContext::new(&[]).await;
    let client = context.create_client();
    ensure_version_table(&client).await.unwrap();
    create_tables(&context, &reader_table_names()).await;
    mark_version_retired(&client, READER_VERSION).await.unwrap();
    mark_version_active(&client, NEWER_VERSION).await.unwrap();

    let mut reader_config = context.config.clone();
    reader_config
        .session_settings
        .insert("readonly".into(), "1".into());
    let reader_client = reader_config.build_client();
    let shutdown = CancellationToken::new();
    let router = start_reader(reader_client.clone(), &shutdown);

    wait_for_readiness(&router, StatusCode::OK, "ok").await;
    assert_reader_tables_queryable(&reader_client).await;
    assert_reader_is_live(&router, &shutdown).await;
    assert!(
        reader_client
            .execute(&format!(
                "INSERT INTO {} VALUES ({ROW_ID})",
                reader_table_names()[0]
            ))
            .await
            .is_err()
    );
    shutdown.cancel();
}

#[tokio::test]
async fn cold_retained_reader_waits_for_every_required_table() {
    let context = TestContext::new(&[]).await;
    let client = context.create_client();
    let table_names = reader_table_names();
    ensure_version_table(&client).await.unwrap();
    create_tables(&context, &table_names[1..]).await;
    mark_version_retired(&client, READER_VERSION).await.unwrap();
    mark_version_active(&client, NEWER_VERSION).await.unwrap();

    let shutdown = CancellationToken::new();
    let router = start_reader(client.clone(), &shutdown);
    assert_readiness_stays(&router, StatusCode::SERVICE_UNAVAILABLE).await;
    assert_reader_is_live(&router, &shutdown).await;

    create_tables(&context, &table_names[..1]).await;
    wait_for_readiness(&router, StatusCode::OK, "ok").await;
    assert_reader_tables_queryable(&client).await;
    shutdown.cancel();
}

#[tokio::test]
async fn cold_active_reader_waits_for_its_tables() {
    let context = TestContext::new(&[]).await;
    let client = context.create_client();
    ensure_version_table(&client).await.unwrap();
    mark_version_active(&client, READER_VERSION).await.unwrap();

    let shutdown = CancellationToken::new();
    let router = start_reader(client.clone(), &shutdown);
    assert_readiness_stays(&router, StatusCode::SERVICE_UNAVAILABLE).await;
    assert_reader_is_live(&router, &shutdown).await;

    create_tables(&context, &reader_table_names()).await;
    wait_for_readiness(&router, StatusCode::OK, "ok").await;
    assert_reader_tables_queryable(&client).await;
    shutdown.cancel();
}

#[tokio::test]
async fn cold_reader_requires_a_retained_schema_record_not_just_tables() {
    let context = TestContext::new(&[]).await;
    let client = context.create_client();
    ensure_version_table(&client).await.unwrap();
    create_tables(&context, &reader_table_names()).await;
    mark_version_active(&client, NEWER_VERSION).await.unwrap();

    let shutdown = CancellationToken::new();
    let router = start_reader(client.clone(), &shutdown);
    assert_readiness_stays(&router, StatusCode::SERVICE_UNAVAILABLE).await;

    mark_version_dropped(&client, READER_VERSION).await.unwrap();
    assert_readiness_stays(&router, StatusCode::SERVICE_UNAVAILABLE).await;
    assert_reader_is_live(&router, &shutdown).await;

    mark_version_retired(&client, READER_VERSION).await.unwrap();
    wait_for_readiness(&router, StatusCode::OK, "ok").await;
    assert_reader_tables_queryable(&client).await;
    shutdown.cancel();
}

#[tokio::test]
async fn rebuilding_older_schema_stays_unready_until_promoted() {
    let context = TestContext::new(&[]).await;
    let client = context.create_client();
    ensure_version_table(&client).await.unwrap();
    create_tables(&context, &reader_table_names()).await;
    mark_version_active(&client, NEWER_VERSION).await.unwrap();
    mark_version_migrating(&client, READER_VERSION)
        .await
        .unwrap();

    let shutdown = CancellationToken::new();
    let router = start_reader(client.clone(), &shutdown);
    wait_for_readiness(&router, StatusCode::SERVICE_UNAVAILABLE, "migrating").await;
    assert_readiness_stays(&router, StatusCode::SERVICE_UNAVAILABLE).await;
    assert_reader_is_live(&router, &shutdown).await;

    mark_version_retired(&client, NEWER_VERSION).await.unwrap();
    mark_version_active(&client, READER_VERSION).await.unwrap();
    wait_for_readiness(&router, StatusCode::OK, "ok").await;
    assert_reader_tables_queryable(&client).await;
    shutdown.cancel();
}
