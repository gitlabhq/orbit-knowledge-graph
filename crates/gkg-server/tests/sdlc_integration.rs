//! Integration tests for the SDLC module.
//!
//! These tests require a Docker-compatible runtime (Docker, Colima, etc).

use std::sync::Arc;

use arrow::array::{BinaryArray, UInt8Array, UInt64Array};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use clickhouse_arrow::{ArrowClient, ClientBuilder};
use etl_engine::clickhouse::{ClickHouseConfiguration, ClickHouseDestination};
use etl_engine::module::{HandlerContext, Module};
use etl_engine::testkit::{MockMetricCollector, MockNatsServices, TestEnvelopeFactory};
use futures::StreamExt;
use gkg_server::indexer::modules::SdlcModule;
use serial_test::serial;
use testcontainers::GenericImage;
use testcontainers::core::{ContainerPort, ImageExt};
use testcontainers::runners::AsyncRunner;

const CLICKHOUSE_IMAGE: &str = "clickhouse/clickhouse-server";
const CLICKHOUSE_TAG: &str = "25.11";
const TEST_USERNAME: &str = "default";
const TEST_PASSWORD: &str = "testpass";
const TEST_DATABASE: &str = "test";

const MAX_CONNECTION_ATTEMPTS: u32 = 30;
const CONNECTION_RETRY_DELAY_MS: u64 = 500;

const SCHEMA_SQL: &str = include_str!("fixtures/schema.sql");

struct TestContext {
    _container: testcontainers::ContainerAsync<GenericImage>,
    config: ClickHouseConfiguration,
    host: String,
    port: u16,
}

impl TestContext {
    async fn new() -> Self {
        let (container, host, port) = start_clickhouse_container().await;
        setup_database(&host, port).await;
        let config = create_config(&host, port);

        Self {
            _container: container,
            config,
            host,
            port,
        }
    }

    fn create_destination(&self) -> ClickHouseDestination {
        ClickHouseDestination::new(self.config.clone()).expect("failed to create destination")
    }

    async fn create_client(&self) -> ArrowClient {
        ClientBuilder::new()
            .with_endpoint(format!("{}:{}", self.host, self.port))
            .with_database(TEST_DATABASE)
            .with_username(TEST_USERNAME)
            .with_password(TEST_PASSWORD)
            .build_arrow()
            .await
            .expect("failed to connect")
    }

    async fn query(&self, sql: &str) -> Vec<RecordBatch> {
        let client = self.create_client().await;
        client
            .query(sql, None)
            .await
            .expect("query failed")
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect("failed to collect results")
    }

    async fn execute(&self, sql: &str) {
        let client = self.create_client().await;
        client.execute(sql, None).await.expect("execute failed");
    }
}

async fn start_clickhouse_container() -> (testcontainers::ContainerAsync<GenericImage>, String, u16)
{
    let native_port = ContainerPort::Tcp(9000);

    let container = GenericImage::new(CLICKHOUSE_IMAGE, CLICKHOUSE_TAG)
        .with_exposed_port(native_port)
        .with_env_var("CLICKHOUSE_USER", TEST_USERNAME)
        .with_env_var("CLICKHOUSE_PASSWORD", TEST_PASSWORD)
        .with_env_var("CLICKHOUSE_DB", TEST_DATABASE)
        .start()
        .await
        .expect("failed to start ClickHouse container");

    let host = container
        .get_host()
        .await
        .expect("failed to get container host");

    let port = container
        .get_host_port_ipv4(native_port)
        .await
        .expect("failed to get ClickHouse native port");

    let host = if host.to_string() == "localhost" {
        "127.0.0.1".to_string()
    } else {
        host.to_string()
    };

    (container, host, port)
}

async fn setup_database(host: &str, port: u16) {
    let mut attempts = 0;
    let client = loop {
        match ClientBuilder::new()
            .with_endpoint(format!("{host}:{port}"))
            .with_username(TEST_USERNAME)
            .with_password(TEST_PASSWORD)
            .build_arrow()
            .await
        {
            Ok(client) => break client,
            Err(_) if attempts < MAX_CONNECTION_ATTEMPTS => {
                attempts += 1;
                tokio::time::sleep(std::time::Duration::from_millis(CONNECTION_RETRY_DELAY_MS))
                    .await;
            }
            Err(e) => panic!("failed to connect to ClickHouse after {attempts} attempts: {e}"),
        }
    };

    for statement in SCHEMA_SQL.split(';') {
        let statement = statement.trim();
        if statement.is_empty() {
            continue;
        }
        client
            .execute(statement, None)
            .await
            .unwrap_or_else(|e| panic!("failed to execute schema statement: {e}\n{statement}"));
    }
}

fn create_config(host: &str, port: u16) -> ClickHouseConfiguration {
    ClickHouseConfiguration {
        database: TEST_DATABASE.to_string(),
        url: format!("{host}:{port}"),
        username: TEST_USERNAME.to_string(),
        password: Some(TEST_PASSWORD.to_string()),
    }
}

fn create_handler_context(destination: Arc<ClickHouseDestination>) -> HandlerContext {
    HandlerContext::new(
        destination,
        Arc::new(MockMetricCollector::new()),
        Arc::new(MockNatsServices::new()),
    )
}

fn create_payload(watermark: DateTime<Utc>) -> String {
    serde_json::json!({
        "watermark": watermark.to_rfc3339()
    })
    .to_string()
}

fn binary_as_str(array: &BinaryArray, index: usize) -> &str {
    std::str::from_utf8(array.value(index)).expect("invalid UTF-8")
}

#[tokio::test]
#[serial]
async fn user_handler_processes_and_transforms_users() {
    let context = TestContext::new().await;

    context
        .execute(
            "INSERT INTO siphon_users (
                id, username, email, name, first_name, last_name, state,
                public_email, preferred_language, last_activity_on, private_profile,
                admin, auditor, external, user_type, created_at, updated_at, _siphon_replicated_at
            ) VALUES
            (1, 'alice', 'alice@test.com', 'Alice Smith', 'Alice', 'Smith', 'active',
             'alice.public@test.com', 'en', '2024-01-15', false, true, false, false, 0,
             '2023-01-01', '2024-01-15', '2024-01-20 12:00:00'),
            (2, 'bob', 'bob@test.com', 'Bob Jones', 'Bob', 'Jones', 'active',
             'bob.public@test.com', 'es', '2024-01-10', true, false, false, true, 1,
             '2023-06-15', '2024-01-10', '2024-01-20 12:00:00'),
            (3, 'charlie', 'charlie@test.com', 'Charlie Brown', 'Charlie', 'Brown', 'blocked',
             '', 'fr', '2024-01-05', false, false, true, false, 4,
             '2023-09-20', '2024-01-05', '2024-01-20 12:00:00')",
        )
        .await;

    let sdlc_module = SdlcModule::new(&context.config)
        .await
        .expect("failed to create SDLC module");

    let handlers = sdlc_module.handlers();
    assert_eq!(handlers.len(), 1);

    let user_handler = &handlers[0];
    assert_eq!(user_handler.name(), "user-handler");

    let watermark = DateTime::parse_from_rfc3339("2024-01-21T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);

    let envelope = TestEnvelopeFactory::simple(&create_payload(watermark));
    let destination = Arc::new(context.create_destination());
    let handler_context = create_handler_context(destination);

    user_handler
        .handle(handler_context, envelope)
        .await
        .expect("handler should succeed");

    let result = context.query("SELECT * FROM users ORDER BY id").await;

    assert!(!result.is_empty(), "result should not be empty");

    let batch = &result[0];
    assert_eq!(batch.num_rows(), 3);

    let user_type_column = batch
        .column_by_name("user_type")
        .expect("user_type column should exist")
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("user_type should be BinaryArray");

    assert_eq!(binary_as_str(user_type_column, 0), "human");
    assert_eq!(binary_as_str(user_type_column, 1), "support_bot");
    assert_eq!(binary_as_str(user_type_column, 2), "service_user");

    let is_admin_column = batch
        .column_by_name("is_admin")
        .expect("is_admin column should exist")
        .as_any()
        .downcast_ref::<UInt8Array>()
        .expect("is_admin should be UInt8Array");

    assert_eq!(is_admin_column.value(0), 1);
    assert_eq!(is_admin_column.value(1), 0);
    assert_eq!(is_admin_column.value(2), 0);
}

#[tokio::test]
#[serial]
async fn user_handler_uses_watermark_for_incremental_processing() {
    let context = TestContext::new().await;

    context
        .execute("INSERT INTO user_indexing_watermark (watermark) VALUES ('2024-01-19 00:00:00')")
        .await;

    context
        .execute(
            "INSERT INTO siphon_users (
                id, username, email, name, first_name, last_name, state,
                public_email, preferred_language, last_activity_on, private_profile,
                admin, auditor, external, user_type, created_at, updated_at, _siphon_replicated_at
            ) VALUES
            (1, 'old_user', 'old@test.com', 'Old User', 'Old', 'User', 'active',
             '', 'en', '2024-01-01', false, false, false, false, 0,
             '2023-01-01', '2024-01-01', '2024-01-18 12:00:00'),
            (2, 'new_user', 'new@test.com', 'New User', 'New', 'User', 'active',
             '', 'en', '2024-01-20', false, false, false, false, 0,
             '2024-01-19', '2024-01-20', '2024-01-20 12:00:00')",
        )
        .await;

    let sdlc_module = SdlcModule::new(&context.config)
        .await
        .expect("failed to create SDLC module");

    let handlers = sdlc_module.handlers();
    let user_handler = &handlers[0];

    let watermark = DateTime::parse_from_rfc3339("2024-01-21T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);

    let envelope = TestEnvelopeFactory::simple(&create_payload(watermark));
    let destination = Arc::new(context.create_destination());
    let handler_context = create_handler_context(destination);

    user_handler
        .handle(handler_context, envelope)
        .await
        .expect("handler should succeed");

    let result = context.query("SELECT count() as cnt FROM users").await;
    let count_array = result[0]
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("expected UInt64Array");

    assert_eq!(
        count_array.value(0),
        1,
        "should only process new_user, not old_user"
    );

    let usernames = context.query("SELECT username FROM users").await;
    let username_array = usernames[0]
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("username should be BinaryArray");

    assert_eq!(binary_as_str(username_array, 0), "new_user");
}
