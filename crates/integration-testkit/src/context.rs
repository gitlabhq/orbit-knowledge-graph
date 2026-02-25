use std::sync::Arc;
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use clickhouse_client::{ArrowClickHouseClient, ClickHouseConfiguration};
use query_engine::ParameterizedQuery;
use serde_json::Value;
use testcontainers::core::{ContainerPort, ImageExt};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage};

use crate::get_string_column;

const CLICKHOUSE_IMAGE: &str = "clickhouse/clickhouse-server";
const CLICKHOUSE_TAG: &str = "25.12";
const CLICKHOUSE_HTTP_PORT: u16 = 8123;

const TEST_DATABASE: &str = "test";
const TEST_USERNAME: &str = "default";
const TEST_PASSWORD: &str = "testpass";

const MAX_CONNECTION_ATTEMPTS: u32 = 30;
const CONNECTION_RETRY_DELAY: Duration = Duration::from_millis(500);

pub struct TestContext {
    _container: Arc<ContainerAsync<GenericImage>>,
    pub config: ClickHouseConfiguration,
    url: String,
    schema_sqls: Arc<Vec<String>>,
}

impl TestContext {
    pub async fn new(schema_sqls: &[&str]) -> Self {
        let container = Self::start_container().await;
        let url = Self::extract_url(&container).await;

        Self::wait_for_ready(&url).await;
        Self::run_schema_in(&url, TEST_DATABASE, schema_sqls).await;

        let config = ClickHouseConfiguration {
            database: TEST_DATABASE.to_string(),
            url: url.clone(),
            username: TEST_USERNAME.to_string(),
            password: Some(TEST_PASSWORD.to_string()),
        };

        Self {
            _container: Arc::new(container),
            config,
            url,
            schema_sqls: Arc::new(schema_sqls.iter().map(|s| s.to_string()).collect()),
        }
    }

    pub async fn query(&self, sql: &str) -> Vec<RecordBatch> {
        self.create_client()
            .query_arrow(sql)
            .await
            .expect("query failed")
    }

    pub async fn execute(&self, sql: &str) {
        self.create_client()
            .execute(sql)
            .await
            .expect("execute failed");
    }

    pub async fn query_parameterized(&self, pq: &ParameterizedQuery) -> Vec<RecordBatch> {
        let client = self.create_client();
        let mut query = client.query(&pq.sql);

        for (name, value) in &pq.params {
            query = match value {
                Value::String(s) => query.param(name, s.as_str()),
                Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        query.param(name, i)
                    } else if let Some(f) = n.as_f64() {
                        query.param(name, f)
                    } else {
                        query.param(name, n.to_string())
                    }
                }
                Value::Bool(b) => query.param(name, *b),
                Value::Null => query.param(name, Option::<String>::None),
                _ => query.param(name, value.to_string()),
            };
        }

        query
            .fetch_arrow()
            .await
            .expect("parameterized query failed")
    }

    pub async fn truncate_all_tables(&self) {
        let batches = self
            .query(&format!(
                "SELECT name FROM system.tables WHERE database = '{}' AND engine != 'View'",
                self.config.database
            ))
            .await;
        for batch in &batches {
            let names = get_string_column(batch, "name");
            for i in 0..batch.num_rows() {
                self.execute(&format!("TRUNCATE TABLE `{}`", names.value(i)))
                    .await;
            }
        }
    }

    pub fn create_client(&self) -> ArrowClickHouseClient {
        ArrowClickHouseClient::new(
            &self.url,
            &self.config.database,
            TEST_USERNAME,
            Some(TEST_PASSWORD),
        )
    }

    pub async fn fork(&self, name: &str) -> Self {
        let admin =
            ArrowClickHouseClient::new(&self.url, "default", TEST_USERNAME, Some(TEST_PASSWORD));
        admin
            .execute(&format!("CREATE DATABASE IF NOT EXISTS `{name}`"))
            .await
            .unwrap_or_else(|e| panic!("failed to create database {name}: {e}"));

        let schema_refs: Vec<&str> = self.schema_sqls.iter().map(|s| s.as_str()).collect();
        Self::run_schema_in(&self.url, name, &schema_refs).await;

        Self {
            _container: Arc::clone(&self._container),
            config: ClickHouseConfiguration {
                database: name.to_string(),
                url: self.url.clone(),
                username: TEST_USERNAME.to_string(),
                password: Some(TEST_PASSWORD.to_string()),
            },
            url: self.url.clone(),
            schema_sqls: Arc::clone(&self.schema_sqls),
        }
    }

    async fn start_container() -> ContainerAsync<GenericImage> {
        let port = ContainerPort::Tcp(CLICKHOUSE_HTTP_PORT);

        GenericImage::new(CLICKHOUSE_IMAGE, CLICKHOUSE_TAG)
            .with_exposed_port(port)
            .with_env_var("CLICKHOUSE_USER", TEST_USERNAME)
            .with_env_var("CLICKHOUSE_PASSWORD", TEST_PASSWORD)
            .with_env_var("CLICKHOUSE_DB", TEST_DATABASE)
            .start()
            .await
            .expect("failed to start ClickHouse container")
    }

    async fn extract_url(container: &ContainerAsync<GenericImage>) -> String {
        let host = container
            .get_host()
            .await
            .expect("failed to get container host");

        let port = container
            .get_host_port_ipv4(ContainerPort::Tcp(CLICKHOUSE_HTTP_PORT))
            .await
            .expect("failed to get ClickHouse HTTP port");

        let host = match host.to_string().as_str() {
            "localhost" => "127.0.0.1".to_string(),
            other => other.to_string(),
        };

        format!("http://{host}:{port}")
    }

    async fn wait_for_ready(url: &str) {
        let client = ArrowClickHouseClient::new(url, "default", TEST_USERNAME, Some(TEST_PASSWORD));

        for attempt in 1..=MAX_CONNECTION_ATTEMPTS {
            if client.execute("SELECT 1").await.is_ok() {
                return;
            }
            if attempt == MAX_CONNECTION_ATTEMPTS {
                panic!("ClickHouse not ready after {MAX_CONNECTION_ATTEMPTS} attempts");
            }
            tokio::time::sleep(CONNECTION_RETRY_DELAY).await;
        }
    }

    async fn run_schema_in(url: &str, database: &str, schema_sqls: &[&str]) {
        let client = ArrowClickHouseClient::new(url, database, TEST_USERNAME, Some(TEST_PASSWORD));

        for schema_sql in schema_sqls {
            for statement in schema_sql.split(';') {
                let statement = statement.trim();
                if statement.is_empty() {
                    continue;
                }
                client
                    .execute(statement)
                    .await
                    .unwrap_or_else(|e| panic!("schema execution failed: {e}\n{statement}"));
            }
        }
    }
}
