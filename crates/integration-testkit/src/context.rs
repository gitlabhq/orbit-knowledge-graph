use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use clickhouse_client::{ArrowClickHouseClient, ClickHouseConfigurationExt};
use gkg_server_config::ClickHouseConfiguration;
use query_engine::compiler::ParameterizedQuery;
use testcontainers::bollard::Docker;
use testcontainers::bollard::query_parameters::{
    ListContainersOptionsBuilder, RemoveContainerOptionsBuilder,
};
use testcontainers::core::{ContainerPort, ImageExt};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage};

use arrow::array::StringArray;
use gkg_utils::arrow::ArrowUtils;

const CLICKHOUSE_IMAGE: &str = "clickhouse/clickhouse-server";
const CLICKHOUSE_TAG: &str = "26.2";
const CLICKHOUSE_HTTP_PORT: u16 = 8123;

const TEST_DATABASE: &str = "test";
const TEST_USERNAME: &str = "default";
const TEST_PASSWORD: &str = "testpass";

const CONTAINER_LABEL_KEY: &str = "gkg-integration-test";
const SESSION_LABEL_KEY: &str = "gkg-session-id";

fn session_id() -> &'static str {
    use std::sync::OnceLock;
    static ID: OnceLock<String> = OnceLock::new();
    ID.get_or_init(|| {
        std::env::var("NEXTEST_RUN_ID").unwrap_or_else(|_| uuid::Uuid::new_v4().to_string())
    })
}

const MAX_CONNECTION_ATTEMPTS: u32 = 200;
const CONNECTION_RETRY_DELAY: Duration = Duration::from_millis(50);

#[derive(Clone)]
pub struct TestContext {
    _container: Arc<ContainerAsync<GenericImage>>,
    pub config: ClickHouseConfiguration,
    url: String,
    schema_sqls: Arc<Vec<String>>,
}

impl TestContext {
    pub async fn new(schema_sqls: &[&str]) -> Self {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let t = std::time::Instant::now();
        let container = Self::start_container().await;
        let url = Self::extract_url(&container).await;
        Self::wait_for_ready(&url).await;
        Self::run_schema_in(&url, TEST_DATABASE, schema_sqls).await;
        eprintln!("[context] new(): {:.2?}", t.elapsed());

        let config = ClickHouseConfiguration {
            database: TEST_DATABASE.to_string(),
            url: url.clone(),
            username: TEST_USERNAME.to_string(),
            password: Some(TEST_PASSWORD.to_string()),
            query_settings: std::collections::HashMap::new(),
            profiling: Default::default(),
        };

        Self {
            _container: Arc::new(container),
            config,
            url,
            schema_sqls: Arc::new(schema_sqls.iter().map(|s| s.to_string()).collect()),
        }
    }

    pub async fn query(&self, sql: &str) -> Vec<RecordBatch> {
        let batches = self
            .create_client()
            .query_arrow(sql)
            .await
            .expect("query failed");

        if batches.len() <= 1 {
            return batches;
        }

        let schema = batches[0].schema();
        vec![concat_batches(&schema, &batches).expect("failed to concat query results")]
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

        for (name, param) in &pq.params {
            query = ArrowClickHouseClient::bind_param(query, name, &param.value, &param.ch_type);
        }

        query
            .fetch_arrow()
            .await
            .expect("parameterized query failed")
    }

    /// Force-merge all ReplacingMergeTree parts so subsequent SELECTs see
    /// every inserted row. Queries `system.tables` for the current database
    /// and runs `OPTIMIZE TABLE … FINAL` on each table concurrently.
    pub async fn optimize_all(&self) {
        let t = std::time::Instant::now();
        let batches = self
            .query(&format!(
                "SELECT table FROM system.parts WHERE database = '{}' AND active GROUP BY table",
                self.config.database
            ))
            .await;

        let stmts: Vec<String> = batches
            .iter()
            .flat_map(|batch| {
                let col = ArrowUtils::get_column_by_name::<StringArray>(batch, "table")
                    .expect("table column");
                (0..batch.num_rows())
                    .map(|i| format!("OPTIMIZE TABLE `{}` FINAL", col.value(i)))
                    .collect::<Vec<_>>()
            })
            .collect();

        futures::future::join_all(stmts.iter().map(|sql| self.execute(sql))).await;
        eprintln!(
            "[optimize_all] {} tables in {:.2?}",
            stmts.len(),
            t.elapsed()
        );
    }

    pub async fn truncate_all_tables(&self) {
        let batches = self
            .query(&format!(
                "SELECT name FROM system.tables WHERE database = '{}' AND engine != 'View'",
                self.config.database
            ))
            .await;
        for batch in &batches {
            let names =
                ArrowUtils::get_column_by_name::<StringArray>(batch, "name").expect("name column");
            for i in 0..batch.num_rows() {
                self.execute(&format!("TRUNCATE TABLE `{}`", names.value(i)))
                    .await;
            }
        }
    }

    pub fn create_client(&self) -> ArrowClickHouseClient {
        self.config.build_client()
    }

    pub async fn fork(&self, name: &str) -> Self {
        let admin = ArrowClickHouseClient::new(
            &self.url,
            "default",
            TEST_USERNAME,
            Some(TEST_PASSWORD),
            &std::collections::HashMap::new(),
        );
        admin
            .execute(&format!("CREATE DATABASE IF NOT EXISTS `{name}`"))
            .await
            .unwrap_or_else(|e| panic!("failed to create database {name}: {e}"));

        let schema_refs: Vec<&str> = self.schema_sqls.iter().map(|s| s.as_str()).collect();
        Self::run_schema_in(&self.url, name, &schema_refs).await;

        // Copy data from parent into fork for tables that exist in both.
        let src = &self.config.database;
        let batches = self
            .query(&format!(
                "SELECT name FROM system.tables WHERE database = '{name}' AND engine != 'View'"
            ))
            .await;
        for batch in &batches {
            let names =
                ArrowUtils::get_column_by_name::<StringArray>(batch, "name").expect("name column");
            for i in 0..batch.num_rows() {
                let table = names.value(i);
                admin
                    .execute(&format!(
                        "INSERT INTO `{name}`.`{table}` SELECT * FROM `{src}`.`{table}`"
                    ))
                    .await
                    .unwrap_or_else(|e| {
                        panic!("failed to copy {src}.{table} -> {name}.{table}: {e}")
                    });
            }
        }

        Self {
            _container: Arc::clone(&self._container),
            config: ClickHouseConfiguration {
                database: name.to_string(),
                url: self.url.clone(),
                username: TEST_USERNAME.to_string(),
                password: Some(TEST_PASSWORD.to_string()),
                query_settings: std::collections::HashMap::new(),
                profiling: Default::default(),
            },
            url: self.url.clone(),
            schema_sqls: Arc::clone(&self.schema_sqls),
        }
    }

    async fn start_container() -> ContainerAsync<GenericImage> {
        let t = std::time::Instant::now();
        Self::cleanup_stale_containers().await;

        let port = ContainerPort::Tcp(CLICKHOUSE_HTTP_PORT);

        let container = GenericImage::new(CLICKHOUSE_IMAGE, CLICKHOUSE_TAG)
            .with_exposed_port(port)
            .with_env_var("CLICKHOUSE_USER", TEST_USERNAME)
            .with_env_var("CLICKHOUSE_PASSWORD", TEST_PASSWORD)
            .with_env_var("CLICKHOUSE_DB", TEST_DATABASE)
            .with_label(CONTAINER_LABEL_KEY, "true")
            .with_label(SESSION_LABEL_KEY, session_id())
            .start()
            .await
            .expect("failed to start ClickHouse container");
        eprintln!("[context] start_container: {:.2?}", t.elapsed());
        container
    }

    /// Remove containers from previous test runs that weren't cleaned up
    /// (e.g. because the process was killed without running Drop handlers).
    /// Only targets containers with the `gkg-integration-test` label whose
    /// `gkg-session-id` differs from the current run, so concurrent tests
    /// within the same nextest invocation never kill each other.
    async fn cleanup_stale_containers() {
        let docker = match Docker::connect_with_defaults() {
            Ok(d) => d,
            Err(_) => return,
        };

        let current_session = session_id();

        let filters = HashMap::from([(
            "label".to_string(),
            vec![format!("{CONTAINER_LABEL_KEY}=true")],
        )]);

        let options = ListContainersOptionsBuilder::default()
            .all(true)
            .filters(&filters)
            .build();

        let containers = docker
            .list_containers(Some(options))
            .await
            .unwrap_or_default();

        let remove_opts = RemoveContainerOptionsBuilder::default().force(true).build();

        for container in containers {
            let is_current_session = container
                .labels
                .as_ref()
                .and_then(|labels| labels.get(SESSION_LABEL_KEY))
                .is_some_and(|id| id == current_session);

            if is_current_session {
                continue;
            }

            if let Some(id) = container.id {
                let _ = docker
                    .remove_container(&id, Some(remove_opts.clone()))
                    .await;
            }
        }
    }

    async fn extract_url(container: &ContainerAsync<GenericImage>) -> String {
        let t = std::time::Instant::now();
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

        let url = format!("http://{host}:{port}");
        eprintln!("[context] extract_url: {:.2?}", t.elapsed());
        url
    }

    async fn wait_for_ready(url: &str) {
        let t = std::time::Instant::now();
        let client = ArrowClickHouseClient::new(
            url,
            "default",
            TEST_USERNAME,
            Some(TEST_PASSWORD),
            &std::collections::HashMap::new(),
        );

        for attempt in 1..=MAX_CONNECTION_ATTEMPTS {
            if client.execute("SELECT 1").await.is_ok() {
                eprintln!("[context] wait_for_ready: {:.2?}", t.elapsed());
                return;
            }
            if attempt == MAX_CONNECTION_ATTEMPTS {
                panic!("ClickHouse not ready after {MAX_CONNECTION_ATTEMPTS} attempts");
            }
            tokio::time::sleep(CONNECTION_RETRY_DELAY).await;
        }
    }

    async fn run_schema_in(url: &str, database: &str, schema_sqls: &[&str]) {
        let t = std::time::Instant::now();
        let client = ArrowClickHouseClient::new(
            url,
            database,
            TEST_USERNAME,
            Some(TEST_PASSWORD),
            &std::collections::HashMap::new(),
        );

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
        eprintln!("[context] run_schema: {:.2?}", t.elapsed());
    }
}
