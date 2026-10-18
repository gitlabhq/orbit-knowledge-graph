use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use clickhouse_client::{ArrowClickHouseClient, ClickHouseConfigurationExt};
use orbit_server_config::{AppConfig, ClickHouseConfiguration};
use query_engine::compiler::ParameterizedQuery;
use testcontainers::bollard::Docker;
use testcontainers::bollard::query_parameters::{
    ListContainersOptionsBuilder, RemoveContainerOptionsBuilder,
};
use testcontainers::core::{ContainerPort, ImageExt};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage};

use arrow::array::{StringArray, UInt64Array};
use orbit_arrow::ArrowUtils;

pub const CLICKHOUSE_IMAGE: &str = "clickhouse/clickhouse-server";
pub const CLICKHOUSE_TAG: &str = "26.6";
const CLICKHOUSE_HTTP_PORT: u16 = 8123;

const TEST_DATABASE: &str = "test";
const TEST_USERNAME: &str = "default";
const TEST_PASSWORD: &str = "testpass";
const SHARED_CLICKHOUSE_URL: &str = "GKG_TEST_CLICKHOUSE_URL";
const TEMPLATE_READY_TABLE: &str = "_gkg_template_ready";
const TEMPLATE_ATTEMPTS: u32 = 1200;

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
    _container: Option<Arc<ContainerAsync<GenericImage>>>,
    pub config: ClickHouseConfiguration,
    url: String,
    schema_sqls: Arc<Vec<String>>,
}

impl TestContext {
    pub async fn new(schema_sqls: &[&str]) -> Self {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let t = std::time::Instant::now();
        let (container, url) = match std::env::var(SHARED_CLICKHOUSE_URL) {
            Ok(url) => (None, url),
            Err(_) => {
                let container = Arc::new(Self::start_container().await);
                let url = Self::extract_url(&container).await;
                (Some(container), url)
            }
        };
        Self::wait_for_ready(&url).await;
        let database = if container.is_none() {
            unique_database(TEST_DATABASE)
        } else {
            TEST_DATABASE.to_string()
        };
        if container.is_none() {
            Self::create_database(&url, &database).await;
        }
        Self::run_schema_in(&url, &database, schema_sqls).await;
        eprintln!("[context] new(): {:.2?}", t.elapsed());

        let config = test_configuration(&url, &database);

        Self {
            _container: container,
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
        self.try_query_parameterized(pq)
            .await
            .expect("parameterized query failed")
    }

    /// Like [`query_parameterized`] but returns the ClickHouse error instead of
    /// panicking, so callers (e.g. the corpus smoke test) can collect per-query
    /// failures rather than aborting on the first one.
    pub async fn try_query_parameterized(
        &self,
        pq: &ParameterizedQuery,
    ) -> Result<Vec<RecordBatch>, String> {
        let client = self.create_client();
        let mut query = client.query(&pq.sql);

        for (name, param) in &pq.params {
            query = ArrowClickHouseClient::bind_param(query, name, &param.value, &param.data_type);
        }

        query.fetch_arrow().await.map_err(|e| e.to_string())
    }

    pub async fn explain_plan(&self, pq: &ParameterizedQuery) -> serde_json::Value {
        let text = self
            .create_client()
            .explain_plan(&pq.render())
            .await
            .expect("explain failed");
        serde_json::from_str(&text).expect("explain output is JSON")
    }

    /// Force-merge all ReplacingMergeTree parts so subsequent SELECTs see
    /// every inserted row.
    pub async fn optimize_all(&self) {
        let t = std::time::Instant::now();
        let batches = self
            .query(&format!(
                "SELECT table FROM system.parts WHERE database = '{}' AND active \
                 GROUP BY table HAVING count() > 1",
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
                "SELECT name FROM system.tables WHERE database = '{}' AND engine NOT IN ('View', 'Dictionary')",
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
        self.config.build_client("graph")
    }

    pub async fn isolated(schema_sqls: &[&str]) -> Self {
        let context = Self::new(&[]).await;
        let database = unique_database("isolated");
        Self::create_database(&context.url, &database).await;
        Self::run_schema_in(&context.url, &database, schema_sqls).await;
        Self {
            _container: context._container,
            config: test_configuration(&context.url, &database),
            url: context.url,
            schema_sqls: Arc::new(schema_sqls.iter().map(|sql| sql.to_string()).collect()),
        }
    }

    pub async fn fork(&self, name: &str) -> Self {
        let database = self.fork_database_name(name);
        let schema_refs: Vec<&str> = self.schema_sqls.iter().map(String::as_str).collect();
        Self::clone_database(&self.url, &self.config.database, &database, &schema_refs).await;

        Self {
            _container: self._container.clone(),
            config: test_configuration(&self.url, &database),
            url: self.url.clone(),
            schema_sqls: Arc::clone(&self.schema_sqls),
        }
    }

    pub async fn cached_fork<F, Fut>(&self, key: &str, initialize: F) -> Self
    where
        F: FnOnce(TestContext) -> Fut,
        Fut: Future<Output = ()>,
    {
        if self._container.is_some() {
            let context = self.clone();
            initialize(context.clone()).await;
            return context;
        }

        let input = format!("{}\0{key}", self.schema_sqls.join("\0"));
        let hash = ontology::migrations::sha256_hex(&input);
        let template = format!("gkg_seed_{}", &hash[..16]);
        let admin = test_client(&self.url, "default");
        let client = test_client(&self.url, &template);

        match admin
            .execute(&format!("CREATE DATABASE `{template}`"))
            .await
        {
            Ok(()) => {
                let schema_refs: Vec<&str> = self.schema_sqls.iter().map(String::as_str).collect();
                Self::clone_tables(&self.url, &self.config.database, &template, &schema_refs).await;
                let context = Self {
                    _container: None,
                    config: test_configuration(&self.url, &template),
                    url: self.url.clone(),
                    schema_sqls: Arc::clone(&self.schema_sqls),
                };
                initialize(context).await;
                client
                    .execute(&format!(
                        "CREATE TABLE `{template}`.`{TEMPLATE_READY_TABLE}` \
                         (ready Bool) ENGINE = Memory AS SELECT true"
                    ))
                    .await
                    .unwrap_or_else(|e| {
                        panic!("failed to mark seed template {template} ready: {e}")
                    });
            }
            Err(error) if error.to_string().contains("DATABASE_ALREADY_EXISTS") => {
                Self::wait_for_template(&client, &template).await;
            }
            Err(error) => panic!("failed to initialize seed template {template}: {error}"),
        }

        let database = unique_database(key);
        let schema_refs: Vec<&str> = self.schema_sqls.iter().map(String::as_str).collect();
        Self::clone_database(&self.url, &template, &database, &schema_refs).await;
        Self {
            _container: None,
            config: test_configuration(&self.url, &database),
            url: self.url.clone(),
            schema_sqls: Arc::clone(&self.schema_sqls),
        }
    }

    pub fn fork_database_name(&self, name: &str) -> String {
        let hash = ontology::migrations::sha256_hex(name);
        format!(
            "{}_{}_{}",
            self.config.database,
            database_label(name).chars().take(24).collect::<String>(),
            &hash[..8]
        )
    }

    pub async fn drop_fork(&self, name: &str) {
        let database = self.fork_database_name(name);
        self.execute(&format!("DROP DATABASE IF EXISTS `{database}` SYNC"))
            .await;
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
            .with_env_var("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1")
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
        let settings = test_settings();
        for attempt in 1..=MAX_CONNECTION_ATTEMPTS {
            let client = ArrowClickHouseClient::new(
                url,
                "default",
                TEST_USERNAME,
                Some(TEST_PASSWORD),
                &settings,
                &std::collections::HashMap::new(),
                "graph",
            );
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
        let client = test_client(url, database);
        let (tables, objects) = schema_statements(database, schema_sqls);
        Self::execute_statements(&client, tables).await;
        Self::execute_schema_objects(&client, objects).await;
        eprintln!("[context] run_schema: {:.2?}", t.elapsed());
    }

    async fn wait_for_template(client: &ArrowClickHouseClient, database: &str) {
        for attempt in 1..=TEMPLATE_ATTEMPTS {
            let query = format!(
                "SELECT count() AS ready_count FROM `{database}`.`{TEMPLATE_READY_TABLE}` WHERE ready"
            );
            if let Ok(batches) = client.query_arrow(&query).await
                && batches.first().is_some_and(|batch| {
                    ArrowUtils::get_column_by_name::<UInt64Array>(batch, "ready_count")
                        .is_some_and(|column| column.value(0) == 1)
                })
            {
                return;
            }
            if attempt == TEMPLATE_ATTEMPTS {
                panic!("ClickHouse template {database} was not ready");
            }
            tokio::time::sleep(CONNECTION_RETRY_DELAY).await;
        }
    }

    async fn clone_database(url: &str, source: &str, target: &str, schema_sqls: &[&str]) {
        Self::create_database(url, target).await;
        Self::clone_tables(url, source, target, schema_sqls).await;
    }

    async fn clone_tables(url: &str, source: &str, target: &str, schema_sqls: &[&str]) {
        let client = test_client(url, target);
        let query = format!(
            "SELECT name FROM system.tables WHERE database = '{source}' \
             AND engine NOT IN ('View', 'MaterializedView', 'Dictionary') \
             AND name != '{TEMPLATE_READY_TABLE}'"
        );
        let batches = client
            .query_arrow(&query)
            .await
            .unwrap_or_else(|e| panic!("failed to list tables in {source}: {e}"));
        for batch in &batches {
            let names =
                ArrowUtils::get_column_by_name::<StringArray>(batch, "name").expect("name column");
            for row in 0..batch.num_rows() {
                let table = names.value(row);
                client
                    .execute(&format!(
                        "CREATE TABLE `{target}`.`{table}` CLONE AS `{source}`.`{table}`"
                    ))
                    .await
                    .unwrap_or_else(|e| panic!("failed to clone {source}.{table}: {e}"));
            }
        }
        let (_, objects) = schema_statements(target, schema_sqls);
        Self::execute_schema_objects(&client, objects).await;
    }

    async fn create_database(url: &str, database: &str) {
        test_client(url, "default")
            .execute(&format!("CREATE DATABASE `{database}`"))
            .await
            .unwrap_or_else(|e| panic!("failed to create database {database}: {e}"));
    }

    async fn execute_statements(client: &ArrowClickHouseClient, statements: Vec<String>) {
        for statement in statements {
            client
                .execute(&statement)
                .await
                .unwrap_or_else(|e| panic!("schema execution failed: {e}\n{statement}"));
        }
    }

    async fn execute_schema_objects(client: &ArrowClickHouseClient, statements: Vec<String>) {
        Self::execute_statements(client, statements).await;
    }
}

fn test_settings() -> HashMap<String, String> {
    HashMap::from([
        ("async_insert".to_string(), "0".to_string()),
        ("fsync_metadata".to_string(), "0".to_string()),
    ])
}

fn test_client(url: &str, database: &str) -> ArrowClickHouseClient {
    ArrowClickHouseClient::new(
        url,
        database,
        TEST_USERNAME,
        Some(TEST_PASSWORD),
        &test_settings(),
        &HashMap::new(),
        "graph",
    )
}

fn test_configuration(url: &str, database: &str) -> ClickHouseConfiguration {
    ClickHouseConfiguration {
        database: database.to_string(),
        url: url.to_string(),
        username: TEST_USERNAME.to_string(),
        password: Some(TEST_PASSWORD.to_string()),
        session_settings: test_settings(),
        replicated: false,
        insert_settings: HashMap::new(),
        profiling: AppConfig::embedded_defaults().graph.profiling,
    }
}

fn schema_statements(database: &str, schema_sqls: &[&str]) -> (Vec<String>, Vec<String>) {
    let statements = schema_sqls.iter().flat_map(|schema_sql| {
        schema_sql
            .replace("$DICTIONARY_USER", TEST_USERNAME)
            .replace("$DICTIONARY_PASSWORD", TEST_PASSWORD)
            .replace("$DICTIONARY_SECURE", "0")
            .replace("$DICTIONARY_DATABASE", database)
            .split(';')
            .map(str::trim)
            .filter(|statement| !statement.is_empty())
            .map(str::to_string)
            .collect::<Vec<_>>()
    });
    statements.partition(|statement| statement_kind(statement).starts_with("CREATE TABLE"))
}

fn unique_database(label: &str) -> String {
    format!(
        "gkg_{}_{}",
        database_label(label),
        uuid::Uuid::new_v4().simple()
    )
}

fn database_label(label: &str) -> String {
    label
        .chars()
        .filter(|character| character.is_ascii_alphanumeric() || *character == '_')
        .take(48)
        .collect()
}

fn statement_kind(statement: &str) -> String {
    statement
        .lines()
        .find(|line| !line.trim().is_empty() && !line.trim_start().starts_with("--"))
        .unwrap_or_default()
        .trim_start()
        .to_ascii_uppercase()
}
