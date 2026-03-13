# Zero-nodes investigation — code dump for external agent

## Problem

Integration tests use `run_subtests!` macro which forks a **separate ClickHouse database** per subtest via `TestContext::fork()`. Each forked DB gets its own schema and seed data. With 20 subtests (current main), tests pass reliably. When scaled to 44 subtests, we intermittently get `expected 6 nodes, got 0` — a `SELECT` from a freshly seeded table returns no rows.

All tables use `ReplacingMergeTree`. There is only ONE insert per table per forked DB. Queries do NOT use `FINAL`. Adding `OPTIMIZE TABLE ... FINAL` after each INSERT seems to fix it, but we don't understand why a single INSERT into a fresh empty table would need that.

## Hypotheses

1. **ClickHouse under concurrent load** — 44 forks = 44 `CREATE DATABASE`, ~440 `CREATE TABLE`, ~220 `INSERT`, ~44 `SELECT` all against a single Docker container. Under load, maybe ClickHouse buffers/delays writes via HTTP even though the HTTP 200 was returned.

2. **Async insert behavior** — ClickHouse 25.12 might have `async_insert` enabled by default or under load, meaning INSERT returns before data lands in a part.

## Questions for the agent

1. Does ClickHouse 25.12 enable `async_insert` by default? Does the `clickhouse` Rust crate (v0.14) set any async insert options?
2. For `ReplacingMergeTree` with a single INSERT into an empty table, is there ANY scenario where a subsequent `SELECT` (same connection/session or different) returns 0 rows?
3. Is `OPTIMIZE TABLE ... FINAL` the right fix, or should we set `async_insert=0` on the client, or use `SELECT ... FINAL`, or something else?
4. Could 44 concurrent `CREATE DATABASE` + schema DDL operations cause ClickHouse to delay or drop subsequent DML operations?

## Code

### ClickHouse container: image `clickhouse/clickhouse-server:25.12`
### Rust client: `clickhouse` crate v0.14 (features: inserter, rustls-tls-ring, rustls-tls-native-roots)

---

### `crates/integration-testkit/src/lib.rs` — run_subtests! macro

```rust
/// Fork a database per subtest and run all subtests in parallel.
#[macro_export]
macro_rules! run_subtests {
    ($ctx:expr, $($test_fn:path),+ $(,)?) => {
        futures::future::join_all(vec![
            $(
                Box::pin(async {
                    let name = stringify!($test_fn).replace("::", "_").replace(' ', "");
                    let db = $ctx.fork(&name).await;
                    eprintln!("--- {}", name);
                    $test_fn(&db).await;
                }) as std::pin::Pin<Box<dyn std::future::Future<Output = ()> + '_>>,
            )+
        ]).await;
    };
}
```

---

### `crates/integration-testkit/src/context.rs` — TestContext (full file)

```rust
use std::sync::Arc;
use std::time::Duration;

use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use clickhouse_client::{ArrowClickHouseClient, ClickHouseConfiguration};
use query_engine::ParameterizedQuery;
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
```

---

### `crates/clickhouse-client/src/arrow_client.rs` — ArrowClickHouseClient (relevant parts)

```rust
#[derive(Clone)]
pub struct ArrowClickHouseClient {
    client: Client,
    base_url: String,
}

impl ArrowClickHouseClient {
    pub fn new(url: &str, database: &str, username: &str, password: Option<&str>) -> Self {
        let mut client = Client::default()
            .with_url(url)
            .with_database(database)
            .with_user(username)
            .with_option("output_format_arrow_string_as_string", "1")
            .with_option("output_format_arrow_fixed_string_as_fixed_byte_array", "1");

        if let Some(password) = password {
            client = client.with_password(password);
        }

        Self {
            client,
            base_url: url.to_string(),
        }
    }

    pub async fn execute(&self, sql: &str) -> Result<(), ClickHouseError> {
        self.query(sql).execute().await
    }

    // ...
}

pub struct ArrowQuery {
    inner: Query,
}

impl ArrowQuery {
    pub async fn execute(self) -> Result<(), ClickHouseError> {
        self.inner.execute().await.map_err(ClickHouseError::Query)
    }

    pub async fn fetch_arrow(self) -> Result<Vec<RecordBatch>, ClickHouseError> {
        let mut cursor = self
            .inner
            .fetch_bytes("ArrowStream")
            .map_err(ClickHouseError::Query)?;

        let mut buffer = Vec::new();
        loop {
            match cursor.next().await {
                Ok(Some(chunk)) => buffer.extend(chunk),
                Ok(None) => break,
                Err(e) => return Err(ClickHouseError::Query(e)),
            }
        }

        if buffer.is_empty() {
            return Ok(Vec::new());
        }

        let data_cursor = Cursor::new(buffer);
        let reader =
            StreamReader::try_new(data_cursor, None).map_err(ClickHouseError::ArrowDecode)?;

        reader
            .map(|result| result.map_err(ClickHouseError::ArrowDecode))
            .collect()
    }
}
```

Note: the `execute` method calls `self.inner.execute()` which is `clickhouse::Query::execute()` from the `clickhouse` crate v0.14. This sends the SQL via HTTP POST and waits for the response.

---

### `crates/integration-tests/tests/common.rs` — shared test infrastructure

```rust
pub use integration_testkit::{
    GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL, TestContext, get_boolean_column, get_int64_column,
    get_string_column, get_uint64_column,
};

pub fn test_security_context() -> SecurityContext {
    SecurityContext::new(1, vec!["1/".into()]).expect("valid security context")
}
```

---

### `crates/integration-tests/tests/server/data_correctness.rs` — seed() and run_query()

```rust
async fn run_query(ctx: &TestContext, json: &str, svc: &MockRedactionService) -> ResponseView {
    let ontology = Arc::new(load_ontology());
    let client = Arc::new(ctx.create_client());
    let security_ctx = test_security_context();
    let compiled = Arc::new(compile(json, &ontology, &security_ctx).unwrap());

    let batches = ctx.query_parameterized(&compiled.base).await;
    let mut result = QueryResult::from_batches(&batches, &compiled.base.result_context);
    let redacted_count = run_redaction(&mut result, svc);

    let mut pipeline_ctx = QueryPipelineContext {
        compiled: Some(Arc::clone(&compiled)),
        ontology: Arc::clone(&ontology),
        client,
        security_context: Some(security_ctx),
    };
    let claims = gkg_server::auth::Claims::dummy();
    let mut req = PipelineRequest::<gkg_server::proto::ExecuteQueryMessage> {
        claims: &claims,
        query_json: "",
        tx: None,
        stream: None,
    };
    let mut obs = PipelineObserver::start();

    let output = HydrationStage
        .execute(
            RedactionOutput {
                query_result: result,
                redacted_count,
            },
            &mut pipeline_ctx,
            &mut req,
            &mut obs,
        )
        .await
        .expect("pipeline should succeed");

    let value = GraphFormatter.format(&output.query_result, &output.result_context, &pipeline_ctx);
    assert_valid(&value);
    let response =
        serde_json::from_value(value).expect("response should deserialize to GraphResponse");
    ResponseView::for_query(&compiled.input, response)
}

async fn seed(ctx: &TestContext) {
    ctx.execute(
        "INSERT INTO gl_user (id, username, name, state, user_type) VALUES
         (1, 'alice', 'Alice Admin', 'active', 'human'),
         (2, 'bob', 'Bob Builder', 'active', 'human'),
         (3, 'charlie', 'Charlie Private', 'active', 'human'),
         (4, 'diana', 'Diana Developer', 'active', 'project_bot'),
         (5, 'eve', 'Eve External', 'blocked', 'service_account')",
    )
    .await;

    ctx.execute(
        "INSERT INTO gl_group (id, name, visibility_level, traversal_path) VALUES
         (100, 'Public Group', 'public', '1/100/'),
         (101, 'Private Group', 'private', '1/101/'),
         (102, 'Internal Group', 'internal', '1/102/')",
    )
    .await;

    ctx.execute(
        "INSERT INTO gl_project (id, name, visibility_level, traversal_path) VALUES
         (1000, 'Public Project', 'public', '1/100/1000/'),
         (1001, 'Private Project', 'private', '1/101/1001/'),
         (1002, 'Internal Project', 'internal', '1/100/1002/'),
         (1003, 'Secret Project', 'private', '1/101/1003/'),
         (1004, 'Shared Project', 'public', '1/102/1004/')",
    )
    .await;

    ctx.execute(
        "INSERT INTO gl_merge_request (id, iid, title, state, source_branch, target_branch, traversal_path) VALUES
         (2000, 1, 'Add feature A', 'opened', 'feature-a', 'main', '1/100/1000/'),
         (2001, 2, 'Fix bug B', 'opened', 'fix-b', 'main', '1/100/1000/'),
         (2002, 3, 'Refactor C', 'merged', 'refactor-c', 'main', '1/101/1001/'),
         (2003, 4, 'Update D', 'closed', 'update-d', 'main', '1/102/1004/')",
    )
    .await;

    ctx.execute(
        "INSERT INTO gl_edge (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind) VALUES
         ('1/100/', 1, 'User', 'MEMBER_OF', 100, 'Group'),
         ('1/102/', 1, 'User', 'MEMBER_OF', 102, 'Group'),
         ('1/100/', 2, 'User', 'MEMBER_OF', 100, 'Group'),
         ('1/101/', 3, 'User', 'MEMBER_OF', 101, 'Group'),
         ('1/101/', 4, 'User', 'MEMBER_OF', 101, 'Group'),
         ('1/102/', 4, 'User', 'MEMBER_OF', 102, 'Group'),
         ('1/101/', 5, 'User', 'MEMBER_OF', 101, 'Group'),
         ('1/100/1000/', 100, 'Group', 'CONTAINS', 1000, 'Project'),
         ('1/100/1002/', 100, 'Group', 'CONTAINS', 1002, 'Project'),
         ('1/101/1001/', 101, 'Group', 'CONTAINS', 1001, 'Project'),
         ('1/101/1003/', 101, 'Group', 'CONTAINS', 1003, 'Project'),
         ('1/102/1004/', 102, 'Group', 'CONTAINS', 1004, 'Project'),
         ('1/100/1000/', 1, 'User', 'AUTHORED', 2000, 'MergeRequest'),
         ('1/100/1000/', 1, 'User', 'AUTHORED', 2001, 'MergeRequest'),
         ('1/101/1001/', 2, 'User', 'AUTHORED', 2002, 'MergeRequest'),
         ('1/102/1004/', 3, 'User', 'AUTHORED', 2003, 'MergeRequest')",
    )
    .await;
}
```

---

### `config/graph.sql` — table DDL (relevant tables)

```sql
CREATE TABLE IF NOT EXISTS gl_user (
    id Int64,
    username String DEFAULT '',
    email String DEFAULT '',
    name String DEFAULT '',
    first_name String DEFAULT '',
    last_name String DEFAULT '',
    state String DEFAULT '',
    avatar_url Nullable(String),
    public_email Nullable(String),
    preferred_language Nullable(String),
    last_activity_on Nullable(Date32),
    private_profile Bool DEFAULT false,
    is_admin Bool DEFAULT false,
    is_auditor Bool DEFAULT false,
    is_external Bool DEFAULT false,
    user_type String DEFAULT '',
    created_at Nullable(DateTime64(6, 'UTC')),
    updated_at Nullable(DateTime64(6, 'UTC')),
    _version DateTime64(6, 'UTC') DEFAULT now64(6),
    _deleted Bool DEFAULT false
) ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY (id) PRIMARY KEY (id)
SETTINGS allow_experimental_replacing_merge_with_cleanup = 1;

CREATE TABLE IF NOT EXISTS gl_group (
    id Int64,
    name Nullable(String),
    description Nullable(String),
    visibility_level String DEFAULT '',
    traversal_path String DEFAULT '',
    created_at Nullable(DateTime64(6, 'UTC')),
    updated_at Nullable(DateTime64(6, 'UTC')),
    _version DateTime64(6, 'UTC') DEFAULT now64(6),
    _deleted Bool DEFAULT false
) ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY (id) PRIMARY KEY (id)
SETTINGS allow_experimental_replacing_merge_with_cleanup = 1;

-- All other tables (gl_project, gl_merge_request, gl_note, gl_edge)
-- follow the same pattern: ReplacingMergeTree(_version, _deleted)
```

---

### Key observations

- ClickHouse image: `clickhouse/clickhouse-server:25.12`
- Rust client: `clickhouse` crate v0.14
- All tables: `ReplacingMergeTree(_version, _deleted)` with `allow_experimental_replacing_merge_with_cleanup = 1`
- No custom ClickHouse config is passed to the container (no users.xml, no config.xml overrides)
- No `async_insert` or `wait_for_async_insert` options set on the Rust client
- `create_client()` creates a NEW client on every call (no connection pooling or session reuse)
- The `execute()` method awaits the HTTP response before returning
- With 20 concurrent subtests: passes reliably. With 44: intermittent 0-row results on freshly seeded tables.
