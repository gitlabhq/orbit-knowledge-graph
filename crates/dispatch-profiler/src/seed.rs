//! Generates a dispatcher-sized datalake and graph in ClickHouse.
//!
//! Rows are produced server-side from `numbers()`, so seeding a million
//! projects never passes through the profiled process and cannot contaminate
//! the measurement.

use std::collections::HashMap;

use anyhow::Context;
use clickhouse_client::ArrowClickHouseClient;
use orbit_server_config::ClickHouseConfiguration;

/// Copied from `fixtures/siphon.sql`. The projections and the
/// `ReplacingMergeTree(version, deleted)` engine are what production has, and
/// both change how much the dispatcher's prefix queries have to read.
const NAMESPACE_PATHS_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS namespace_traversal_paths
(
    `id` Int64 DEFAULT 0,
    `traversal_path` String DEFAULT '0/',
    `version` DateTime64(6, 'UTC') DEFAULT now(),
    `deleted` Bool DEFAULT false,
    PROJECTION by_traversal_path (SELECT * ORDER BY traversal_path)
) ENGINE = ReplacingMergeTree(version, deleted)
PRIMARY KEY id
ORDER BY id
SETTINGS index_granularity = 512, deduplicate_merge_projection_mode = 'rebuild'
"#;

const PROJECT_PATHS_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS project_namespace_traversal_paths
(
    `id` Int64 DEFAULT 0,
    `traversal_path` String DEFAULT '0/',
    `version` DateTime64(6, 'UTC') DEFAULT now(),
    `deleted` Bool DEFAULT false,
    PROJECTION by_traversal_path (SELECT * ORDER BY traversal_path)
) ENGINE = ReplacingMergeTree(version, deleted)
PRIMARY KEY id
ORDER BY id
SETTINGS index_granularity = 512, deduplicate_merge_projection_mode = 'rebuild'
"#;

const ENABLED_NAMESPACES_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS siphon_knowledge_graph_enabled_namespaces
(
    `id` Int64 CODEC(DoubleDelta, ZSTD(1)),
    `root_namespace_id` Int64,
    `created_at` DateTime64(6, 'UTC') CODEC(Delta(8), ZSTD(1)),
    `updated_at` DateTime64(6, 'UTC') CODEC(Delta(8), ZSTD(1)),
    `traversal_path` String DEFAULT '0/' CODEC(ZSTD(3)),
    `_siphon_replicated_at` DateTime64(6, 'UTC') DEFAULT now64(6, 'UTC') CODEC(ZSTD(1)),
    `_siphon_watermark` DateTime64(6, 'UTC') DEFAULT _siphon_replicated_at,
    INDEX idx_siphon_watermark_minmax _siphon_watermark TYPE minmax GRANULARITY 1,
    `_siphon_deleted` Bool DEFAULT false CODEC(ZSTD(1)),
    PROJECTION pg_pkey_ordered (SELECT * ORDER BY id),
    PROJECTION root_namespace_id_ordered (SELECT * ORDER BY root_namespace_id)
)
ENGINE = ReplacingMergeTree(_siphon_replicated_at, _siphon_deleted)
PRIMARY KEY (traversal_path, root_namespace_id, id)
ORDER BY (traversal_path, root_namespace_id, id)
SETTINGS index_granularity = 2048, deduplicate_merge_projection_mode = 'rebuild'
"#;

/// gitlab.com root namespace and project ids are eight to nine digits, and the
/// dispatcher holds one `String` per project path, so the digit count is part
/// of what is being measured.
const FIRST_NAMESPACE_ID: i64 = 60_000_000;
const FIRST_PROJECT_ID: i64 = 70_000_000;
const ORGANIZATION_ID: i64 = 1;

#[derive(Clone, Copy)]
pub struct Shape {
    pub namespaces: u64,
    pub projects: u64,
    /// Share of projects that already carry a checkpoint row for the current
    /// schema version, i.e. the part of the backfill already done.
    pub checkpointed_pct: u64,
    /// Segments in a project's traversal path. 3 is a project directly under a
    /// root namespace; 4 and 5 model subgroup nesting.
    pub path_depth: usize,
}

pub struct Databases {
    pub datalake: String,
    pub graph: String,
}

pub struct Seeder {
    url: String,
    username: String,
    password: Option<String>,
    pub databases: Databases,
}

impl Seeder {
    pub fn new(
        url: &str,
        username: &str,
        password: Option<&str>,
        datalake: &str,
        graph: &str,
    ) -> Self {
        Self {
            url: url.to_string(),
            username: username.to_string(),
            password: password.map(str::to_string),
            databases: Databases {
                datalake: datalake.to_string(),
                graph: graph.to_string(),
            },
        }
    }

    pub fn configuration(&self, database: &str) -> ClickHouseConfiguration {
        ClickHouseConfiguration {
            database: database.to_string(),
            url: self.url.clone(),
            username: self.username.clone(),
            password: self.password.clone(),
            session_settings: HashMap::new(),
            quorum_writes: false,
            insert_settings: HashMap::new(),
            profiling: Default::default(),
        }
    }

    pub fn client(&self, database: &str) -> ArrowClickHouseClient {
        ArrowClickHouseClient::new(
            &self.url,
            database,
            &self.username,
            self.password.as_deref(),
            &HashMap::new(),
            &HashMap::new(),
        )
    }

    pub async fn wait_ready(&self, attempts: u32) -> anyhow::Result<()> {
        let client = self.client("default");
        for attempt in 1..=attempts {
            if client.execute("SELECT 1").await.is_ok() {
                return Ok(());
            }
            if attempt == attempts {
                anyhow::bail!("ClickHouse at {} not ready", self.url);
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
        Ok(())
    }

    pub async fn seed(&self, shape: Shape, checkpoint_table: &str) -> anyhow::Result<()> {
        let default = self.client("default");
        for database in [&self.databases.datalake, &self.databases.graph] {
            default
                .execute(&format!("DROP DATABASE IF EXISTS {database}"))
                .await
                .with_context(|| format!("dropping {database}"))?;
            default
                .execute(&format!("CREATE DATABASE {database}"))
                .await
                .with_context(|| format!("creating {database}"))?;
        }

        self.seed_datalake(shape).await?;
        self.seed_graph(shape, checkpoint_table).await
    }

    async fn seed_datalake(&self, shape: Shape) -> anyhow::Result<()> {
        let client = self.client(&self.databases.datalake);
        for ddl in [
            NAMESPACE_PATHS_DDL,
            PROJECT_PATHS_DDL,
            ENABLED_NAMESPACES_DDL,
        ] {
            client.execute(ddl).await.context("datalake DDL")?;
        }

        let namespace_path = namespace_path_expression();
        client
            .execute(&format!(
                "INSERT INTO namespace_traversal_paths (id, traversal_path, version, deleted) \
                 SELECT {FIRST_NAMESPACE_ID} + number, {namespace_path}, now64(6), false \
                 FROM numbers({})",
                shape.namespaces
            ))
            .await
            .context("seeding namespace_traversal_paths")?;

        client
            .execute(&format!(
                "INSERT INTO siphon_knowledge_graph_enabled_namespaces \
                 (id, root_namespace_id, created_at, updated_at, traversal_path, \
                 _siphon_replicated_at, _siphon_watermark, _siphon_deleted) \
                 SELECT number + 1, {FIRST_NAMESPACE_ID} + number, now64(6), now64(6), \
                 {namespace_path}, now64(6), now64(6), false \
                 FROM numbers({})",
                shape.namespaces
            ))
            .await
            .context("seeding siphon_knowledge_graph_enabled_namespaces")?;

        client
            .execute(&format!(
                "INSERT INTO project_namespace_traversal_paths \
                 (id, traversal_path, version, deleted) \
                 SELECT {FIRST_PROJECT_ID} + number, {}, now64(6), false \
                 FROM numbers({})",
                project_path_expression(shape),
                shape.projects
            ))
            .await
            .context("seeding project_namespace_traversal_paths")?;

        Ok(())
    }

    async fn seed_graph(&self, shape: Shape, checkpoint_table: &str) -> anyhow::Result<()> {
        let client = self.client(&self.databases.graph);
        client
            .execute(&checkpoint_table_ddl(checkpoint_table)?)
            .await
            .context("checkpoint table DDL")?;

        if shape.checkpointed_pct == 0 {
            return Ok(());
        }

        // Checkpoint the low ids so the pending remainder is a contiguous
        // suffix; which projects are covered does not change the row counts the
        // dispatcher holds.
        let checkpointed = shape.projects * shape.checkpointed_pct / 100;
        client
            .execute(&format!(
                "INSERT INTO {checkpoint_table} \
                 (traversal_path, project_id, branch, last_task_id, last_commit, indexed_at, \
                 _version, _deleted) \
                 SELECT {}, {FIRST_PROJECT_ID} + number, 'main', number, \
                 lower(hex(MD5(toString(number)))), now64(6), 1, false \
                 FROM numbers({checkpointed})",
                project_path_expression(shape),
            ))
            .await
            .context("seeding code_indexing_checkpoint")?;

        Ok(())
    }
}

/// `<org>/<root namespace>/`
fn namespace_path_expression() -> String {
    format!("concat('{ORGANIZATION_ID}/', toString({FIRST_NAMESPACE_ID} + number), '/')")
}

/// `<org>/<root namespace>/[subgroup/]*<project namespace>/`, with projects
/// distributed round-robin over the namespaces so every namespace has the same
/// number of them.
fn project_path_expression(shape: Shape) -> String {
    let namespaces = shape.namespaces.max(1);
    let root = format!(
        "concat('{ORGANIZATION_ID}/', toString({FIRST_NAMESPACE_ID} + (number % {namespaces})), '/')"
    );
    let mut expression = root;
    // Subgroup ids are drawn from the project id space so their digit count
    // matches production, which is what makes the path string realistic.
    for depth in 0..shape.path_depth.saturating_sub(3) {
        expression = format!(
            "concat({expression}, toString({} + intDiv(number, {})), '/')",
            FIRST_PROJECT_ID + 1_000_000 * (depth as i64 + 1),
            100 * (depth + 1)
        );
    }
    format!("concat({expression}, toString({FIRST_PROJECT_ID} + number), '/')")
}

/// The checkpoint table's shape comes from the ontology, so the profiler reads
/// the same DDL the dispatcher's migration would have created.
fn checkpoint_table_ddl(prefixed_name: &str) -> anyhow::Result<String> {
    use query_engine::compiler::{emit_create_table, generate_graph_tables_with_prefix};

    let ontology = ontology::Ontology::load_embedded().context("ontology must load")?;
    let prefix = prefixed_name
        .strip_suffix("code_indexing_checkpoint")
        .context("checkpoint table name must end in code_indexing_checkpoint")?;
    generate_graph_tables_with_prefix(&ontology, prefix)
        .iter()
        .find(|table| table.name == prefixed_name)
        .map(emit_create_table)
        .with_context(|| format!("{prefixed_name} is not in the generated graph schema"))
}
