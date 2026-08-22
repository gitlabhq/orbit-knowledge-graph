use std::collections::HashMap;

use anyhow::Context;
use clickhouse_client::ArrowClickHouseClient;
use orbit_server_config::ClickHouseConfiguration;

/// Mirrors `indexer::schema::version::SCHEMA_VERSION`, which is not public
/// outside the crate's own schema module.
pub fn table_prefix() -> String {
    let version: u32 = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../config/SCHEMA_VERSION"
    ))
    .trim()
    .parse()
    .expect("SCHEMA_VERSION must parse");
    if version == 0 {
        String::new()
    } else {
        format!("v{version}_")
    }
}

pub fn graph_schema_sql(prefix: &str) -> String {
    use query_engine::compiler::{
        emit_create_materialized_view, emit_create_table,
        generate_graph_materialized_views_with_prefix, generate_graph_tables_with_prefix,
    };

    let ontology = ontology::Ontology::load_embedded().expect("ontology must load");
    let mut stmts: Vec<String> = generate_graph_tables_with_prefix(&ontology, prefix)
        .iter()
        .map(|t| format!("{};", emit_create_table(t)))
        .collect();
    for mv in &generate_graph_materialized_views_with_prefix(&ontology, prefix) {
        stmts.push(format!("{};", emit_create_materialized_view(mv)));
    }
    stmts.join("\n")
}

pub struct ClickHouse {
    pub config: ClickHouseConfiguration,
}

impl ClickHouse {
    pub fn new(url: &str, database: &str, username: &str, password: Option<&str>) -> Self {
        Self {
            config: ClickHouseConfiguration {
                database: database.to_string(),
                url: url.to_string(),
                username: username.to_string(),
                password: password.map(|p| p.to_string()),
                session_settings: HashMap::new(),
                quorum_writes: false,
                insert_settings: HashMap::new(),
                profiling: Default::default(),
            },
        }
    }

    fn client_for(&self, database: &str) -> ArrowClickHouseClient {
        ArrowClickHouseClient::new(
            &self.config.url,
            database,
            &self.config.username,
            self.config.password.as_deref(),
            &HashMap::new(),
            &HashMap::new(),
        )
    }

    pub async fn wait_ready(&self, attempts: u32) -> anyhow::Result<()> {
        let client = self.client_for("default");
        for attempt in 1..=attempts {
            if client.execute("SELECT 1").await.is_ok() {
                return Ok(());
            }
            if attempt == attempts {
                anyhow::bail!("ClickHouse at {} not ready", self.config.url);
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
        Ok(())
    }

    /// Drops and recreates the target database, then applies the
    /// ontology-generated graph DDL so every run starts from an empty,
    /// current-schema set of tables.
    pub async fn reset_schema(&self) -> anyhow::Result<usize> {
        let db = &self.config.database;
        let root = self.client_for("default");
        root.execute(&format!("DROP DATABASE IF EXISTS `{db}`"))
            .await
            .with_context(|| format!("dropping database {db}"))?;
        root.execute(&format!("CREATE DATABASE `{db}`"))
            .await
            .with_context(|| format!("creating database {db}"))?;

        let client = self.client_for(db);
        let sql = graph_schema_sql(&table_prefix());
        let mut applied = 0usize;
        for statement in sql.split(';') {
            let statement = statement.trim();
            if statement.is_empty() {
                continue;
            }
            client
                .execute(statement)
                .await
                .with_context(|| format!("applying DDL: {statement}"))?;
            applied += 1;
        }
        Ok(applied)
    }

    pub async fn row_counts(&self, prefix: &str) -> anyhow::Result<Vec<(String, u64)>> {
        use arrow::array::{StringArray, UInt64Array};

        let client = self.client_for(&self.config.database);
        let batches = client
            .query_arrow(&format!(
                "SELECT table, sum(rows) AS rows FROM system.parts \
                 WHERE database = '{}' AND active AND startsWith(table, '{prefix}') \
                 GROUP BY table HAVING rows > 0 ORDER BY rows DESC",
                self.config.database
            ))
            .await?;

        let mut out = Vec::new();
        for batch in batches {
            let tables = batch
                .column_by_name("table")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                .context("table column")?;
            let rows = batch
                .column_by_name("rows")
                .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
                .context("rows column")?;
            for i in 0..batch.num_rows() {
                out.push((tables.value(i).to_string(), rows.value(i)));
            }
        }
        Ok(out)
    }
}
