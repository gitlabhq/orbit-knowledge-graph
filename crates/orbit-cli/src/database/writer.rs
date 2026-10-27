use std::fs::File;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use duckdb_client::DuckDbClient;
use ontology::Ontology;

use super::{locks::lock_database_waiting, schema};
use crate::workspace::GitInfo;

const GRAPH_SCHEMA: &str = include_str!(concat!(env!("CONFIG_DIR"), "/graph_local.sql"));

pub struct GraphWriter {
    client: DuckDbClient,
    _lock: File,
}

impl GraphWriter {
    pub fn open_waiting(path: &Path, git: &GitInfo, wait: Duration) -> Result<Self> {
        let lock = lock_database_waiting(path, wait)?;
        let revision = format!("{}:{}", git.project_id, git.commit_sha);
        if path.exists() {
            let client = DuckDbClient::open_read_only(path)?;
            if let Some(owner) = schema::stored_meta(&client, "revision")? {
                anyhow::ensure!(
                    owner == revision,
                    "{} belongs to another checkout revision; choose a different --db path",
                    path.display()
                );
            }
        }
        schema::ensure_graph_schema(path, GRAPH_SCHEMA)?;
        let client = DuckDbClient::open(path)?;
        client.execute(
            "INSERT INTO _orbit_meta (key, value) VALUES ('revision', ?1) ON CONFLICT DO NOTHING",
            &[revision.into()],
        )?;
        Ok(Self {
            client,
            _lock: lock,
        })
    }

    pub fn clear(&self, ontology: &Ontology) -> Result<()> {
        self.client
            .execute("DELETE FROM _orbit_meta WHERE key = 'indexed_commit'", &[])?;
        let local = LocalTables::of(ontology)?;
        let mut tables: Vec<&str> = local.entities.iter().map(|entity| entity.table).collect();
        tables.push(local.edges);
        self.client
            .delete_all_data(&tables)
            .context("failed to clear revision data")?;
        Ok(())
    }

    /// Deletes the rows of `paths`: their files, the entities in them, and
    /// every edge that starts or ends at one of those.
    pub fn delete_files(&self, ontology: &Ontology, paths: &[String]) -> Result<()> {
        if paths.is_empty() {
            return Ok(());
        }
        let local = LocalTables::of(ontology)?;
        self.client.execute(
            "CREATE OR REPLACE TEMP TABLE _orbit_deleted_paths (path VARCHAR)",
            &[],
        )?;
        self.client.insert_batch(
            "_orbit_deleted_paths",
            &RecordBatch::try_from_iter([(
                "path",
                std::sync::Arc::new(arrow::array::StringArray::from_iter_values(paths))
                    as arrow::array::ArrayRef,
            )])?,
        )?;
        let in_paths = |entity: &LocalEntity| {
            format!(
                "{} IN (SELECT path FROM _orbit_deleted_paths)",
                entity.file_column
            )
        };
        let ids = local
            .entities
            .iter()
            .map(|entity| format!("SELECT id FROM {} WHERE {}", entity.table, in_paths(entity)))
            .collect::<Vec<_>>()
            .join(" UNION ALL ");
        self.client.execute(
            &format!("CREATE OR REPLACE TEMP TABLE _orbit_deleted_ids AS {ids}"),
            &[],
        )?;
        self.client.execute(
            &format!(
                "DELETE FROM {} WHERE source_id IN (SELECT id FROM _orbit_deleted_ids)
                 OR target_id IN (SELECT id FROM _orbit_deleted_ids)",
                local.edges
            ),
            &[],
        )?;
        for entity in &local.entities {
            self.client.execute(
                &format!("DELETE FROM {} WHERE {}", entity.table, in_paths(entity)),
                &[],
            )?;
        }
        self.client.execute("DROP TABLE _orbit_deleted_ids", &[])?;
        self.client
            .execute("DROP TABLE _orbit_deleted_paths", &[])?;
        Ok(())
    }

    /// Deletes every directory and the edges between directories; the edges
    /// from a directory to a file go with the file.
    pub fn delete_directories(&self, ontology: &Ontology) -> Result<()> {
        let local = LocalTables::of(ontology)?;
        let directory = local
            .entities
            .iter()
            .find(|entity| entity.name == DIRECTORY)
            .context("the ontology has no local Directory entity")?;
        self.client.execute(
            &format!(
                "DELETE FROM {} WHERE source_kind = ?1 AND target_kind = ?1",
                local.edges
            ),
            &[DIRECTORY.into()],
        )?;
        self.client
            .execute(&format!("DELETE FROM {}", directory.table), &[])?;
        Ok(())
    }

    pub fn insert_batch(&self, table: &str, batch: &RecordBatch) -> Result<()> {
        self.client.insert_batch(table, batch)?;
        Ok(())
    }

    pub fn count(&self, sql: &str) -> Result<usize> {
        Ok(duckdb_client::scalar_i64(&self.client.query_arrow(sql)?) as usize)
    }

    /// Runs `write` in one transaction, so a failure leaves the graph as it was.
    pub fn in_transaction<T>(&self, write: impl FnOnce() -> Result<T>) -> Result<T> {
        self.client.execute("BEGIN TRANSACTION", &[])?;
        match write() {
            Ok(value) => {
                self.client.execute("COMMIT", &[])?;
                Ok(value)
            }
            Err(error) => {
                let _ = self.client.execute("ROLLBACK", &[]);
                Err(error)
            }
        }
    }

    pub fn complete(&self, commit: &str) -> Result<()> {
        self.client.execute(
            "INSERT OR REPLACE INTO _orbit_meta (key, value) VALUES ('indexed_commit', ?1)",
            &[commit.into()],
        )?;
        Ok(())
    }
}

const DIRECTORY: &str = "Directory";

/// The local graph tables, as the ontology names them.
struct LocalTables<'a> {
    edges: &'a str,
    entities: Vec<LocalEntity<'a>>,
}

/// A local entity's table, and the column that names the file a row is in.
struct LocalEntity<'a> {
    name: &'a str,
    table: &'a str,
    file_column: &'a str,
}

impl<'a> LocalTables<'a> {
    fn of(ontology: &'a Ontology) -> Result<Self> {
        let edges = ontology
            .local_edge_table_name()
            .context("local_db.edge_table.name must be configured")?;
        let entities = ontology
            .local_entity_names()
            .into_iter()
            .map(|name| {
                let node = ontology
                    .get_node(name)
                    .with_context(|| format!("local entity {name} is not in the ontology"))?;
                let has = |field: &str| node.fields.iter().any(|f| f.name == field);
                let file_column = ["file_path", "path"]
                    .into_iter()
                    .find(|column| has(column))
                    .with_context(|| {
                        format!("local entity {name} has no file_path or path field")
                    })?;
                Ok(LocalEntity {
                    name: node.name.as_str(),
                    table: node.destination_table.as_str(),
                    file_column,
                })
            })
            .collect::<Result<_>>()?;
        Ok(Self { edges, entities })
    }
}
