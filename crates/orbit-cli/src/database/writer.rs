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
        let mut tables: Vec<&str> = ontology
            .local_entity_names()
            .iter()
            .map(|name| {
                ontology
                    .get_node(name)
                    .expect("local entity must exist")
                    .destination_table
                    .as_str()
            })
            .collect();
        tables.push(
            ontology
                .local_edge_table_name()
                .context("local_db.edge_table.name must be configured")?,
        );
        self.client
            .delete_all_data(&tables)
            .context("failed to clear revision data")?;
        Ok(())
    }

    /// Whether the last index of `commit` finished writing.
    pub fn is_complete(&self, commit: &str) -> Result<bool> {
        let batches = self.client.query_arrow_json(
            "SELECT CAST(count(*) AS BIGINT) FROM _orbit_meta WHERE key = 'indexed_commit' AND value = ?1",
            &[commit.into()],
        )?;
        Ok(duckdb_client::scalar_i64(&batches) > 0)
    }

    /// Runs `write` in one transaction, marking the graph incomplete until
    /// [`Self::complete`]; a failure leaves the graph as it was.
    pub fn transaction<T>(&self, write: impl FnOnce() -> Result<T>) -> Result<T> {
        self.client.execute("BEGIN TRANSACTION", &[])?;
        let written = self
            .client
            .execute("DELETE FROM _orbit_meta WHERE key = 'indexed_commit'", &[])
            .map_err(anyhow::Error::from)
            .and_then(|_| write());
        match written {
            Ok(value) => {
                self.client.execute("COMMIT", &[])?;
                Ok(value)
            }
            Err(error) => {
                self.client.execute("ROLLBACK", &[])?;
                Err(error)
            }
        }
    }

    /// Deletes the rows of `paths`: their files, the entities in them, and
    /// every edge that starts or ends at one of those.
    pub fn delete_files(&self, paths: &[String]) -> Result<()> {
        if paths.is_empty() {
            return Ok(());
        }
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
        self.client.execute(
            "CREATE OR REPLACE TEMP TABLE _orbit_deleted_ids AS
             SELECT id FROM gl_file WHERE path IN (SELECT path FROM _orbit_deleted_paths)
             UNION ALL SELECT id FROM gl_definition WHERE file_path IN (SELECT path FROM _orbit_deleted_paths)
             UNION ALL SELECT id FROM gl_imported_symbol WHERE file_path IN (SELECT path FROM _orbit_deleted_paths)",
            &[],
        )?;
        for sql in [
            "DELETE FROM gl_edge WHERE source_id IN (SELECT id FROM _orbit_deleted_ids)
             OR target_id IN (SELECT id FROM _orbit_deleted_ids)",
            "DELETE FROM gl_file WHERE path IN (SELECT path FROM _orbit_deleted_paths)",
            "DELETE FROM gl_definition WHERE file_path IN (SELECT path FROM _orbit_deleted_paths)",
            "DELETE FROM gl_imported_symbol WHERE file_path IN (SELECT path FROM _orbit_deleted_paths)",
            "DROP TABLE _orbit_deleted_ids",
            "DROP TABLE _orbit_deleted_paths",
        ] {
            self.client.execute(sql, &[])?;
        }
        Ok(())
    }

    /// Deletes every directory and the edges between directories; the edges
    /// from a directory to a file go with the file.
    pub fn delete_directories(&self) -> Result<()> {
        self.client.execute(
            "DELETE FROM gl_edge WHERE source_kind = 'Directory' AND target_kind = 'Directory'",
            &[],
        )?;
        self.client.execute("DELETE FROM gl_directory", &[])?;
        Ok(())
    }

    pub fn insert_batch(&self, table: &str, batch: &RecordBatch) -> Result<()> {
        self.client.insert_batch(table, batch)?;
        Ok(())
    }

    pub fn count(&self, sql: &str) -> Result<usize> {
        Ok(duckdb_client::scalar_i64(&self.client.query_arrow(sql)?) as usize)
    }

    pub fn delete_files(
        &self,
        edge_table: &str,
        tables: &[(&str, &str)],
        files: &[&str],
    ) -> Result<()> {
        if files.is_empty() || tables.is_empty() {
            return Ok(());
        }
        let list = files
            .iter()
            .map(|file| duckdb_client::sql_lit(file))
            .collect::<Vec<_>>()
            .join(", ");
        let ids = tables
            .iter()
            .map(|(table, column)| format!("SELECT id FROM {table} WHERE {column} IN ({list})"))
            .collect::<Vec<_>>()
            .join(" UNION ALL ");
        self.client.execute(
            &format!("DELETE FROM {edge_table} WHERE source_id IN ({ids}) OR target_id IN ({ids})"),
            &[],
        )?;
        for (table, column) in tables {
            self.client.execute(
                &format!("DELETE FROM {table} WHERE {column} IN ({list})"),
                &[],
            )?;
        }
        Ok(())
    }

    /// Runs `write` in one transaction, so a failure leaves the graph as it was.
    pub fn in_transaction(&self, write: impl FnOnce() -> Result<()>) -> Result<()> {
        self.client.execute("BEGIN TRANSACTION", &[])?;
        match write() {
            Ok(()) => {
                self.client.execute("COMMIT", &[])?;
                Ok(())
            }
            Err(error) => {
                let _ = self.client.execute("ROLLBACK", &[]);
                Err(error)
            }
        }
    }

    pub fn complete(&self, commit: &str) -> Result<()> {
        self.client.execute(
            "INSERT INTO _orbit_meta (key, value) VALUES ('indexed_commit', ?1)",
            &[commit.into()],
        )?;
        Ok(())
    }
}
