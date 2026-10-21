use std::fs::File;
use std::path::Path;

use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use duckdb_client::DuckDbClient;
use ontology::Ontology;

use super::{lock_database, schema};
use crate::workspace::GitInfo;

const GRAPH_SCHEMA: &str = include_str!(concat!(env!("CONFIG_DIR"), "/graph_local.sql"));

pub struct GraphWriter {
    client: DuckDbClient,
    _lock: File,
}

impl GraphWriter {
    pub fn open(path: &Path, git: &GitInfo) -> Result<Self> {
        let lock = lock_database(path)?;
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
            .map(|file| format!("'{}'", file.replace('\'', "''")))
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

    pub fn complete(&self, commit: &str) -> Result<()> {
        self.client.execute(
            "INSERT INTO _orbit_meta (key, value) VALUES ('indexed_commit', ?1)",
            &[commit.into()],
        )?;
        Ok(())
    }
}
