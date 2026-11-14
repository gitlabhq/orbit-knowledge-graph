//! The DuckDB lakes the deployment reads, one in-memory database per DuckDB connection, created
//! from the packages and filled from the seed.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use anyhow::{Context, bail};
use compiler::dag::{Connections, Peer};
use duckdb_client::dag::DuckDbExecutor;
use duckdb_client::{DuckDbClient, OpenMode};
use query_data_model::{Catalog, ConnectionId, DuckDb};
use serde_json::Value;

use crate::seed::TableRows;

pub struct Lake {
    pub connection: ConnectionId,
    catalog: Arc<Catalog<DuckDb>>,
    client: Arc<Mutex<DuckDbClient>>,
}

pub struct Lakes(Vec<Lake>);

impl Lakes {
    pub fn open(connections: &Connections) -> anyhow::Result<Self> {
        let mut lakes = Vec::new();
        for peer in connections.peers() {
            let Peer::DuckDb(catalog) = peer else {
                continue;
            };
            let client = DuckDbClient::open(OpenMode::InMemory)?;
            client.initialize_schema(&catalog.create_sql())?;
            lakes.push(Lake {
                connection: catalog.connection(),
                catalog: Arc::clone(catalog),
                client: Arc::new(Mutex::new(client)),
            });
        }
        Ok(Self(lakes))
    }

    pub fn executors(&self) -> impl Iterator<Item = (ConnectionId, DuckDbExecutor)> + '_ {
        self.0.iter().map(|lake| {
            let executor = DuckDbExecutor {
                client: Arc::clone(&lake.client),
            };
            (lake.connection, executor)
        })
    }

    /// Empties every lake table and inserts the seeded rows, each into the lake that holds it.
    pub fn reload(&self, tables: &[TableRows]) -> anyhow::Result<usize> {
        let mut owners: BTreeMap<String, &Lake> = BTreeMap::new();
        for lake in &self.0 {
            for table in lake.catalog.tables() {
                owners.insert(table.name.clone(), lake);
            }
        }
        for seeded in tables {
            if !owners.contains_key(&seeded.table) {
                bail!(
                    "the lake seed names table {}, which no lake declares",
                    seeded.table
                );
            }
        }
        let mut inserted = 0;
        for lake in &self.0 {
            let client = lake
                .client
                .lock()
                .map_err(|_| anyhow::anyhow!("a lake lock is poisoned"))?;
            for table in lake.catalog.tables() {
                client.execute(&format!("DELETE FROM {}", table.name), &[])?;
            }
            for seeded in tables
                .iter()
                .filter(|seeded| std::ptr::eq(owners[&seeded.table], lake))
            {
                for row in &seeded.rows {
                    let columns: Vec<&str> = row.keys().map(String::as_str).collect();
                    let marks = vec!["?"; columns.len()].join(", ");
                    let sql = format!(
                        "INSERT INTO {} ({}) VALUES ({marks})",
                        seeded.table,
                        columns.join(", ")
                    );
                    let values: Vec<Value> = row.values().cloned().collect();
                    client
                        .execute(&sql, &values)
                        .with_context(|| format!("seeding {}", seeded.table))?;
                    inserted += 1;
                }
            }
        }
        Ok(inserted)
    }
}
