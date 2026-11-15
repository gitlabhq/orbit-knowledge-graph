//! The lakes the deployment reads: each DuckDB connection's tables as Parquet files on the demo's
//! RustFS, landed from the seed the way the Data Extraction Service lands them, and read through
//! the same S3 connection the server opens.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use anyhow::{Context, bail};
use compiler::dag::{Connections, Peer};
use duckdb_client::dag::DuckDbExecutor;
use duckdb_client::{DuckDbClient, sql_lit};
use orbit_server_config::{
    DuckDbConnection, DuckDbLocation, S3Credentials, S3Endpoint, S3UrlStyle,
};
use query_data_model::{Catalog, ConnectionId, DuckDb};
use serde_json::Value;

use crate::config::Lake as Settings;
use crate::seed::TableRows;

struct Lake {
    name: String,
    connection: ConnectionId,
    catalog: Arc<Catalog<DuckDb>>,
    settings: DuckDbConnection,
}

pub struct Lakes {
    lakes: Vec<Lake>,
    readers: Mutex<Vec<(ConnectionId, Arc<Mutex<DuckDbClient>>)>>,
    settings: Settings,
}

impl Lakes {
    pub fn new(connections: &Connections, settings: &Settings) -> Self {
        let lakes = connections
            .peers()
            .iter()
            .filter_map(|peer| match peer {
                Peer::DuckDb(catalog) => Some(catalog),
                _ => None,
            })
            .map(|catalog| {
                let name = catalog
                    .model()
                    .connection(catalog.connection())
                    .name()
                    .to_string();
                let settings = DuckDbConnection {
                    extension_directory: settings.extension_directory.clone(),
                    s3: S3Endpoint {
                        endpoint: settings.endpoint.clone(),
                        region: settings.region.clone(),
                        url_style: S3UrlStyle::Path,
                        use_ssl: false,
                    },
                    credentials: S3Credentials {
                        access_key_id: settings.access_key.clone(),
                        secret_access_key: settings.secret_key.clone(),
                        session_token: None,
                    },
                    location: DuckDbLocation::Parquet {
                        bucket: settings.bucket.clone(),
                        prefix: format!("lake/{name}/"),
                    },
                };
                Lake {
                    connection: catalog.connection(),
                    catalog: Arc::clone(catalog),
                    name,
                    settings,
                }
            })
            .collect();
        Self {
            lakes,
            readers: Mutex::new(Vec::new()),
            settings: settings.clone(),
        }
    }

    /// An executor per lake, reading its tables through the server's S3 connection.
    pub fn executors(&self) -> Vec<(ConnectionId, DuckDbExecutor)> {
        let readers = self
            .readers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        readers
            .iter()
            .map(|(connection, client)| {
                (
                    *connection,
                    DuckDbExecutor {
                        client: Arc::clone(client),
                    },
                )
            })
            .collect()
    }

    /// Lands the seeded rows of every lake table on S3 as one Parquet file, then opens each lake.
    pub fn reload(&self, tables: &[TableRows]) -> anyhow::Result<usize> {
        let mut seeded: BTreeMap<&str, &TableRows> = BTreeMap::new();
        for table in tables {
            seeded.insert(table.table.as_str(), table);
        }
        let known: Vec<String> = self
            .lakes
            .iter()
            .flat_map(|lake| lake.catalog.tables().map(|table| table.name.clone()))
            .collect();
        if let Some(unknown) = seeded
            .keys()
            .find(|name| !known.iter().any(|known| known == *name))
        {
            bail!("the lake seed names table {unknown}, which no lake declares");
        }
        let writer = self.writer()?;
        let mut rows = 0;
        for lake in &self.lakes {
            for table in lake.catalog.tables() {
                rows += land(
                    &writer,
                    lake,
                    table,
                    seeded.get(table.name.as_str()).copied(),
                    &self.settings.bucket,
                )
                .with_context(|| format!("landing {} on {}", table.name, lake.name))?;
            }
        }
        let mut readers = Vec::new();
        for lake in &self.lakes {
            let client = DuckDbClient::open_s3(&lake.name, &lake.settings, &lake.catalog)?;
            readers.push((lake.connection, Arc::new(Mutex::new(client))));
        }
        *self
            .readers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = readers;
        Ok(rows)
    }

    /// The connection that writes the seed to S3; only the seed step holds its secret.
    fn writer(&self) -> anyhow::Result<duckdb::Connection> {
        let settings = &self.settings;
        let writer = duckdb::Connection::open_in_memory()?;
        writer.execute_batch(&format!(
            "SET extension_directories = [{}]; LOAD httpfs; \
             CREATE SECRET demo_seed (TYPE s3, PROVIDER config, KEY_ID {}, SECRET {}, REGION {}, \
             ENDPOINT {}, URL_STYLE 'path', USE_SSL false);",
            sql_lit(&settings.extension_directory.display().to_string()),
            sql_lit(&settings.access_key),
            sql_lit(&settings.secret_key),
            sql_lit(&settings.region),
            sql_lit(&settings.endpoint),
        ))?;
        Ok(writer)
    }
}

fn land(
    writer: &duckdb::Connection,
    lake: &Lake,
    table: &query_data_model::implementations::relational::Table,
    seeded: Option<&TableRows>,
    bucket: &str,
) -> anyhow::Result<usize> {
    let entity = table.entity.context("a lake table holds an entity")?;
    let sources = lake.catalog.model().sources();
    let pipeline = sources
        .entity_pipelines(entity)
        .next()
        .context("the lake table has no writer")?;
    let lifecycle = sources.data_source(pipeline);
    let columns: Vec<String> = table
        .columns
        .iter()
        .map(|column| format!("\"{}\" {}", column.name, column.storage_type))
        .chain([
            format!("\"{}\" TIMESTAMP DEFAULT now()", lifecycle.version),
            format!("\"{}\" TIMESTAMP DEFAULT now()", lifecycle.watermark),
            format!("\"{}\" BOOLEAN DEFAULT false", lifecycle.deleted),
        ])
        .collect();
    let staged = format!("\"{}\"", table.name);
    writer.execute_batch(&format!(
        "CREATE OR REPLACE TABLE {staged} ({})",
        columns.join(", ")
    ))?;
    let rows = seeded.map_or(&[][..], |seeded| seeded.rows.as_slice());
    for row in rows {
        let names: Vec<String> = row.keys().map(|name| format!("\"{name}\"")).collect();
        let values: Vec<String> = row.values().map(literal).collect();
        writer.execute_batch(&format!(
            "INSERT INTO {staged} ({}) VALUES ({})",
            names.join(", "),
            values.join(", ")
        ))?;
    }
    let key = format!(
        "s3://{bucket}/lake/{}/{}/part-0.parquet",
        lake.name, table.name
    );
    writer.execute_batch(&format!(
        "COPY {staged} TO {} (FORMAT parquet)",
        sql_lit(&key)
    ))?;
    Ok(rows.len())
}

fn literal(value: &Value) -> String {
    match value {
        Value::Null => "NULL".into(),
        Value::Bool(flag) => flag.to_string(),
        Value::Number(number) => number.to_string(),
        Value::String(text) => sql_lit(text),
        other => sql_lit(&other.to_string()),
    }
}
