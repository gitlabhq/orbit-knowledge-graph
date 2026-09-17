use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use duckdb_client::DuckDbClient;

use super::config;

pub(crate) fn create_test_db() -> Result<DuckDbClient> {
    let client =
        DuckDbClient::open(Path::new(":memory:")).context("failed to open in-memory DuckDB")?;
    client
        .initialize_schema(config::local_ddl())
        .context("failed to initialize local DDL")?;
    Ok(client)
}

pub(crate) fn on_batch_for(client: &Arc<Mutex<DuckDbClient>>) -> Arc<code_graph::v2::OnBatch> {
    let client = Arc::clone(client);
    Arc::new(
        move |table: &str, batch: arrow::record_batch::RecordBatch| {
            if batch.num_rows() == 0 {
                return Ok(());
            }
            client
                .lock()
                .unwrap()
                .insert_batch(table, &batch)
                .map_err(|e| code_graph::v2::SinkError(format!("DuckDB write to {table}: {e}")))
        },
    )
}
