use std::sync::{Arc, Mutex, PoisonError};

use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use duckdb_client::{DuckDbClient, OpenMode};
use indexer::handler::HandlerError;
use indexer::modules::sdlc::{
    BlockTransform, SOURCE_DATA_TABLE, SqlTransformFactory, TableBatch, Transformation,
};
use orbit_arrow::prepare_batch;

/// Every transform shares one in-memory database, so the process runs one DuckDB thread pool.
pub fn sql_transform() -> duckdb_client::Result<SqlTransformFactory> {
    let database = Arc::new(Mutex::new(DuckDbClient::open(OpenMode::InMemoryArrow)?));
    Ok(Arc::new(move |name, transforms| {
        let outputs = transforms
            .iter()
            .map(|t| t.destination_table.clone())
            .collect();
        Arc::new(DuckDbTransform {
            name,
            outputs,
            transforms: transforms.into(),
            database: database.clone(),
        })
    }))
}

/// Row-wise SQL projection over the extracted block, one destination table per [`Transformation`].
struct DuckDbTransform {
    name: String,
    outputs: Vec<String>,
    transforms: Arc<[Transformation]>,
    database: Arc<Mutex<DuckDbClient>>,
}

fn project_block(
    connection: &DuckDbClient,
    transforms: &[Transformation],
    block: RecordBatch,
) -> Result<Vec<TableBatch>, String> {
    connection
        .create_temp_table(SOURCE_DATA_TABLE, block)
        .map_err(|err| err.to_string())?;
    let mut outputs = Vec::new();
    for (output_index, transform) in transforms.iter().enumerate() {
        let batch = project(connection, transform)
            .map_err(|err| format!("{}: {err}", transform.destination_table))?;
        if let Some(batch) = batch {
            outputs.push(TableBatch {
                output_index,
                batch,
            });
        }
    }
    Ok(outputs)
}

fn project(
    connection: &DuckDbClient,
    transform: &Transformation,
) -> duckdb_client::Result<Option<RecordBatch>> {
    let batches = connection.query_arrow(&transform.sql)?;
    let Some(first) = batches.first() else {
        return Ok(None);
    };
    let batch = concat_batches(&first.schema(), &batches)?;
    Ok(Some(prepare_batch(&batch, &transform.dict_encode_columns)))
}

#[async_trait]
impl BlockTransform for DuckDbTransform {
    fn name(&self) -> &str {
        &self.name
    }

    fn outputs(&self) -> &[String] {
        &self.outputs
    }

    async fn transform(&self, block: &RecordBatch) -> Result<Vec<TableBatch>, HandlerError> {
        let connection = {
            let database = self.database.lock().unwrap_or_else(PoisonError::into_inner);
            DuckDbClient::open(OpenMode::Connection(&database)).map_err(|err| err.to_string())
        };
        let transforms = self.transforms.clone();
        let block = block.clone();
        tokio::task::spawn_blocking(move || project_block(&connection?, &transforms, block))
            .await
            .map_err(|err| err.to_string())
            .and_then(|projected| projected)
            .map_err(|err| {
                HandlerError::Processing(format!("failed to transform {}: {err}", self.name))
            })
    }
}
