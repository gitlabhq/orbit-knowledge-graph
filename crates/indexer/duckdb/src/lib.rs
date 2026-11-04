use std::sync::{Arc, Mutex, PoisonError};

use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use duckdb_client::{DuckDbClient, OpenMode};
use indexer::handler::HandlerError;
use indexer::modules::sdlc::{
    BlockTransform, SOURCE_DATA_TABLE, SqlTransformFactory, TableBatch, Transformation,
};
use orbit_arrow::prepare_batches;

/// Every transform shares one in-memory database, so the process runs one DuckDB thread pool.
pub fn sql_transform() -> duckdb_client::Result<SqlTransformFactory> {
    let database = DuckDbClient::open(OpenMode::InMemory)?;
    database.register_arrow_scan()?;
    let database = Arc::new(Mutex::new(database));
    Ok(Arc::new(move |name, transforms| {
        Arc::new(DuckDbTransform::new(name, transforms, database.clone()))
    }))
}

/// Row-wise SQL projection over the extracted block, one destination table per [`Transformation`].
struct DuckDbTransform {
    name: String,
    outputs: Vec<String>,
    transforms: Arc<[Transformation]>,
    database: Arc<Mutex<DuckDbClient>>,
}

impl DuckDbTransform {
    fn new(
        name: String,
        transforms: Vec<Transformation>,
        database: Arc<Mutex<DuckDbClient>>,
    ) -> Self {
        let outputs = transforms
            .iter()
            .map(|t| t.destination_table.clone())
            .collect();
        Self {
            name,
            outputs,
            transforms: transforms.into(),
            database,
        }
    }

    fn connect(&self) -> Result<DuckDbClient, HandlerError> {
        self.database
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .try_clone()
            .map_err(|err| {
                HandlerError::Processing(format!(
                    "failed to connect {} to DuckDB: {err}",
                    self.name
                ))
            })
    }
}

fn project_block(
    name: &str,
    connection: &DuckDbClient,
    transforms: &[Transformation],
    block: RecordBatch,
) -> Result<Vec<TableBatch>, HandlerError> {
    connection
        .create_temp_table(SOURCE_DATA_TABLE, block)
        .map_err(|err| {
            HandlerError::Processing(format!("failed to load block for {name}: {err}"))
        })?;
    let mut outputs = Vec::new();
    for (output_index, transform) in transforms.iter().enumerate() {
        let failed = |err: &dyn std::fmt::Display| {
            HandlerError::Processing(format!(
                "failed to transform {name} for {}: {err}",
                transform.destination_table
            ))
        };
        let batches = connection
            .query_arrow(&transform.sql)
            .map_err(|err| failed(&err))?;
        let Some(first) = batches.first() else {
            continue;
        };
        let mut batch = concat_batches(&first.schema(), &batches).map_err(|err| failed(&err))?;
        prepare_batches(
            std::slice::from_mut(&mut batch),
            &transform.dict_encode_columns,
        );
        outputs.push(TableBatch {
            output_index,
            batch,
        });
    }
    Ok(outputs)
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
        let connection = self.connect()?;
        let name = self.name.clone();
        let transforms = self.transforms.clone();
        let block = block.clone();
        tokio::task::spawn_blocking(move || project_block(&name, &connection, &transforms, block))
            .await
            .map_err(|err| {
                HandlerError::Processing(format!("transform {} did not finish: {err}", self.name))
            })?
    }
}
