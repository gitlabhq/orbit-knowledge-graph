use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use indexer::handler::HandlerError;
use indexer::modules::sdlc::{
    BlockTransform, SOURCE_DATA_TABLE, SqlTransformFactory, TableBatch, Transformation,
};
use orbit_arrow::prepare_batches;

pub fn sql_transform() -> SqlTransformFactory {
    Arc::new(|name, transforms| Arc::new(DataFusionTransform::new(name, transforms)))
}

/// Row-wise SQL projection over the extracted block, one destination table per [`Transformation`].
struct DataFusionTransform {
    name: String,
    session: SessionContext,
    transforms: Vec<Transformation>,
    outputs: Vec<String>,
}

impl DataFusionTransform {
    fn new(name: String, transforms: Vec<Transformation>) -> Self {
        let outputs = transforms
            .iter()
            .map(|t| t.destination_table.clone())
            .collect();
        Self {
            name,
            session: SessionContext::new(),
            transforms,
            outputs,
        }
    }

    fn load_block(&self, block: &RecordBatch) -> Result<(), HandlerError> {
        let _ = self.session.deregister_table(SOURCE_DATA_TABLE);
        let mem_table =
            MemTable::try_new(block.schema(), vec![vec![block.clone()]]).map_err(|err| {
                HandlerError::Processing(format!(
                    "failed to create mem table for {}: {err}",
                    self.name
                ))
            })?;
        self.session
            .register_table(SOURCE_DATA_TABLE, Arc::new(mem_table))
            .map_err(|err| {
                HandlerError::Processing(format!(
                    "failed to register table for {}: {err}",
                    self.name
                ))
            })?;
        Ok(())
    }

    async fn project(&self, transform: &Transformation) -> Result<Vec<RecordBatch>, HandlerError> {
        let dataframe = self.session.sql(&transform.sql).await.map_err(|err| {
            HandlerError::Processing(format!(
                "failed to plan transform {} for {}: {err}",
                self.name, transform.destination_table
            ))
        })?;
        let mut batches = dataframe.collect().await.map_err(|err| {
            HandlerError::Processing(format!(
                "failed to transform {} for {}: {err}",
                self.name, transform.destination_table
            ))
        })?;
        prepare_batches(&mut batches, &transform.dict_encode_columns);
        Ok(batches)
    }
}

#[async_trait]
impl BlockTransform for DataFusionTransform {
    fn name(&self) -> &str {
        &self.name
    }

    fn outputs(&self) -> &[String] {
        &self.outputs
    }

    async fn transform(&self, block: &RecordBatch) -> Result<Vec<TableBatch>, HandlerError> {
        self.load_block(block)?;

        let mut outputs = Vec::new();
        for (output_index, transform) in self.transforms.iter().enumerate() {
            for batch in self.project(transform).await? {
                if batch.num_rows() > 0 {
                    outputs.push(TableBatch {
                        output_index,
                        batch,
                    });
                }
            }
        }

        let _ = self.session.deregister_table(SOURCE_DATA_TABLE);
        Ok(outputs)
    }
}
