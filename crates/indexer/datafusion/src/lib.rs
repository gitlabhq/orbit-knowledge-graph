use std::io::Cursor;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::arrow as datafusion_arrow;
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
        let block = to_datafusion(block).map_err(|err| self.conversion_error(err))?;
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
        let batches = dataframe.collect().await.map_err(|err| {
            HandlerError::Processing(format!(
                "failed to transform {} for {}: {err}",
                self.name, transform.destination_table
            ))
        })?;
        let mut batches = batches
            .iter()
            .map(from_datafusion)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| self.conversion_error(err))?;
        prepare_batches(&mut batches, &transform.dict_encode_columns);
        Ok(batches)
    }

    fn conversion_error(&self, err: impl std::fmt::Display) -> HandlerError {
        HandlerError::Processing(format!(
            "failed to convert Arrow batches for {}: {err}",
            self.name
        ))
    }
}

type ConversionError = Box<dyn std::error::Error + Send + Sync>;

/// DataFusion pins an older Arrow major than the workspace. An IPC stream hands each batch across
/// the two versions without unsafe code.
fn to_datafusion(
    batch: &RecordBatch,
) -> Result<datafusion_arrow::record_batch::RecordBatch, ConversionError> {
    let mut bytes = Vec::new();
    let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut bytes, &batch.schema())?;
    writer.write(batch)?;
    writer.finish()?;
    drop(writer);
    let mut reader =
        datafusion_arrow::ipc::reader::StreamReader::try_new(Cursor::new(bytes), None)?;
    Ok(reader.next().ok_or("empty IPC stream")??)
}

fn from_datafusion(
    batch: &datafusion_arrow::record_batch::RecordBatch,
) -> Result<RecordBatch, ConversionError> {
    let mut bytes = Vec::new();
    let mut writer =
        datafusion_arrow::ipc::writer::StreamWriter::try_new(&mut bytes, &batch.schema())?;
    writer.write(batch)?;
    writer.finish()?;
    drop(writer);
    let mut reader = arrow::ipc::reader::StreamReader::try_new(Cursor::new(bytes), None)?;
    Ok(reader.next().ok_or("empty IPC stream")??)
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
