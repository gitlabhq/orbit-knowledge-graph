//! The pluggable transform stage. `data_fusion` is the built-in SQL projection
//! (the default). Other transforms are registered by name and resolved from the
//! ontology's `etl.transform` field (e.g. `system_notes`).

use std::collections::HashMap;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use gkg_utils::arrow::prepare_batches;

use ontology::DEFAULT_TRANSFORM;

use crate::handler::HandlerError;

use super::plan::{Plan, SOURCE_DATA_TABLE, Transformation};

/// A transformed batch tagged with the [`BlockTransform::outputs`] entry it
/// writes to.
pub(in crate::modules::sdlc) struct TableBatch {
    pub output_index: usize,
    pub batch: RecordBatch,
}

/// Turns one extracted block into rows for one or more destination tables. The
/// pipeline drives this per block; everything around it (paging, checkpoint,
/// retry, streaming writes) is transform-agnostic. A transform receives its
/// dependencies (datalake handle, config) at construction via the registry
/// factory; the namespace scope travels in the block's `traversal_path` column.
#[async_trait]
pub(in crate::modules::sdlc) trait BlockTransform: Send + Sync {
    fn name(&self) -> &str;
    fn outputs(&self) -> &[String];
    async fn transform(&self, block: &RecordBatch) -> Result<Vec<TableBatch>, HandlerError>;
}

/// The built-in `data_fusion` transform: row-wise SQL projection over the
/// extracted block, driving one destination table per [`Transformation`].
pub(in crate::modules::sdlc) struct DataFusionTransform {
    name: String,
    session: SessionContext,
    transforms: Vec<Transformation>,
    outputs: Vec<String>,
}

impl DataFusionTransform {
    pub(in crate::modules::sdlc) fn new(name: String, transforms: Vec<Transformation>) -> Self {
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

type TransformFactory = Box<dyn Fn(&Plan) -> Arc<dyn BlockTransform> + Send + Sync>;

/// Maps a transform name to a factory. `data_fusion` is built in; custom
/// transforms self-register here so a plan referencing one resolves instead of
/// being skipped.
pub(in crate::modules::sdlc) struct TransformRegistry {
    factories: HashMap<String, TransformFactory>,
}

impl Default for TransformRegistry {
    fn default() -> Self {
        let mut registry = Self {
            factories: HashMap::new(),
        };
        registry.register(DEFAULT_TRANSFORM, |plan| {
            Arc::new(DataFusionTransform::new(
                plan.name.clone(),
                plan.transforms.clone(),
            ))
        });
        registry
    }
}

impl TransformRegistry {
    pub(in crate::modules::sdlc) fn register(
        &mut self,
        name: impl Into<String>,
        factory: impl Fn(&Plan) -> Arc<dyn BlockTransform> + Send + Sync + 'static,
    ) {
        self.factories.insert(name.into(), Box::new(factory));
    }

    pub(in crate::modules::sdlc) fn contains(&self, name: &str) -> bool {
        self.factories.contains_key(name)
    }

    pub(in crate::modules::sdlc) fn build(
        &self,
        name: &str,
        plan: &Plan,
    ) -> Result<Arc<dyn BlockTransform>, HandlerError> {
        self.factories
            .get(name)
            .map(|factory| factory(plan))
            .ok_or_else(|| {
                HandlerError::Processing(format!("no transform registered for '{name}'"))
            })
    }
}
