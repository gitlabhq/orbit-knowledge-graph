//! The pluggable transform stage. The SQL projection for `data_fusion` plans comes
//! from an injected [`SqlTransformFactory`]. Other transforms are registered by
//! name and resolved from the ontology's `etl.transform` field (e.g. `system_notes`).

pub(in crate::modules::sdlc) mod system_notes;

use std::collections::HashMap;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;

use crate::handler::HandlerError;

use super::plan::{Plan, TransformSpec, Transformation};

/// A transformed batch tagged with the [`BlockTransform::outputs`] entry it
/// writes to.
pub struct TableBatch {
    pub output_index: usize,
    pub batch: RecordBatch,
}

/// Turns one extracted block into rows for one or more destination tables. The
/// pipeline drives this per block; everything around it (paging, checkpoint,
/// retry, streaming writes) is transform-agnostic. A transform receives its
/// dependencies (datalake handle, config) at construction via the registry
/// factory; the namespace scope travels in the block's `traversal_path` column.
#[async_trait]
pub trait BlockTransform: Send + Sync {
    fn name(&self) -> &str;
    fn outputs(&self) -> &[String];
    async fn transform(&self, block: &RecordBatch) -> Result<Vec<TableBatch>, HandlerError>;
}

pub(in crate::modules::sdlc) type TransformFactory =
    Box<dyn Fn(&Plan) -> Arc<dyn BlockTransform> + Send + Sync>;

pub type SqlTransformFactory =
    Arc<dyn Fn(String, Vec<Transformation>) -> Arc<dyn BlockTransform> + Send + Sync>;

/// Holds the SQL projection factory and the custom transforms, keyed by name.
pub(in crate::modules::sdlc) struct TransformRegistry {
    sql: SqlTransformFactory,
    factories: HashMap<String, TransformFactory>,
}

impl TransformRegistry {
    pub(in crate::modules::sdlc) fn new(sql: SqlTransformFactory) -> Self {
        Self {
            sql,
            factories: HashMap::new(),
        }
    }

    /// Called from each transform module's `register()` during handler setup,
    /// before the registry is frozen behind an `Arc`.
    pub(in crate::modules::sdlc) fn register(
        &mut self,
        name: impl Into<String>,
        factory: TransformFactory,
    ) {
        self.factories.insert(name.into(), factory);
    }

    /// A `DataFusion` plan always builds; a `Rust` plan only when its transform
    /// has been registered (otherwise the handler is skipped).
    pub(in crate::modules::sdlc) fn is_registered(&self, transform: &TransformSpec) -> bool {
        match transform {
            TransformSpec::DataFusion(_) => true,
            TransformSpec::Rust(name) => self.factories.contains_key(name),
        }
    }

    pub(in crate::modules::sdlc) fn sql(
        &self,
        name: String,
        transforms: Vec<Transformation>,
    ) -> Arc<dyn BlockTransform> {
        (self.sql)(name, transforms)
    }

    pub(in crate::modules::sdlc) fn build(
        &self,
        plan: &Plan,
    ) -> Result<Arc<dyn BlockTransform>, HandlerError> {
        match &plan.transform {
            TransformSpec::DataFusion(transforms) => {
                Ok(self.sql(plan.name.clone(), transforms.clone()))
            }
            TransformSpec::Rust(name) => self
                .factories
                .get(name)
                .map(|factory| factory(plan))
                .ok_or_else(|| {
                    HandlerError::Processing(format!("no transform registered for '{name}'"))
                }),
        }
    }
}
