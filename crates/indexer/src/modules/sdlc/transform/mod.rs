pub(in crate::modules::sdlc) mod system_notes;

use std::collections::HashMap;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;

use crate::handler::HandlerError;

use super::plan::{Plan, TransformSpec, Transformation};

pub struct TableBatch {
    pub output_index: usize,
    pub batch: RecordBatch,
}

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

    pub(in crate::modules::sdlc) fn register(
        &mut self,
        name: impl Into<String>,
        factory: TransformFactory,
    ) {
        self.factories.insert(name.into(), factory);
    }

    pub(in crate::modules::sdlc) fn is_registered(&self, transform: &TransformSpec) -> bool {
        match transform {
            TransformSpec::DataFusion(_) => true,
            TransformSpec::Rust(name) => self.factories.contains_key(name),
        }
    }

    pub(in crate::modules::sdlc) fn build(
        &self,
        plan: &Plan,
    ) -> Result<Arc<dyn BlockTransform>, HandlerError> {
        match &plan.transform {
            TransformSpec::DataFusion(transforms) => {
                Ok((self.sql)(plan.name.clone(), transforms.clone()))
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
