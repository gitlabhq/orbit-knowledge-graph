use crate::redaction::QueryResult;

use super::super::metrics::PipelineObserver;
use super::super::types::{ExecutionOutput, ExtractionOutput};

pub struct ExtractionStage;

impl ExtractionStage {
    pub fn execute(input: ExecutionOutput, _obs: &PipelineObserver) -> ExtractionOutput {
        ExtractionOutput {
            query_result: QueryResult::from_batches(&input.batches, &input.result_context),
        }
    }
}
