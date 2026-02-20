use crate::redaction::QueryResult;

use super::super::types::{ExecutionOutput, ExtractionOutput};

pub struct ExtractionStage;

impl ExtractionStage {
    pub fn execute(input: ExecutionOutput) -> ExtractionOutput {
        ExtractionOutput {
            query_result: QueryResult::from_batches(&input.batches, &input.result_context),
        }
    }
}
