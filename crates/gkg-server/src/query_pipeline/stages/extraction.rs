use crate::redaction::{QueryResult, RedactionMessage};

use super::super::error::PipelineError;
use super::super::metrics::PipelineObserver;
use super::super::types::{
    ExecutionOutput, ExtractionOutput, PipelineRequest, QueryPipelineContext,
};
use super::PipelineStage;

#[derive(Clone)]
pub struct ExtractionStage;

impl ExtractionStage {
    fn process(input: ExecutionOutput) -> ExtractionOutput {
        ExtractionOutput {
            query_result: QueryResult::from_batches(&input.batches, &input.result_context),
        }
    }
}

impl<M: RedactionMessage> PipelineStage<M> for ExtractionStage {
    type Input = ExecutionOutput;
    type Output = ExtractionOutput;

    async fn execute(
        &self,
        input: Self::Input,
        _ctx: &mut QueryPipelineContext,
        _req: &mut PipelineRequest<'_, M>,
        _obs: &mut PipelineObserver,
    ) -> Result<Self::Output, PipelineError> {
        Ok(Self::process(input))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use query_engine::ResultContext;
    use std::sync::Arc;

    #[test]
    fn wires_batches_and_context_into_query_result() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_gkg_p_id", DataType::Int64, false),
            Field::new("_gkg_p_type", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["Project", "Project"])),
            ],
        )
        .unwrap();

        let mut ctx_result = ResultContext::new();
        ctx_result.add_node("p", "Project");

        let output = ExtractionStage::process(ExecutionOutput {
            batches: vec![batch],
            result_context: ctx_result,
        });

        assert_eq!(output.query_result.len(), 2);
        assert!(output.query_result.ctx().get("p").is_some());
    }
}
