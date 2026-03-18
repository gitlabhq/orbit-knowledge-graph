use querying_types::QueryResult;

use crate::types::{ExecutionOutput, ExtractionOutput};

pub struct ExtractionStage;

impl ExtractionStage {
    pub fn execute(&self, input: ExecutionOutput) -> ExtractionOutput {
        ExtractionOutput {
            query_result: QueryResult::from_batches(&input.batches, &input.result_context),
        }
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

        let stage = ExtractionStage;
        let output = stage.execute(ExecutionOutput {
            batches: vec![batch],
            result_context: ctx_result,
        });

        assert_eq!(output.query_result.len(), 2);
        assert!(output.query_result.ctx().get("p").is_some());
    }
}
