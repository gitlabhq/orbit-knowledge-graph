use querying_pipeline::{PipelineError, PipelineObserver, PipelineStage, QueryPipelineContext};
use querying_types::QueryResult;

use crate::types::{ExecutionOutput, ExtractionOutput};

#[derive(Clone)]
pub struct ExtractionStage;

impl PipelineStage for ExtractionStage {
    type Input = ExecutionOutput;
    type Output = ExtractionOutput;

    async fn execute(
        &self,
        ctx: &mut QueryPipelineContext,
        _obs: &mut dyn PipelineObserver,
    ) -> Result<Self::Output, PipelineError> {
        let input = ctx.phases.get::<ExecutionOutput>().ok_or_else(|| {
            PipelineError::Execution("ExecutionOutput not found in phases".into())
        })?;
        Ok(ExtractionOutput {
            query_result: QueryResult::from_batches(&input.batches, &input.result_context),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use ontology::Ontology;
    use query_engine::ResultContext;
    use querying_pipeline::NoOpObserver;
    use std::sync::Arc;

    #[tokio::test]
    async fn wires_batches_and_context_into_query_result() {
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

        let mut ctx = QueryPipelineContext {
            query_json: String::new(),
            compiled: None,
            ontology: Arc::new(Ontology::new()),
            security_context: None,
            server_extensions: Default::default(),
            phases: Default::default(),
        };
        ctx.phases.insert(ExecutionOutput {
            batches: vec![batch],
            result_context: ctx_result,
        });
        let mut obs = NoOpObserver;

        let output = ExtractionStage.execute(&mut ctx, &mut obs).await.unwrap();
        assert_eq!(output.query_result.len(), 2);
        assert!(output.query_result.ctx().get("p").is_some());
    }
}
