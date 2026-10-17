use arrow::array::{Array, Int64Array, ListArray, StringArray, StructArray};
use arrow::record_batch::RecordBatch;
use compiler::constants::{neighbor_id_column, neighbor_type_column, path_column};
use compiler::{QueryType, ResultContext};
use pipeline::{PipelineError, PipelineObserver, PipelineStage, QueryPipelineContext};
use types::QueryResult;

use crate::types::{ExecutionOutput, ExtractionOutput};

#[derive(Clone)]
pub struct ExtractionStage;

impl PipelineStage for ExtractionStage {
    type Input = ExecutionOutput;
    type Output = ExtractionOutput;

    async fn execute(
        &self,
        ctx: &mut QueryPipelineContext,
        obs: &mut dyn PipelineObserver,
    ) -> Result<Self::Output, PipelineError> {
        let input = ctx
            .phases
            .get::<ExecutionOutput>()
            .ok_or_else(|| PipelineError::Execution("ExecutionOutput not found in phases".into()))
            .inspect_err(|e| obs.record_error(e))?;
        validate_dynamic_nodes(&input.batches, &input.result_context)
            .inspect_err(|e| obs.record_error(e))?;
        Ok(ExtractionOutput {
            query_result: QueryResult::from_batches(&input.batches, &input.result_context),
        })
    }
}

fn validate_dynamic_nodes(
    batches: &[RecordBatch],
    context: &ResultContext,
) -> Result<(), PipelineError> {
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let valid = match context.query_type {
            Some(QueryType::PathFinding) => valid_path(batch, path_column()),
            Some(QueryType::Neighbors) => valid_neighbors(batch),
            _ => context
                .edges()
                .iter()
                .filter_map(|edge| edge.path_column.as_deref())
                .all(|column| valid_path(batch, column)),
        };
        if !valid {
            return Err(PipelineError::Authorization(
                "query result has invalid dynamic authorization metadata".into(),
            ));
        }
    }
    Ok(())
}

fn valid_neighbors(batch: &RecordBatch) -> bool {
    let schema = batch.schema();
    let (Ok(id), Ok(entity_type)) = (
        schema.index_of(neighbor_id_column()),
        schema.index_of(neighbor_type_column()),
    ) else {
        return false;
    };
    batch
        .column(id)
        .as_any()
        .downcast_ref::<Int64Array>()
        .is_some_and(|ids| ids.null_count() == 0)
        && batch
            .column(entity_type)
            .as_any()
            .downcast_ref::<StringArray>()
            .is_some_and(|types| types.null_count() == 0)
}

fn valid_path(batch: &RecordBatch, column: &str) -> bool {
    let Ok(index) = batch.schema().index_of(column) else {
        return false;
    };
    let Some(paths) = batch.column(index).as_any().downcast_ref::<ListArray>() else {
        return false;
    };
    let Some(nodes) = paths.values().as_any().downcast_ref::<StructArray>() else {
        return false;
    };
    paths.null_count() == 0
        && nodes.null_count() == 0
        && nodes.num_columns() >= 2
        && nodes
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .is_some_and(|ids| ids.null_count() == 0)
        && nodes
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .is_some_and(|types| types.null_count() == 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use compiler::ResultContext;
    use ontology::Ontology;
    use pipeline::NoOpObserver;
    use std::sync::Arc;

    #[tokio::test]
    async fn wires_batches_and_context_into_query_result() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_orbit_p_id", DataType::Int64, false),
            Field::new("_orbit_p_type", DataType::Utf8, false),
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
            frontend: compiler::Frontend::JsonDsl,
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
