use crate::error::PipelineError;
use crate::formatters::ResultFormatter;
use crate::types::{HydrationOutput, PipelineOutput, QueryPipelineContext};

#[derive(Clone)]
pub struct FormattingStage<F: ResultFormatter> {
    formatter: F,
}

impl<F: ResultFormatter> FormattingStage<F> {
    pub fn new(formatter: F) -> Self {
        Self { formatter }
    }

    pub fn execute(
        &self,
        input: HydrationOutput,
        ctx: &QueryPipelineContext,
    ) -> Result<PipelineOutput, PipelineError> {
        let row_count = input.query_result.authorized_count();
        let formatted = self
            .formatter
            .format(&input.query_result, &input.result_context, ctx);

        let query_type = input
            .result_context
            .query_type
            .map(|qt| <&str>::from(qt).to_string())
            .unwrap_or_default();

        Ok(PipelineOutput {
            formatted_result: formatted,
            query_type,
            raw_query_strings: vec![ctx.compiled()?.base.sql.clone()],
            row_count,
            redacted_count: input.redacted_count,
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
    use query_engine::{
        CompiledQueryContext, HydrationPlan, ParameterizedQuery, QueryType, ResultContext,
    };
    use querying_types::QueryResult;
    use serde_json::{Value, json};
    use std::collections::HashMap;
    use std::sync::Arc;

    #[derive(Clone)]
    struct ConstFormatter(Value);

    impl ResultFormatter for ConstFormatter {
        fn format(&self, _: &QueryResult, _: &ResultContext, _: &QueryPipelineContext) -> Value {
            self.0.clone()
        }
    }

    #[test]
    fn assembles_output_with_correct_counts() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_gkg_p_id", DataType::Int64, false),
            Field::new("_gkg_p_type", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["Project", "Project", "Project"])),
            ],
        )
        .unwrap();

        let mut result_ctx = ResultContext::new();
        result_ctx.add_node("p", "Project");

        let mut qr = QueryResult::from_batches(&[batch], &result_ctx);
        qr.rows_mut()[0].set_unauthorized();

        let input = HydrationOutput {
            result_context: qr.ctx().clone(),
            query_result: qr,
            redacted_count: 1,
        };
        let ctx = QueryPipelineContext {
            compiled: Some(Arc::new(CompiledQueryContext {
                query_type: QueryType::Search,
                base: ParameterizedQuery {
                    sql: "SELECT 1".to_string(),
                    params: HashMap::new(),
                    result_context: ResultContext::new(),
                },
                hydration: HydrationPlan::None,
                input: serde_json::from_value(serde_json::json!({
                    "query_type": "search",
                    "node": {"id": "p", "entity": "Project"},
                    "limit": 10
                }))
                .unwrap(),
            })),
            ontology: Arc::new(Ontology::new()),
            security_context: None,
        };

        let stage = FormattingStage::new(ConstFormatter(json!(["ok"])));
        let output = stage.execute(input, &ctx).unwrap();

        assert_eq!(output.formatted_result, json!(["ok"]));
        assert_eq!(output.row_count, 2);
        assert_eq!(output.redacted_count, 1);
        assert_eq!(output.raw_query_strings, vec!["SELECT 1"]);
        assert_eq!(output.query_type, "");
    }
}
