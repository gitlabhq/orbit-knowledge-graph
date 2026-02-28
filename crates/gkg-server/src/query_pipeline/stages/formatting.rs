use std::sync::Arc;

use ontology::Ontology;

use super::super::formatter::ResultFormatter;
use super::super::metrics::PipelineObserver;
use super::super::types::{CompilationOutput, HydrationOutput, PipelineOutput};

pub struct FormattingStage<F: ResultFormatter> {
    formatter: F,
    ontology: Arc<Ontology>,
}

impl<F: ResultFormatter> FormattingStage<F> {
    pub fn new(formatter: F, ontology: Arc<Ontology>) -> Self {
        Self {
            formatter,
            ontology,
        }
    }

    pub fn execute(
        &self,
        input: HydrationOutput,
        compiled: &CompilationOutput,
        _obs: &PipelineObserver,
    ) -> PipelineOutput {
        let row_count = input.query_result.authorized_count();
        let generated_sql = &compiled.compiled_query.base.sql;
        let formatted =
            self.formatter
                .format(&input.query_result, &input.result_context, &self.ontology);

        PipelineOutput {
            formatted_result: formatted,
            generated_sql: Some(generated_sql.clone()),
            row_count,
            redacted_count: input.redacted_count,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use query_engine::{CompiledQuery, HydrationPlan, ParameterizedQuery, ResultContext};
    use serde_json::{Value, json};
    use std::collections::HashMap;

    use crate::redaction::QueryResult;

    struct ConstFormatter(Value);

    impl ResultFormatter for ConstFormatter {
        fn format(&self, _: &QueryResult, _: &ResultContext, _: &Ontology) -> Value {
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

        let mut ctx = ResultContext::new();
        ctx.add_node("p", "Project");

        let mut qr = QueryResult::from_batches(&[batch], &ctx);
        qr.rows_mut()[0].set_unauthorized();

        let input = HydrationOutput {
            result_context: qr.ctx().clone(),
            query_result: qr,
            redacted_count: 1,
        };
        let compiled = CompilationOutput {
            compiled_query: CompiledQuery {
                base: ParameterizedQuery {
                    sql: "SELECT 1".to_string(),
                    params: HashMap::new(),
                    result_context: ResultContext::new(),
                },
                hydration: HydrationPlan::None,
            },
        };

        let stage = FormattingStage::new(ConstFormatter(json!(["ok"])), Arc::new(Ontology::new()));
        let output = stage.execute(input, &compiled, &PipelineObserver::start());

        assert_eq!(output.formatted_result, json!(["ok"]));
        assert_eq!(output.generated_sql.as_deref(), Some("SELECT 1"));
        assert_eq!(output.row_count, 2); // 3 total - 1 redacted
        assert_eq!(output.redacted_count, 1);
    }
}
