use std::sync::Arc;

use ontology::Ontology;

use super::super::formatter::ResultFormatter;
use super::super::types::{PipelineOutput, RedactionOutput};

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

    pub fn execute(&self, input: RedactionOutput) -> PipelineOutput {
        let row_count = input.query_result.authorized_count();
        let formatted =
            self.formatter
                .format(&input.query_result, &input.result_context, &self.ontology);

        PipelineOutput {
            formatted_result: formatted,
            generated_sql: Some(input.generated_sql),
            row_count,
            redacted_count: input.redacted_count,
        }
    }
}
