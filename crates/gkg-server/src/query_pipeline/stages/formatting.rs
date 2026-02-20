use std::sync::Arc;

use ontology::Ontology;

use super::super::formatter::ResultFormatter;
use super::super::types::PipelineOutput;
use crate::redaction::QueryResult;
use query_engine::ResultContext;

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
        query_result: QueryResult,
        result_context: ResultContext,
        redacted_count: usize,
        generated_sql: String,
    ) -> PipelineOutput {
        let row_count = query_result.authorized_count();
        let formatted = self
            .formatter
            .format(&query_result, &result_context, &self.ontology);

        PipelineOutput {
            formatted_result: formatted,
            generated_sql: Some(generated_sql),
            row_count,
            redacted_count,
        }
    }
}
