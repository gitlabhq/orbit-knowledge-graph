use querying_pipeline::{PipelineError, PipelineObserver, PipelineStage, QueryPipelineContext};

use crate::types::{HydrationOutput, PipelineOutput};

use crate::formatters::ResultFormatter;

#[derive(Clone)]
pub struct FormattingStage<F: ResultFormatter> {
    formatter: F,
}

impl<F: ResultFormatter> FormattingStage<F> {
    pub fn new(formatter: F) -> Self {
        Self { formatter }
    }
}

impl<F: ResultFormatter + Clone + Send + Sync> PipelineStage for FormattingStage<F> {
    type Input = HydrationOutput;
    type Output = PipelineOutput;

    async fn execute(
        &self,
        input: Self::Input,
        ctx: &mut QueryPipelineContext,
        _obs: &mut dyn PipelineObserver,
    ) -> Result<Self::Output, PipelineError> {
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
