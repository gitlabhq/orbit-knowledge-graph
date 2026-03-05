use std::time::Instant;

use tracing::info;

use crate::redaction::{RedactionMessage, RedactionService};

use super::super::error::PipelineError;
use super::super::metrics::PipelineObserver;
use super::super::types::{
    AuthorizationOutput, ExtractionOutput, PipelineRequest, QueryPipelineContext,
};
use super::PipelineStage;

#[derive(Clone)]
pub struct AuthorizationStage;

impl<M: RedactionMessage> PipelineStage<M> for AuthorizationStage {
    type Input = ExtractionOutput;
    type Output = AuthorizationOutput;

    async fn execute(
        &self,
        input: Self::Input,
        _ctx: &mut QueryPipelineContext,
        req: &mut PipelineRequest<'_, M>,
        obs: &mut PipelineObserver,
    ) -> Result<Self::Output, PipelineError> {
        let t = Instant::now();

        let resources_to_check = input.query_result.resource_checks();
        let authorizations = if resources_to_check.is_empty() {
            info!("No redaction required, returning result directly");
            Vec::new()
        } else {
            let tx = req
                .tx
                .ok_or_else(|| PipelineError::Streaming("tx not available".into()))?;
            let stream = req
                .stream
                .as_deref_mut()
                .ok_or_else(|| PipelineError::Streaming("stream not available".into()))?;
            RedactionService::request_authorization(&resources_to_check, tx, stream)
                .await
                .map_err(PipelineError::from)?
                .authorizations
        };

        obs.authorized(t.elapsed());

        Ok(AuthorizationOutput {
            query_result: input.query_result,
            authorizations,
        })
    }
}
