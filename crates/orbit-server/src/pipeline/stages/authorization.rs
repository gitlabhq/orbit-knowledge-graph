use std::time::Instant;

use orbit_log_fields::info;
use tokio::sync::mpsc;
use tonic::{Status, Streaming};

use crate::proto::ExecuteQueryMessage;
use crate::redaction::{RedactionExchangeError, RedactionService};

use query_engine::pipeline::{
    PipelineError, PipelineObserver, PipelineStage, QueryPipelineContext,
};
use query_engine::shared::{AuthorizationOutput, ExtractionOutput};

#[derive(Clone)]
pub struct AuthorizationStage;

impl PipelineStage for AuthorizationStage {
    type Input = ExtractionOutput;
    type Output = AuthorizationOutput;

    async fn execute(
        &self,
        ctx: &mut QueryPipelineContext,
        obs: &mut dyn PipelineObserver,
    ) -> Result<Self::Output, PipelineError> {
        let input = ctx
            .phases
            .remove::<ExtractionOutput>()
            .ok_or_else(|| {
                PipelineError::Authorization("ExtractionOutput not found in phases".into())
            })
            .inspect_err(|e| obs.record_error(e))?;
        let t = Instant::now();

        let resources_to_check = input.query_result.resource_checks();
        let authorizations = if resources_to_check.is_empty() {
            info!("No redaction required, returning result directly");
            Vec::new()
        } else {
            let tx = ctx
                .server_extensions
                .get::<mpsc::Sender<Result<ExecuteQueryMessage, Status>>>()
                .ok_or_else(|| {
                    PipelineError::Authorization("tx not available in server_extensions".into())
                })
                .inspect_err(|e| obs.record_error(e))?
                .clone();
            let stream = ctx
                .server_extensions
                .get_mut::<Streaming<ExecuteQueryMessage>>()
                .ok_or_else(|| {
                    PipelineError::Authorization("stream not available in server_extensions".into())
                })
                .inspect_err(|e| obs.record_error(e))?;
            RedactionService::request_authorization(&resources_to_check, &tx, stream)
                .await
                .map_err(RedactionExchangeError::into_pipeline_error)
                .inspect_err(|e| obs.record_error(e))?
                .authorizations
        };

        obs.authorized(t.elapsed());

        Ok(AuthorizationOutput {
            query_result: input.query_result,
            authorizations,
        })
    }
}
