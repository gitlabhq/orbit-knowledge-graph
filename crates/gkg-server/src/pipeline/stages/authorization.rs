use std::time::Instant;

use tokio::sync::mpsc;
use tonic::{Status, Streaming};
use tracing::info;

use crate::proto::ExecuteQueryMessage;
use crate::redaction::RedactionService;

use crate::pipeline::types::AuthorizationOutput;
use querying_pipeline::{PipelineError, PipelineObserver, PipelineStage, QueryPipelineContext};
use querying_shared_stages::ExtractionOutput;

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
        let input = ctx.phases.get::<ExtractionOutput>().ok_or_else(|| {
            PipelineError::Authorization("ExtractionOutput not found in phases".into())
        })?;
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
                })?
                .clone();
            let stream = ctx
                .server_extensions
                .get_mut::<Streaming<ExecuteQueryMessage>>()
                .ok_or_else(|| {
                    PipelineError::Authorization("stream not available in server_extensions".into())
                })?;
            RedactionService::request_authorization(&resources_to_check, &tx, stream)
                .await
                .map_err(|e| PipelineError::Authorization(format!("{e:?}")))?
                .authorizations
        };

        obs.authorized(t.elapsed());

        Ok(AuthorizationOutput {
            query_result: input.query_result.clone(),
            authorizations,
        })
    }
}
