use std::time::Instant;

use tokio::sync::{Mutex, mpsc};
use tonic::{Status, Streaming};
use tracing::info;

use crate::proto::ExecuteQueryMessage;
use crate::redaction::RedactionService;

use crate::pipeline::types::AuthorizationOutput;
use querying_pipeline::{PipelineError, PipelineObserver, PipelineStage, QueryPipelineContext};
use querying_shared_stages::ExtractionOutput;

/// gRPC authorization channel inserted into `ctx.server_extensions` before the pipeline runs.
pub struct AuthorizationChannel {
    pub tx: mpsc::Sender<Result<ExecuteQueryMessage, Status>>,
    pub stream: Mutex<Streaming<ExecuteQueryMessage>>,
}

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
            let channel = ctx
                .server_extensions
                .get::<AuthorizationChannel>()
                .ok_or_else(|| {
                    PipelineError::Authorization("AuthorizationChannel not available".into())
                })?;
            let mut stream = channel.stream.lock().await;
            RedactionService::request_authorization(&resources_to_check, &channel.tx, &mut stream)
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
