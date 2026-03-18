use std::time::Instant;

use tokio::sync::{Mutex, mpsc};
use tonic::{Status, Streaming};
use tracing::info;

use crate::proto::ExecuteQueryMessage;
use crate::redaction::RedactionService;

use querying_pipeline::{PipelineError, PipelineObserver, PipelineStage, QueryPipelineContext};
use querying_shared_stages::{AuthorizationOutput, ExtractionOutput};

pub struct GrpcAuthorizer<'a> {
    tx: &'a mpsc::Sender<Result<ExecuteQueryMessage, Status>>,
    stream: Mutex<&'a mut Streaming<ExecuteQueryMessage>>,
}

impl<'a> GrpcAuthorizer<'a> {
    pub fn new(
        tx: &'a mpsc::Sender<Result<ExecuteQueryMessage, Status>>,
        stream: &'a mut Streaming<ExecuteQueryMessage>,
    ) -> Self {
        Self {
            tx,
            stream: Mutex::new(stream),
        }
    }
}

impl PipelineStage for GrpcAuthorizer<'_> {
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
            let mut stream = self.stream.lock().await;
            RedactionService::request_authorization(&resources_to_check, self.tx, &mut stream)
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
