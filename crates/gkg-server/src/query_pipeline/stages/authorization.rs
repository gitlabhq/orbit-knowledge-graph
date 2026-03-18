use std::time::Instant;

use tokio::sync::{Mutex, mpsc};
use tonic::{Status, Streaming};
use tracing::info;

use crate::redaction::{RedactionMessage, RedactionService};

use querying_pipeline::{
    AuthorizationOutput, ExtractionOutput, PipelineError, PipelineObserver, PipelineStage,
    QueryPipelineContext,
};

/// Authorizer that performs authorization via gRPC bidirectional streaming to Rails.
/// The `Streaming` is wrapped in a `Mutex` because `PipelineStage::execute` takes `&self`
/// but `request_authorization` needs `&mut Streaming`. The lock is always uncontended
/// since each `GrpcAuthorizer` is created per-request and used exactly once.
pub struct GrpcAuthorizer<'a, M: RedactionMessage> {
    tx: &'a mpsc::Sender<Result<M, Status>>,
    stream: Mutex<&'a mut Streaming<M>>,
}

impl<'a, M: RedactionMessage> GrpcAuthorizer<'a, M> {
    pub fn new(tx: &'a mpsc::Sender<Result<M, Status>>, stream: &'a mut Streaming<M>) -> Self {
        Self {
            tx,
            stream: Mutex::new(stream),
        }
    }
}

impl<M: RedactionMessage + 'static> PipelineStage for GrpcAuthorizer<'_, M> {
    type Input = ExtractionOutput;
    type Output = AuthorizationOutput;

    async fn execute(
        &self,
        input: Self::Input,
        _ctx: &mut QueryPipelineContext,
        obs: &mut dyn PipelineObserver,
    ) -> Result<Self::Output, PipelineError> {
        let t = Instant::now();

        let resources_to_check = input.query_result.resource_checks();
        let authorizations = if resources_to_check.is_empty() {
            info!("No redaction required, returning result directly");
            Vec::new()
        } else {
            let mut stream = self.stream.lock().await;
            RedactionService::request_authorization(&resources_to_check, self.tx, &mut *stream)
                .await
                .map_err(|e| PipelineError::Authorization(format!("{e:?}")))?
                .authorizations
        };

        obs.authorized(t.elapsed());

        Ok(AuthorizationOutput {
            query_result: input.query_result,
            authorizations,
        })
    }
}
