use std::time::Instant;

use async_trait::async_trait;
use tokio::sync::mpsc;
use tonic::{Status, Streaming};
use tracing::info;

use crate::redaction::{RedactionMessage, RedactionService};

use querying_pipeline::{
    AuthorizationOutput, Authorizer, ExtractionOutput, PipelineError, PipelineObserver,
};

pub struct GrpcAuthorizer<'a, M: RedactionMessage> {
    tx: &'a mpsc::Sender<Result<M, Status>>,
    stream: &'a mut Streaming<M>,
}

impl<'a, M: RedactionMessage> GrpcAuthorizer<'a, M> {
    pub fn new(tx: &'a mpsc::Sender<Result<M, Status>>, stream: &'a mut Streaming<M>) -> Self {
        Self { tx, stream }
    }
}

#[async_trait]
impl<M: RedactionMessage + 'static> Authorizer for GrpcAuthorizer<'_, M> {
    async fn authorize(
        &mut self,
        input: ExtractionOutput,
        obs: &mut dyn PipelineObserver,
    ) -> Result<AuthorizationOutput, PipelineError> {
        let t = Instant::now();

        let resources_to_check = input.query_result.resource_checks();
        let authorizations = if resources_to_check.is_empty() {
            info!("No redaction required, returning result directly");
            Vec::new()
        } else {
            RedactionService::request_authorization(&resources_to_check, self.tx, self.stream)
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
