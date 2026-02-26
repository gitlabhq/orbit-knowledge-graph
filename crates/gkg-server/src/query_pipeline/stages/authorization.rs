use std::time::Instant;

use tokio::sync::mpsc;
use tonic::{Status, Streaming};
use tracing::info;

use crate::redaction::{RedactionMessage, RedactionService};

use super::super::error::PipelineError;
use super::super::metrics::PipelineObserver;
use super::super::types::{AuthorizationOutput, ExtractionOutput};

pub struct AuthorizationStage;

impl AuthorizationStage {
    pub async fn execute<M: RedactionMessage>(
        input: ExtractionOutput,
        tx: &mpsc::Sender<Result<M, Status>>,
        stream: &mut Streaming<M>,
        obs: &mut PipelineObserver,
    ) -> Result<AuthorizationOutput, PipelineError> {
        let t = Instant::now();

        let resources_to_check = input.query_result.resource_checks();
        let authorizations = if resources_to_check.is_empty() {
            info!("No redaction required, returning result directly");
            Vec::new()
        } else {
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
