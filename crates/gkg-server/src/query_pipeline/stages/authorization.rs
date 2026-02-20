use tokio::sync::mpsc;
use tonic::{Status, Streaming};
use tracing::info;

use crate::redaction::{RedactionExchangeError, RedactionMessage, RedactionService};

use super::super::types::{AuthorizationOutput, ExtractionOutput};

pub struct AuthorizationStage;

impl AuthorizationStage {
    pub async fn execute<M: RedactionMessage>(
        input: ExtractionOutput,
        tx: &mpsc::Sender<Result<M, Status>>,
        stream: &mut Streaming<M>,
    ) -> Result<AuthorizationOutput, RedactionExchangeError> {
        let resources_to_check = input.query_result.resource_checks();
        let authorizations = if resources_to_check.is_empty() {
            info!("No redaction required, returning result directly");
            Vec::new()
        } else {
            RedactionService::request_authorization(&resources_to_check, tx, stream)
                .await?
                .authorizations
        };

        Ok(AuthorizationOutput {
            query_result: input.query_result,
            authorizations,
        })
    }
}
