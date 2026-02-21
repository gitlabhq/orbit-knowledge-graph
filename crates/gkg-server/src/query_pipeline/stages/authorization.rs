use tokio::sync::mpsc;
use tonic::{Status, Streaming};
use tracing::info;

use crate::redaction::{QueryResult, RedactionExchangeError, RedactionMessage, RedactionService};

use super::super::types::AuthorizationOutput;

pub struct AuthorizationStage;

impl AuthorizationStage {
    pub async fn execute<M: RedactionMessage>(
        query_result: QueryResult,
        tx: &mpsc::Sender<Result<M, Status>>,
        stream: &mut Streaming<M>,
    ) -> Result<AuthorizationOutput, RedactionExchangeError> {
        let resources_to_check = query_result.resource_checks();
        let authorizations = if resources_to_check.is_empty() {
            info!("No redaction required, returning result directly");
            Vec::new()
        } else {
            RedactionService::request_authorization(&resources_to_check, tx, stream)
                .await?
                .authorizations
        };

        Ok(AuthorizationOutput {
            query_result,
            authorizations,
        })
    }
}
