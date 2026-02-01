use tokio::sync::mpsc;
use tonic::{Status, Streaming};

use crate::redaction::{RedactionExchangeError, RedactionMessage, RedactionService};

use super::super::types::{AuthorizationOutput, ExtractionOutput};

pub struct AuthorizationStage;

impl AuthorizationStage {
    pub async fn execute<M: RedactionMessage>(
        input: ExtractionOutput,
        tx: &mpsc::Sender<Result<M, Status>>,
        stream: &mut Streaming<M>,
    ) -> Result<AuthorizationOutput, RedactionExchangeError> {
        let authorizations = if input.redaction_plan.resources_to_check.is_empty() {
            Vec::new()
        } else {
            RedactionService::request_authorization(
                &input.redaction_plan.resources_to_check,
                tx,
                stream,
            )
            .await?
            .authorizations
        };

        Ok(AuthorizationOutput {
            query_result: input.query_result,
            result_context: input.result_context,
            authorizations,
            entity_to_resource_map: input.redaction_plan.entity_to_resource_map,
            generated_sql: input.generated_sql,
        })
    }
}
