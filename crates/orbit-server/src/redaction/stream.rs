use futures::StreamExt;
use tokio::sync::mpsc;
use tonic::{Status, Streaming};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::proto::{
    ExecuteQueryMessage, RedactionExchange, RedactionRequired,
    ResourceToAuthorize as ProtoResourceToAuthorize, execute_query_message, redaction_exchange,
};

use super::{ResourceAuthorization, ResourceCheck};

#[derive(Debug)]
pub enum RedactionExchangeError {
    StreamClosed,
    ReceiveFailed(Status),
    InvalidMessage(&'static str),
    ResultIdMismatch { expected: String, received: String },
    ClientError { code: String, message: String },
}

impl RedactionExchangeError {
    pub fn into_status(self) -> Status {
        match self {
            Self::StreamClosed => {
                Status::cancelled("Client closed stream without sending redaction response")
            }
            Self::ReceiveFailed(s) => s,
            Self::InvalidMessage(msg) => Status::invalid_argument(msg),
            Self::ResultIdMismatch { expected, received } => {
                warn!(expected = %expected, received = %received, "result_id mismatch");
                Status::invalid_argument("result_id mismatch in redaction response")
            }
            Self::ClientError { code, message } => {
                warn!(code = %code, message = %message, "Client sent error");
                Status::aborted(format!("{}: {}", code, message))
            }
        }
    }
}

pub struct RedactionService;

impl RedactionService {
    pub async fn request_authorization(
        resources: &[ResourceCheck],
        check_boundaries: bool,
        require_ability: bool,
        tx: &mpsc::Sender<Result<ExecuteQueryMessage, Status>>,
        stream: &mut Streaming<ExecuteQueryMessage>,
    ) -> Result<Vec<ResourceAuthorization>, RedactionExchangeError> {
        let result_id = Uuid::new_v4().to_string();

        let proto_resources: Vec<ProtoResourceToAuthorize> = resources
            .iter()
            .map(|r| ProtoResourceToAuthorize {
                resource_type: r.resource_type.clone(),
                resource_ids: r.ids.clone(),
                abilities: vec![r.ability.clone()],
                permission: r.permission.clone(),
            })
            .collect();

        info!(
            result_id = %result_id,
            resource_count = proto_resources.len(),
            "Requesting redaction authorization"
        );

        let redaction_required = RedactionExchange {
            content: Some(redaction_exchange::Content::Required(RedactionRequired {
                result_id: result_id.clone(),
                resources: proto_resources,
                check_boundaries,
            })),
        };

        let _ = tx
            .send(Ok(ExecuteQueryMessage {
                content: Some(execute_query_message::Content::Redaction(
                    redaction_required,
                )),
            }))
            .await;

        let redaction_msg = match stream.next().await {
            Some(Ok(msg)) => msg,
            Some(Err(e)) => {
                error!(error = %e, "Failed to receive redaction response");
                return Err(RedactionExchangeError::ReceiveFailed(e));
            }
            None => {
                warn!("Client closed stream without sending redaction response");
                return Err(RedactionExchangeError::StreamClosed);
            }
        };

        let redaction_exchange = match redaction_msg.content {
            Some(execute_query_message::Content::Redaction(r)) => r,
            Some(execute_query_message::Content::Error(e)) => {
                return Err(RedactionExchangeError::ClientError {
                    code: e.code,
                    message: e.message,
                });
            }
            _ => {
                warn!("Expected RedactionExchange");
                return Err(RedactionExchangeError::InvalidMessage(
                    "Expected RedactionExchange",
                ));
            }
        };

        let redaction_response = match redaction_exchange.content {
            Some(redaction_exchange::Content::Response(r)) => r,
            _ => {
                warn!("Expected RedactionResponse in exchange");
                return Err(RedactionExchangeError::InvalidMessage(
                    "Expected RedactionResponse",
                ));
            }
        };

        if redaction_response.result_id != result_id {
            return Err(RedactionExchangeError::ResultIdMismatch {
                expected: result_id,
                received: redaction_response.result_id,
            });
        }

        redaction_response
            .authorizations
            .into_iter()
            .map(|a| {
                let ability =
                    response_ability(&a.resource_type, &a.ability, resources, require_ability)?;
                Ok(ResourceAuthorization {
                    resource_type: a.resource_type,
                    ability,
                    authorized: a.authorized,
                })
            })
            .collect()
    }
}

fn response_ability(
    resource_type: &str,
    ability: &str,
    resources: &[ResourceCheck],
    require_ability: bool,
) -> Result<String, RedactionExchangeError> {
    if !ability.is_empty() {
        return Ok(ability.to_string());
    }
    let mut matching = resources
        .iter()
        .filter(|r| r.resource_type == resource_type);
    match (require_ability, matching.next(), matching.next()) {
        (false, Some(resource), None) => Ok(resource.ability.clone()),
        _ => Err(RedactionExchangeError::InvalidMessage(
            "authorization response must identify its ability",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_response_ability_requires_one_classic_operation() {
        let project = ResourceCheck {
            resource_type: "project".into(),
            ability: "read_project".into(),
            permission: "read_project".into(),
            ids: vec![1],
        };
        let code = ResourceCheck {
            ability: "read_code".into(),
            permission: "read_code".into(),
            ..project.clone()
        };
        assert_eq!(
            response_ability("project", "", std::slice::from_ref(&project), false).unwrap(),
            "read_project"
        );
        assert!(response_ability("project", "", std::slice::from_ref(&project), true).is_err());
        assert!(response_ability("project", "", &[project, code], false).is_err());
    }

    #[test]
    fn test_redaction_exchange_error_into_status() {
        let err = RedactionExchangeError::StreamClosed;
        let status = err.into_status();
        assert_eq!(status.code(), tonic::Code::Cancelled);

        let err = RedactionExchangeError::InvalidMessage("test");
        let status = err.into_status();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);

        let err = RedactionExchangeError::ResultIdMismatch {
            expected: "a".to_string(),
            received: "b".to_string(),
        };
        let status = err.into_status();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);

        let err = RedactionExchangeError::ClientError {
            code: "test".to_string(),
            message: "msg".to_string(),
        };
        let status = err.into_status();
        assert_eq!(status.code(), tonic::Code::Aborted);
    }
}
