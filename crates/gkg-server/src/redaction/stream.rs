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

pub struct RedactionExchangeResult {
    pub authorizations: Vec<ResourceAuthorization>,
}

pub trait RedactionMessage: Sized + Send {
    fn wrap_redaction(exchange: RedactionExchange) -> Self;
    fn unwrap_redaction(self) -> Result<RedactionExchange, RedactionExchangeError>;
}

impl RedactionMessage for ExecuteQueryMessage {
    fn wrap_redaction(exchange: RedactionExchange) -> Self {
        Self {
            content: Some(execute_query_message::Content::Redaction(exchange)),
        }
    }

    fn unwrap_redaction(self) -> Result<RedactionExchange, RedactionExchangeError> {
        match self.content {
            Some(execute_query_message::Content::Redaction(r)) => Ok(r),
            Some(execute_query_message::Content::Error(e)) => {
                Err(RedactionExchangeError::ClientError {
                    code: e.code,
                    message: e.message,
                })
            }
            _ => {
                warn!("Expected RedactionExchange");
                Err(RedactionExchangeError::InvalidMessage(
                    "Expected RedactionExchange",
                ))
            }
        }
    }
}

pub struct RedactionService;

impl RedactionService {
    pub async fn request_authorization<M: RedactionMessage>(
        resources: &[ResourceCheck],
        tx: &mpsc::Sender<Result<M, Status>>,
        stream: &mut Streaming<M>,
    ) -> Result<RedactionExchangeResult, RedactionExchangeError> {
        let result_id = Uuid::new_v4().to_string();

        let proto_resources: Vec<ProtoResourceToAuthorize> = resources
            .iter()
            .map(|r| ProtoResourceToAuthorize {
                resource_type: r.resource_type.clone(),
                resource_ids: r.ids.clone(),
                abilities: vec![r.ability.clone()],
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
            })),
        };

        let _ = tx.send(Ok(M::wrap_redaction(redaction_required))).await;

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

        let redaction_exchange = redaction_msg.unwrap_redaction()?;

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

        let authorizations = redaction_response
            .authorizations
            .into_iter()
            .map(|a| ResourceAuthorization {
                resource_type: a.resource_type,
                authorized: a.authorized,
            })
            .collect();

        Ok(RedactionExchangeResult { authorizations })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
