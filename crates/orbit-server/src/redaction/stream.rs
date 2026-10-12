use std::collections::HashMap;

use futures::StreamExt;
use tokio::sync::mpsc;
use tonic::{Status, Streaming};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::proto::{
    ExecuteQueryMessage, RedactionExchange, RedactionRequired,
    ResourceAuthorization as ProtoResourceAuthorization,
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
    DuplicateResourceType(String),
    IncompleteResponse { resource_type: String },
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

        let authorizations = match_authorizations(resources, redaction_response.authorizations)?;
        Ok(RedactionExchangeResult { authorizations })
    }
}

fn match_authorizations(
    checks: &[ResourceCheck],
    response: Vec<ProtoResourceAuthorization>,
) -> Result<Vec<ResourceAuthorization>, RedactionExchangeError> {
    let mut answers = HashMap::with_capacity(response.len());
    for auth in response {
        let key = (auth.resource_type, auth.ability);
        if answers.contains_key(&key) {
            return Err(RedactionExchangeError::DuplicateResourceType(key.0));
        }
        answers.insert(key, auth.authorized);
    }

    checks
        .iter()
        .map(|check| {
            let resource_type = check.resource_type.clone();
            let authorized = answers
                .remove(&(resource_type.clone(), check.ability.clone()))
                .or_else(|| answers.remove(&(resource_type.clone(), String::new())))
                .filter(|authorized| check.ids.iter().all(|id| authorized.contains_key(id)))
                .ok_or_else(|| RedactionExchangeError::IncompleteResponse {
                    resource_type: resource_type.clone(),
                })?;
            Ok(ResourceAuthorization {
                resource_type,
                ability: check.ability.clone(),
                authorized,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(resource_type: &str, ability: &str, ids: &[i64]) -> ResourceCheck {
        ResourceCheck {
            resource_type: resource_type.into(),
            ids: ids.to_vec(),
            ability: ability.into(),
        }
    }

    fn answer(
        resource_type: &str,
        ability: &str,
        ids: &[(i64, bool)],
    ) -> ProtoResourceAuthorization {
        ProtoResourceAuthorization {
            resource_type: resource_type.into(),
            ability: ability.into(),
            authorized: ids.iter().copied().collect(),
        }
    }

    #[test]
    fn answers_tagged_with_an_ability_match_their_own_check() {
        let checks = [
            check("project", "read_project", &[1]),
            check("project", "read_code", &[1]),
        ];
        let matched = match_authorizations(
            &checks,
            vec![
                answer("project", "read_code", &[(1, false)]),
                answer("project", "read_project", &[(1, true)]),
            ],
        )
        .unwrap();
        let decisions: Vec<_> = matched
            .iter()
            .map(|a| (a.ability.as_str(), a.authorized[&1]))
            .collect();
        assert_eq!(decisions, [("read_project", true), ("read_code", false)]);
    }

    #[test]
    fn an_untagged_answer_matches_the_only_check_for_its_type() {
        let checks = [check("project", "read_code", &[1, 2])];
        let matched = match_authorizations(
            &checks,
            vec![answer("project", "", &[(1, true), (2, false)])],
        )
        .unwrap();
        assert_eq!(matched[0].ability, "read_code");
        assert_eq!(matched[0].authorized.len(), 2);
    }

    #[test]
    fn one_untagged_answer_cannot_serve_two_abilities() {
        let checks = [
            check("project", "read_project", &[1]),
            check("project", "read_code", &[1]),
        ];
        let err =
            match_authorizations(&checks, vec![answer("project", "", &[(1, true)])]).unwrap_err();
        assert!(matches!(
            err,
            RedactionExchangeError::IncompleteResponse { resource_type } if resource_type == "project"
        ));
    }

    #[test]
    fn a_missing_resource_type_fails_the_exchange() {
        let checks = [check("package", "read_package", &[1])];
        let err =
            match_authorizations(&checks, vec![answer("project", "", &[(1, true)])]).unwrap_err();
        assert!(matches!(
            err,
            RedactionExchangeError::IncompleteResponse { resource_type } if resource_type == "package"
        ));
    }

    #[test]
    fn a_missing_id_fails_the_exchange() {
        let checks = [check("project", "read_project", &[1, 2])];
        let err =
            match_authorizations(&checks, vec![answer("project", "", &[(1, true)])]).unwrap_err();
        assert!(matches!(
            err,
            RedactionExchangeError::IncompleteResponse { .. }
        ));
    }

    #[test]
    fn a_duplicate_answer_fails_the_exchange() {
        let checks = [check("project", "read_project", &[1])];
        let err = match_authorizations(
            &checks,
            vec![
                answer("project", "read_project", &[(1, true)]),
                answer("project", "read_project", &[(1, true)]),
            ],
        )
        .unwrap_err();
        assert!(matches!(
            err,
            RedactionExchangeError::DuplicateResourceType(t) if t == "project"
        ));
    }
}
