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
    ResultIdMismatch {
        expected: String,
        received: String,
    },
    ClientError {
        code: String,
        message: String,
    },
    DuplicateAnswer {
        resource_type: String,
        ability: String,
    },
    IncompleteResponse {
        resource_type: String,
    },
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
        by_ability: bool,
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

        let authorizations =
            match_authorizations(resources, redaction_response.authorizations, by_ability)?;
        Ok(RedactionExchangeResult { authorizations })
    }
}

fn match_authorizations(
    checks: &[ResourceCheck],
    response: Vec<ProtoResourceAuthorization>,
    by_ability: bool,
) -> Result<Vec<ResourceAuthorization>, RedactionExchangeError> {
    let mut answers = HashMap::with_capacity(response.len());
    for auth in response {
        let key = (auth.resource_type, auth.ability);
        if answers.contains_key(&key) {
            let (resource_type, ability) = key;
            return Err(RedactionExchangeError::DuplicateAnswer {
                resource_type,
                ability,
            });
        }
        answers.insert(key, auth.authorized);
    }

    checks
        .iter()
        .map(|check| {
            let answer_ability = if by_ability {
                check.ability.as_str()
            } else {
                ""
            };
            let authorized = answers
                .remove(&(check.resource_type.clone(), answer_ability.to_string()))
                .filter(|authorized| check.ids.iter().all(|id| authorized.contains_key(id)))
                .ok_or_else(|| RedactionExchangeError::IncompleteResponse {
                    resource_type: check.resource_type.clone(),
                })?;
            Ok(ResourceAuthorization {
                resource_type: check.resource_type.clone(),
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

    fn incomplete(err: RedactionExchangeError, expected: &str) -> bool {
        matches!(err, RedactionExchangeError::IncompleteResponse { resource_type } if resource_type == expected)
    }

    #[test]
    fn answers_by_ability_match_their_own_check() {
        let checks = [
            check("project", "read_project", &[1]),
            check("project", "read_code", &[1]),
        ];
        let answers = vec![
            answer("project", "read_code", &[(1, false)]),
            answer("project", "read_project", &[(1, true)]),
        ];
        let decisions: Vec<_> = match_authorizations(&checks, answers, true)
            .unwrap()
            .iter()
            .map(|a| (a.ability.clone(), a.authorized[&1]))
            .collect();
        assert_eq!(
            decisions,
            [
                ("read_project".to_string(), true),
                ("read_code".to_string(), false)
            ]
        );
    }

    #[test]
    fn a_client_without_the_flag_answers_each_type_untagged() {
        let checks = [check("project", "read_code", &[1, 2])];
        let matched = match_authorizations(
            &checks,
            vec![answer("project", "", &[(1, true), (2, false)])],
            false,
        )
        .unwrap();
        assert_eq!(matched[0].ability, "read_code");
        assert_eq!(matched[0].authorized.len(), 2);
    }

    #[test]
    fn a_client_with_the_flag_must_tag_every_answer() {
        let checks = [
            check("project", "read_project", &[1]),
            check("project", "read_code", &[1]),
        ];
        let answers = vec![
            answer("project", "", &[(1, true)]),
            answer("project", "read_project", &[(1, true)]),
        ];
        assert!(incomplete(
            match_authorizations(&checks, answers, true).unwrap_err(),
            "project"
        ));
    }

    #[test]
    fn a_missing_resource_type_fails_the_exchange() {
        let checks = [check("package", "read_package", &[1])];
        let err = match_authorizations(&checks, vec![answer("project", "", &[(1, true)])], false)
            .unwrap_err();
        assert!(incomplete(err, "package"));
    }

    #[test]
    fn a_missing_id_fails_the_exchange() {
        let checks = [check("project", "read_project", &[1, 2])];
        let err = match_authorizations(&checks, vec![answer("project", "", &[(1, true)])], false)
            .unwrap_err();
        assert!(incomplete(err, "project"));
    }

    #[test]
    fn a_duplicate_answer_fails_the_exchange() {
        let checks = [check("project", "read_project", &[1])];
        let answers = vec![
            answer("project", "read_project", &[(1, true)]),
            answer("project", "read_project", &[(1, true)]),
        ];
        assert!(matches!(
            match_authorizations(&checks, answers, true).unwrap_err(),
            RedactionExchangeError::DuplicateAnswer { resource_type, ability }
                if resource_type == "project" && ability == "read_project"
        ));
    }
}
