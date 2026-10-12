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
        let mut authorizations = Vec::with_capacity(resources.len());
        for round in rounds_with_unique_resource_types(resources) {
            authorizations.extend(Self::exchange(&round, tx, stream).await?);
        }
        Ok(RedactionExchangeResult { authorizations })
    }

    async fn exchange<M: RedactionMessage>(
        round: &[&ResourceCheck],
        tx: &mpsc::Sender<Result<M, Status>>,
        stream: &mut Streaming<M>,
    ) -> Result<Vec<ResourceAuthorization>, RedactionExchangeError> {
        let result_id = Uuid::new_v4().to_string();

        let proto_resources: Vec<ProtoResourceToAuthorize> = round
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

        match_authorizations(round, redaction_response.authorizations)
    }
}

fn rounds_with_unique_resource_types(resources: &[ResourceCheck]) -> Vec<Vec<&ResourceCheck>> {
    let mut rounds: Vec<Vec<&ResourceCheck>> = Vec::new();
    for check in resources {
        let open_round = rounds
            .iter_mut()
            .find(|round| round.iter().all(|c| c.resource_type != check.resource_type));
        match open_round {
            Some(round) => round.push(check),
            None => rounds.push(vec![check]),
        }
    }
    rounds
}

fn match_authorizations(
    round: &[&ResourceCheck],
    response: Vec<ProtoResourceAuthorization>,
) -> Result<Vec<ResourceAuthorization>, RedactionExchangeError> {
    let mut by_type = HashMap::with_capacity(response.len());
    for auth in response {
        if by_type.contains_key(&auth.resource_type) {
            return Err(RedactionExchangeError::DuplicateResourceType(
                auth.resource_type,
            ));
        }
        by_type.insert(auth.resource_type, auth.authorized);
    }

    round
        .iter()
        .map(|check| {
            let authorized = by_type
                .remove(&check.resource_type)
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

    fn answer(resource_type: &str, ids: &[i64]) -> ProtoResourceAuthorization {
        ProtoResourceAuthorization {
            resource_type: resource_type.into(),
            authorized: ids.iter().map(|id| (*id, true)).collect(),
        }
    }

    fn round_shape(rounds: &[Vec<&ResourceCheck>]) -> Vec<Vec<(String, String)>> {
        rounds
            .iter()
            .map(|round| {
                round
                    .iter()
                    .map(|c| (c.resource_type.clone(), c.ability.clone()))
                    .collect()
            })
            .collect()
    }

    #[test]
    fn one_round_when_every_resource_type_is_unique() {
        let checks = [
            check("project", "read_project", &[1]),
            check("merge_request", "read_merge_request", &[2]),
        ];
        assert_eq!(rounds_with_unique_resource_types(&checks).len(), 1);
    }

    #[test]
    fn a_shared_resource_type_moves_each_extra_ability_to_a_new_round() {
        let checks = [
            check("project", "read_project", &[1]),
            check("project", "read_code", &[1]),
            check("merge_request", "read_merge_request", &[2]),
        ];
        let pair = |t: &str, a: &str| (t.to_string(), a.to_string());
        assert_eq!(
            round_shape(&rounds_with_unique_resource_types(&checks)),
            vec![
                vec![
                    pair("project", "read_project"),
                    pair("merge_request", "read_merge_request")
                ],
                vec![pair("project", "read_code")],
            ]
        );
    }

    #[test]
    fn matched_authorizations_carry_the_requested_ability() {
        let code = check("project", "read_code", &[1, 2]);
        let matched = match_authorizations(&[&code], vec![answer("project", &[1, 2])]).unwrap();
        assert_eq!(matched[0].ability, "read_code");
        assert_eq!(matched[0].authorized.len(), 2);
    }

    #[test]
    fn a_missing_resource_type_fails_the_exchange() {
        let package = check("package", "read_package", &[1]);
        let err = match_authorizations(&[&package], vec![answer("project", &[1])]).unwrap_err();
        assert!(matches!(
            err,
            RedactionExchangeError::IncompleteResponse { resource_type } if resource_type == "package"
        ));
    }

    #[test]
    fn a_missing_id_fails_the_exchange() {
        let project = check("project", "read_project", &[1, 2]);
        let err = match_authorizations(&[&project], vec![answer("project", &[1])]).unwrap_err();
        assert!(matches!(
            err,
            RedactionExchangeError::IncompleteResponse { .. }
        ));
    }

    #[test]
    fn a_duplicate_resource_type_fails_the_exchange() {
        let project = check("project", "read_project", &[1]);
        let err = match_authorizations(
            &[&project],
            vec![answer("project", &[1]), answer("project", &[1])],
        )
        .unwrap_err();
        assert!(matches!(
            err,
            RedactionExchangeError::DuplicateResourceType(t) if t == "project"
        ));
    }
}
