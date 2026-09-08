use std::sync::Arc;
use std::time::Duration;

use integration_testkit::{TestContext, load_ontology};
use jsonwebtoken::{EncodingKey, Header, encode};
use orbit_server::auth::{Claims, JwtValidator};
use orbit_server::cluster_health::ClusterHealthChecker;
use orbit_server::grpc::OrbitServiceImpl;
use orbit_server::proto::orbit_service_client::OrbitServiceClient;
use orbit_server::proto::orbit_service_server::OrbitServiceServer;
use orbit_server::proto::{
    ExecuteQueryMessage, ExecuteQueryRequest, QueryType, RedactionExchange, RedactionResponse,
    ResourceAuthorization, ResponseFormat, execute_query_message::Content as MessageContent,
    execute_query_result::Content as ResultContent,
    redaction_exchange::Content as RedactionContent,
};
use orbit_server::redaction::RedactionMessage;
use orbit_server_config::{AnalyticsConfig, GrpcConfig};
use prost::Message;
use query_engine::formatters::GraphResponse;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;

use super::DummyClaims;

const TEST_SIGNING_SECRET: &str = "response-budget-test-secret-at-least-32-bytes";

pub struct QueryClient {
    client: OrbitServiceClient<OrbitServiceServer<OrbitServiceImpl>>,
}

pub struct QueryPage {
    pub response: GraphResponse,
    pub encoded_bytes: usize,
}

impl QueryClient {
    pub fn new(context: &TestContext, response_budget: usize) -> Self {
        let service = OrbitServiceImpl::new(
            Arc::new(JwtValidator::new(TEST_SIGNING_SECRET, 0).unwrap()),
            Arc::new(load_ontology()),
            &context.config,
            ClusterHealthChecker::default().into_arc(),
            GrpcConfig::default().stream_timeout_secs,
            Arc::new(AnalyticsConfig::default()),
        )
        .with_max_query_response_bytes(response_budget);
        let client = OrbitServiceClient::new(OrbitServiceServer::new(service))
            .max_decoding_message_size(GrpcConfig::default().max_query_response_bytes);
        Self { client }
    }

    pub async fn query(&self, query: &Value) -> QueryPage {
        let token = encode(
            &Header::default(),
            &Claims::dummy(),
            &EncodingKey::from_secret(TEST_SIGNING_SECRET.as_bytes()),
        )
        .unwrap();
        let (sender, receiver) = mpsc::channel(1);
        sender
            .send(ExecuteQueryMessage {
                content: Some(MessageContent::Request(ExecuteQueryRequest {
                    query: query.to_string(),
                    format: ResponseFormat::Raw.into(),
                    query_type: QueryType::Json.into(),
                })),
            })
            .await
            .unwrap();

        let mut request = Request::new(ReceiverStream::new(receiver));
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
        let mut client = self.client.clone();
        let mut stream = client.execute_query(request).await.unwrap().into_inner();

        loop {
            let message = tokio::time::timeout(Duration::from_secs(30), stream.message())
                .await
                .unwrap()
                .unwrap()
                .expect("query must return a result");
            let encoded_bytes = message.encoded_len();

            match message.content.unwrap() {
                MessageContent::Redaction(RedactionExchange {
                    content: Some(RedactionContent::Required(required)),
                }) => {
                    let authorizations = required
                        .resources
                        .iter()
                        .map(|resource| ResourceAuthorization {
                            resource_type: resource.resource_type.clone(),
                            authorized: resource
                                .resource_ids
                                .iter()
                                .map(|id| (*id, true))
                                .collect(),
                        })
                        .collect();
                    let response = RedactionExchange {
                        content: Some(RedactionContent::Response(RedactionResponse {
                            result_id: required.result_id,
                            authorizations,
                        })),
                    };
                    sender
                        .send(ExecuteQueryMessage::wrap_redaction(response))
                        .await
                        .unwrap();
                }
                MessageContent::Result(result) => {
                    let Some(ResultContent::ResultJson(json)) = result.content else {
                        panic!("expected a RAW response");
                    };
                    return QueryPage {
                        response: serde_json::from_str(&json).unwrap(),
                        encoded_bytes,
                    };
                }
                other => panic!("unexpected message: {other:?}"),
            }
        }
    }
}
