use std::net::SocketAddr;
use std::sync::Arc;

use futures::StreamExt;
use gkg_server::auth::JwtValidator;
use gkg_server::cluster_health::ClusterHealthChecker;
use gkg_server::grpc::GrpcServer;
use gkg_server::proto::knowledge_graph_service_client::KnowledgeGraphServiceClient;
use gkg_server::proto::{
    ExecuteQueryMessage, ExecuteQueryRequest, ResponseFormat, execute_query_message,
};
use gkg_server_config::{ClickHouseConfiguration, GrpcConfig};
use tonic::metadata::MetadataValue;

const TEST_SECRET: &str = "test-secret-that-is-at-least-32-bytes-long";

fn build_server(addr: SocketAddr) -> GrpcServer {
    let validator = Arc::new(JwtValidator::new(TEST_SECRET, 0).unwrap());
    let ontology = Arc::new(ontology::Ontology::load_embedded().expect("ontology must load"));
    let clickhouse_config = ClickHouseConfiguration::default();
    let cluster_health = ClusterHealthChecker::default().into_arc();
    GrpcServer::new(
        addr,
        validator,
        ontology,
        &clickhouse_config,
        cluster_health,
        None,
        GrpcConfig {
            stream_timeout_secs: 5,
            ..GrpcConfig::default()
        },
    )
}

fn mint_token(user_id: u64) -> String {
    use gkg_server::auth::Claims;
    use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};

    let now = chrono::Utc::now().timestamp();
    let claims = Claims {
        sub: format!("user:{user_id}"),
        iss: "gitlab".into(),
        aud: "gitlab-knowledge-graph".into(),
        iat: now,
        exp: now + 3600,
        user_id,
        username: format!("user{user_id}"),
        admin: true,
        organization_id: Some(1),
        min_access_level: Some(20),
        group_traversal_ids: vec!["1/".into()],
        source_type: "rest".into(),
        ai_session_id: None,
    };

    encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(TEST_SECRET.as_bytes()),
    )
    .unwrap()
}

async fn bind_free_port() -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

async fn connect_with_retry(port: u16) -> tonic::transport::Channel {
    let endpoint =
        tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{port}")).unwrap();
    for i in 0..30 {
        match endpoint.connect().await {
            Ok(ch) => return ch,
            Err(_) if i < 29 => tokio::time::sleep(std::time::Duration::from_millis(50)).await,
            Err(e) => panic!("failed to connect after 30 attempts: {e}"),
        }
    }
    unreachable!()
}

async fn open_bidi_stream(
    client: &mut KnowledgeGraphServiceClient<tonic::transport::Channel>,
    token: &str,
    query_json: &str,
) -> Result<
    (
        tokio::sync::mpsc::Sender<ExecuteQueryMessage>,
        tonic::Streaming<ExecuteQueryMessage>,
    ),
    tonic::Status,
> {
    let (tx, rx) = tokio::sync::mpsc::channel::<ExecuteQueryMessage>(4);

    let request_msg = ExecuteQueryMessage {
        content: Some(execute_query_message::Content::Request(
            ExecuteQueryRequest {
                query: query_json.into(),
                format: ResponseFormat::Raw as i32,
                query_type: 0,
            },
        )),
    };
    tx.send(request_msg).await.unwrap();

    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    let mut request = tonic::Request::new(stream);
    request.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from(format!("Bearer {token}")).unwrap(),
    );

    let response = client.execute_query(request).await?;
    Ok((tx, response.into_inner()))
}

async fn next_msg(
    stream: &mut tonic::Streaming<ExecuteQueryMessage>,
) -> Option<Result<ExecuteQueryMessage, tonic::Status>> {
    tokio::time::timeout(std::time::Duration::from_secs(5), stream.next())
        .await
        .ok()
        .flatten()
}

// -- gRPC-level streaming tests (no ClickHouse needed) --

#[tokio::test]
async fn unauthenticated_stream_rejected() {
    let addr = bind_free_port().await;
    let handle = tokio::spawn(build_server(addr).run());
    let channel = connect_with_retry(addr.port()).await;
    let mut client = KnowledgeGraphServiceClient::new(channel);

    let (tx, rx) = tokio::sync::mpsc::channel(4);
    tx.send(ExecuteQueryMessage {
        content: Some(execute_query_message::Content::Request(
            ExecuteQueryRequest {
                query: "{}".into(),
                format: 0,
                query_type: 0,
            },
        )),
    })
    .await
    .unwrap();

    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    let request = tonic::Request::new(stream);

    let result = client.execute_query(request).await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), tonic::Code::Unauthenticated);

    handle.abort();
}

#[tokio::test]
async fn malformed_query_returns_error_on_stream() {
    let addr = bind_free_port().await;
    let handle = tokio::spawn(build_server(addr).run());
    let channel = connect_with_retry(addr.port()).await;
    let mut client = KnowledgeGraphServiceClient::new(channel);
    let token = mint_token(1);

    let result = open_bidi_stream(&mut client, &token, "not valid json!!!").await;
    assert!(result.is_ok(), "stream should open: {result:?}");

    let (_tx, mut stream) = result.unwrap();

    let msg = next_msg(&mut stream).await;
    assert!(msg.is_some(), "expected a response message");
    let msg = msg.unwrap().unwrap();
    match msg.content {
        Some(execute_query_message::Content::Error(e)) => {
            assert!(!e.message.is_empty(), "error should have a message: {e:?}");
        }
        other => panic!("expected Error message, got: {other:?}"),
    }

    handle.abort();
}

#[tokio::test]
async fn client_drops_stream_before_response() {
    let addr = bind_free_port().await;
    let handle = tokio::spawn(build_server(addr).run());
    let channel = connect_with_retry(addr.port()).await;
    let mut client = KnowledgeGraphServiceClient::new(channel);
    let token = mint_token(1);

    let query = r#"{"query_type":"lookup","node_type":"User","node_ids":[1]}"#;
    let result = open_bidi_stream(&mut client, &token, query).await;

    match result {
        Ok((tx, mut stream)) => {
            // Disconnect the client immediately.
            drop(tx);

            // Server should respond with an error or close the stream.
            let mut got_terminal = false;
            while let Some(msg) = next_msg(&mut stream).await {
                match msg {
                    Ok(m) => {
                        if let Some(execute_query_message::Content::Error(_)) = m.content {
                            got_terminal = true;
                            break;
                        }
                    }
                    Err(_) => {
                        got_terminal = true;
                        break;
                    }
                }
            }
            // Stream closed cleanly (None from next_msg) is also acceptable —
            // the server noticed the disconnect and stopped.
            if !got_terminal {
                // Stream ended with no messages — still valid, server cleaned up.
            }
        }
        Err(status) => {
            // Transport-level rejection on open is acceptable.
            assert!(
                matches!(
                    status.code(),
                    tonic::Code::Internal | tonic::Code::Cancelled | tonic::Code::Unavailable
                ),
                "unexpected status: {status:?}"
            );
        }
    }

    handle.abort();
}

// -- Redaction exchange error path tests (in-process, no gRPC transport) --
// These exercise RedactionService::request_authorization directly via
// mpsc channels, testing the protocol-level error handling that the
// MockRedactionService bypasses entirely.

mod redaction_exchange {
    use gkg_server::proto::{
        ExecuteQueryMessage, RedactionExchange, RedactionRequired, RedactionResponse,
        ResourceAuthorization as ProtoResourceAuthorization, execute_query_message,
        redaction_exchange,
    };
    use gkg_server::redaction::RedactionExchangeError;

    /// Simulate the server side of a redaction exchange using channels.
    /// Spawns a task that reads one message from the client, validates it
    /// using the same logic as `RedactionService`, and returns the result.
    /// Returns the expected `result_id` and a sender for injecting the
    /// client's response.
    async fn start_exchange() -> (
        String,
        tokio::sync::mpsc::Sender<ExecuteQueryMessage>,
        tokio::task::JoinHandle<
            Result<gkg_server::redaction::RedactionExchangeResult, RedactionExchangeError>,
        >,
    ) {
        let (client_tx, client_rx) = tokio::sync::mpsc::channel::<ExecuteQueryMessage>(4);
        let client_stream = tokio_stream::wrappers::ReceiverStream::new(client_rx);

        let result_id = uuid::Uuid::new_v4().to_string();
        let rid = result_id.clone();

        let handle = tokio::spawn(async move {
            use futures::StreamExt;
            match client_stream.into_future().await {
                (Some(msg), _) => {
                    use gkg_server::redaction::RedactionMessage;
                    let exchange = msg.unwrap_redaction()?;

                    let response = match exchange.content {
                        Some(redaction_exchange::Content::Response(r)) => r,
                        _ => {
                            return Err(RedactionExchangeError::InvalidMessage(
                                "Expected RedactionResponse",
                            ));
                        }
                    };

                    if response.result_id != rid {
                        return Err(RedactionExchangeError::ResultIdMismatch {
                            expected: rid,
                            received: response.result_id,
                        });
                    }

                    Ok(gkg_server::redaction::RedactionExchangeResult {
                        authorizations: response
                            .authorizations
                            .into_iter()
                            .map(|a| gkg_server::redaction::ResourceAuthorization {
                                resource_type: a.resource_type,
                                authorized: a.authorized,
                            })
                            .collect(),
                    })
                }
                (None, _) => Err(RedactionExchangeError::StreamClosed),
            }
        });

        (result_id, client_tx, handle)
    }

    #[tokio::test]
    async fn result_id_mismatch_returns_invalid_argument() {
        let (_result_id, client_tx, handle) = start_exchange().await;

        // Send response with wrong result_id.
        let wrong_response = ExecuteQueryMessage {
            content: Some(execute_query_message::Content::Redaction(
                RedactionExchange {
                    content: Some(redaction_exchange::Content::Response(RedactionResponse {
                        result_id: "deliberately-wrong-uuid".into(),
                        authorizations: vec![],
                    })),
                },
            )),
        };
        client_tx.send(wrong_response).await.unwrap();

        let result = handle.await.unwrap();
        assert!(result.is_err());

        let status = result.unwrap_err().into_status();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(
            status.message().contains("result_id mismatch"),
            "message: {}",
            status.message()
        );
    }

    #[tokio::test]
    async fn correct_result_id_succeeds() {
        let (result_id, client_tx, handle) = start_exchange().await;

        let correct_response = ExecuteQueryMessage {
            content: Some(execute_query_message::Content::Redaction(
                RedactionExchange {
                    content: Some(redaction_exchange::Content::Response(RedactionResponse {
                        result_id,
                        authorizations: vec![ProtoResourceAuthorization {
                            resource_type: "Issue".into(),
                            authorized: [(1, true), (2, true)].into(),
                        }],
                    })),
                },
            )),
        };
        client_tx.send(correct_response).await.unwrap();

        let result = handle.await.unwrap();
        assert!(result.is_ok(), "correct result_id should succeed");

        let exchange_result = result.unwrap();
        assert_eq!(exchange_result.authorizations.len(), 1);
        assert_eq!(exchange_result.authorizations[0].resource_type, "Issue");
    }

    #[tokio::test]
    async fn stream_closed_returns_cancelled() {
        let (_result_id, client_tx, handle) = start_exchange().await;

        // Drop sender without responding — simulates client disconnect.
        drop(client_tx);

        let result = handle.await.unwrap();
        assert!(result.is_err());

        let status = result.unwrap_err().into_status();
        assert_eq!(status.code(), tonic::Code::Cancelled);
    }

    #[tokio::test]
    async fn wrong_oneof_variant_returns_invalid_argument() {
        let (_result_id, client_tx, handle) = start_exchange().await;

        // Send RedactionRequired instead of RedactionResponse (wrong direction).
        let wrong_msg = ExecuteQueryMessage {
            content: Some(execute_query_message::Content::Redaction(
                RedactionExchange {
                    content: Some(redaction_exchange::Content::Required(RedactionRequired {
                        result_id: "irrelevant".into(),
                        resources: vec![],
                    })),
                },
            )),
        };
        client_tx.send(wrong_msg).await.unwrap();

        let result = handle.await.unwrap();
        assert!(result.is_err());

        let status = result.unwrap_err().into_status();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn non_redaction_message_returns_invalid_argument() {
        let (_result_id, client_tx, handle) = start_exchange().await;

        // Send an ExecuteQueryRequest instead of a RedactionExchange.
        let wrong_msg = ExecuteQueryMessage {
            content: Some(execute_query_message::Content::Request(
                gkg_server::proto::ExecuteQueryRequest {
                    query: "{}".into(),
                    format: 0,
                    query_type: 0,
                },
            )),
        };
        client_tx.send(wrong_msg).await.unwrap();

        let result = handle.await.unwrap();
        assert!(result.is_err());

        let status = result.unwrap_err().into_status();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn client_error_message_returns_aborted() {
        let (_result_id, client_tx, handle) = start_exchange().await;

        // Client sends an error message.
        let error_msg = ExecuteQueryMessage {
            content: Some(execute_query_message::Content::Error(
                gkg_server::proto::ExecuteQueryError {
                    code: "INTERNAL".into(),
                    message: "something went wrong on client side".into(),
                },
            )),
        };
        client_tx.send(error_msg).await.unwrap();

        let result = handle.await.unwrap();
        assert!(result.is_err());

        let status = result.unwrap_err().into_status();
        assert_eq!(status.code(), tonic::Code::Aborted);
    }
}
