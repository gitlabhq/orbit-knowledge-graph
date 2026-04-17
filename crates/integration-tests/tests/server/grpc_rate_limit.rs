use std::net::SocketAddr;
use std::sync::Arc;

use futures::StreamExt;
use gkg_server::auth::JwtValidator;
use gkg_server::cluster_health::ClusterHealthChecker;
use gkg_server::grpc::GrpcServer;
use gkg_server::proto::knowledge_graph_service_client::KnowledgeGraphServiceClient;
use gkg_server::proto::{ExecuteQueryMessage, ExecuteQueryRequest, ResponseFormat};
use gkg_server::rate_limit::QueryRateLimiter;
use gkg_server_config::{ClickHouseConfiguration, GrpcConfig, RateLimitConfig};
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::MetadataValue;
use tonic::transport::Endpoint;

const TEST_SECRET: &str = "test-secret-that-is-at-least-32-bytes-long";

fn build_server(addr: SocketAddr, rate_limit: RateLimitConfig) -> GrpcServer {
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
        GrpcConfig::default(),
    )
    .with_rate_limiter(QueryRateLimiter::new(&rate_limit))
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
        admin: false,
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
    let endpoint = Endpoint::from_shared(format!("http://127.0.0.1:{port}")).unwrap();
    for i in 0..30 {
        match endpoint.connect().await {
            Ok(ch) => return ch,
            Err(_) if i < 29 => tokio::time::sleep(std::time::Duration::from_millis(50)).await,
            Err(e) => panic!("failed to connect after 30 attempts: {e}"),
        }
    }
    unreachable!()
}

fn query_request_msg() -> ExecuteQueryMessage {
    use gkg_server::proto::execute_query_message;
    ExecuteQueryMessage {
        content: Some(execute_query_message::Content::Request(
            ExecuteQueryRequest {
                query: r#"{"query_type":"lookup","node_type":"User","node_ids":[1]}"#.into(),
                format: ResponseFormat::Raw as i32,
                query_type: 0,
            },
        )),
    }
}

/// Open a bidi ExecuteQuery stream with auth (sends query immediately).
async fn execute_query_stream(
    client: &mut KnowledgeGraphServiceClient<tonic::transport::Channel>,
    token: &str,
) -> Result<tonic::Streaming<ExecuteQueryMessage>, tonic::Status> {
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    tx.send(query_request_msg()).await.unwrap();
    let stream = ReceiverStream::new(rx);

    let mut request = tonic::Request::new(stream);
    request.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from(format!("Bearer {token}")).unwrap(),
    );

    let response = client.execute_query(request).await?;
    Ok(response.into_inner())
}

/// Open a bidi ExecuteQuery stream but don't send the query body yet.
/// The server's spawned task will block at `receive_query_request` while
/// holding the rate limiter permit. Returns the sender so the caller can
/// keep the stream alive (holding the permit) or drop it to release.
async fn open_holding_stream(
    client: &mut KnowledgeGraphServiceClient<tonic::transport::Channel>,
    token: &str,
) -> Result<
    (
        tokio::sync::mpsc::Sender<ExecuteQueryMessage>,
        tonic::Streaming<ExecuteQueryMessage>,
    ),
    tonic::Status,
> {
    let (tx, rx) = tokio::sync::mpsc::channel::<ExecuteQueryMessage>(4);
    // Don't send anything — server will wait for the request, holding the permit.
    let stream = ReceiverStream::new(rx);

    let mut request = tonic::Request::new(stream);
    request.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from(format!("Bearer {token}")).unwrap(),
    );

    let response = client.execute_query(request).await?;
    Ok((tx, response.into_inner()))
}

#[tokio::test]
async fn global_concurrency_rejects_with_resource_exhausted() {
    let addr = bind_free_port().await;
    let server = build_server(
        addr,
        RateLimitConfig {
            max_concurrent_queries: 1,
            per_user_max_requests: 0, // disabled
            per_user_window_secs: 60,
            per_user_max_entries: 100,
        },
    );
    let handle = tokio::spawn(server.run());

    let channel = connect_with_retry(addr.port()).await;
    let mut client = KnowledgeGraphServiceClient::new(channel);
    let token = mint_token(1);

    // First query opens a stream but doesn't send the request body, so the
    // server's spawned task blocks at receive_query_request while holding the
    // rate limiter permit. This keeps the global semaphore slot occupied.
    let (hold_tx, _hold_stream) = open_holding_stream(&mut client, &token)
        .await
        .expect("first stream should open");

    // Give the server a moment to spawn the handler task.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Second query should be rejected immediately — the only slot is taken.
    let result = execute_query_stream(&mut client, &token).await;

    assert!(result.is_err(), "expected rejection, got success");
    let status = result.unwrap_err();
    assert_eq!(
        status.code(),
        tonic::Code::ResourceExhausted,
        "expected ResourceExhausted, got: {status:?}"
    );
    assert!(
        status.message().contains("concurrency"),
        "message should mention concurrency: {}",
        status.message()
    );

    // Clean up: drop the sender to unblock the first stream.
    drop(hold_tx);
    handle.abort();
}

#[tokio::test]
async fn per_user_rate_limit_rejects_excess_requests() {
    let addr = bind_free_port().await;
    let server = build_server(
        addr,
        RateLimitConfig {
            max_concurrent_queries: 0, // disabled
            per_user_max_requests: 2,
            per_user_window_secs: 60,
            per_user_max_entries: 100,
        },
    );
    let handle = tokio::spawn(server.run());

    let channel = connect_with_retry(addr.port()).await;
    let mut client = KnowledgeGraphServiceClient::new(channel);
    let token = mint_token(1);

    // First two queries should be accepted (they'll fail at ClickHouse, but
    // the rate limiter lets them through).
    for i in 0..2 {
        let result = execute_query_stream(&mut client, &token).await;
        assert!(result.is_ok(), "query {i} should be accepted: {result:?}");
        // Consume the stream to let the server finish.
        let mut s = result.unwrap();
        while s.next().await.is_some() {}
    }

    // Third query should be rejected.
    let result = execute_query_stream(&mut client, &token).await;
    assert!(result.is_err(), "third query should be rejected");
    let status = result.unwrap_err();
    assert_eq!(status.code(), tonic::Code::ResourceExhausted);
    assert!(
        status.message().contains("per-user"),
        "message should mention per-user: {}",
        status.message()
    );

    handle.abort();
}

#[tokio::test]
async fn different_users_have_independent_rate_limits() {
    let addr = bind_free_port().await;
    let server = build_server(
        addr,
        RateLimitConfig {
            max_concurrent_queries: 0,
            per_user_max_requests: 1,
            per_user_window_secs: 60,
            per_user_max_entries: 100,
        },
    );
    let handle = tokio::spawn(server.run());

    let channel = connect_with_retry(addr.port()).await;
    let mut client = KnowledgeGraphServiceClient::new(channel);

    let token_user1 = mint_token(1);
    let token_user2 = mint_token(2);

    // User 1 uses their single slot.
    let r = execute_query_stream(&mut client, &token_user1).await;
    assert!(r.is_ok());
    let mut s = r.unwrap();
    while s.next().await.is_some() {}

    // User 1 is now rate-limited.
    let r = execute_query_stream(&mut client, &token_user1).await;
    assert!(r.is_err());
    assert_eq!(r.unwrap_err().code(), tonic::Code::ResourceExhausted);

    // User 2 should still be allowed.
    let r = execute_query_stream(&mut client, &token_user2).await;
    assert!(r.is_ok(), "user 2 should not be affected by user 1's limit");

    handle.abort();
}

#[tokio::test]
async fn no_rate_limiter_allows_all_queries() {
    let addr = bind_free_port().await;
    // Build server without rate limiter (default — no .with_rate_limiter call).
    let validator = Arc::new(JwtValidator::new(TEST_SECRET, 0).unwrap());
    let ontology = Arc::new(ontology::Ontology::load_embedded().expect("ontology must load"));
    let server = GrpcServer::new(
        addr,
        validator,
        ontology,
        &ClickHouseConfiguration::default(),
        ClusterHealthChecker::default().into_arc(),
        None,
        GrpcConfig::default(),
    );
    let handle = tokio::spawn(server.run());

    let channel = connect_with_retry(addr.port()).await;
    let mut client = KnowledgeGraphServiceClient::new(channel);
    let token = mint_token(1);

    // Fire 10 queries — none should be rate-limited.
    for i in 0..10 {
        let r = execute_query_stream(&mut client, &token).await;
        assert!(
            r.is_ok(),
            "query {i} should be accepted when rate limiter is disabled: {:?}",
            r.unwrap_err()
        );
        let mut s = r.unwrap();
        while s.next().await.is_some() {}
    }

    handle.abort();
}
