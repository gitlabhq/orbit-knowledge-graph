use std::net::SocketAddr;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

use super::*;
use crate::auth::claims::TraversalPathClaim;
use crate::grpc::GrpcServer;
use crate::proto::{ExecuteQueryRequest, execute_query_message};

const WAIT: Duration = Duration::from_secs(10);

async fn serve_until(
    shutdown: CancellationToken,
    drain_timeout_secs: u64,
) -> (SocketAddr, JoinHandle<Result<(), tonic::transport::Error>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let defaults = orbit_server_config::AppConfig::embedded_defaults();
    let mut grpc = defaults.grpc;
    grpc.shutdown_drain_timeout_secs = drain_timeout_secs;
    let server = GrpcServer::new(
        Arc::new(mock_validator()),
        ActiveSchema::pinned(test_ontology()),
        test_client(),
        ClusterHealthChecker::default().into_arc(),
        None,
        grpc,
        Arc::new(defaults.analytics),
    );
    let handle = tokio::spawn(server.run_until_shutdown(listener, shutdown));
    (addr, handle)
}

async fn connect(addr: SocketAddr) -> OrbitServiceClient<tonic::transport::Channel> {
    OrbitServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap()
}

/// Opens a query stream and holds it open: the server waits for the first
/// message until the returned sender sends it.
async fn open_query_stream(
    client: &mut OrbitServiceClient<tonic::transport::Channel>,
) -> (
    mpsc::Sender<ExecuteQueryMessage>,
    tonic::Streaming<ExecuteQueryMessage>,
) {
    let (tx, rx) = mpsc::channel(1);
    let request = signed_request(
        ReceiverStream::new(rx),
        Claims {
            source_type: SourceType::Mcp,
            group_traversal_ids: vec![TraversalPathClaim {
                path: TraversalPath::new_unchecked("1/"),
                access_levels: vec![20],
            }],
            ..test_claims()
        },
    );
    let responses = client.execute_query(request).await.unwrap().into_inner();
    (tx, responses)
}

// The query fails to compile, so the server answers without ClickHouse.
fn query_message() -> ExecuteQueryMessage {
    ExecuteQueryMessage {
        content: Some(execute_query_message::Content::Request(
            ExecuteQueryRequest {
                query: "MATCH (a:NoSuchEntity) RETURN a.id".into(),
                language: QueryLanguage::Gql as i32,
                ..Default::default()
            },
        )),
    }
}

async fn wait_until_refused(addr: SocketAddr) {
    tokio::time::timeout(WAIT, async {
        while tokio::net::TcpStream::connect(addr).await.is_ok() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("listener still accepts connections after shutdown");
}

// HTTP/2 graceful shutdown sends a first GOAWAY that still admits streams,
// then a final one after a ping round trip, so refusal is not immediate.
async fn wait_until_new_streams_fail(client: &mut OrbitServiceClient<tonic::transport::Channel>) {
    tokio::time::timeout(WAIT, async {
        while client
            .list_tools(authed_request(ListToolsRequest::default()))
            .await
            .is_ok()
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("connection still accepts new streams after shutdown");
}

#[tokio::test]
async fn shutdown_drains_in_flight_streams_and_refuses_new_ones() {
    let shutdown = CancellationToken::new();
    let (addr, server) = serve_until(shutdown.clone(), 150).await;
    let mut client = connect(addr).await;
    let (tx, mut responses) = open_query_stream(&mut client).await;

    shutdown.cancel();
    wait_until_refused(addr).await;
    wait_until_new_streams_fail(&mut client).await;
    assert!(
        !server.is_finished(),
        "server must wait for the open stream"
    );

    tx.send(query_message()).await.unwrap();
    drop(tx);
    let response = responses
        .message()
        .await
        .expect("in-flight stream was cut off")
        .expect("in-flight stream ended without a response");
    assert!(
        matches!(
            response.content,
            Some(execute_query_message::Content::Error(_))
        ),
        "{response:?}"
    );
    assert!(responses.message().await.unwrap().is_none());

    tokio::time::timeout(WAIT, server)
        .await
        .expect("server did not stop after the drain")
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn shutdown_stops_waiting_after_the_drain_timeout() {
    let shutdown = CancellationToken::new();
    let (addr, server) = serve_until(shutdown.clone(), 1).await;
    let mut client = connect(addr).await;
    let (_tx, _responses) = open_query_stream(&mut client).await;

    shutdown.cancel();
    tokio::time::timeout(WAIT, server)
        .await
        .expect("server ignored the drain timeout")
        .unwrap()
        .unwrap();
}
