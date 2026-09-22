use super::*;
use crate::proto::ExecuteQueryRequest;
use crate::proto::orbit_service_client::OrbitServiceClient;
use crate::proto::orbit_service_server::OrbitServiceServer;
use tokio_stream::wrappers::TcpListenerStream;

#[tokio::test]
async fn execute_query_routes_by_language_and_rejects_unknown_selectors() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let shutdown = tokio_util::sync::CancellationToken::new();
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(OrbitServiceServer::new(test_service()))
            .serve_with_incoming_shutdown(
                TcpListenerStream::new(listener),
                shutdown.clone().cancelled_owned(),
            ),
    );
    let mut client = OrbitServiceClient::connect(format!("http://{address}"))
        .await
        .unwrap();
    for (query_type, language, succeeds) in
        [(0, 1, true), (0, 0, false), (99, 1, false), (0, 99, false)]
    {
        let request = ExecuteQueryRequest {
            query: "CALL db.schema('User')".into(),
            format: ResponseFormat::Raw as i32,
            query_type,
            language,
        };
        let mut stream = client
            .execute_query(authed_request(tokio_stream::iter([ExecuteQueryMessage {
                content: Some(execute_query_message::Content::Request(request)),
            }])))
            .await
            .unwrap()
            .into_inner();
        let response = tokio::time::timeout(std::time::Duration::from_secs(10), stream.message())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        match response.content.unwrap() {
            execute_query_message::Content::Result(_) => assert!(succeeds),
            execute_query_message::Content::Error(_) => assert!(!succeeds),
            other => panic!("unexpected response: {other:?}"),
        }
        assert!(stream.message().await.unwrap().is_none());
    }
    shutdown.cancel();
    server.await.unwrap().unwrap();
}
