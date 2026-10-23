use super::*;
use crate::grpc::server::with_compression;
use tonic::codec::CompressionEncoding;

async fn response_encoding(accept_encoding: Option<&'static str>) -> Option<String> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(with_compression(OrbitServiceServer::new(test_service())))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let channel = tower::ServiceBuilder::new()
        .layer(tower::util::MapRequestLayer::new(
            move |mut request: tonic::codegen::http::Request<tonic::body::Body>| {
                match accept_encoding {
                    Some(value) => request
                        .headers_mut()
                        .insert("grpc-accept-encoding", value.parse().unwrap()),
                    None => request.headers_mut().remove("grpc-accept-encoding"),
                };
                request
            },
        ))
        .service(channel);
    let mut client = OrbitServiceClient::new(channel)
        .accept_compressed(CompressionEncoding::Gzip)
        .accept_compressed(CompressionEncoding::Zstd);

    let request = authed_request(GetGraphSchemaRequest {
        format: ResponseFormat::Llm as i32,
        ..Default::default()
    });
    let response = client.get_graph_schema(request).await.unwrap();
    response
        .metadata()
        .get("grpc-encoding")
        .map(|value| value.to_str().unwrap().to_string())
}

#[tokio::test]
async fn c_core_client_gets_gzip() {
    assert_eq!(
        response_encoding(Some("identity, deflate, gzip")).await,
        Some("gzip".into())
    );
}

#[tokio::test]
async fn zstd_client_gets_zstd() {
    assert_eq!(
        response_encoding(Some("identity, zstd")).await,
        Some("zstd".into())
    );
}

#[tokio::test]
async fn client_offering_both_gets_its_first_choice() {
    assert_eq!(
        response_encoding(Some("zstd, gzip")).await,
        Some("zstd".into())
    );
    assert_eq!(
        response_encoding(Some("gzip, zstd")).await,
        Some("gzip".into())
    );
}

#[tokio::test]
async fn client_advertising_neither_gets_identity() {
    assert_eq!(response_encoding(Some("identity, deflate")).await, None);
    assert_eq!(response_encoding(None).await, None);
}
