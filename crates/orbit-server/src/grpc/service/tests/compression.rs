use super::*;
use crate::grpc::GrpcServer;
use tonic::codec::CompressionEncoding;

const CURRENT_PATH: &str = "/orbit.v1.OrbitService/";
const LEGACY_PATH: &str = "/gkg.v1.KnowledgeGraphService/";

async fn serve_registered_server() -> tonic::transport::Channel {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let defaults = orbit_server_config::AppConfig::embedded_defaults();
    let server = GrpcServer::new(
        Arc::new(mock_validator()),
        ActiveSchema::pinned(test_ontology()),
        test_client(),
        ClusterHealthChecker::default().into_arc(),
        None,
        defaults.grpc,
        Arc::new(defaults.analytics),
    );
    tokio::spawn(server.run(listener));
    tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap()
}

async fn response_encoding(
    service_path: &'static str,
    accept_encoding: Option<&'static str>,
) -> Option<String> {
    let channel = tower::ServiceBuilder::new()
        .layer(tower::util::MapRequestLayer::new(
            move |mut request: tonic::codegen::http::Request<tonic::body::Body>| {
                match accept_encoding {
                    Some(value) => request
                        .headers_mut()
                        .insert("grpc-accept-encoding", value.parse().unwrap()),
                    None => request.headers_mut().remove("grpc-accept-encoding"),
                };
                let path = request.uri().path().replacen(CURRENT_PATH, service_path, 1);
                *request.uri_mut() = path.parse().unwrap();
                request
            },
        ))
        .service(serve_registered_server().await);
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
async fn negotiates_encoding_on_current_and_legacy_paths() {
    let cases = [
        (Some("identity, deflate, gzip"), Some("gzip")),
        (Some("identity,deflate,gzip"), Some("gzip")),
        (Some("identity, zstd"), Some("zstd")),
        (Some("zstd, gzip"), Some("zstd")),
        (Some("gzip, zstd"), Some("gzip")),
        (Some("identity, deflate"), None),
        (None, None),
    ];
    for path in [CURRENT_PATH, LEGACY_PATH] {
        for (accept, expected) in cases {
            assert_eq!(
                response_encoding(path, accept).await.as_deref(),
                expected,
                "{path} accepting {accept:?}"
            );
        }
    }
}

#[tokio::test]
async fn accepts_compressed_requests() {
    for encoding in [CompressionEncoding::Gzip, CompressionEncoding::Zstd] {
        let mut client =
            OrbitServiceClient::new(serve_registered_server().await).send_compressed(encoding);
        client
            .get_graph_schema(authed_request(GetGraphSchemaRequest::default()))
            .await
            .unwrap_or_else(|status| panic!("{encoding:?} request rejected: {status}"));
    }
}
