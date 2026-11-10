//! End-to-end check of the extensions platform over a real gRPC round trip,
//! without any integration name. A fake provider serves `example.widgets`,
//! verifies the request JWT, and Orbit drives the whole path: connect, fetch the
//! manifest, fetch and validate the package, merge it into the core model, and
//! derive the extension graph tables.

use std::net::SocketAddr;
use std::pin::Pin;

use extensions::loader::load_merged_ontology;
use extensions::proto::provider_server::{Provider, ProviderServer};
use extensions::proto::{
    GetManifestRequest, GetPackageRequest, LiveRequest, LiveResponse, Manifest,
    Package as ProtoPackage, PackageRef,
};
use extensions::provider::{ProviderClient, ProviderConfig};
use extensions::storage::extension_create_statements;
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use tonic::transport::Server;
use tonic::{Request, Response, Status};

const WIDGETS: &str = include_str!("fixtures/example.widgets.yaml");
const SCOPES_TABLE: &str = "ex_scopes";

struct FakeProvider {
    digest: String,
}

#[tonic::async_trait]
impl Provider for FakeProvider {
    async fn get_manifest(
        &self,
        _request: Request<GetManifestRequest>,
    ) -> Result<Response<Manifest>, Status> {
        Ok(Response::new(Manifest {
            provider: "example".into(),
            scopes_table: SCOPES_TABLE.into(),
            packages: vec![PackageRef {
                name: "example.widgets".into(),
                version: "0.1.0".into(),
                digest: self.digest.clone(),
            }],
        }))
    }

    async fn get_package(
        &self,
        request: Request<GetPackageRequest>,
    ) -> Result<Response<ProtoPackage>, Status> {
        let request = request.into_inner();
        assert_eq!(request.name, "example.widgets");
        Ok(Response::new(ProtoPackage {
            name: request.name,
            version: request.version,
            digest: self.digest.clone(),
            document_yaml: WIDGETS.to_string(),
        }))
    }

    type LiveStream =
        Pin<Box<dyn tokio_stream::Stream<Item = Result<LiveResponse, Status>> + Send>>;

    async fn live(
        &self,
        _request: Request<LiveRequest>,
    ) -> Result<Response<Self::LiveStream>, Status> {
        Err(Status::unimplemented("live is a later phase"))
    }
}

/// Rejects a call whose bearer token does not verify under the shared key with
/// the contract's issuer and audience. This is the provider side of E2 auth.
fn verify_bearer(key: Vec<u8>) -> impl FnMut(Request<()>) -> Result<Request<()>, Status> + Clone {
    move |request: Request<()>| {
        let header = request
            .metadata()
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .ok_or_else(|| Status::unauthenticated("missing bearer token"))?;
        let mut validation = Validation::new(Algorithm::HS256);
        validation.set_audience(&["orbit-provider"]);
        validation.set_issuer(&["orbit"]);
        jsonwebtoken::decode::<serde_json::Value>(
            header,
            &DecodingKey::from_secret(&key),
            &validation,
        )
        .map_err(|e| Status::unauthenticated(format!("bad token: {e}")))?;
        Ok(request)
    }
}

async fn spawn_provider(key: Vec<u8>, digest: String) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let service = ProviderServer::with_interceptor(FakeProvider { digest }, verify_bearer(key));
    tokio::spawn(async move {
        Server::builder()
            .add_service(service)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    addr
}

#[tokio::test]
async fn orbit_loads_a_provider_package_over_grpc() {
    let key = vec![9u8; 32];
    let digest = extensions::package::short_digest(WIDGETS);
    let addr = spawn_provider(key.clone(), digest.clone()).await;

    let key_dir = tempfile::tempdir().unwrap();
    let key_path = key_dir.path().join("provider.key");
    std::fs::write(&key_path, &key).unwrap();

    let config = ProviderConfig {
        name: "example".into(),
        url: format!("http://{addr}"),
        jwt_key_file: key_path.to_str().unwrap().to_string(),
        refresh_secs: 60,
    };
    let mut client = ProviderClient::connect(&config)
        .await
        .expect("client connects");

    let manifest = client.get_manifest().await.expect("manifest");
    assert_eq!(manifest.scopes_table, SCOPES_TABLE);
    let reference = &manifest.packages[0];

    let package = client
        .get_package(
            &reference.name,
            &reference.version,
            reference.digest.clone(),
        )
        .await
        .expect("package fetched and validated");
    assert_eq!(package.name(), "example.widgets");

    let ontology = load_merged_ontology(orbit_versions::VERSIONS.schema, &[package])
        .expect("package merges into the core model");
    let names: Vec<&str> = ontology.nodes().map(|n| n.name.as_str()).collect();
    assert!(names.contains(&"Widget") && names.contains(&"Gadget"));

    let statements = extension_create_statements(&ontology, orbit_versions::VERSIONS.schema)
        .expect("extension ddl");
    assert_eq!(statements.len(), 2, "one graph table per extension entity");
    assert!(
        statements
            .iter()
            .all(|(_, ddl)| ddl.contains("ReplacingMergeTree"))
    );
}

#[tokio::test]
async fn a_request_without_a_valid_jwt_is_rejected() {
    let digest = extensions::package::short_digest(WIDGETS);
    let addr = spawn_provider(vec![1u8; 32], digest).await;

    let wrong_key_dir = tempfile::tempdir().unwrap();
    let wrong_key_path = wrong_key_dir.path().join("wrong.key");
    std::fs::write(&wrong_key_path, vec![2u8; 32]).unwrap();

    let config = ProviderConfig {
        name: "example".into(),
        url: format!("http://{addr}"),
        jwt_key_file: wrong_key_path.to_str().unwrap().to_string(),
        refresh_secs: 60,
    };
    let mut client = ProviderClient::connect(&config)
        .await
        .expect("client connects");
    let error = client
        .get_manifest()
        .await
        .expect_err("a wrong key is rejected");
    assert!(format!("{error}").contains("authenticated") || format!("{error}").contains("token"));
}

#[tokio::test]
async fn bootstrap_merges_a_live_provider_into_the_model() {
    let key = vec![9u8; 32];
    let digest = extensions::package::short_digest(WIDGETS);
    let addr = spawn_provider(key.clone(), digest).await;

    let key_dir = tempfile::tempdir().unwrap();
    let key_path = key_dir.path().join("provider.key");
    std::fs::write(&key_path, &key).unwrap();

    let config = ProviderConfig {
        name: "example".into(),
        url: format!("http://{addr}"),
        jwt_key_file: key_path.to_str().unwrap().to_string(),
        refresh_secs: 60,
    };
    let model = extensions::bootstrap::load_provider_model(
        std::slice::from_ref(&config),
        orbit_versions::VERSIONS.schema,
        extensions::loader::CoreScope::Keep,
    )
    .await
    .expect("provider model loads");

    assert_eq!(model.scopes_tables, vec![SCOPES_TABLE.to_string()]);
    let names: Vec<&str> = model.ontology.nodes().map(|n| n.name.as_str()).collect();
    assert!(
        names.contains(&"Widget") && names.contains(&"Gadget"),
        "provider entities merged"
    );
    assert!(names.contains(&"Project"), "core entities still present");
}

#[tokio::test]
async fn bootstrap_serves_core_when_a_provider_is_unreachable() {
    let config = ProviderConfig {
        name: "down".into(),
        url: "http://127.0.0.1:1".into(),
        jwt_key_file: "/nonexistent".into(),
        refresh_secs: 60,
    };
    let model = extensions::bootstrap::load_provider_model(
        std::slice::from_ref(&config),
        orbit_versions::VERSIONS.schema,
        extensions::loader::CoreScope::Keep,
    )
    .await
    .expect("an unreachable provider still yields the core model");

    assert!(
        model.packages.is_empty(),
        "no packages from a down provider"
    );
    let names: Vec<&str> = model.ontology.nodes().map(|n| n.name.as_str()).collect();
    assert!(
        names.contains(&"Project"),
        "core served despite the provider being down"
    );
    assert!(
        !names.contains(&"Widget"),
        "no extension entities without a reachable provider"
    );
}
