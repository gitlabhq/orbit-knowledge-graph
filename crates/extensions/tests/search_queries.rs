//! Searchable live entities end to end, with no integration name: a fake provider
//! serves `messages.search` rows that carry a graph id and outgoing edges, and
//! Orbit's search engine seeds rows, pushes filters down, joins the seed edges to
//! the next indexed hop, and (for the inbound shape) finds the non-indexed rows
//! that point at an indexed anchor. These mirror the two acceptance queries:
//!
//! - `(m:SlackMessage)-[:CITES]->(i:Incident)-[:CITES]->(g:GitLabRef) WHERE m.text CONTAINS 'deploy'`
//! - `(i:Incident)<-[:CITES]-(m:SlackMessage)-[:CITES]->(g:GitLabRef) WHERE i.reference = 'INC-15808'`
//!
//! The indexed hops (Incident, GitLabRef tables) are not exercised here; the test
//! asserts the id sets the engine hands the stored query, plus the seed rows.

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::pin::Pin;
use std::time::Duration;

use extensions::live::LiveValue;
use extensions::package::{Package, short_digest};
use extensions::proto::provider_server::{Provider, ProviderServer};
use extensions::proto::{
    Edge, GetManifestRequest, GetPackageRequest, LiveRequest, LiveResponse, Manifest,
    Package as ProtoPackage, PackageRef, Row, Status as ProtoStatus, Value, live_response, value,
};
use extensions::provider::{ProviderClient, ProviderConfig};
use extensions::search::{
    Caller, InboundEdge, InboundTarget, SearchCatalog, SearchError, SearchSpec, run_inbound_search,
    run_search, target_ids,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

const MESSAGES: &str = include_str!("fixtures/example.messages.yaml");
const SCOPE_PATH: &str = "1/9000000000042/";
const USER_ID: i64 = 42;

/// A message the fake provider can return: its graph id, searchable text, and its
/// outgoing edges as `(relationship, target entity, target id)`.
struct FakeMessage {
    id: i64,
    text: &'static str,
    edges: Vec<(&'static str, &'static str, i64)>,
}

#[derive(Default)]
struct SearchBehavior {
    latency: Duration,
    fail_with: Option<ProtoStatus>,
}

struct FakeProvider {
    digest: String,
    messages: Vec<FakeMessage>,
    behavior: SearchBehavior,
}

#[tonic::async_trait]
impl Provider for FakeProvider {
    async fn get_manifest(
        &self,
        _request: Request<GetManifestRequest>,
    ) -> Result<Response<Manifest>, Status> {
        Ok(Response::new(Manifest {
            provider: "example".into(),
            scopes_table: "ex_scopes".into(),
            packages: vec![PackageRef {
                name: "example.messages".into(),
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
        Ok(Response::new(ProtoPackage {
            name: request.name,
            version: request.version,
            digest: self.digest.clone(),
            document_yaml: MESSAGES.to_string(),
        }))
    }

    type LiveStream =
        Pin<Box<dyn tokio_stream::Stream<Item = Result<LiveResponse, Status>> + Send>>;

    async fn live(
        &self,
        request: Request<LiveRequest>,
    ) -> Result<Response<Self::LiveStream>, Status> {
        let request = request.into_inner();
        let query = request.filters.get("text").cloned().unwrap_or_default();
        let hits: Vec<LiveResponse> = self
            .messages
            .iter()
            .filter(|message| message.text.contains(&query))
            .map(row_response)
            .collect();
        let fail = self.behavior.fail_with;
        let latency = self.behavior.latency;

        let (tx, rx) = tokio::sync::mpsc::channel(64);
        tokio::spawn(async move {
            if !latency.is_zero() {
                tokio::time::sleep(latency).await;
            }
            if let Some(status) = fail {
                let _ = tx.send(Ok(status_response(status))).await;
                return;
            }
            for hit in hits {
                let _ = tx.send(Ok(hit)).await;
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

fn row_response(message: &FakeMessage) -> LiveResponse {
    let properties = HashMap::from([(
        "text".to_string(),
        Value {
            kind: Some(value::Kind::StringValue(message.text.to_string())),
        },
    )]);
    let edges = message
        .edges
        .iter()
        .map(|(relationship, target_entity, target_id)| Edge {
            relationship: (*relationship).to_string(),
            target_entity: (*target_entity).to_string(),
            target_id: *target_id,
        })
        .collect();
    LiveResponse {
        kind: Some(live_response::Kind::Row(Row {
            key: None,
            properties,
            id: message.id,
            edges,
        })),
    }
}

fn status_response(status: ProtoStatus) -> LiveResponse {
    LiveResponse {
        kind: Some(live_response::Kind::Status(extensions::proto::KeyStatus {
            key: None,
            status: status as i32,
            message: "boom".into(),
        })),
    }
}

fn fake_messages() -> Vec<FakeMessage> {
    vec![
        FakeMessage {
            id: 101,
            text: "deploy failed, see INC-15808",
            edges: vec![("CITES", "Incident", 10), ("CITES", "GitLabRef", 900)],
        },
        FakeMessage {
            id: 102,
            text: "deploy rollback complete",
            edges: vec![("CITES", "Incident", 20), ("CITES", "GitLabRef", 901)],
        },
        FakeMessage {
            id: 103,
            text: "team lunch",
            edges: vec![("CITES", "GitLabRef", 902)],
        },
        FakeMessage {
            id: 104,
            text: "INC-15808 postmortem notes",
            edges: vec![("CITES", "Incident", 20), ("CITES", "GitLabRef", 903)],
        },
    ]
}

async fn spawn_provider(behavior: SearchBehavior) -> (SocketAddr, Vec<u8>) {
    let key = vec![9u8; 32];
    let provider = FakeProvider {
        digest: short_digest(MESSAGES),
        messages: fake_messages(),
        behavior,
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(ProviderServer::new(provider))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    (addr, key)
}

async fn connect(addr: SocketAddr, key: Vec<u8>) -> ProviderClient {
    let key_dir = tempfile::tempdir().unwrap();
    let key_path = key_dir.path().join("provider.key");
    std::fs::write(&key_path, &key).unwrap();
    ProviderClient::connect(&ProviderConfig {
        name: "example".into(),
        url: format!("http://{addr}"),
        jwt_key_file: key_path.to_str().unwrap().to_string(),
        refresh_secs: 60,
    })
    .await
    .expect("client connects")
}

fn scope_paths() -> [String; 1] {
    [SCOPE_PATH.to_string()]
}

fn slack_spec() -> SearchSpec {
    let package = Package::parse(MESSAGES, short_digest(MESSAGES)).expect("fixture parses");
    SearchCatalog::from_packages(&[package])
        .get("SlackMessage")
        .expect("SlackMessage is searchable")
        .clone()
}

fn text_of(row: &extensions::search::SeedRow) -> &str {
    match row.properties.get("text") {
        Some(LiveValue::String(text)) => text,
        other => panic!("expected a text property, got {other:?}"),
    }
}

#[tokio::test]
async fn start_of_pattern_seed_feeds_the_first_hop_id_set() {
    let (addr, key) = spawn_provider(SearchBehavior::default()).await;
    let client = connect(addr, key).await;
    let spec = slack_spec();

    let filters = BTreeMap::from([("text".to_string(), "deploy".to_string())]);
    let scopes = scope_paths();
    let caller = Caller {
        scope_paths: &scopes,
        user_id: USER_ID,
    };
    let rows = run_search(&client, &spec, caller, filters, Duration::from_secs(3))
        .await
        .expect("search runs");

    let mut ids: Vec<i64> = rows.iter().map(|row| row.id).collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![101, 102], "the seed rows are the deploy messages");

    let incident_ids = target_ids(&rows, "CITES", "Incident");
    assert_eq!(
        incident_ids,
        vec![10, 20],
        "the first hop (m)-[:CITES]->(i:Incident) is constrained to the seed's cited incidents"
    );
    assert!(
        rows.iter().any(|row| text_of(row).contains("deploy")),
        "the seed m nodes carry their live properties"
    );
}

#[tokio::test]
async fn inbound_search_finds_rows_that_point_at_an_indexed_anchor() {
    let (addr, key) = spawn_provider(SearchBehavior::default()).await;
    let client = connect(addr, key).await;
    let spec = slack_spec();

    // The indexed part resolved Incident 10 (reference INC-15808) before the search.
    let targets = vec![InboundTarget {
        id: 10,
        cited_by_value: "INC-15808".to_string(),
    }];

    let scopes = scope_paths();
    let caller = Caller {
        scope_paths: &scopes,
        user_id: USER_ID,
    };
    let matches = run_inbound_search(
        &client,
        &spec,
        InboundEdge {
            relationship: "CITES",
            target_entity: "Incident",
        },
        targets,
        &BTreeMap::new(),
        caller,
        Duration::from_secs(3),
    )
    .await
    .expect("inbound search runs");

    assert_eq!(matches.len(), 1);
    let anchor = &matches[0];
    assert_eq!(anchor.target_id, 10);
    let ids: Vec<i64> = anchor.rows.iter().map(|row| row.id).collect();
    assert_eq!(
        ids,
        vec![101],
        "only the message that actually cites incident 10 is kept; the INC-15808 decoy citing 20 is dropped"
    );
    assert_eq!(
        target_ids(&anchor.rows, "CITES", "GitLabRef"),
        vec![900],
        "the rest of the pattern (m)-[:CITES]->(g:GitLabRef) follows the kept rows' edges"
    );
}

#[tokio::test]
async fn more_anchors_than_max_rows_is_a_clear_error() {
    let (addr, key) = spawn_provider(SearchBehavior::default()).await;
    let client = connect(addr, key).await;
    let mut spec = slack_spec();
    spec.max_rows = 1;

    let targets = (0..2)
        .map(|i| InboundTarget {
            id: i,
            cited_by_value: String::new(),
        })
        .collect();

    let scopes = scope_paths();
    let caller = Caller {
        scope_paths: &scopes,
        user_id: USER_ID,
    };
    let error = run_inbound_search(
        &client,
        &spec,
        InboundEdge {
            relationship: "CITES",
            target_entity: "Incident",
        },
        targets,
        &BTreeMap::new(),
        caller,
        Duration::from_secs(3),
    )
    .await
    .expect_err("too many anchors is rejected");
    assert!(
        matches!(error, SearchError::TooManyTargets { .. }),
        "{error:?}"
    );
}

#[tokio::test]
async fn a_provider_error_fails_the_search_rather_than_returning_partial_rows() {
    let (addr, key) = spawn_provider(SearchBehavior {
        fail_with: Some(ProtoStatus::Error),
        ..Default::default()
    })
    .await;
    let client = connect(addr, key).await;
    let spec = slack_spec();

    let scopes = scope_paths();
    let caller = Caller {
        scope_paths: &scopes,
        user_id: USER_ID,
    };
    let error = run_search(
        &client,
        &spec,
        caller,
        BTreeMap::from([("text".to_string(), "deploy".to_string())]),
        Duration::from_secs(3),
    )
    .await
    .expect_err("a provider error is surfaced");
    assert!(matches!(error, SearchError::Provider { .. }), "{error:?}");
}

#[tokio::test]
async fn a_search_past_its_deadline_is_a_clear_timeout() {
    let (addr, key) = spawn_provider(SearchBehavior {
        latency: Duration::from_secs(30),
        ..Default::default()
    })
    .await;
    let client = connect(addr, key).await;
    let spec = slack_spec();

    let scopes = scope_paths();
    let caller = Caller {
        scope_paths: &scopes,
        user_id: USER_ID,
    };
    let error = run_search(
        &client,
        &spec,
        caller,
        BTreeMap::from([("text".to_string(), "deploy".to_string())]),
        Duration::from_millis(200),
    )
    .await
    .expect_err("a search past its deadline errors");
    assert!(matches!(error, SearchError::Timeout { .. }), "{error:?}");
}
