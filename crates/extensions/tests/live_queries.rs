//! Live queries end to end, with no integration name: a fake provider serves the
//! `example.widgets` live operations over gRPC, and Orbit's live engine resolves
//! them in parallel, honours deadlines, and reports a partial status per key. The
//! compile-time rules (a live property is returnable but never filterable) are
//! checked against the merged model.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use extensions::live::{
    KeyStatus, LiveCall, LiveCatalog, LiveError, LiveOperationSpec, LiveProvider, LiveStream,
    LiveValue, run_live_operations,
};
use extensions::loader::load_merged_ontology;
use extensions::package::{Package, short_digest};
use extensions::proto::provider_server::{Provider, ProviderServer};
use extensions::proto::{
    GetManifestRequest, GetPackageRequest, Key, LiveRequest, LiveResponse, Manifest,
    Package as ProtoPackage, PackageRef, Row, Status as ProtoStatus, Value, live_response, value,
};
use extensions::provider::{ProviderClient, ProviderConfig};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

const WIDGETS: &str = include_str!("fixtures/example.widgets.yaml");
const GADGETS: &str = include_str!("fixtures/example.gadgets.yaml");
const SCOPE_PATH: &str = "1/9000000000042/";
const USER_ID: i64 = 42;

/// How the fake provider answers one operation: a delay before it streams, keys it
/// never answers (to force a timeout), and keys it fails with a status.
#[derive(Clone, Default)]
struct OperationBehavior {
    latency: Duration,
    answer_only_first: Option<usize>,
    fail: HashMap<Vec<String>, ProtoStatus>,
}

#[derive(Clone, Default)]
struct FakeProvider {
    digest: String,
    behaviors: HashMap<String, OperationBehavior>,
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
        request: Request<LiveRequest>,
    ) -> Result<Response<Self::LiveStream>, Status> {
        let request = request.into_inner();
        let behavior = self
            .behaviors
            .get(&request.operation)
            .cloned()
            .unwrap_or_default();
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        tokio::spawn(async move {
            if !behavior.latency.is_zero() {
                tokio::time::sleep(behavior.latency).await;
            }
            for (index, key) in request.keys.iter().enumerate() {
                if behavior.answer_only_first.is_some_and(|n| index >= n) {
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    return;
                }
                if let Some(status) = behavior.fail.get(&key.values) {
                    let _ = tx.send(Ok(status_response(key, *status))).await;
                    continue;
                }
                let _ = tx.send(Ok(row_response(&request.operation, key))).await;
                let _ = tx.send(Ok(status_response(key, ProtoStatus::Ok))).await;
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

fn row_response(operation: &str, key: &Key) -> LiveResponse {
    let mut properties = HashMap::new();
    match operation {
        "widgets.get" => {
            properties.insert(
                "colour".into(),
                string_value(format!("colour-{}", key.values[0])),
            );
            properties.insert("weight".into(), float_value(1.5));
        }
        "gadgets.get" => {
            properties.insert("shininess".into(), float_value(2.0));
        }
        _ => {}
    }
    LiveResponse {
        kind: Some(live_response::Kind::Row(Row {
            key: Some(key.clone()),
            properties,
        })),
    }
}

fn status_response(key: &Key, status: ProtoStatus) -> LiveResponse {
    LiveResponse {
        kind: Some(live_response::Kind::Status(extensions::proto::KeyStatus {
            key: Some(key.clone()),
            status: status as i32,
            message: String::new(),
        })),
    }
}

fn string_value(value: String) -> Value {
    Value {
        kind: Some(value::Kind::StringValue(value)),
    }
}

fn float_value(value: f64) -> Value {
    Value {
        kind: Some(value::Kind::FloatValue(value)),
    }
}

async fn spawn_provider(behaviors: HashMap<String, OperationBehavior>) -> (SocketAddr, Vec<u8>) {
    let key = vec![9u8; 32];
    let digest = short_digest(WIDGETS);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let provider = FakeProvider { digest, behaviors };
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

fn widgets_catalog() -> LiveCatalog {
    let package = Package::parse(WIDGETS, short_digest(WIDGETS)).expect("fixture parses");
    LiveCatalog::from_packages(&[package])
}

fn spec(catalog: &LiveCatalog, operation_id: &str) -> LiveOperationSpec {
    catalog
        .get(operation_id)
        .expect("operation present")
        .clone()
}

fn behavior(latency_ms: u64) -> OperationBehavior {
    OperationBehavior {
        latency: Duration::from_millis(latency_ms),
        ..Default::default()
    }
}

#[tokio::test]
async fn a_returned_live_property_is_resolved_from_the_provider() {
    let behaviors = HashMap::from([("widgets.get".to_string(), behavior(0))]);
    let (addr, key) = spawn_provider(behaviors).await;
    let client = connect(addr, key).await;
    let catalog = widgets_catalog();

    let call = LiveCall {
        spec: spec(&catalog, "example.widgets|widgets.get"),
        keys: vec![vec!["w1".into(), "active".into()]],
    };
    let outcomes = run_live_operations(
        &client,
        &[SCOPE_PATH.into()],
        USER_ID,
        vec![call],
        Duration::from_secs(3),
    )
    .await
    .expect("live operations run");

    let outcome = &outcomes[0];
    let key = vec!["w1".to_string(), "active".to_string()];
    assert_eq!(outcome.statuses[&key], KeyStatus::Ok);
    let row = &outcome.rows[&key];
    assert_eq!(row["colour"], LiveValue::String("colour-w1".into()));
    assert_eq!(row["weight"], LiveValue::Float(1.5));
}

#[tokio::test]
async fn two_operations_run_in_parallel_not_in_sequence() {
    let behaviors = HashMap::from([
        ("widgets.get".to_string(), behavior(300)),
        ("gadgets.get".to_string(), behavior(300)),
    ]);
    let (addr, key) = spawn_provider(behaviors).await;
    let client = connect(addr, key).await;
    let catalog = widgets_catalog();

    let calls = vec![
        LiveCall {
            spec: spec(&catalog, "example.widgets|widgets.get"),
            keys: vec![vec!["w1".into(), "active".into()]],
        },
        LiveCall {
            spec: spec(&catalog, "example.widgets|gadgets.get"),
            keys: vec![vec!["g1".into()]],
        },
    ];

    let started = Instant::now();
    let outcomes = run_live_operations(
        &client,
        &[SCOPE_PATH.into()],
        USER_ID,
        calls,
        Duration::from_secs(3),
    )
    .await
    .expect("live operations run");
    let elapsed = started.elapsed();

    assert_eq!(outcomes.len(), 2);
    assert!(
        elapsed < Duration::from_millis(550),
        "two 300ms operations in parallel should finish well under 600ms, took {elapsed:?}"
    );
    assert!(
        elapsed >= Duration::from_millis(250),
        "the slower operation still gates the result, took {elapsed:?}"
    );
}

#[tokio::test]
async fn a_timed_out_key_is_null_while_the_rest_stay_intact() {
    let behaviors = HashMap::from([(
        "widgets.get".to_string(),
        OperationBehavior {
            answer_only_first: Some(1),
            ..Default::default()
        },
    )]);
    let (addr, key) = spawn_provider(behaviors).await;
    let client = connect(addr, key).await;
    let catalog = widgets_catalog();

    let mut operation = spec(&catalog, "example.widgets|widgets.get");
    operation.timeout_ms = 250;
    let answered = vec!["w1".to_string(), "active".to_string()];
    let timed_out = vec!["w2".to_string(), "active".to_string()];
    let call = LiveCall {
        spec: operation,
        keys: vec![answered.clone(), timed_out.clone()],
    };

    let outcomes = run_live_operations(
        &client,
        &[SCOPE_PATH.into()],
        USER_ID,
        vec![call],
        Duration::from_secs(3),
    )
    .await
    .expect("live operations run");

    let outcome = &outcomes[0];
    assert_eq!(outcome.statuses[&answered], KeyStatus::Ok);
    assert!(
        outcome.rows.contains_key(&answered),
        "the fast key resolved"
    );
    assert_eq!(outcome.statuses[&timed_out], KeyStatus::Timeout);
    assert!(
        !outcome.rows.contains_key(&timed_out),
        "the slow key yields null, not a stale value"
    );
}

#[tokio::test]
async fn a_key_set_larger_than_max_keys_fails_the_query() {
    let (addr, key) = spawn_provider(HashMap::new()).await;
    let client = connect(addr, key).await;
    let catalog = widgets_catalog();

    let mut operation = spec(&catalog, "example.widgets|widgets.get");
    operation.max_keys = 2;
    let call = LiveCall {
        spec: operation,
        keys: vec![
            vec!["a".into(), "s".into()],
            vec!["b".into(), "s".into()],
            vec!["c".into(), "s".into()],
        ],
    };

    let error = run_live_operations(
        &client,
        &[SCOPE_PATH.into()],
        USER_ID,
        vec![call],
        Duration::from_secs(3),
    )
    .await
    .expect_err("an oversized key set is rejected");
    assert!(
        matches!(error, LiveError::MaxKeysExceeded { .. }),
        "{error:?}"
    );
}

#[tokio::test]
async fn a_where_on_a_live_property_is_a_compile_error_but_a_return_is_not() {
    let package = Package::parse(WIDGETS, short_digest(WIDGETS)).expect("fixture parses");
    let ontology = load_merged_ontology(orbit_versions::VERSIONS.schema, &[package])
        .expect("merged model loads");
    let data_model =
        query_data_model::ClickHouseDataModel::derive(Arc::new(ontology)).expect("model derives");
    let data_model = Arc::new(data_model);
    let security =
        compiler::SecurityContext::new(USER_ID, vec![SCOPE_PATH.into()]).expect("security context");

    let returned = compiler::compile_model(
        "MATCH (w:Widget) WHERE w.name = \"hinge\" RETURN w.colour LIMIT 10",
        compiler::Frontend::Gql,
        &data_model,
        &security,
    );
    assert!(
        returned.is_ok(),
        "a live property must be returnable: {returned:?}"
    );

    let filtered = compiler::compile_model(
        "MATCH (w:Widget) WHERE w.name = \"hinge\" AND w.colour = \"red\" RETURN w.id LIMIT 10",
        compiler::Frontend::Gql,
        &data_model,
        &security,
    );
    assert!(
        filtered.is_err(),
        "a WHERE on a live property must be a compile error"
    );
}

#[tokio::test]
async fn connect_live_providers_maps_each_package_to_a_working_client() {
    let behaviors = HashMap::from([("widgets.get".to_string(), behavior(0))]);
    let (addr, key) = spawn_provider(behaviors).await;
    let key_dir = tempfile::tempdir().unwrap();
    let key_path = key_dir.path().join("provider.key");
    std::fs::write(&key_path, &key).unwrap();
    let config = ProviderConfig {
        name: "example".into(),
        url: format!("http://{addr}"),
        jwt_key_file: key_path.to_str().unwrap().to_string(),
        refresh_secs: 60,
    };

    let providers =
        extensions::bootstrap::connect_live_providers(std::slice::from_ref(&config)).await;
    let provider = providers
        .get("example.widgets")
        .expect("the package maps to a client");

    let catalog = widgets_catalog();
    let call = LiveCall {
        spec: spec(&catalog, "example.widgets|widgets.get"),
        keys: vec![vec!["w1".into(), "active".into()]],
    };
    let outcomes = run_live_operations(
        provider.as_ref(),
        &[SCOPE_PATH.into()],
        USER_ID,
        vec![call],
        Duration::from_secs(3),
    )
    .await
    .expect("the mapped client resolves a live call");
    let key = vec!["w1".to_string(), "active".to_string()];
    assert_eq!(outcomes[0].statuses[&key], KeyStatus::Ok);
}

#[tokio::test]
async fn a_package_without_a_live_block_makes_no_live_call() {
    let package = Package::parse(GADGETS, short_digest(GADGETS)).expect("fixture parses");
    let catalog = LiveCatalog::from_packages(&[package]);
    assert!(catalog.is_empty(), "no live block means no live operations");

    let outcomes = run_live_operations(
        &PanicProvider,
        &[SCOPE_PATH.into()],
        USER_ID,
        Vec::new(),
        Duration::from_secs(3),
    )
    .await
    .expect("no calls run");
    assert!(
        outcomes.is_empty(),
        "no operation, no outcome, no provider hit"
    );
}

/// Orbit's live-stage overhead apart from the source: one live operation against a
/// provider that answers at once. The difference from a no-live baseline is the
/// gRPC round trip plus the engine's orchestration. Ignored by default; run with
/// `cargo test -p extensions --test live_queries -- --ignored --nocapture`.
#[tokio::test]
#[ignore = "benchmark; run explicitly"]
async fn live_stage_overhead_p50_p95() {
    const ITERATIONS: usize = 200;
    let behaviors = HashMap::from([("widgets.get".to_string(), behavior(0))]);
    let (addr, key) = spawn_provider(behaviors).await;
    let client = connect(addr, key).await;
    let catalog = widgets_catalog();
    let operation = spec(&catalog, "example.widgets|widgets.get");

    let mut baseline = Vec::with_capacity(ITERATIONS);
    let mut with_live = Vec::with_capacity(ITERATIONS);
    for _ in 0..ITERATIONS {
        let start = Instant::now();
        run_live_operations(
            &client,
            &[SCOPE_PATH.into()],
            USER_ID,
            Vec::new(),
            Duration::from_secs(3),
        )
        .await
        .unwrap();
        baseline.push(start.elapsed());

        let call = LiveCall {
            spec: operation.clone(),
            keys: vec![vec!["w1".into(), "active".into()]],
        };
        let start = Instant::now();
        run_live_operations(
            &client,
            &[SCOPE_PATH.into()],
            USER_ID,
            vec![call],
            Duration::from_secs(3),
        )
        .await
        .unwrap();
        with_live.push(start.elapsed());
    }

    let (b50, b95) = percentiles(&mut baseline);
    let (l50, l95) = percentiles(&mut with_live);
    println!("no-live   p50={b50:?} p95={b95:?}");
    println!("one-live  p50={l50:?} p95={l95:?}");
    println!(
        "overhead  p50={:?} p95={:?}",
        l50.saturating_sub(b50),
        l95.saturating_sub(b50)
    );
    assert!(
        l95 < Duration::from_millis(30),
        "one live stage p95 {l95:?} should stay under the 30ms Orbit-overhead target"
    );
}

fn percentiles(samples: &mut [Duration]) -> (Duration, Duration) {
    samples.sort_unstable();
    let at = |p: f64| samples[((samples.len() as f64 * p) as usize).min(samples.len() - 1)];
    (at(0.50), at(0.95))
}

struct PanicProvider;

#[async_trait]
impl LiveProvider for PanicProvider {
    async fn live(&self, _request: LiveRequest) -> Result<LiveStream, Status> {
        panic!("the provider must not be called when a query has no live properties");
    }
}
