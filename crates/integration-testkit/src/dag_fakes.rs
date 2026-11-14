//! Executors over the seeded fakes, so DAG scenarios run code and live stages end to end the
//! way `SeededColumnResolver` fakes Gitaly. Only tests register them; the server registers none.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use compiler::dag::{Backend, Connections, Contract, Env, Frontend, Peer, Request, Source};
use compiler::orbit_authorization::Key;
use compiler::orbit_authorization::{Answers, AuthError, Check, Checks, Module, Need, Provider};
use query_data_model::catalog::Row;
use query_data_model::implementations::live::DataExtraction;
use query_data_model::implementations::orbit_code::{CodeSearch, RequestField};
use query_data_model::{Catalog, ClickHouse, Model};
use query_engine::shared::dag::{
    DagError, Event, Executed, Executor, ExecutorError, Executors, Outcome, Recording, Runner,
    from_rows,
};
use serde_json::Value;

use crate::seeded_remote::{SeededCodeSearch, SeededDataExtraction};

fn unexpected(request: &Request) -> ExecutorError {
    ExecutorError(format!("this fake runs no {request:?}"))
}

/// The rows a fake returns, field by field in contract order.
fn batch(
    rows: &[Row],
    fields: &[String],
    contract: Option<&Contract>,
) -> Result<Executed, ExecutorError> {
    let contract =
        contract.ok_or_else(|| ExecutorError("a fake stage request carries a contract".into()))?;
    Ok(Executed {
        batches: vec![from_rows(rows, fields, contract)?],
        log: Vec::new(),
    })
}

/// Rows of a request without a contract, as a backend names them: integers as Int64, anything
/// else as text.
fn named(rows: &[Row], fields: &[String]) -> Result<Executed, ExecutorError> {
    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use arrow::datatypes::{Field, Schema};
    let mut schema = Vec::new();
    let mut arrays: Vec<ArrayRef> = Vec::new();
    for field in fields {
        let values: Vec<&Value> = rows
            .iter()
            .map(|row| row.get(field).unwrap_or(&Value::Null))
            .collect();
        let array: ArrayRef = match values.iter().all(|value| value.is_null() || value.is_i64()) {
            true => Arc::new(
                values
                    .iter()
                    .map(|value| value.as_i64())
                    .collect::<Int64Array>(),
            ),
            false => Arc::new(
                values
                    .iter()
                    .map(|value| match value {
                        Value::Null => None,
                        Value::String(text) => Some(text.clone()),
                        other => Some(other.to_string()),
                    })
                    .collect::<StringArray>(),
            ),
        };
        schema.push(Field::new(field, array.data_type().clone(), true));
        arrays.push(array);
    }
    let batch = arrow::record_batch::RecordBatch::try_new(Arc::new(Schema::new(schema)), arrays)
        .map_err(|error| ExecutorError(error.to_string()))?;
    Ok(Executed {
        batches: vec![batch],
        log: Vec::new(),
    })
}

/// Orbit Next's code indexing web servers, over `code_search.yaml`.
pub struct CodeExecutor(pub SeededCodeSearch);

#[async_trait]
impl Executor for CodeExecutor {
    async fn execute(
        &self,
        request: &Request,
        contract: Option<&Contract>,
    ) -> Result<Executed, ExecutorError> {
        let Request::Code(search) = request else {
            return Err(unexpected(request));
        };
        let rows = self
            .0
            .search(search)
            .map_err(|error| ExecutorError(error.0))?;
        let fields: Vec<String> = search
            .fields
            .iter()
            .map(|field| match field {
                RequestField::Line => "line".to_string(),
                RequestField::Column(name)
                | RequestField::Capture(name)
                | RequestField::Repository(name)
                | RequestField::Branch(name) => name.clone(),
            })
            .collect();
        batch(&rows, &fields, contract)
    }
}

/// The Data Extraction Service over `data_extraction.yaml`, signed in as one user.
pub struct LiveExecutor {
    pub service: SeededDataExtraction,
    pub user: String,
}

#[async_trait]
impl Executor for LiveExecutor {
    async fn execute(
        &self,
        request: &Request,
        contract: Option<&Contract>,
    ) -> Result<Executed, ExecutorError> {
        let Request::Live(call) = request else {
            return Err(unexpected(request));
        };
        let rows = self
            .service
            .extract(&self.user, call)
            .map_err(|error| ExecutorError(error.0))?;
        batch(&rows, &call.fields, contract)
    }
}

/// The graph connection in tests that assert plans without a container: each query returns the
/// seeded rows that hold any of the names its SQL gives its columns, so each stage reads the rows
/// seeded for its own bindings.
pub struct SeededGraph(pub Vec<Row>);

#[async_trait]
impl Executor for SeededGraph {
    async fn execute(
        &self,
        request: &Request,
        contract: Option<&Contract>,
    ) -> Result<Executed, ExecutorError> {
        let Request::Query(compiled) = request else {
            return Err(unexpected(request));
        };
        let columns = &compiled.base.columns;
        let rows: Vec<Row> = self
            .0
            .iter()
            .filter(|row| columns.iter().any(|column| row.contains_key(column)))
            .cloned()
            .collect();
        match (rows.is_empty(), contract) {
            (true, _) => Ok(Executed::default()),
            (false, Some(_)) => batch(&rows, columns, contract),
            (false, None) => named(&rows, columns),
        }
    }
}

/// A Rails stand-in whose checks on `resource` allow the listed keys; it allows every key of any
/// other resource.
pub struct AllowedKeys {
    pub resource: String,
    pub allowed: BTreeSet<Key>,
}

#[async_trait]
impl Provider<Check> for AllowedKeys {
    async fn call(&self, query: Check) -> Result<Vec<BTreeSet<Key>>, AuthError> {
        Ok(query
            .requests
            .into_iter()
            .map(|request| {
                let listed = request.resource == self.resource;
                request
                    .ids
                    .into_iter()
                    .filter(|key| !listed || self.allowed.contains(key))
                    .collect()
            })
            .collect())
    }
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AllowedOptions {
    pub resource: String,
    pub allowed: Vec<i64>,
}

impl Module for AllowedKeys {
    const KIND: &'static str = "fake.rails";
    type Options = AllowedOptions;

    fn build(options: AllowedOptions) -> Result<Self, AuthError> {
        Ok(Self {
            resource: options.resource,
            allowed: options.allowed.into_iter().map(Key::Int).collect(),
        })
    }

    fn answers(self: Arc<Self>, _: &[Need]) -> Result<Answers, AuthError> {
        Ok(Answers {
            check: Some(self),
            ..Answers::default()
        })
    }
}

impl AllowedKeys {
    /// The deployment's `rails` checks, which allow only `allowed` of `resource`.
    pub fn checks(resource: &str, allowed: impl IntoIterator<Item = Key>) -> Checks {
        let check = Self {
            resource: resource.to_string(),
            allowed: allowed.into_iter().collect(),
        };
        Checks::default().with("rails", Arc::new(check))
    }
}

pub fn seeded_code() -> Arc<dyn Executor> {
    Arc::new(CodeExecutor(SeededCodeSearch::from_seed_file()))
}

pub fn seeded_live(user: &str) -> Arc<dyn Executor> {
    Arc::new(LiveExecutor {
        service: SeededDataExtraction::from_seed_file(),
        user: user.to_string(),
    })
}

/// The embedded packages, the demo Slack package and the branch-head variant.
pub fn dag_packages() -> query_data_model::Packages {
    let heads = crate::authorization_variants::platform(&["orbit_next.heads"]);
    query_data_model::testkit::with_demo().with(heads)
}

/// The DAG packages, plus releases on a second ClickHouse connection keyed by UUID, the
/// deployments that name them on the graph connection, reviews on a DuckDB connection,
/// snippets that a code store on their own connection searches, and live posts whose `via`
/// heads are keyed by an integer and by an identity not named `id`.
pub fn peer_packages() -> query_data_model::Packages {
    let peers = crate::authorization_variants::platform(&[
        "peers.releases",
        "peers.deployments",
        "peers.lake",
        "peers.snippets",
        "peers.guilds",
    ]);
    dag_packages().with(peers)
}

/// `packages` with text replaced in some fragments, keeping each package's tier.
pub fn edited(
    packages: query_data_model::Packages,
    edits: &[(&str, &str, &str)],
) -> query_data_model::Packages {
    use std::collections::BTreeMap;
    let tiers = packages.tiers().clone();
    let mut by_tier: BTreeMap<query_data_model::Tier, BTreeMap<String, String>> = BTreeMap::new();
    for (name, text) in packages.sources() {
        let mut text = text.clone();
        for (path, find, with) in edits.iter().filter(|(path, ..)| *path == name) {
            assert!(text.contains(find), "{path} holds {find}");
            text = text.replace(find, with);
        }
        let tier = tiers[name.split('/').next().unwrap_or_default()];
        by_tier.entry(tier).or_default().insert(name.clone(), text);
    }
    by_tier
        .into_iter()
        .map(|(tier, sources)| query_data_model::Packages::new(tier, sources))
        .reduce(query_data_model::Packages::with)
        .expect("at least one package")
}

/// `packages` with `Head` enforcing only its `read_code` check on the code store.
pub fn heads_under_none(packages: query_data_model::Packages) -> query_data_model::Packages {
    edited(
        packages,
        &[(
            "orbit_next.heads/authorization.yaml",
            "enforce: {orbit_code: {scope_key: {via: HEAD_IN}}}",
            "enforce: {orbit_code: none}\n    checks: [{provider: rails, permission: read_code, resource: project, object: Project, id: project_id}]",
        )],
    )
}

pub fn dag_model() -> Arc<query_data_model::Model> {
    dag_packages().deployment("demo").unwrap()
}

/// Every connection of a deployment's model, and its ClickHouse graph connection.
pub fn dag_connections(model: &Arc<Model>) -> (Arc<Catalog<ClickHouse>>, Connections) {
    let connections = Connections::of(model).unwrap();
    let graph = connections
        .peers()
        .iter()
        .find_map(|peer| match peer {
            Peer::ClickHouse(catalog) => Some(Arc::clone(catalog)),
            _ => None,
        })
        .expect("the deployment stores a graph on ClickHouse");
    (graph, connections)
}

/// An executor that counts the requests it runs.
pub struct Counted {
    pub inner: Arc<dyn Executor>,
    pub requests: AtomicUsize,
}

#[async_trait]
impl Executor for Counted {
    async fn execute(
        &self,
        request: &Request,
        contract: Option<&Contract>,
    ) -> Result<Executed, ExecutorError> {
        self.requests.fetch_add(1, Ordering::Relaxed);
        self.inner.execute(request, contract).await
    }
}

/// The fake each served backend runs on: `graph` for ClickHouse, the seeded code and live fakes
/// for the remotes, signed in to the live service as `user`.
fn fake(backend: Backend, graph: &Arc<dyn Executor>, user: &str) -> Option<Arc<dyn Executor>> {
    match backend {
        Backend::ClickHouse => Some(Arc::clone(graph)),
        Backend::OrbitCode => Some(seeded_code()),
        Backend::Live => Some(seeded_live(user)),
        _ => None,
    }
}

/// `graph` on the ClickHouse connection and the seeded code and live fakes on every served
/// remote, signed in to the live service as `user`.
pub fn dag_executors(connections: &Connections, graph: Arc<dyn Executor>, user: &str) -> Executors {
    let mut executors = Executors::default();
    for peer in connections.peers() {
        if let Some(executor) = fake(peer.backend(), &graph, user) {
            executors = executors.with(peer.connection(), executor);
        }
    }
    executors
}

/// `run_dag` with every executor counted: the events, the outcome, and the requests each
/// backend's executor ran.
pub async fn run_counted(
    model: &Arc<Model>,
    graph: Arc<dyn Executor>,
    query: &str,
    grants: &compiler::orbit_authorization::Grants,
    user: &str,
    checks: &Checks,
) -> (
    Vec<Event>,
    Result<Outcome, DagError>,
    BTreeMap<&'static str, usize>,
) {
    let (catalog, connections) = dag_connections(model);
    let mut executors = Executors::default();
    let mut counted = Vec::new();
    for peer in connections.peers() {
        if let Some(inner) = fake(peer.backend(), &graph, user) {
            let executor = Arc::new(Counted {
                inner,
                requests: AtomicUsize::new(0),
            });
            counted.push((<&str>::from(peer.backend()), Arc::clone(&executor)));
            executors = executors.with(peer.connection(), executor);
        }
    }
    let frontend = Frontend::Server(catalog);
    let (events, outcome) =
        run_served(&frontend, &connections, &executors, query, grants, checks).await;
    let requests = counted
        .into_iter()
        .map(|(backend, executor)| (backend, executor.requests.load(Ordering::Relaxed)))
        .collect();
    (events, outcome, requests)
}

/// Runs a query through the DAG runner with `graph` on the graph connection and the seeded code
/// and live fakes on theirs, signed in to the live service as `user`.
pub async fn run_dag(
    model: &Arc<Model>,
    graph: Arc<dyn Executor>,
    query: &str,
    grants: &compiler::orbit_authorization::Grants,
    user: &str,
    checks: &Checks,
) -> (Vec<Event>, Result<Outcome, DagError>) {
    let (catalog, connections) = dag_connections(model);
    let executors = dag_executors(&connections, graph, user);
    run_served(
        &Frontend::Server(catalog),
        &connections,
        &executors,
        query,
        grants,
        checks,
    )
    .await
}

/// Runs a query through the DAG runner of a deployment that serves `connections` through
/// `executors`, recording every event.
pub async fn run_served(
    frontend: &Frontend,
    connections: &Connections,
    executors: &Executors,
    query: &str,
    grants: &compiler::orbit_authorization::Grants,
    checks: &Checks,
) -> (Vec<Event>, Result<Outcome, DagError>) {
    let env = Env {
        frontend,
        connections,
        grants,
        limits: query_data_model::testkit::limits(),
        trace: true,
    };
    let mut observer = Recording::default();
    let outcome = Runner::new(env, executors, checks, &mut observer)
        .run(Source::Text(query))
        .await;
    (observer.0, outcome)
}

/// The edges the graph response of `outcome` returns once `redaction` answers its result's
/// checks, each as `From:id -TYPE-> To:id`, sorted.
pub fn response_edges(
    outcome: &Outcome,
    redaction: &crate::mock_redaction::MockRedactionService,
) -> Vec<String> {
    use query_engine::formatters::GraphFormatter;
    let compiled = &outcome.compiled;
    let context = &compiled.base.result_context;
    let mut result =
        orbit_server::redaction::QueryResult::from_batches(&outcome.executed.batches, context)
            .expect("the joined rows read as a result");
    let redacted_count = result.apply_authorizations(&redaction.check(&result.resource_checks()));
    let output = query_engine::shared::PipelineOutput {
        row_count: result.authorized_count(),
        redacted_count,
        query_type: compiled.query_type.to_string(),
        raw_query_strings: Vec::new(),
        compiled: Arc::clone(compiled),
        query_result: result,
        result_context: context.clone(),
        execution_log: Vec::new(),
        pagination: None,
    };
    let mut edges: Vec<String> = GraphFormatter
        .build_response(&output)
        .edges
        .iter()
        .map(|edge| {
            let (from, to) = (&edge.from, &edge.to);
            format!(
                "{from}:{} -{}-> {to}:{}",
                edge.from_id, edge.edge_type, edge.to_id
            )
        })
        .collect();
    edges.sort();
    edges
}
