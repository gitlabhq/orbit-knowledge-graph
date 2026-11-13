//! Executors over the seeded fakes, so DAG scenarios run code and live stages end to end the
//! way `SeededColumnResolver` fakes Gitaly. Only tests register them; the server registers none.

use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;
use compiler::dag::Request;
use compiler::dag::{Backend, Connections, Env, Frontend, Peer, Source};
use compiler::orbit_authorization::Key;
use compiler::orbit_authorization::{Answers, AuthError, Check, Checks, Module, Need, Provider};
use compiler::remote::Resolves;
use query_data_model::catalog::Row;
use query_data_model::implementations::live::DataExtraction;
use query_data_model::implementations::orbit_code::{CodeSearch, RequestField};
use query_data_model::{Catalog, ClickHouse, Model};
use query_engine::shared::dag::{
    DagError, Event, Executed, Executor, ExecutorError, Executors, Outcome, Recording, Runner,
    from_rows,
};
use serde_json::{Value, json};

use crate::seeded_remote::{SeededCodeSearch, SeededDataExtraction, seeded_scopes};

fn unexpected(request: &Request) -> ExecutorError {
    ExecutorError(format!("this fake runs no {request:?}"))
}

fn batch(rows: &[Row], fields: &[String]) -> Result<Executed, ExecutorError> {
    Ok(Executed {
        batches: vec![from_rows(rows, fields)?],
        log: Vec::new(),
    })
}

/// Orbit Next's code indexing web servers, over `code_search.yaml`.
pub struct CodeExecutor(pub SeededCodeSearch);

#[async_trait]
impl Executor for CodeExecutor {
    async fn execute(&self, request: &Request) -> Result<Executed, ExecutorError> {
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
        batch(&rows, &fields)
    }
}

/// The Data Extraction Service over `data_extraction.yaml`, signed in as one user.
pub struct LiveExecutor {
    pub service: SeededDataExtraction,
    pub user: String,
}

#[async_trait]
impl Executor for LiveExecutor {
    async fn execute(&self, request: &Request) -> Result<Executed, ExecutorError> {
        let Request::Live(call) = request else {
            return Err(unexpected(request));
        };
        let rows = self
            .service
            .extract(&self.user, call)
            .map_err(|error| ExecutorError(error.0))?;
        batch(&rows, &call.fields)
    }
}

/// The graph connection in tests that assert plans without a container: each query returns the
/// seeded rows, and each scope lookup the seeded namespaces.
pub struct SeededGraph(pub Vec<Row>);

#[async_trait]
impl Executor for SeededGraph {
    async fn execute(&self, request: &Request) -> Result<Executed, ExecutorError> {
        match request {
            Request::Query(_) if self.0.is_empty() => Ok(Executed::default()),
            Request::Query(_) => {
                let mut fields: Vec<String> = Vec::new();
                for key in self.0.iter().flat_map(|row| row.keys()) {
                    if !fields.contains(key) {
                        fields.push(key.clone());
                    }
                }
                batch(&self.0, &fields)
            }
            Request::Lookup(lookup) => {
                let seeded = seeded_scopes();
                let row = |id: Value, path: &str| {
                    json!({compiler::remote::KEY: id, lookup.scope.as_str(): path})
                        .as_object()
                        .cloned()
                        .unwrap_or_default()
                };
                let rows: Vec<Row> = match lookup.resolves {
                    Resolves::Repositories => seeded
                        .repositories
                        .iter()
                        .map(|(id, path)| row(Value::from(*id), path.as_str()))
                        .collect(),
                    Resolves::Workspaces => seeded
                        .workspaces
                        .iter()
                        .map(|(id, path)| row(Value::from(id.as_str()), path.as_str()))
                        .collect(),
                };
                let fields = [compiler::remote::KEY.to_string(), lookup.scope.clone()];
                batch(&rows, &fields)
            }
            other => Err(unexpected(other)),
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
/// deployments that name them on the graph connection, reviews on a DuckDB connection, and
/// snippets that a code store on their own connection searches.
pub fn peer_packages() -> query_data_model::Packages {
    let peers = crate::authorization_variants::platform(&[
        "peers.releases",
        "peers.deployments",
        "peers.lake",
        "peers.snippets",
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

/// `graph` on the ClickHouse connection and the seeded code and live fakes on every served
/// remote, signed in to the live service as `user`.
pub fn dag_executors(connections: &Connections, graph: Arc<dyn Executor>, user: &str) -> Executors {
    let mut executors = Executors::default();
    for peer in connections.peers() {
        let executor = match peer.backend() {
            Backend::ClickHouse => Arc::clone(&graph),
            Backend::OrbitCode => seeded_code(),
            Backend::Live => seeded_live(user),
            _ => continue,
        };
        executors = executors.with(peer.connection(), executor);
    }
    executors
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
