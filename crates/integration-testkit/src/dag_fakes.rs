//! Executors over the seeded fakes, so DAG scenarios run code and live stages end to end the
//! way `SeededColumnResolver` fakes Gitaly. Only tests register them; the server registers none.

use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;
use compiler::dag::Request;
use compiler::orbit_authorization::Key;
use compiler::orbit_authorization::{Answers, AuthError, Check, Checks, Module, Need, Provider};
use compiler::remote::Resolves;
use query_data_model::catalog::Row;
use query_data_model::implementations::live::DataExtraction;
use query_data_model::implementations::orbit_code::{CodeSearch, RequestField};
use query_engine::shared::dag::{Executed, Executor, ExecutorError, from_rows};
use serde_json::{Value, json};

use crate::seeded_remote::{SeededCodeSearch, SeededDataExtraction, seeded_namespaces};

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
                RequestField::Path => "path".to_string(),
                RequestField::Line => "line".to_string(),
                RequestField::Capture(name)
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
                let seeded = seeded_namespaces();
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

pub fn dag_model() -> Arc<query_data_model::Model> {
    dag_packages().load().unwrap()
}

/// `graph` on the relational connection and the seeded code and live fakes on every served
/// remote, signed in to the live service as `user`.
pub fn dag_executors(
    remotes: &compiler::dag::Remotes,
    connection: &str,
    graph: Arc<dyn Executor>,
    user: &str,
) -> query_engine::shared::dag::Executors {
    let mut executors = query_engine::shared::dag::Executors::default().with(connection, graph);
    for target in remotes.targets() {
        let executor = match target.backend() {
            compiler::dag::Backend::OrbitCode => seeded_code(),
            _ => seeded_live(user),
        };
        executors = executors.with(target.connection(), executor);
    }
    executors
}

/// Runs a query through the DAG runner with `graph` on the relational connection and the seeded
/// code and live fakes on theirs, signed in to the live service as `user`.
pub async fn run_dag(
    model: &Arc<query_data_model::Model>,
    graph: Arc<dyn Executor>,
    query: &str,
    grants: &compiler::orbit_authorization::Grants,
    user: &str,
    checks: &Checks,
) -> (
    Vec<query_engine::shared::dag::Event>,
    Result<query_engine::shared::dag::Outcome, query_engine::shared::dag::DagError>,
) {
    use compiler::dag::{Env, Primary, Remotes, Source};
    use query_data_model::{Catalog, ClickHouse};
    let primary = Primary::ClickHouse(Arc::new(Catalog::<ClickHouse>::from_model(model).unwrap()));
    let remotes = Remotes::all(model);
    let executors = dag_executors(&remotes, primary.connection(), graph, user);
    let env = Env {
        primary: &primary,
        remotes: &remotes,
        grants,
        limits: query_data_model::testkit::limits(),
        trace: true,
    };
    let mut observer = query_engine::shared::dag::Recording::default();
    let outcome = query_engine::shared::dag::Runner::new(env, &executors, checks, &mut observer)
        .run(Source::Text(query))
        .await;
    (observer.0, outcome)
}
