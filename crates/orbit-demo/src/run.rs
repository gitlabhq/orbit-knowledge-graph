//! One query run: the caller signs in, the real DAG runner runs the query over the demo's
//! executors, and every runner event goes to the run's log as it happens. The server's own
//! stages after the DAG then build the run's response, which its formatters render.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use std::collections::{BTreeMap, BTreeSet};

use compiler::dag::{Backend, Env, Runs, Source, StageQuery, split_query};
use compiler::surface;
use query_data_model::Model;
use query_data_model::catalog::Row;
use query_data_model::surface::Statement;
use query_engine::pipeline::{
    NoOpObserver, PipelineError, PipelineRunner, QueryPipelineContext, TypeMap,
};
use query_engine::shared::dag::{Event, Executor, Executors, Observer, Outcome, Runner};
use query_engine::shared::{DagStage, PipelineOutput};
use serde_json::{Value, json};
use tokio::sync::Notify;

use crate::callers::Session;
use crate::executors::{CodeHttp, LiveHttp, Sampled, Samples, json_rows};
use crate::{Demo, plan_text};

const ROWS: usize = 500;

/// A run's events as JSON, in order, for any number of readers, and the response the server
/// built from it. Each event carries `at_ms`, the time since the run began, which the page
/// replays the run from.
pub struct RunLog {
    events: Mutex<Vec<String>>,
    done: AtomicBool,
    notify: Notify,
    begun: Instant,
    response: OnceLock<Result<PipelineOutput, String>>,
}

impl Default for RunLog {
    fn default() -> Self {
        Self {
            events: Mutex::default(),
            done: AtomicBool::default(),
            notify: Notify::new(),
            begun: Instant::now(),
            response: OnceLock::new(),
        }
    }
}

impl RunLog {
    /// The response of a run whose DAG finished, set before its `done` event.
    pub fn response(&self) -> Option<&Result<PipelineOutput, String>> {
        self.response.get()
    }

    pub fn push(&self, mut event: Value) {
        if let Value::Object(fields) = &mut event {
            fields.insert(
                "at_ms".into(),
                json!(self.begun.elapsed().as_secs_f64() * 1000.0),
            );
        }
        self.events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(event.to_string());
        self.notify.notify_waiters();
    }

    fn finish(&self) {
        self.done.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    /// The event at `index`, waiting for it; `None` once the run finished without it.
    pub async fn next(&self, index: usize) -> Option<String> {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let done = self.done.load(Ordering::SeqCst);
            let event = self
                .events
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(index)
                .cloned();
            if event.is_some() {
                return event;
            }
            if done {
                return None;
            }
            notified.await;
        }
    }
}

/// Forwards each runner event, and after each stage's rows the samples its executors returned.
struct Streaming {
    log: Arc<RunLog>,
    samples: Samples,
}

impl Observer for Streaming {
    fn event(&mut self, event: Event) {
        let mut samples = self
            .samples
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Event::Started { .. } = event {
            samples.clear();
        }
        let rows = match &event {
            Event::Rows { stage, .. } => Some(*stage),
            _ => None,
        };
        self.log
            .push(serde_json::to_value(&event).unwrap_or_default());
        if let Some(stage) = rows {
            let taken = std::mem::take(&mut *samples);
            self.log
                .push(json!({"event": "samples", "stage": stage, "samples": taken}));
        }
    }
}

pub async fn run(demo: Arc<Demo>, query: String, caller: String, log: Arc<RunLog>) {
    match execute(&demo, &query, &caller, &log).await {
        Ok(rows) => log.push(json!({"event": "done", "count": rows.len(), "rows": rows})),
        Err(error) => log.push(json!({"event": "error", "error": format!("{error:#}")})),
    }
    log.finish();
}

async fn execute(
    demo: &Demo,
    query: &str,
    caller: &str,
    log: &Arc<RunLog>,
) -> anyhow::Result<Vec<Row>> {
    let callers = crate::fakes::current(&demo.callers);
    let session = callers.sign_in(caller, &demo.model).await?;
    log.push(json!({"event": "caller", "caller": caller, "grants": grants(demo, &session.grants)}));
    let samples = Samples::default();
    let sampled = |inner: Arc<dyn Executor>| -> Arc<dyn Executor> {
        Arc::new(Sampled {
            inner,
            samples: Arc::clone(&samples),
            model: Arc::clone(&demo.model),
        })
    };
    let connection = demo.catalog.connection();
    let graph = orbit_server::pipeline::graph_executors(
        Arc::clone(&demo.store.reader),
        &demo.catalog,
        "orbit-demo",
        "orbit-demo".into(),
        Vec::new(),
    );
    let mut executors =
        Executors::default().with(connection, sampled(Arc::clone(graph.get(connection)?)));
    for (connection, lake) in demo.lakes.executors() {
        executors = executors.with(connection, sampled(Arc::new(lake)));
    }
    let handle = session.slack_handle().await.unwrap_or_default();
    for peer in demo.connections.peers() {
        let executor: Arc<dyn Executor> = match peer.backend() {
            Backend::OrbitCode => Arc::new(CodeHttp(demo.addresses.orbit_next)),
            Backend::Live => Arc::new(LiveHttp {
                address: demo.addresses.slack,
                handle: handle.clone(),
            }),
            Backend::ClickHouse | Backend::DuckDb | Backend::Combine => continue,
        };
        executors = executors.with(peer.connection(), sampled(executor));
    }
    let checks = session.checks(&demo.model, Arc::clone(log));
    let env = || Env {
        frontend: &demo.frontend,
        connections: &demo.connections,
        grants: &session.grants,
        limits: query_data_model::testkit::limits(),
        trace: true,
    };
    let parse = || {
        let admin = session.grants.caller().admin();
        let catalog = surface::catalog(&demo.catalog, &demo.connections, env().limits, admin);
        surface::parse(query, catalog)
    };
    log.push(plans(demo, parse()?, &env()));
    let env = env();
    let mut observer = Streaming {
        log: Arc::clone(log),
        samples: Arc::clone(&samples),
    };
    let outcome = Runner::new(env, &executors, &checks, &mut observer)
        .run(Source::Parsed(parse()?))
        .await?;
    let rows = json_rows(&outcome.executed.batches, ROWS);
    let response = respond(demo, query, session, outcome)
        .await
        .map_err(|error| error.to_string());
    let _ = log.response.set(response);
    Ok(rows)
}

/// The response the server builds from the run's outcome, through its own stages after the DAG.
async fn respond(
    demo: &Demo,
    query: &str,
    session: Session,
    outcome: Outcome,
) -> Result<PipelineOutput, PipelineError> {
    let mut server_extensions = TypeMap::default();
    server_extensions.insert(Arc::clone(&demo.store.reader));
    server_extensions.insert(Arc::clone(&demo.catalog));
    server_extensions.insert(*query_data_model::testkit::limits());
    server_extensions.insert(session.deployment);
    server_extensions.insert(session.caller);
    let mut ctx = QueryPipelineContext {
        query_json: query.to_string(),
        compiled: None,
        security_context: None,
        grants: Some(session.grants),
        server_extensions,
        phases: TypeMap::default(),
    };
    let ran = DagStage::hand_off(&mut ctx, outcome);
    let mut silent = NoOpObserver;
    orbit_server::pipeline::respond(PipelineRunner::resume(&mut ctx, &mut silent, ran))
        .await?
        .finish()
        .ok_or_else(|| PipelineError::custom("the output stage built no response"))
}

/// The query's logical plan, and each stage's sub-plan as a readable tree with the packages that
/// declare what it reads. The runner reports its plans as Debug text, so the demo splits a second
/// parse of the same query to print them. Combine reads every stage's rows, so it shows every
/// stage's packages.
fn plans(demo: &Demo, statement: Statement, env: &Env<'_>) -> Value {
    let logical = plan_text::tree(&statement.plan, &demo.model);
    let Ok(dag) = split_query(Source::Parsed(statement), env) else {
        return json!({"event": "plans", "logical": logical, "stages": {}});
    };
    let every: BTreeSet<String> = dag
        .stages
        .iter()
        .flat_map(|stage| packages(&demo.model, &stage.runs))
        .collect();
    let stages: BTreeMap<String, Value> = dag
        .stages
        .iter()
        .map(|stage| {
            let (plan, packages) = match &stage.query {
                StageQuery::Gql(statement) => (
                    Some(plan_text::tree(&statement.plan, &demo.model)),
                    packages(&demo.model, &stage.runs),
                ),
                _ => (None, every.clone()),
            };
            let info = json!({"plan": plan, "packages": packages});
            (stage.id.to_string(), info)
        })
        .collect();
    json!({"event": "plans", "logical": logical, "stages": stages})
}

/// The packages that declare the entities a stage reads and the stores it reads them from.
fn packages(model: &Model, runs: &Runs) -> BTreeSet<String> {
    let Runs::On { stores, .. } = runs else {
        return BTreeSet::new();
    };
    stores
        .iter()
        .flat_map(|(entity, store)| {
            let stored = model.store(*store).source(*entity);
            [
                Some(&model.graph().entity(*entity).package),
                stored.map(|source| &source.package),
            ]
        })
        .flatten()
        .cloned()
        .collect()
}

/// The caller's scope answer for every scope and permission the packages read with.
fn grants(demo: &Demo, grants: &compiler::orbit_authorization::Grants) -> Vec<Value> {
    let access = demo.model.access();
    access
        .needs()
        .into_iter()
        .map(|(scope, permission)| {
            let (scope, permission) = (
                &access.scope(scope).name,
                &access.permission(permission).name,
            );
            let answer = grants
                .scope_set(scope, permission)
                .map(|set| format!("{set:?}"))
                .unwrap_or_else(|error| format!("{error}"));
            json!({"scope": scope, "permission": permission, "answer": answer})
        })
        .collect()
}
