//! One query run: the caller signs in, the real DAG runner runs the query over the demo's
//! executors, and every runner event goes to the run's log as it happens.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use compiler::dag::{Backend, Dag, Env, Source, StageQuery, describe, split_query};
use compiler::remote::row_checks;
use query_data_model::catalog::Row;
use query_engine::shared::dag::{Event, Executor, Executors, Observer, Runner};
use serde_json::{Value, json};
use tokio::sync::Notify;

use crate::Demo;
use crate::executors::{CodeHttp, LiveHttp, Sampled, Samples, json_rows};

const ROWS: usize = 500;

/// A run's events as JSON, in order, for any number of readers.
#[derive(Default)]
pub struct RunLog {
    events: Mutex<Vec<String>>,
    done: AtomicBool,
    notify: Notify,
}

impl RunLog {
    pub fn push(&self, event: Value) {
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
        })
    };
    let connection = demo.primary.connection();
    let graph = orbit_server::pipeline::graph_executors(
        Arc::clone(&demo.store.reader),
        &demo.catalog,
        "orbit-demo",
        "orbit-demo".into(),
        Vec::new(),
    );
    let mut executors =
        Executors::default().with(connection, sampled(Arc::clone(graph.get(connection)?)));
    let handle = session.slack_handle().await.unwrap_or_default();
    for target in demo.remotes.targets() {
        let executor: Arc<dyn Executor> = match target.backend() {
            Backend::OrbitCode => Arc::new(CodeHttp(demo.addresses.orbit_next)),
            _ => Arc::new(LiveHttp {
                address: demo.addresses.slack,
                handle: handle.clone(),
            }),
        };
        executors = executors.with(target.connection(), sampled(executor));
    }
    let checks = session.checks();
    let env = || Env {
        primary: &demo.primary,
        remotes: &demo.remotes,
        grants: &session.grants,
        limits: query_data_model::testkit::limits(),
        trace: true,
    };
    if let Ok(dag) = split_query(Source::Text(query), &env()) {
        log.push(json!({"event": "stages", "stages": stages(demo, &dag)}));
    }
    let env = env();
    let mut observer = Streaming {
        log: Arc::clone(log),
        samples: Arc::clone(&samples),
    };
    let outcome = Runner::new(env, &executors, &checks, &mut observer)
        .run(Source::Text(query))
        .await?;
    Ok(json_rows(&outcome.executed.batches, ROWS))
}

/// Each stage's logical sub-plan and the row checks its remote rows go through, from a split
/// of the query the runner then repeats.
fn stages(demo: &Demo, dag: &Dag) -> Vec<Value> {
    dag.stages
        .iter()
        .map(|stage| {
            let (plan, checks) = match &stage.query {
                StageQuery::Gql(statement) => {
                    let checks: Vec<Value> = match stage.backend.relational() {
                        true => Vec::new(),
                        false => row_checks(&statement.plan, &demo.model)
                            .into_iter()
                            .map(|check| {
                                json!({"provider": check.provider, "permission": check.permission,
                                       "resource": check.resource, "field": check.field})
                            })
                            .collect(),
                    };
                    (describe(&statement.plan), checks)
                }
                StageQuery::Engine(query, _) => (vec![format!("{query:?}")], Vec::new()),
                StageQuery::Hydration(_) => (vec!["Hydration".to_string()], Vec::new()),
            };
            json!({"stage": stage.id, "plan": plan, "row_checks": checks, "response": stage.response})
        })
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
