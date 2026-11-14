//! ADR 024 execution DAGs end to end over the seeded fakes: GQL text, the logical plan, the
//! stage split, each stage's planned, emitted and rendered plan, and the rows the Combine stage returns.

mod heads;
mod joins;
mod peers;
mod policies;
mod server;
mod single;

use std::sync::Arc;

use arrow::array::{Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use compiler::dag::Rendered;
use compiler::orbit_authorization::Checks;
use compiler::orbit_authorization::{Grants, Key};
use query_data_model::catalog::Row;
use query_engine::pipeline::{AuthorizedPath, SecurityContext};
use query_engine::shared::dag::{DagError, Event, Outcome};
use serde_json::{Value, json};

use crate::dag_fakes::{AllowedKeys, SeededGraph, dag_model, run_counted, run_dag};

/// The properties `names` of `entity` in the DAG model, as a key input lists them.
fn properties(entity: &str, names: &[&str]) -> Vec<query_data_model::PropertyId> {
    let model = dag_model();
    let entity = model.names().entity(entity).unwrap();
    names
        .iter()
        .map(|name| model.names().property(entity, name).unwrap())
        .collect()
}

fn caller(path: &str) -> Grants {
    let session = SecurityContext::new_with_roles(1, vec![AuthorizedPath::new(path, 20)]).unwrap();
    let config = orbit_server_config::AppConfig::embedded_defaults().authorization;
    orbit_server::authorization::gitlab::session_grants(&config, &dag_model(), session).unwrap()
}

/// One run: every event the observer recorded, and what the run returned.
struct Run {
    events: Vec<Event>,
    outcome: Result<Outcome, DagError>,
}

impl Run {
    fn rendered(&self, stage: usize) -> Vec<Rendered> {
        self.events
            .iter()
            .filter_map(|event| match event {
                Event::Rendered {
                    stage: at,
                    requests,
                } if *at == stage => Some(requests.clone()),
                _ => None,
            })
            .flatten()
            .collect()
    }

    fn plan(&self, stage: usize, emitted: bool) -> Vec<String> {
        self.events
            .iter()
            .find_map(|event| match (event, emitted) {
                (Event::Planned { stage: at, plan }, false)
                | (Event::Emitted { stage: at, plan }, true)
                    if *at == stage =>
                {
                    Some(plan.clone())
                }
                _ => None,
            })
            .unwrap_or_default()
    }

    fn kinds(&self) -> Vec<String> {
        self.events
            .iter()
            .map(|event| match event {
                Event::Logical { .. } | Event::Split { .. } => event.kind().to_string(),
                Event::Keys { from, to, .. } => format!("keys {from}->{to}"),
                Event::Started { stage }
                | Event::Planned { stage, .. }
                | Event::Emitted { stage, .. }
                | Event::Rendered { stage, .. }
                | Event::Rows { stage, .. }
                | Event::Finished { stage, .. }
                | Event::Failed { stage, .. } => format!("{} {stage}", event.kind()),
            })
            .collect()
    }

    fn rows(self) -> Vec<Row> {
        match self.outcome {
            Ok(Outcome { executed, .. }) => json_rows(&executed.batches),
            Err(error) => panic!("the run failed: {error}"),
        }
    }

    /// The graph response's edges, with every result check denied.
    fn edges(&self) -> Vec<String> {
        match &self.outcome {
            Ok(outcome) => crate::dag_fakes::response_edges(
                outcome,
                &crate::mock_redaction::MockRedactionService::new(),
            ),
            Err(error) => panic!("the run failed: {error}"),
        }
    }

    fn error(self) -> String {
        match self.outcome {
            Ok(_) => panic!("the run succeeded"),
            Err(error) => error.to_string(),
        }
    }
}

async fn run_with(
    query: &str,
    grants: &Grants,
    graph: Vec<Row>,
    user: &str,
    checks: &Checks,
) -> Run {
    let graph = Arc::new(SeededGraph(graph));
    let (events, outcome) = run_dag(&dag_model(), graph, query, grants, user, checks).await;
    Run { events, outcome }
}

/// `run_with`, and the requests each backend's executor ran.
async fn counted(
    query: &str,
    grants: &Grants,
    graph: Vec<Row>,
    checks: &Checks,
) -> (Run, std::collections::BTreeMap<&'static str, usize>) {
    let graph = Arc::new(SeededGraph(graph));
    let (events, outcome, requests) =
        run_counted(&dag_model(), graph, query, grants, "alice", checks).await;
    (Run { events, outcome }, requests)
}

/// Runs a query as `alice`, whose code checks allow every seeded repository.
async fn run(query: &str, grants: &Grants, graph: Vec<Row>) -> Run {
    let checks = AllowedKeys::checks("project", [Key::Int(42), Key::Int(77)]);
    run_with(query, grants, graph, "alice", &checks).await
}

fn row(value: Value) -> Row {
    value.as_object().cloned().unwrap()
}

/// The rows a project lookup returns. The seeded graph runs no SQL, so each scenario seeds what
/// the lookup's scope filter keeps for its caller.
fn projects(ids: &[i64]) -> Vec<Row> {
    ids.iter().map(|id| row(json!({"scope_id": id}))).collect()
}

/// The rows a workspace lookup returns: each workspace, and the group it is installed in.
fn workspaces(installed: &[(&str, i64)]) -> Vec<Row> {
    installed
        .iter()
        .map(|(workspace, group)| row(json!({"scope_id": workspace, "scope1_id": group})))
        .collect()
}

/// Rows of Arrow batches as JSON, integers as numbers and text as strings.
fn json_rows(batches: &[RecordBatch]) -> Vec<Row> {
    let mut rows = Vec::new();
    for batch in batches {
        for index in 0..batch.num_rows() {
            let mut row = Row::new();
            for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
                let value = if column.is_null(index) {
                    Value::Null
                } else if let Some(ints) = column.as_any().downcast_ref::<Int64Array>() {
                    Value::from(ints.value(index))
                } else if let Some(text) = column.as_any().downcast_ref::<StringArray>() {
                    Value::from(text.value(index))
                } else {
                    Value::from(format!("{column:?}"))
                };
                row.insert(field.name().clone(), value);
            }
            rows.push(row);
        }
    }
    rows
}
