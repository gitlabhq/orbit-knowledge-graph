//! ADR 024 execution DAGs end to end over the seeded fakes: GQL text, the logical plan, the
//! stage split, each stage's planned, emitted and rendered plan, and the rows the engine returns.

mod joins;
mod single;

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use compiler::dag::{Env, Primary, Remotes, Rendered, Source};
use compiler::orbit_authorization::{Grants, Key};
use query_data_model::catalog::Row;
use query_data_model::testkit::limits;
use query_data_model::{Catalog, ClickHouse, Model, testkit};
use query_engine::pipeline::{AuthorizedPath, SecurityContext};
use query_engine::shared::dag::{
    DagError, Event, Executors, Outcome, Recording, RowChecks, Runner,
};
use serde_json::Value;

use crate::dag_fakes::{AllowedKeys, SeededGraph, seeded_code, seeded_live};

fn model() -> Arc<Model> {
    testkit::with_demo().load().unwrap()
}

fn caller(path: &str) -> Grants {
    let session = SecurityContext::new_with_roles(1, vec![AuthorizedPath::new(path, 20)]).unwrap();
    let config = orbit_server_config::AppConfig::embedded_defaults().authorization;
    orbit_server::authorization::gitlab::session_grants(&config, &model(), session).unwrap()
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
            Ok(Outcome::Rows { batches, .. }) => json_rows(&batches),
            Ok(Outcome::Relational { executed, .. }) => json_rows(&executed.batches),
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
    checks: &dyn RowChecks,
) -> Run {
    let model = model();
    let primary = Primary::ClickHouse(Arc::new(Catalog::<ClickHouse>::from_model(&model).unwrap()));
    let remotes = Remotes::all(&model);
    let mut executors =
        Executors::default().with(primary.connection(), Arc::new(SeededGraph(graph)));
    for target in remotes.targets() {
        let executor = match target.backend() {
            compiler::dag::Backend::OrbitCode => seeded_code(),
            _ => seeded_live(user),
        };
        executors = executors.with(target.connection(), executor);
    }
    let env = Env {
        primary: &primary,
        remotes: &remotes,
        grants,
        limits: limits(),
        trace: true,
    };
    let mut observer = Recording::default();
    let outcome = Runner::new(env, &executors, checks, &mut observer)
        .run(Source::Text(query))
        .await;
    Run {
        events: observer.0,
        outcome,
    }
}

/// Runs a query as `alice`, whose code checks allow every seeded repository.
async fn run(query: &str, grants: &Grants, graph: Vec<Row>) -> Run {
    let checks = AllowedKeys(BTreeSet::from([Key::Int(42), Key::Int(77)]));
    run_with(query, grants, graph, "alice", &checks).await
}

fn row(value: Value) -> Row {
    value.as_object().cloned().unwrap()
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
