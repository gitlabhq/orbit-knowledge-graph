//! A DuckDB frontend with remote stages: a live stage's workspace lookup runs on the DuckDB
//! connection that stores its `via` chain, and a DuckDB stage feeds a live stage its keys.

use std::sync::{Arc, Mutex};

use compiler::dag::{Connections, Frontend, Rendered};
use compiler::orbit_authorization::{Checks, Grants};
use duckdb_client::dag::DuckDbExecutor;
use duckdb_client::{DuckDbClient, OpenMode};
use query_data_model::plan::live::Workspaces;
use query_data_model::{DuckDb, Model};
use query_engine::pipeline::{AuthorizedPath, SecurityContext};
use query_engine::shared::dag::Executors;
use serde_json::{Value, json};

use super::{Run, json_rows};
use crate::dag_fakes::{peer_packages, run_served, seeded_live};

fn grants(model: &Model, paths: &[&str]) -> Grants {
    let paths = paths
        .iter()
        .map(|path| AuthorizedPath::new(*path, 20))
        .collect();
    let session = SecurityContext::new_with_roles(1, paths).unwrap();
    let config = orbit_server_config::AppConfig::embedded_defaults().authorization;
    orbit_server::authorization::gitlab::session_grants(&config, model, session).unwrap()
}

/// The `lake` DuckDB connection as the frontend, with its teams and escalations, and the Data
/// Extraction Service fake signed in as alice.
struct Lake {
    model: Arc<Model>,
    frontend: Frontend,
    connections: Connections,
    executors: Executors,
}

fn lake() -> Lake {
    let model = peer_packages()
        .deployed(&query_data_model::testkit::deployments(), "lake")
        .unwrap();
    let catalog = Arc::new(model.physical::<DuckDb>("lake").unwrap());
    let client = DuckDbClient::open(OpenMode::InMemory).unwrap();
    client.initialize_schema(&catalog.create_sql()).unwrap();
    for statement in [
        "INSERT INTO gl_team (id, name, traversal_path) VALUES ('T01', 'Orbit', '1/9970/'), ('T02', 'Vault', '1/555/')",
        "INSERT INTO gl_escalation (id, reason, message_id, traversal_path) VALUES (1, 'paged', 'm1', '1/9970/'), (2, 'paged', 'm5', '1/555/'), (3, 'leak', 'm4', '1/9970/')",
    ] {
        client.execute(statement, &[]).unwrap();
    }
    let connections = Connections::of(&model).unwrap();
    let executors = Executors::default()
        .with(
            "lake",
            Arc::new(DuckDbExecutor {
                client: Arc::new(Mutex::new(client)),
            }),
        )
        .with("data_extraction", seeded_live("alice"));
    Lake {
        model,
        frontend: Frontend::Local(catalog),
        connections,
        executors,
    }
}

impl Lake {
    async fn run(&self, query: &str, paths: &[&str]) -> Run {
        let grants = grants(&self.model, paths);
        let (events, outcome) = run_served(
            &self.frontend,
            &self.connections,
            &self.executors,
            query,
            &grants,
            &Checks::default(),
        )
        .await;
        Run { events, outcome }
    }
}

fn column(run: Run, name: &str) -> Vec<Value> {
    let outcome = run
        .outcome
        .unwrap_or_else(|error| panic!("the run failed: {error}"));
    json_rows(&outcome.executed.batches)
        .into_iter()
        .map(|row| row[name].clone())
        .collect()
}

fn calls(run: &Run, stage: usize) -> Vec<Workspaces> {
    run.rendered(stage)
        .into_iter()
        .filter_map(|request| match request {
            Rendered::Live(call) => Some(call.workspaces),
            _ => None,
        })
        .collect()
}

fn lookups(run: &Run, stage: usize) -> Vec<String> {
    run.rendered(stage)
        .into_iter()
        .filter_map(|request| match request {
            Rendered::Sql { sql, .. } => Some(sql),
            _ => None,
        })
        .collect()
}

const MESSAGES: &str =
    "MATCH (m:TeamMessage) WHERE m.channel = 'orbit-dev' RETURN m.text ORDER BY m.text LIMIT 10";

/// The live stage's workspace lookup runs as DuckDB SQL on the lake, so each caller reads the
/// workspaces of its teams, and a caller who can read no team sends no call.
#[tokio::test]
async fn a_duckdb_frontend_looks_up_a_live_stages_workspaces_on_duckdb() {
    let lake = lake();
    let both = lake.run(MESSAGES, &["1/9970/", "1/555/"]).await;
    assert_eq!(
        calls(&both, 0),
        [Workspaces::Only(vec!["T01".into(), "T02".into()])]
    );
    assert!(lookups(&both, 0)[0].contains("FROM gl_team AS k0"));
    assert_eq!(
        column(both, "m_text"),
        [
            json!("deploy to staging at 5pm"),
            json!("deploy uses the production keys"),
            json!("lunch at noon?"),
        ]
    );

    let one = lake.run(MESSAGES, &["1/9970/"]).await;
    assert_eq!(calls(&one, 0), [Workspaces::Only(vec!["T01".into()])]);
    assert_eq!(
        column(one, "m_text"),
        [json!("deploy to staging at 5pm"), json!("lunch at noon?")]
    );

    let none = lake.run(MESSAGES, &["1/101/"]).await;
    assert!(calls(&none, 0).is_empty());
    assert!(column(none, "m_text").is_empty());
}

const ESCALATED: &str = "MATCH (e:Escalation)-[:ESCALATES]->(m:TeamMessage) WHERE e.reason = 'paged' RETURN e.id, m.text ORDER BY e.id LIMIT 10";

/// A DuckDB stage reads the escalations the caller can read and hands their message ids to the
/// live stage, which reads only the admitted workspaces; the engine joins both.
#[tokio::test]
async fn a_duckdb_stage_feeds_its_keys_to_a_live_stage() {
    let lake = lake();
    let both = lake.run(ESCALATED, &["1/9970/", "1/555/"]).await;
    let sql = lookups(&both, 0);
    assert!(
        sql[0].contains("starts_with(e.traversal_path, $"),
        "{sql:?}"
    );
    let rows: Vec<(Value, Value)> = {
        let outcome = both.outcome.as_ref().unwrap();
        json_rows(&outcome.executed.batches)
            .into_iter()
            .map(|row| (row["e_id"].clone(), row["m_text"].clone()))
            .collect()
    };
    assert_eq!(
        rows,
        [
            (json!(1), json!("deploy to staging at 5pm")),
            (json!(2), json!("deploy uses the production keys")),
        ]
    );

    let one = lake.run(ESCALATED, &["1/9970/"]).await;
    assert_eq!(column(one, "e_id"), [json!(1)]);

    let none = lake.run(ESCALATED, &["1/101/"]).await;
    assert!(calls(&none, 1).is_empty());
    assert!(column(none, "e_id").is_empty());
}
