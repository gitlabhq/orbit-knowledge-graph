//! A DuckDB frontend with remote stages: a live stage's workspace lookup is a stage on the DuckDB
//! connection that stores its `via` chain, and a DuckDB stage feeds a live stage its keys.

use std::sync::{Arc, Mutex};

use compiler::dag::{
    Connections, Env, Frontend, Handle, Output, Rendered, Runs, Source, split_query,
};
use compiler::orbit_authorization::{Checks, Grants};
use duckdb_client::dag::DuckDbExecutor;
use duckdb_client::{DuckDbClient, OpenMode};
use query_data_model::plan::live::Workspaces;
use query_data_model::{DuckDb, Model};
use query_engine::pipeline::{AuthorizedPath, SecurityContext};
use query_engine::shared::dag::Executors;
use serde_json::{Value, json};

use super::{Run, caller, json_rows, row, run};
use crate::dag_fakes::{SeededGraph, peer_packages, run_served, seeded_live};

fn grants(model: &Model, paths: &[&str], admin: bool) -> Grants {
    let paths = paths
        .iter()
        .map(|path| AuthorizedPath::new(*path, 20))
        .collect();
    let session = SecurityContext::new_with_roles(1, paths)
        .unwrap()
        .with_role(admin, None);
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
    let catalog = Arc::new(
        model
            .physical::<DuckDb>(model.names().connection("lake").unwrap())
            .unwrap(),
    );
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
            model.names().connection("lake").unwrap(),
            Arc::new(DuckDbExecutor {
                client: Arc::new(Mutex::new(client)),
            }),
        )
        .with(
            model.names().connection("data_extraction").unwrap(),
            seeded_live("alice"),
        );
    Lake {
        model,
        frontend: Frontend::Local(catalog),
        connections,
        executors,
    }
}

impl Lake {
    async fn run(&self, query: &str, paths: &[&str]) -> Run {
        self.run_as(query, &grants(&self.model, paths, false)).await
    }

    async fn run_as(&self, query: &str, grants: &Grants) -> Run {
        let (events, outcome) = run_served(
            &self.frontend,
            &self.connections,
            &self.executors,
            query,
            grants,
            &Checks::default(),
        )
        .await;
        Run { events, outcome }
    }

    fn env<'a>(&'a self, grants: &'a Grants) -> Env<'a> {
        Env {
            frontend: &self.frontend,
            connections: &self.connections,
            grants,
            limits: query_data_model::testkit::limits(),
            trace: false,
        }
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

/// The live stage's workspace lookup is a DuckDB stage on the lake, so each caller reads the
/// workspaces of its teams, and a caller who can read no team sends no call.
#[tokio::test]
async fn a_duckdb_frontend_looks_up_a_live_stages_workspaces_on_duckdb() {
    let lake = lake();
    let both = lake.run(MESSAGES, &["1/9970/", "1/555/"]).await;
    assert_eq!(
        calls(&both, 1),
        [Workspaces::Only(vec!["T01".into(), "T02".into()])]
    );
    assert!(
        lookups(&both, 0)[0].starts_with(
            "SELECT scope.id AS scope_id FROM (SELECT * FROM gl_team AS scope WHERE (starts_with(scope.traversal_path, $1)"
        ),
        "{:?}",
        lookups(&both, 0)
    );
    assert_eq!(
        column(both, "m_text"),
        [
            json!("deploy to staging at 5pm"),
            json!("deploy uses the production keys"),
            json!("lunch at noon?"),
        ]
    );

    let one = lake.run(MESSAGES, &["1/9970/"]).await;
    assert_eq!(calls(&one, 1), [Workspaces::Only(vec!["T01".into()])]);
    assert_eq!(
        column(one, "m_text"),
        [json!("deploy to staging at 5pm"), json!("lunch at noon?")]
    );

    let none = lake.run(MESSAGES, &["1/101/"]).await;
    assert!(calls(&none, 1).is_empty());
    assert!(column(none, "m_text").is_empty());
}

const ESCALATED: &str = "MATCH (e:Escalation)-[:ESCALATES]->(m:TeamMessage) WHERE e.reason = 'paged' RETURN e.id, m.text ORDER BY e.id LIMIT 10";

/// A DuckDB stage reads the escalations the caller can read and hands their message ids to the
/// live stage, which reads only the admitted workspaces; the Combine stage joins both.
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
    assert_eq!(
        both.edges(),
        [
            "Escalation:1 -ESCALATES-> TeamMessage:m1",
            "Escalation:2 -ESCALATES-> TeamMessage:m5",
        ]
    );

    let one = lake.run(ESCALATED, &["1/9970/"]).await;
    assert_eq!(one.edges(), ["Escalation:1 -ESCALATES-> TeamMessage:m1"]);
    assert_eq!(column(one, "e_id"), [json!(1)]);

    let none = lake.run(ESCALATED, &["1/101/"]).await;
    assert!(calls(&none, 2).is_empty());
    assert!(none.edges().is_empty());
    assert!(column(none, "e_id").is_empty());
}

const FIRST_NAME: &str = "MATCH (u:User) WHERE u.id = 1 RETURN u.first_name LIMIT 5";

/// A DuckDB frontend refuses an admin-only property to a caller who is no administrator, with
/// the server frontend's error, and an administrator reads it from the graph connection.
#[tokio::test]
async fn a_duckdb_frontend_returns_admin_only_properties_to_administrators_alone() {
    let mut lake = lake();
    let alice = row(json!({"u_first_name": "Alice", "_orbit_u_id": 1, "_orbit_u_type": "User"}));
    let main = lake.model.names().connection("main").unwrap();
    lake.executors = lake
        .executors
        .with(main, Arc::new(SeededGraph(vec![alice])));

    let refused = lake.run(FIRST_NAME, &["1/9970/"]).await.error();
    assert!(
        refused.contains("User.first_name is available to administrators only"),
        "{refused}"
    );
    let server = run(FIRST_NAME, &caller("1/9970/"), Vec::new()).await;
    assert_eq!(refused, server.error());

    let admin = grants(&lake.model, &["1/9970/"], true);
    let read = lake.run_as(FIRST_NAME, &admin).await;
    assert!(lookups(&read, 0)[0].contains("u.first_name"));
    assert_eq!(column(read, "u_first_name"), [json!("Alice")]);
}

/// A DuckDB frontend restricts a statement it did not parse, so a plan parsed for an
/// administrator reads no admin-only property for a caller who is not one.
#[test]
fn a_duckdb_frontend_restricts_a_statement_parsed_for_an_administrator() {
    let lake = lake();
    let Frontend::Local(catalog) = &lake.frontend else {
        unreachable!("the lake serves a DuckDB frontend");
    };
    let limits = query_data_model::testkit::limits();
    let split = |admin: bool| {
        let parsed = compiler::surface::parse(
            FIRST_NAME,
            compiler::surface::catalog(catalog, &lake.connections, limits, true),
        )
        .unwrap();
        let grants = grants(&lake.model, &["1/9970/"], admin);
        split_query(Source::Parsed(parsed), &lake.env(&grants))
    };

    let error = split(false).unwrap_err().to_string();
    assert!(
        error.contains("column \"first_name\" for User: field requires administrator access"),
        "{error}"
    );
    split(true).unwrap();
}

/// A node a DuckDB frontend returns whole from another connection's stage carries no
/// admin-only property, as a node a single stage returns whole does.
#[test]
fn a_whole_node_across_connections_returns_no_admin_only_property() {
    let lake = lake();
    let query = "MATCH (v:Review)-[:REVIEWS]->(m:MergeRequest)<-[:AUTHORED]-(u:User) RETURN v.id, u LIMIT 5";
    let names = lake.model.names();
    let main = names.connection("main").unwrap();
    let user = names.entity("User").unwrap();
    let username = names.property(user, "username").unwrap();
    let first_name = names.property(user, "first_name").unwrap();
    for admin in [false, true] {
        let grants = grants(&lake.model, &["1/9970/"], admin);
        let env = lake.env(&grants);
        let graph = split_query(Source::Text(query), &env)
            .unwrap()
            .stages
            .into_iter()
            .find(|stage| matches!(stage.runs, Runs::On { connection, .. } if connection == main))
            .unwrap();
        let contract = graph.contract.expect("the graph stage feeds Combine");
        let returns = |property| {
            let output = Output::Node {
                binding: "u".into(),
                handle: Handle::Property(property),
            };
            contract.position(&output).is_some()
        };
        assert!(returns(username), "{contract:?}");
        assert!(!returns(first_name), "{contract:?}");
    }
}

/// A caller who is no administrator aggregates on a DuckDB frontend over nodes another
/// connection keys by scope.
#[test]
fn a_duckdb_frontend_aggregates_over_another_connections_scoped_nodes() {
    let lake = lake();
    let grants = grants(&lake.model, &["1/9970/"], false);
    let query = "MATCH (p:Project) RETURN count(p) AS projects";
    split_query(Source::Text(query), &lake.env(&grants)).unwrap();
}

/// A hidden property is unknown on a DuckDB frontend, as on the server frontend, on the DuckDB
/// store and on a remote one, and to administrators too.
#[tokio::test]
async fn a_duckdb_frontend_knows_no_hidden_property() {
    let lake = lake();
    let project = "MATCH (p:Project) WHERE p.id = 1 RETURN p.traversal_path LIMIT 5";
    let server = run(project, &caller("1/9970/"), Vec::new()).await;
    assert_eq!(
        lake.run(project, &["1/9970/"]).await.error(),
        server.error()
    );

    let team = "MATCH (t:Team) RETURN t.name, t.traversal_path ORDER BY t.name LIMIT 5";
    for (query, unknown) in [
        (project, "unknown property Project.traversal_path"),
        (team, "unknown property Team.traversal_path"),
    ] {
        for admin in [false, true] {
            let grants = grants(&lake.model, &["1/9970/"], admin);
            let error = lake.run_as(query, &grants).await.error();
            assert!(error.contains(unknown), "{error}");
        }
    }
}
