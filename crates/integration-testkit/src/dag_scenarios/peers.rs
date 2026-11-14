//! A DuckDB frontend with remote stages: a live stage's workspace lookup is a stage on the DuckDB
//! connection that stores its `via` chain, and a DuckDB stage feeds a live stage its keys.

use std::sync::{Arc, Mutex};

use compiler::dag::{
    Connections, Env, Frontend, Handle, Output, Rendered, Runs, Source, split_query,
};
use compiler::orbit_authorization::{Checks, Grants, Key};
use duckdb_client::dag::DuckDbExecutor;
use duckdb_client::{DuckDbClient, OpenMode};
use query_data_model::plan::live::Workspaces;
use query_data_model::{DuckDb, Model};
use query_engine::pipeline::{AuthorizedPath, SecurityContext};
use query_engine::shared::dag::{Event, Executors};
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

/// The `lake` DuckDB connection as the frontend, with its teams, escalations, guilds and realms,
/// and the Data Extraction Service fake signed in as alice.
struct Lake {
    model: Arc<Model>,
    frontend: Frontend,
    connections: Connections,
    executors: Executors,
}

fn lake() -> Lake {
    lake_with(&[])
}

/// The lake, with `statements` run on its database after the seed.
fn lake_with(statements: &[&str]) -> Lake {
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
        "INSERT INTO gl_guild (id, name, traversal_path) VALUES (8, 'Masons', '1/9970/')",
        "INSERT INTO gl_realm (code, name, traversal_path) VALUES ('north', 'North', '1/9970/'), ('south', 'South', '1/555/')",
    ]
    .iter()
    .chain(statements)
    {
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

const GUILD_POSTS: &str =
    "MATCH (p:GuildPost) WHERE p.channel = 'guild-chat' RETURN p.text ORDER BY p.text LIMIT 10";

const REALM_POSTS: &str =
    "MATCH (p:RealmPost) WHERE p.channel = 'realm-chat' RETURN p.text ORDER BY p.text LIMIT 10";

/// The lake with guild 7 on the graph connection, Guild's complete store; the lake's own copy
/// of guilds holds only guild 8.
fn guilds() -> Lake {
    let mut lake = lake();
    let main = lake.model.names().connection("main").unwrap();
    let graph = SeededGraph(vec![row(json!({"scope_id": 7}))]);
    lake.executors = lake.executors.with(main, Arc::new(graph));
    lake
}

/// A `via` head keyed by an integer hands its keys on as integers, and the live call names them
/// as its workspaces.
#[tokio::test]
async fn a_lookup_hands_on_integer_head_keys() {
    let ran = guilds().run(GUILD_POSTS, &["1/9970/"]).await;
    let scoped = ran.events.iter().find_map(|event| match event {
        Event::Keys {
            from: 0, sample, ..
        } => Some(sample.clone()),
        _ => None,
    });
    assert_eq!(scoped, Some(vec![Key::Int(7)]));
    assert_eq!(calls(&ran, 1), [Workspaces::Only(vec![Key::Int(7)])]);
    assert_eq!(column(ran, "p_text"), [json!("smiths forge at dawn")]);
}

/// A `via` head whose identity is `code` is read by that column, and each caller's live call
/// names the realms its scope holds.
#[tokio::test]
async fn a_lookup_reads_a_head_identity_not_named_id() {
    let lake = lake();
    let one = lake.run(REALM_POSTS, &["1/9970/"]).await;
    assert!(
        lookups(&one, 0)[0].starts_with("SELECT scope.code AS scope_code FROM "),
        "{:?}",
        lookups(&one, 0)
    );
    assert_eq!(calls(&one, 1), [Workspaces::Only(vec!["north".into()])]);
    assert_eq!(column(one, "p_text"), [json!("snow in the north pass")]);

    let both = lake.run(REALM_POSTS, &["1/9970/", "1/555/"]).await;
    assert_eq!(
        calls(&both, 1),
        [Workspaces::Only(vec!["north".into(), "south".into()])]
    );
    assert_eq!(
        column(both, "p_text"),
        [
            json!("harvest in the south"),
            json!("snow in the north pass")
        ]
    );
}

/// The lookup runs where placement puts its head, on Guild's complete store, whichever order the
/// deployment lists its peers in: both orders place it on the same store and read the same rows.
#[tokio::test]
async fn a_lookup_runs_on_the_heads_complete_store_in_any_peer_order() {
    let lake = guilds();
    let names = lake.model.names();
    let (main, guild, graph) = (
        names.connection("main").unwrap(),
        names.entity("Guild").unwrap(),
        names.store("graph").unwrap(),
    );
    let grants = grants(&lake.model, &["1/9970/"], false);
    let listed = lake.connections.peers().to_vec();
    let reversed: Vec<_> = listed.iter().rev().cloned().collect();
    let mut placed = Vec::new();
    let mut texts = Vec::new();
    for peers in [listed, reversed] {
        let connections = Connections::new(peers);
        let env = Env {
            connections: &connections,
            ..lake.env(&grants)
        };
        let dag = split_query(Source::Text(GUILD_POSTS), &env).unwrap();
        placed.push(dag.stages[0].runs.clone());
        let (events, outcome) = run_served(
            &lake.frontend,
            &connections,
            &lake.executors,
            GUILD_POSTS,
            &grants,
            &Checks::default(),
        )
        .await;
        texts.push(column(Run { events, outcome }, "p_text"));
    }
    assert_eq!(
        placed,
        vec![
            Runs::On {
                connection: main,
                stores: vec![(guild, graph)],
            };
            2
        ]
    );
    assert_eq!(texts, vec![vec![json!("smiths forge at dawn")]; 2]);
}

/// A lookup is a stage, so the stage cap bounds it: the caller's 1,000 teams reach the live call,
/// and a 1,001st fails the query instead of widening the call.
#[tokio::test]
async fn a_lookup_past_the_stage_cap_fails_and_one_at_it_runs() {
    let teams = |count: usize| {
        format!("INSERT INTO gl_team SELECT 'X' || i, 'bulk', '1/9970/' FROM range({count}) t(i)")
    };
    let at = teams(999);
    let ran = lake_with(&[&at]).run(MESSAGES, &["1/9970/"]).await;
    let [Workspaces::Only(workspaces)] = &calls(&ran, 1)[..] else {
        panic!("the live stage renders one call");
    };
    assert_eq!(workspaces.len(), 1000);
    assert_eq!(
        column(ran, "m_text"),
        [json!("deploy to staging at 5pm"), json!("lunch at noon?")]
    );

    let past = teams(1000);
    let error = lake_with(&[&past])
        .run(MESSAGES, &["1/9970/"])
        .await
        .error();
    assert_eq!(
        error,
        "limit exceeded: stage 0 returned more than 1000 rows; a stage hands at most 1000 rows to the next"
    );
}
