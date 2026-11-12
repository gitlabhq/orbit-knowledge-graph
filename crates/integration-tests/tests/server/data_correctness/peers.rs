//! Queries across relational peers on the test container: releases on a second ClickHouse
//! connection keyed by UUID, the rollouts on the graph connection that name them, and reviews on
//! a DuckDB connection, each stage run by its own connection's executor.

use std::sync::{Arc, Mutex};

use arrow::record_batch::RecordBatch;
use compiler::dag::{Connections, Frontend, Rendered};
use compiler::orbit_authorization::{Checks, Key};
use duckdb_client::dag::DuckDbExecutor;
use duckdb_client::{DuckDbClient, OpenMode};
use integration_testkit::dag_fakes::{peer_packages, run_served};
use integration_testkit::{TABLE_PREFIX, TestContext, t};
use orbit_server::pipeline::graph_executors;
use query_data_model::implementations::clickhouse::versioned;
use query_data_model::{ClickHouse, DuckDb, Model};
use query_engine::shared::dag::{Event, Executors, Outcome};

use super::dag::{grants, ints, strings};

const RELEASE_A: &str = "61f0c404-5cb3-11e7-907b-a6006ad3dba0";
const RELEASE_B: &str = "0b8e4f5a-1d2c-4e3f-9a8b-7c6d5e4f3a2b";

/// The peer packages with the test container's table prefix, and the graph, releases and lake
/// connections they serve.
struct Peers {
    model: Arc<Model>,
    frontend: Frontend,
    connections: Connections,
    executors: Executors,
}

fn model() -> Arc<Model> {
    let fragments = versioned(peer_packages().fragments().unwrap(), &TABLE_PREFIX).unwrap();
    Arc::new(Model::load(&fragments).unwrap())
}

/// Creates and fills the rollout and release tables on the container and the review table in an
/// in-memory DuckDB, on top of the DAG seed.
pub(super) async fn seed(ctx: &TestContext) {
    let unprefixed = peer_packages().load().unwrap();
    for (connection, table) in [("main", "gl_rollout"), ("releases", "gl_release")] {
        let catalog = unprefixed.physical::<ClickHouse>(connection).unwrap();
        let schema = catalog.schema().unwrap();
        let table = schema.table(table).unwrap();
        ctx.execute(&table.to_create_sql(&TABLE_PREFIX)).await;
    }
    for statement in [
        format!(
            "INSERT INTO {} (id, environment, release_id, traversal_path) VALUES (1, 'production', '{RELEASE_A}', '1/100/42/'), (2, 'production', '{RELEASE_B}', '1/100/42/'), (3, 'staging', '{RELEASE_A}', '1/100/42/')",
            t("gl_rollout")
        ),
        format!(
            "INSERT INTO {} (id, name, merge_request_id, traversal_path) VALUES ('{RELEASE_A}', 'v1.0', 2010, '1/100/42/'), ('{RELEASE_B}', 'v1.1', 2011, '1/555/')",
            t("gl_release")
        ),
    ] {
        ctx.execute(&statement).await;
    }
}

fn lake(model: &Arc<Model>) -> DuckDbClient {
    let catalog = model.physical::<DuckDb>("lake").unwrap();
    let client = DuckDbClient::open(OpenMode::InMemory).unwrap();
    client.initialize_schema(&catalog.create_sql()).unwrap();
    client
        .execute(
            "INSERT INTO gl_review (id, verdict, merge_request_id, traversal_path) VALUES (1, 'approve', 2010, '1/100/42/'), (2, 'reject', 2011, '1/100/43/'), (3, 'approve', 2010, '1/555/')",
            &[],
        )
        .unwrap();
    client
}

fn peers(ctx: &TestContext) -> Peers {
    let model = model();
    let connections = Connections::serving(&model, &["main", "releases", "lake"]).unwrap();
    let main = Arc::new(model.physical::<ClickHouse>("main").unwrap());
    let releases = model.physical::<ClickHouse>("releases").unwrap();
    let client = Arc::new(ctx.create_client());
    let graph = graph_executors(
        Arc::clone(&client),
        &main,
        "base",
        "gkg;peers".into(),
        Vec::new(),
    );
    let second = graph_executors(client, &releases, "base", "gkg;peers".into(), Vec::new());
    let executors = Executors::default()
        .with("main", Arc::clone(graph.get("main").unwrap()))
        .with("releases", Arc::clone(second.get("releases").unwrap()))
        .with(
            "lake",
            Arc::new(DuckDbExecutor {
                client: Arc::new(Mutex::new(lake(&model))),
            }),
        );
    Peers {
        model,
        frontend: Frontend::Server(main),
        connections,
        executors,
    }
}

impl Peers {
    async fn run(&self, query: &str, paths: &[&str]) -> (Vec<Event>, Vec<RecordBatch>) {
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
        match outcome {
            Ok(Outcome { executed, .. }) => (events, executed.batches),
            Err(error) => panic!("{query} failed for {paths:?}: {error}"),
        }
    }
}

fn sql(events: &[Event], stage: usize) -> Vec<String> {
    events
        .iter()
        .filter_map(|event| match event {
            Event::Rendered {
                stage: at,
                requests,
            } if *at == stage => Some(requests.clone()),
            _ => None,
        })
        .flatten()
        .filter_map(|request| match request {
            Rendered::Sql { sql, .. } => Some(sql),
            _ => None,
        })
        .collect()
}

fn keys(events: &[Event]) -> Vec<Key> {
    events
        .iter()
        .find_map(|event| match event {
            Event::Keys { sample, .. } => Some(sample.clone()),
            _ => None,
        })
        .unwrap_or_default()
}

const ROLLED_OUT: &str = "MATCH (d:Rollout {environment: 'production'})-[:DEPLOYS]->(r:Release) RETURN d.id, r.name ORDER BY d.id LIMIT 10";

/// Production rollouts on the graph connection hand their release UUIDs to the releases
/// connection, a second ClickHouse; each caller reads only the releases its scope holds, and a
/// caller with no rollout sends nothing to the second connection.
pub(super) async fn rollouts_on_one_clickhouse_join_releases_on_another_by_uuid(ctx: &TestContext) {
    let peers = peers(ctx);
    let pairs = |batches: &[RecordBatch]| -> Vec<(i64, String)> {
        ints(batches, "d_id")
            .into_iter()
            .zip(strings(batches, "r_name"))
            .collect()
    };
    let (events, both) = peers.run(ROLLED_OUT, &["1/100/", "1/555/"]).await;
    assert_eq!(
        keys(&events),
        [Key::Uuid(RELEASE_B.into()), Key::Uuid(RELEASE_A.into())]
    );
    let releases = sql(&events, 1);
    assert!(releases[0].contains("gl_release AS r"), "{releases:?}");
    assert_eq!(
        pairs(&both),
        [(1, "v1.0".to_string()), (2, "v1.1".to_string())]
    );

    let (_, one) = peers.run(ROLLED_OUT, &["1/100/"]).await;
    assert_eq!(pairs(&one), [(1, "v1.0".to_string())]);

    let (events, none) = peers.run(ROLLED_OUT, &["1/101/"]).await;
    assert!(sql(&events, 1).is_empty());
    assert_eq!(pairs(&none), []);
}

const SHIPPED: &str = "MATCH (r:Release)-[:SHIPS]->(mr:MergeRequest) WHERE r.name IN ['v1.0', 'v1.1'] RETURN r.name, mr.title ORDER BY r.name LIMIT 10";

/// Releases on the second ClickHouse hand their merge request ids to the graph connection.
pub(super) async fn releases_on_a_second_clickhouse_join_the_merge_requests_they_ship(
    ctx: &TestContext,
) {
    let peers = peers(ctx);
    let pairs = |batches: &[RecordBatch]| -> Vec<(String, String)> {
        strings(batches, "r_name")
            .into_iter()
            .zip(strings(batches, "mr_title"))
            .collect()
    };
    let (_, both) = peers.run(SHIPPED, &["1/100/", "1/555/"]).await;
    assert_eq!(
        pairs(&both),
        [
            ("v1.0".to_string(), "Scope tokens".to_string()),
            ("v1.1".to_string(), "Unindexed".to_string()),
        ]
    );
    let (_, one) = peers.run(SHIPPED, &["1/100/"]).await;
    assert_eq!(
        pairs(&one),
        [("v1.0".to_string(), "Scope tokens".to_string())]
    );
    let (events, none) = peers.run(SHIPPED, &["1/101/"]).await;
    assert!(sql(&events, 1).is_empty());
    assert_eq!(pairs(&none), []);
}

const REVIEWED: &str = "MATCH (mr:MergeRequest {state: 'opened'})<-[:REVIEWS]-(v:Review) WHERE mr.id IN [2010, 2011] RETURN mr.iid, v.verdict ORDER BY v.id LIMIT 10";

/// Open merge requests on ClickHouse hand their ids to the DuckDB lake, whose own security phase
/// keeps the reviews in the caller's scope.
pub(super) async fn merge_requests_on_clickhouse_join_their_reviews_on_duckdb(ctx: &TestContext) {
    let peers = peers(ctx);
    let pairs = |batches: &[RecordBatch]| -> Vec<(i64, String)> {
        ints(batches, "mr_iid")
            .into_iter()
            .zip(strings(batches, "v_verdict"))
            .collect()
    };
    let (events, wide) = peers.run(REVIEWED, &["1/100/", "1/555/"]).await;
    let reviews = sql(&events, 1);
    assert!(
        reviews[0].contains("starts_with(v.traversal_path, $"),
        "{reviews:?}"
    );
    assert_eq!(
        pairs(&wide),
        [
            (10, "approve".to_string()),
            (11, "reject".to_string()),
            (10, "approve".to_string()),
        ]
    );
    let (_, narrow) = peers.run(REVIEWED, &["1/100/"]).await;
    assert_eq!(
        pairs(&narrow),
        [(10, "approve".to_string()), (11, "reject".to_string())]
    );
    let (_, one) = peers.run(REVIEWED, &["1/100/42/"]).await;
    assert_eq!(pairs(&one), [(10, "approve".to_string())]);
    let (events, none) = peers.run(REVIEWED, &["1/101/"]).await;
    assert!(sql(&events, 1).is_empty());
    assert_eq!(pairs(&none), []);
}
