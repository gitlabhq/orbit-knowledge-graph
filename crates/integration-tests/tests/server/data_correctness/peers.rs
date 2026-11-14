//! Queries across relational peers on the test container: releases on a second ClickHouse
//! connection keyed by UUID, the rollouts on the graph connection that name them, and reviews on
//! a DuckDB connection, each stage run by its own connection's executor.

use std::sync::{Arc, Mutex};

use arrow::record_batch::RecordBatch;
use compiler::dag::{Connections, Frontend, Rendered};
use compiler::orbit_authorization::Key;
use duckdb_client::dag::DuckDbExecutor;
use duckdb_client::{DuckDbClient, OpenMode};
use integration_testkit::dag_fakes::{
    AllowedKeys, peer_packages, response, response_edges, run_served, seeded_live,
};
use integration_testkit::mock_redaction::MockRedactionService;
use integration_testkit::{TABLE_PREFIX, TestContext, t};
use orbit_arrow::ArrowUtils;
use orbit_server::pipeline::graph_executors;
use query_data_model::implementations::clickhouse::versioned;
use query_data_model::implementations::live::ExtractionRequest;
use query_data_model::plan::live::Workspaces;
use query_data_model::{ClickHouse, Deployments, DuckDb, Model, Packages};
use query_engine::shared::dag::{Event, Executors, Outcome};

use super::dag::{edges, grants, ints, strings};

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

fn guilds_deployments() -> Deployments {
    Deployments::parse("guilds:\n  connections: [main, releases, lake, data_extraction]\n").unwrap()
}

fn model(packages: &Packages, deployments: &Deployments, shape: &str) -> Arc<Model> {
    let fragments = packages
        .fragments()
        .and_then(|fragments| fragments.deployment(deployments, shape))
        .unwrap();
    let fragments = versioned(fragments, &TABLE_PREFIX).unwrap();
    Arc::new(Model::load(&fragments).unwrap())
}

/// Creates and fills the rollout and release tables on the container and the review table in an
/// in-memory DuckDB, on top of the DAG seed.
pub(super) async fn seed(ctx: &TestContext) {
    let unprefixed = peer_packages()
        .deployed(&query_data_model::testkit::deployments(), "peers")
        .unwrap();
    for (connection, table) in [("main", "gl_rollout"), ("releases", "gl_release")] {
        let catalog = unprefixed
            .physical::<ClickHouse>(unprefixed.names().connection(connection).unwrap())
            .unwrap();
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
    let catalog = model
        .physical::<DuckDb>(model.names().connection("lake").unwrap())
        .unwrap();
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
    build(
        ctx,
        &peer_packages(),
        &query_data_model::testkit::deployments(),
        "peers",
    )
}

fn build(ctx: &TestContext, packages: &Packages, deployments: &Deployments, shape: &str) -> Peers {
    let model = model(packages, deployments, shape);
    let connections = Connections::of(&model).unwrap();
    let main = Arc::new(
        model
            .physical::<ClickHouse>(model.names().connection("main").unwrap())
            .unwrap(),
    );
    let releases = model
        .physical::<ClickHouse>(model.names().connection("releases").unwrap())
        .unwrap();
    let client = Arc::new(ctx.create_client());
    let graph = graph_executors(
        Arc::clone(&client),
        &main,
        "base",
        "gkg;peers".into(),
        Vec::new(),
    );
    let second = graph_executors(client, &releases, "base", "gkg;peers".into(), Vec::new());
    let named = |name: &str| model.names().connection(name).unwrap();
    let mut executors = Executors::default()
        .with(named("main"), Arc::clone(graph.get(named("main")).unwrap()))
        .with(
            named("releases"),
            Arc::clone(second.get(named("releases")).unwrap()),
        )
        .with(
            named("lake"),
            Arc::new(DuckDbExecutor {
                client: Arc::new(Mutex::new(lake(&model))),
            }),
        );
    if model.names().connection("data_extraction").is_some() {
        executors = executors.with(named("data_extraction"), seeded_live("alice"));
    }
    Peers {
        model,
        frontend: Frontend::Server(main),
        connections,
        executors,
    }
}

impl Peers {
    async fn run(&self, query: &str, paths: &[&str]) -> (Vec<Event>, Vec<RecordBatch>) {
        let (events, outcome) = self.joined(query, paths).await;
        (events, outcome.executed.batches)
    }

    async fn joined(&self, query: &str, paths: &[&str]) -> (Vec<Event>, Outcome) {
        let grants = grants(&self.model, paths);
        let (events, outcome) = run_served(
            &self.frontend,
            &self.connections,
            &self.executors,
            query,
            &grants,
            &AllowedKeys::checks("merge_request", [Key::Int(2010), Key::Int(2011)]),
        )
        .await;
        match outcome {
            Ok(outcome) => (events, outcome),
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

/// Every peer join returns the edge of the relationship it crosses, between the endpoints both
/// connections return; a release outside the caller's scope or a merge request the redaction
/// service denies takes its edge with it.
pub(super) async fn peer_joins_carry_the_edges_they_cross(ctx: &TestContext) {
    let peers = peers(ctx);
    let (all, wide) = (&[2010, 2011], &["1/100/", "1/555/"]);
    let deploys = [
        format!("Rollout:1 -DEPLOYS-> Release:{RELEASE_A}"),
        format!("Rollout:2 -DEPLOYS-> Release:{RELEASE_B}"),
    ];
    let (_, both) = peers.joined(ROLLED_OUT, wide).await;
    assert_eq!(edges(&both, all), deploys);
    let (_, one) = peers.joined(ROLLED_OUT, &["1/100/"]).await;
    assert_eq!(edges(&one, all), [deploys[0].clone()]);
    let (_, none) = peers.joined(ROLLED_OUT, &["1/101/"]).await;
    assert_eq!(edges(&none, all), Vec::<String>::new());

    let ships = [
        format!("Release:{RELEASE_B} -SHIPS-> MergeRequest:2011"),
        format!("Release:{RELEASE_A} -SHIPS-> MergeRequest:2010"),
    ];
    let (_, both) = peers.joined(SHIPPED, wide).await;
    assert_eq!(edges(&both, all), ships);
    assert_eq!(edges(&both, &[2010]), [ships[1].clone()]);
    let (_, none) = peers.joined(SHIPPED, &["1/101/"]).await;
    assert_eq!(edges(&none, all), Vec::<String>::new());

    let reviews = [
        "Review:1 -REVIEWS-> MergeRequest:2010",
        "Review:2 -REVIEWS-> MergeRequest:2011",
        "Review:3 -REVIEWS-> MergeRequest:2010",
    ];
    let (_, both) = peers.joined(REVIEWED, wide).await;
    assert_eq!(edges(&both, all), reviews);
    let (_, narrow) = peers.joined(REVIEWED, &["1/100/"]).await;
    assert_eq!(edges(&narrow, &[2010]), [reviews[0]]);
    let (_, none) = peers.joined(REVIEWED, &["1/101/"]).await;
    assert_eq!(edges(&none, all), Vec::<String>::new());
}

const SMITHS: &str = "smiths forge at dawn";
const MASONS: &str = "masons meet at noon";
const BADGE_GOLD: &str = "3f2b8c1e-7a4d-4e65-9b0a-1c2d3e4f5a6b";
const BADGE_SILVER: &str = "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d";

/// Creates and fills the guild, clan and guild rank tables: guilds, clans and ranks under
/// `1/100/` and `1/555/`.
pub(super) async fn seed_guilds(ctx: &TestContext) {
    let unprefixed = peer_packages()
        .deployed(&guilds_deployments(), "guilds")
        .unwrap();
    for (connection, table) in [
        ("main", "gl_guild"),
        ("releases", "gl_clan"),
        ("main", "gl_guild_rank"),
    ] {
        let catalog = unprefixed
            .physical::<ClickHouse>(unprefixed.names().connection(connection).unwrap())
            .unwrap();
        let schema = catalog.schema().unwrap();
        let table = schema.table(table).unwrap();
        ctx.execute(&table.to_create_sql(&TABLE_PREFIX)).await;
    }
    let statements = [
        format!(
            "INSERT INTO {} (id, name, traversal_path) VALUES (7, 'Smiths', '1/100/'), (8, 'Masons', '1/555/')",
            t("gl_guild")
        ),
        format!(
            "INSERT INTO {} (slug, name, traversal_path) VALUES ('forge', 'Forge', '1/100/'), ('quarry', 'Quarry', '1/555/')",
            t("gl_clan")
        ),
        format!(
            "INSERT INTO {} (guild, rank, title, granted_at, tier, badge, traversal_path) VALUES (7, 'gold', 'Grandmaster', '2026-03-01 12:00:00', 'gold', '{BADGE_GOLD}', '1/100/'), (8, 'silver', 'Journeyman', '2026-04-02 08:30:00', 'silver', '{BADGE_SILVER}', '1/555/')",
            t("gl_guild_rank")
        ),
    ];
    for statement in statements {
        ctx.execute(&statement).await;
    }
}

fn live_calls(events: &[Event]) -> Vec<ExtractionRequest> {
    events
        .iter()
        .filter_map(|event| match event {
            Event::Rendered { requests, .. } => Some(requests),
            _ => None,
        })
        .flatten()
        .filter_map(|request| match request {
            Rendered::Live(call) => Some(call.clone()),
            _ => None,
        })
        .collect()
}

fn handed_keys(events: &[Event]) -> Vec<Key> {
    events
        .iter()
        .filter_map(|event| match event {
            Event::Keys { sample, .. } => Some(sample.clone()),
            _ => None,
        })
        .flatten()
        .collect()
}

/// A live-post query over scoped heads. For each caller the live call carries exactly the
/// workspaces the caller reads, no key of a head the caller cannot read reaches a later stage,
/// and a caller who reads no head sends no call and hands on no key.
struct Posts<'a> {
    query: &'a str,
    column: &'a str,
}

impl Posts<'_> {
    async fn caller(
        &self,
        peers: &Peers,
        paths: &[&str],
        rows: &[&str],
        workspaces: &[Key],
        denied: &[Key],
    ) {
        let (events, batches) = peers.run(self.query, paths).await;
        let mut texts = strings(&batches, self.column);
        texts.sort();
        assert_eq!(texts, rows, "{paths:?}");
        let sent: Vec<Workspaces> = live_calls(&events)
            .into_iter()
            .map(|call| call.workspaces)
            .collect();
        let handed = handed_keys(&events);
        match workspaces.is_empty() {
            true => {
                assert!(sent.is_empty(), "{paths:?} sent {sent:?}");
                assert!(handed.is_empty(), "{paths:?} handed on {handed:?}");
            }
            false => assert_eq!(sent, [Workspaces::Only(workspaces.to_vec())], "{paths:?}"),
        }
        for key in denied {
            assert!(!handed.contains(key), "{paths:?} handed on {key}");
        }
    }
}

const GUILD_POSTS: &str = "MATCH (p:GuildPost {channel: 'guild-chat'}) RETURN p.text";

/// Posts of guilds keyed by an integer on real ClickHouse: the lookup stage reads each caller's
/// guilds, and only those keys reach the live call.
pub(super) async fn guild_posts_reach_the_live_stage_only_for_the_guilds_the_caller_reads(
    ctx: &TestContext,
) {
    let peers = build(ctx, &peer_packages(), &guilds_deployments(), "guilds");
    let posts = Posts {
        query: GUILD_POSTS,
        column: "p_text",
    };
    posts
        .caller(
            &peers,
            &["1/100/", "1/555/"],
            &[MASONS, SMITHS],
            &[Key::Int(7), Key::Int(8)],
            &[],
        )
        .await;
    posts
        .caller(
            &peers,
            &["1/100/"],
            &[SMITHS],
            &[Key::Int(7)],
            &[Key::Int(8)],
        )
        .await;
    posts
        .caller(&peers, &["1/101/"], &[], &[], &[Key::Int(7), Key::Int(8)])
        .await;
}

const CLAN_POSTS: &str = "MATCH (p:ClanPost {channel: 'clan-chat'}) RETURN p.text";

/// Posts of clans keyed by a text slug that is not named `id`, on the second ClickHouse
/// connection.
pub(super) async fn clan_posts_reach_the_live_stage_only_for_the_clans_the_caller_reads(
    ctx: &TestContext,
) {
    let peers = build(ctx, &peer_packages(), &guilds_deployments(), "guilds");
    let posts = Posts {
        query: CLAN_POSTS,
        column: "p_text",
    };
    let (forge, quarry) = ("forge clan lights the fires", "quarry clan hauls the stone");
    let slug = |slug: &str| Key::Text(slug.into());
    posts
        .caller(
            &peers,
            &["1/100/", "1/555/"],
            &[forge, quarry],
            &[slug("forge"), slug("quarry")],
            &[],
        )
        .await;
    posts
        .caller(
            &peers,
            &["1/100/"],
            &[forge],
            &[slug("forge")],
            &[slug("quarry")],
        )
        .await;
    posts
        .caller(
            &peers,
            &["1/101/"],
            &[],
            &[],
            &[slug("forge"), slug("quarry")],
        )
        .await;
}

const POSTED_IN: &str =
    "MATCH (p:GuildPost {channel: 'guild-chat'})-[:GUILD_OF]->(g:Guild) RETURN p.text, g.name";

/// A pattern that reads the head itself returns each post with its guild, for the guilds the
/// caller reads.
pub(super) async fn guild_posts_join_the_guilds_the_pattern_reads(ctx: &TestContext) {
    let peers = build(ctx, &peer_packages(), &guilds_deployments(), "guilds");
    let pairs = |batches: &[RecordBatch]| {
        let mut pairs: Vec<(String, String)> = strings(batches, "p_text")
            .into_iter()
            .zip(strings(batches, "g_name"))
            .collect();
        pairs.sort();
        pairs
    };
    let smiths = (SMITHS.to_string(), "Smiths".to_string());
    let masons = (MASONS.to_string(), "Masons".to_string());
    let (_, both) = peers.run(POSTED_IN, &["1/100/", "1/555/"]).await;
    assert_eq!(pairs(&both), [masons, smiths.clone()]);
    let (_, one) = peers.run(POSTED_IN, &["1/100/"]).await;
    assert_eq!(pairs(&one), [smiths]);
    let (_, none) = peers.run(POSTED_IN, &["1/101/"]).await;
    assert_eq!(pairs(&none), Vec::<(String, String)>::new());
}

const RANK_POSTS: &str = "MATCH (p:RankPost) WHERE p.id IN ['rp1', 'rp2'] RETURN p.text";
const RANKED: &str = "MATCH (p:RankPost)-[:RANK_POST_OF]->(r:GuildRank) WHERE p.id IN ['rp1', 'rp2'] RETURN p.text, r.rank, r.title, r.granted_at, r.tier, r.badge";

/// Posts whose head has the composite key `(guild, rank)`: the lookup hands composite keys to
/// the live call.
pub(super) async fn rank_posts_reach_the_live_stage_only_for_the_ranks_the_caller_reads(
    ctx: &TestContext,
) {
    let peers = build(ctx, &peer_packages(), &guilds_deployments(), "guilds");
    let posts = Posts {
        query: RANK_POSTS,
        column: "p_text",
    };
    let (gold, silver) = ("gold rank hall opens", "silver rank oath renewed");
    let rank = |guild, rank: &str| Key::Composite(vec![Key::Int(guild), Key::Text(rank.into())]);
    posts
        .caller(
            &peers,
            &["1/100/", "1/555/"],
            &[gold, silver],
            &[rank(7, "gold"), rank(8, "silver")],
            &[],
        )
        .await;
    posts
        .caller(
            &peers,
            &["1/100/"],
            &[gold],
            &[rank(7, "gold")],
            &[rank(8, "silver")],
        )
        .await;
    posts
        .caller(
            &peers,
            &["1/101/"],
            &[],
            &[],
            &[rank(7, "gold"), rank(8, "silver")],
        )
        .await;
}

/// The composite head's columns survive the stage boundary: the timestamp as its instant, the
/// enum as its label and the UUID as its canonical text, each as the response formats it.
pub(super) async fn rank_columns_decode_through_a_stage_boundary(ctx: &TestContext) {
    let peers = build(ctx, &peer_packages(), &guilds_deployments(), "guilds");
    let returned = [
        "p_text",
        "r_rank",
        "r_title",
        "r_granted_at",
        "r_tier",
        "r_badge",
    ];
    let rows = |batches: &[RecordBatch]| -> Vec<String> {
        let mut rows = Vec::new();
        for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
            for index in 0..batch.num_rows() {
                let cells: Vec<String> = returned
                    .iter()
                    .map(|name| {
                        let column = batch.column_by_name(name).unwrap();
                        ArrowUtils::array_value_to_string(column.as_ref(), index).unwrap()
                    })
                    .collect();
                rows.push(cells.join("|"));
            }
        }
        rows.sort();
        rows
    };
    let gold =
        format!("gold rank hall opens|gold|Grandmaster|2026-03-01T12:00:00Z|gold|{BADGE_GOLD}");
    let silver = format!(
        "silver rank oath renewed|silver|Journeyman|2026-04-02T08:30:00Z|silver|{BADGE_SILVER}"
    );
    let (_, both) = peers.run(RANKED, &["1/100/", "1/555/"]).await;
    assert_eq!(rows(&both), [gold.clone(), silver]);
    let (events, one) = peers.run(RANKED, &["1/100/"]).await;
    assert_eq!(rows(&one), [gold]);
    let denied = Key::Composite(vec![Key::Int(8), Key::Text("silver".into())]);
    assert!(!handed_keys(&events).contains(&denied));
    let (events, none) = peers.run(RANKED, &["1/101/"]).await;
    assert_eq!(rows(&none), Vec::<String>::new());
    assert!(live_calls(&events).is_empty());
}

const RANKS_OF_GUILDS: &str =
    "MATCH (r:GuildRank)-[:RANK_OF]->(g:Guild) WHERE r.rank IN ['gold', 'silver'] RETURN r, g";

/// One ClickHouse stage returns guild ranks with their guilds: each rank's id is its typed
/// parts, and each edge leaves the rank by the same id.
pub(super) async fn guild_ranks_return_composite_ids_and_their_edges(ctx: &TestContext) {
    let peers = build(ctx, &peer_packages(), &guilds_deployments(), "guilds");
    let redaction = MockRedactionService::new();
    let nodes = |outcome: &Outcome| -> Vec<String> {
        let mut nodes: Vec<String> = response(outcome, &redaction)["nodes"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|node| format!("{}:{}", node["type"].as_str().unwrap(), node["id"]))
            .collect();
        nodes.sort();
        nodes
    };
    let gold = "GuildRank:[7,\"gold\"] -RANK_OF-> Guild:7";
    let silver = "GuildRank:[8,\"silver\"] -RANK_OF-> Guild:8";
    let (_, both) = peers.joined(RANKS_OF_GUILDS, &["1/100/", "1/555/"]).await;
    assert_eq!(
        nodes(&both),
        [
            "Guild:\"7\"",
            "Guild:\"8\"",
            "GuildRank:[7,\"gold\"]",
            "GuildRank:[8,\"silver\"]"
        ]
    );
    assert_eq!(response_edges(&both, &redaction), [gold, silver]);
    let (_, one) = peers.joined(RANKS_OF_GUILDS, &["1/100/"]).await;
    assert_eq!(nodes(&one), ["Guild:\"7\"", "GuildRank:[7,\"gold\"]"]);
    assert_eq!(response_edges(&one, &redaction), [gold]);
    let (_, none) = peers.joined(RANKS_OF_GUILDS, &["1/101/"]).await;
    assert_eq!(nodes(&none), Vec::<String>::new());
    assert_eq!(response_edges(&none, &redaction), Vec::<String>::new());
}

/// Ranks hydrate by their composite ids, read as a tuple IN under the caller's scope.
pub(super) async fn guild_ranks_hydrate_by_composite_id(ctx: &TestContext) {
    let peers = build(ctx, &peer_packages(), &guilds_deployments(), "guilds");
    let (_, both) = peers.joined(RANKS_OF_GUILDS, &["1/100/", "1/555/"]).await;
    let context = &both.compiled.base.result_context;
    let result =
        orbit_server::redaction::QueryResult::from_batches(&both.executed.batches, context)
            .unwrap();
    let node = context.get("r").unwrap();
    let mut ids: Vec<Key> = result
        .authorized_rows()
        .filter_map(|row| row.get_public_id(node))
        .collect();
    ids.sort();
    let rank = |guild, rank: &str| Key::Composite(vec![Key::Int(guild), Key::Text(rank.into())]);
    assert_eq!(ids, [rank(7, "gold"), rank(8, "silver")]);
    let names = peers.model.names();
    let entity = names.entity("GuildRank").unwrap();
    let columns = [
        names.property(entity, "title").unwrap(),
        names.property(entity, "tier").unwrap(),
    ];
    assert_eq!(
        hydrated(&peers, entity, &ids, &columns, &["1/100/", "1/555/"]).await,
        [
            "(7, gold)|Grandmaster|gold",
            "(8, silver)|Journeyman|silver"
        ]
    );
    assert_eq!(
        hydrated(&peers, entity, &ids, &columns, &["1/100/"]).await,
        ["(7, gold)|Grandmaster|gold"]
    );
    assert_eq!(
        hydrated(&peers, entity, &ids, &columns, &["1/101/"]).await,
        Vec::<String>::new()
    );
}

/// The title and tier of each rank the hydration stage reads for a caller under `paths`, by id.
async fn hydrated(
    peers: &Peers,
    entity: query_data_model::EntityId,
    ids: &[Key],
    columns: &[query_data_model::PropertyId],
    paths: &[&str],
) -> Vec<String> {
    let arm = query_engine::shared::hydration::hydration_arm(
        &peers.model,
        query_data_model::testkit::limits(),
        entity,
        ids,
        columns,
        Vec::new(),
    )
    .unwrap();
    let grants = grants(&peers.model, paths);
    let env = compiler::dag::Env {
        frontend: &peers.frontend,
        connections: &peers.connections,
        grants: &grants,
        limits: query_data_model::testkit::limits(),
        trace: false,
    };
    let request = compiler::HydrationRequest {
        arms: vec![arm],
        options: Default::default(),
    };
    let compiled = compiler::dag::compile_hydration(request, &env).unwrap();
    let Frontend::Server(graph) = &peers.frontend else {
        panic!("the peers frontend is the server's");
    };
    let executor = peers.executors.get(graph.connection()).unwrap();
    let executed = executor.execute(&compiled.request, None).await.unwrap();
    let properties =
        query_engine::shared::hydration::parse_hydration_batches(&executed.batches, &peers.model)
            .unwrap();
    let mut rows: Vec<String> = properties
        .iter()
        .map(|((_, id), row)| {
            let text = |name: &str| row[name].as_string().cloned().unwrap_or_default();
            format!("{id}|{}|{}", text("title"), text("tier"))
        })
        .collect();
    rows.sort();
    rows
}
