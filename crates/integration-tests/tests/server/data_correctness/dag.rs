//! ADR 024's DAG queries with ClickHouse on the test container: relational stages, scope
//! lookup stages included, run real SQL through the server's executor, Orbit Next and the Data
//! Extraction Service run behind their seeded fakes, and the Combine stage joins every side.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use compiler::dag::Rendered;
use compiler::orbit_authorization::{Checks, Grants, Key};
use integration_testkit::dag_fakes::{AllowedKeys, dag_packages, response_edges, run_dag};
use integration_testkit::mock_redaction::MockRedactionService;
use integration_testkit::{TestContext, t};
use orbit_server::pipeline::graph_executors;
use query_engine::pipeline::{AuthorizedPath, SecurityContext};
use query_engine::shared::dag::{DagError, Event, Executor, Outcome};

const QUERY_4: &str = "MATCH (mr:MergeRequest {state: 'opened'})-[:SOURCE_HEAD]->(h:Head)-[:HEAD_FILE]->(f:File) WHERE f.content MATCHES ast('todo!($$$MSG)', 'rust') RETURN mr.iid, h.branch, f.path, match.MSG ORDER BY mr.iid LIMIT 20";

const DISCUSSED: &str = "MATCH (mr:MergeRequest {state: 'opened'})<-[:DISCUSSES]-(m:SlackMessage) RETURN mr.iid, m.text ORDER BY mr.iid LIMIT 10";

const BOUNDED: &str = "MATCH (f:File) WHERE f.project_id IN [42, 77] AND f.content MATCHES ast('compile($$$ARGS)', 'rust') RETURN f.project_id, f.path, match.ARGS LIMIT 20";

/// The rows the DAG scenarios add to the data-correctness seed: the Slack workspace table the
/// DAG model adds, with T01 in group 100 and T02 in group 101; projects 42 and 77, which the code
/// fake indexes; an open merge request on project 42's indexed head, and one on project 43,
/// which the graph does not hold.
pub(super) async fn seed(ctx: &TestContext) {
    let schema = integration_testkit::graph_schema(
        &dag_packages()
            .fragments()
            .and_then(|fragments| fragments.deploy("demo"))
            .unwrap(),
    );
    let table = schema
        .tables
        .iter()
        .find(|table| table.name == "gl_slack_workspace")
        .expect("the DAG model stores Slack workspaces");
    ctx.execute(&table.to_create_sql(&integration_testkit::TABLE_PREFIX))
        .await;
    for statement in [
        format!(
            "INSERT INTO {} (id, name, group_id, traversal_path) VALUES ('T01', 'Orbit', 100, '1/100/'), ('T02', 'Vault', 101, '1/101/')",
            t("gl_slack_workspace")
        ),
        format!(
            "INSERT INTO {} (id, name, full_path, visibility_level, traversal_path) VALUES (42, 'Orbit', 'public-group/orbit', 'public', '1/100/42/'), (77, 'Vault', 'secret/vault', 'private', '1/555/77/')",
            t("gl_project")
        ),
        format!(
            "INSERT INTO {} (id, iid, title, state, source_branch, target_branch, project_id, traversal_path) VALUES (2010, 10, 'Scope tokens', 'opened', 'feature-a', 'main', 42, '1/100/42/'), (2011, 11, 'Unindexed', 'opened', 'feature-a', 'main', 43, '1/100/43/')",
            t("gl_merge_request")
        ),
    ] {
        ctx.execute(&statement).await;
    }
}

/// One run through the server's ClickHouse executor, whose `rails` checks allow `allowed` of
/// the projects: the recorded events and the result rows.
async fn run(
    ctx: &TestContext,
    query: &str,
    paths: &[&str],
    allowed: &[i64],
) -> (Vec<Event>, Vec<RecordBatch>) {
    let (events, outcome) = joined(ctx, query, paths, allowed).await;
    (events, outcome.executed.batches)
}

/// The same run, returning the whole outcome.
async fn joined(
    ctx: &TestContext,
    query: &str,
    paths: &[&str],
    allowed: &[i64],
) -> (Vec<Event>, Outcome) {
    let checks = AllowedKeys::checks("project", allowed.iter().copied().map(Key::Int));
    match run_with(ctx, dag_packages(), query, paths, &checks).await {
        (events, Ok(outcome)) => (events, outcome),
        (_, Err(error)) => panic!("{query} failed for {paths:?}: {error}"),
    }
}

/// The graph response's edges when the redaction service allows `readable` of the merge
/// requests and every user.
pub(super) fn edges(outcome: &Outcome, readable: &[i64]) -> Vec<String> {
    let mut redaction = MockRedactionService::new();
    redaction.allow("merge_request", readable);
    redaction.allow("user", &[1, 2, 3]);
    response_edges(outcome, &redaction)
}

/// One run of `query` over `packages` through the server's ClickHouse executor.
async fn run_with(
    ctx: &TestContext,
    packages: query_data_model::Packages,
    query: &str,
    paths: &[&str],
    checks: &Checks,
) -> (Vec<Event>, Result<Outcome, DagError>) {
    let fragments = packages
        .fragments()
        .and_then(|fragments| fragments.deploy("demo"))
        .unwrap();
    let graph = integration_testkit::versioned_data_model(fragments);
    let model = Arc::clone(graph.shared_model());
    let executors = graph_executors(
        Arc::new(ctx.create_client()),
        &graph,
        "base",
        "gkg;dag".into(),
        Vec::new(),
    );
    let executor: Arc<dyn Executor> = Arc::clone(executors.get(graph.connection()).unwrap());
    let grants = grants(&model, paths);
    run_dag(&model, executor, query, &grants, "alice", checks).await
}

pub(super) fn grants(model: &query_data_model::Model, paths: &[&str]) -> Grants {
    let paths = paths
        .iter()
        .map(|path| AuthorizedPath::new(*path, 20))
        .collect();
    let session = SecurityContext::new_with_roles(1, paths).unwrap();
    let config = orbit_server_config::AppConfig::embedded_defaults().authorization;
    orbit_server::authorization::gitlab::session_grants(&config, model, session).unwrap()
}

pub(super) fn strings(batches: &[RecordBatch], name: &str) -> Vec<String> {
    let mut values = Vec::new();
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let column = batch.column_by_name(name).unwrap();
        let column = column.as_any().downcast_ref::<StringArray>().unwrap();
        values.extend((0..column.len()).map(|row| column.value(row).to_string()));
    }
    values
}

pub(super) fn ints(batches: &[RecordBatch], name: &str) -> Vec<i64> {
    let mut values = Vec::new();
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let column = batch.column_by_name(name).unwrap();
        let column = column.as_any().downcast_ref::<Int64Array>().unwrap();
        values.extend((0..column.len()).map(|row| column.value(row)));
    }
    values
}

/// The requests of `stage` that went to a remote service, not the graph connection.
fn remote_requests(events: &[Event], stage: usize) -> Vec<Rendered> {
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
        .filter(|request| !matches!(request, Rendered::Sql { .. }))
        .collect()
}

/// Query 4's code stage starts at `Head`, which carries no check, and reads the `File`s at each
/// head, whose `read_code` check drops the files of projects the caller cannot read.
pub(super) async fn query_4_drops_the_files_their_read_code_check_denies(ctx: &TestContext) {
    let (events, both) = run(ctx, QUERY_4, &["1/100/"], &[1000, 42]).await;
    let keys = events.iter().find_map(|event| match event {
        Event::Keys { to: 2, sample, .. } => Some(sample.clone()),
        _ => None,
    });
    let head =
        |project, branch: &str| Key::Composite(vec![Key::Int(project), Key::Text(branch.into())]);
    assert_eq!(
        keys,
        Some(vec![
            head(42, "feature-a"),
            head(43, "feature-a"),
            head(1000, "feature-a"),
            head(1000, "fix-b"),
        ])
    );
    let Some(Rendered::Code(code)) = remote_requests(&events, 2).pop() else {
        panic!("the code stage renders a request");
    };
    assert_eq!(code.repositories, [42, 1000]);
    assert_eq!(strings(&both, "f_path"), ["src/feature.rs", "src/auth.rs"]);

    let (_, denied) = run(ctx, QUERY_4, &["1/100/"], &[1000]).await;
    assert_eq!(strings(&denied, "f_path"), ["src/feature.rs"]);

    let (events, unindexed) = run(ctx, QUERY_4, &["1/100/43/"], &[1000, 42]).await;
    assert!(remote_requests(&events, 2).is_empty());
    assert_eq!(strings(&unindexed, "f_path"), Vec::<String>::new());
}

/// The workspace lookup stage reads each SlackWorkspace with its Group under both guards, so
/// each caller reads the workspaces of its groups, and a caller who reads no workspace gets no
/// messages and sends no call.
pub(super) async fn merge_requests_join_the_slack_messages_that_discuss_them(ctx: &TestContext) {
    let pairs = |batches: &[RecordBatch]| {
        let mut pairs: Vec<(i64, String)> = ints(batches, "mr_iid")
            .into_iter()
            .zip(strings(batches, "m_text"))
            .collect();
        pairs.sort();
        pairs
    };
    let (events, both) = run(ctx, DISCUSSED, &["1/100/", "1/101/"], &[]).await;
    let Some(Rendered::Live(call)) = remote_requests(&events, 2).pop() else {
        panic!("the Slack stage renders a call");
    };
    assert_eq!(
        call.workspaces,
        query_data_model::plan::live::Workspaces::Only(vec!["T01".into(), "T02".into()])
    );
    assert_eq!(
        pairs(&both),
        [
            (1, "deploy to staging at 5pm".to_string()),
            (1, "deploy uses the production keys".to_string()),
            (2, "deploy memes".to_string()),
        ]
    );

    let (_, one) = run(ctx, DISCUSSED, &["1/100/"], &[]).await;
    assert_eq!(
        pairs(&one),
        [
            (1, "deploy to staging at 5pm".to_string()),
            (2, "deploy memes".to_string()),
        ]
    );

    let (events, none) = run(ctx, DISCUSSED, &["1/100/1000/"], &[]).await;
    assert!(remote_requests(&events, 2).is_empty());
    assert_eq!(pairs(&none), []);
}

/// The joined rows carry the DISCUSSES edge between the endpoints the Combine stage joins: a message
/// outside the caller's workspaces never comes back, a merge request the redaction service
/// denies takes its edges with it, and a caller who reads nothing gets no edge.
pub(super) async fn the_joined_rows_carry_the_edges_they_cross(ctx: &TestContext) {
    let discusses = [
        "SlackMessage:m1 -DISCUSSES-> MergeRequest:2000",
        "SlackMessage:m4 -DISCUSSES-> MergeRequest:2001",
        "SlackMessage:m5 -DISCUSSES-> MergeRequest:2000",
    ];
    let (_, all) = joined(ctx, DISCUSSED, &["1/100/", "1/101/"], &[]).await;
    assert_eq!(edges(&all, &[2000, 2001]), discusses);
    let (_, part) = joined(ctx, DISCUSSED, &["1/100/"], &[]).await;
    assert_eq!(edges(&part, &[2000]), [discusses[0]]);
    let (_, none) = joined(ctx, DISCUSSED, &["1/100/1000/"], &[]).await;
    assert_eq!(edges(&none, &[2000, 2001]), Vec::<String>::new());
}

/// A code search names projects 42 and 77; each caller searches only the repositories its scope
/// holds, and a guest whose scope holds neither gets no matches and sends no search.
pub(super) async fn a_code_search_reads_only_the_repositories_the_caller_can_read(
    ctx: &TestContext,
) {
    let (events, both) = run(ctx, BOUNDED, &["1/100/", "1/555/"], &[42, 77]).await;
    let Some(Rendered::Code(code)) = remote_requests(&events, 1).pop() else {
        panic!("the code stage renders a search");
    };
    assert_eq!(code.repositories, [42, 77]);
    let projects: BTreeSet<i64> = ints(&both, "f_project_id").into_iter().collect();
    assert_eq!(projects, BTreeSet::from([42, 77]));
    assert!(strings(&both, "f_path").contains(&"src/vault.rs".to_string()));

    let (_, one) = run(ctx, BOUNDED, &["1/100/"], &[42, 77]).await;
    let projects: BTreeSet<i64> = ints(&one, "f_project_id").into_iter().collect();
    assert_eq!(projects, BTreeSet::from([42]));

    let (events, guest) = run(ctx, BOUNDED, &["1/101/"], &[42, 77]).await;
    assert!(remote_requests(&events, 1).is_empty());
    assert_eq!(strings(&guest, "f_path"), Vec::<String>::new());
}

/// The merge request stage's `read_merge_request` check runs before its keys reach the Slack
/// stage: the live call sees only the keys of merge requests the check allows, and none when it
/// allows none.
pub(super) async fn merge_requests_hand_slack_only_the_keys_their_check_allows(ctx: &TestContext) {
    let run = |allowed: &'static [i64]| {
        let checks = AllowedKeys::checks("merge_request", allowed.iter().copied().map(Key::Int));
        async move {
            match run_with(
                ctx,
                dag_packages(),
                DISCUSSED,
                &["1/100/", "1/101/"],
                &checks,
            )
            .await
            {
                (events, Ok(Outcome { executed, .. })) => (events, executed.batches),
                (_, Err(error)) => panic!("{error}"),
            }
        }
    };
    let keys = |events: &[Event]| {
        events.iter().find_map(|event| match event {
            Event::Keys { sample, .. } => Some(sample.clone()),
            _ => None,
        })
    };
    let (events, both) = run(&[2000, 2001]).await;
    assert_eq!(keys(&events), Some(vec![Key::Int(2000), Key::Int(2001)]));
    assert_eq!(ints(&both, "mr_iid").len(), 3);

    let (events, one) = run(&[2000]).await;
    assert_eq!(keys(&events), Some(vec![Key::Int(2000)]));
    assert_eq!(ints(&one, "mr_iid"), [1, 1]);

    let (events, none) = run(&[]).await;
    assert!(remote_requests(&events, 2).is_empty());
    assert_eq!(ints(&none, "mr_iid"), Vec::<i64>::new());
}

/// `Head` enforces only its check and the `File`s at each head enforce `via`: `File`'s lookup
/// stage reads the heads' projects on ClickHouse, so it searches the readable ones instead of
/// dropping them all, and a caller who reads no merge request sends no search.
pub(super) async fn a_head_under_none_keeps_the_files_its_via_chain_admits(ctx: &TestContext) {
    let packages = integration_testkit::dag_fakes::heads_under_none(dag_packages());
    let checks = AllowedKeys::checks("project", [Key::Int(1000), Key::Int(42)]);
    let run = |paths: &'static [&'static str]| {
        let (packages, checks) = (packages.clone(), checks.clone());
        async move {
            match run_with(ctx, packages, QUERY_4, paths, &checks).await {
                (events, Ok(Outcome { executed, .. })) => (events, executed.batches),
                (_, Err(error)) => panic!("{error}"),
            }
        }
    };
    let (events, both) = run(&["1/100/"]).await;
    let Some(Rendered::Code(code)) = remote_requests(&events, 2).pop() else {
        panic!("the code stage renders a request");
    };
    assert_eq!(code.repositories, [42, 1000]);
    assert_eq!(strings(&both, "f_path"), ["src/feature.rs", "src/auth.rs"]);

    let (events, one) = run(&["1/100/42/"]).await;
    let Some(Rendered::Code(code)) = remote_requests(&events, 2).pop() else {
        panic!("the code stage renders a request");
    };
    assert_eq!(code.repositories, [42]);
    assert_eq!(strings(&one, "f_path"), ["src/auth.rs"]);

    let (events, none) = run(&["1/101/"]).await;
    assert!(remote_requests(&events, 2).is_empty());
    assert_eq!(strings(&none, "f_path"), Vec::<String>::new());
}

const DEEP: &str = "MATCH (mr:MergeRequest {state: 'opened', title: 'bulk'})-[:SOURCE_HEAD]->(h:Head)-[:HEAD_FILE]->(f:File) WHERE f.content MATCHES ast('todo!($$$MSG)', 'rust') RETURN mr.iid, f.path ORDER BY mr.iid SKIP 200 LIMIT 50";

/// A merge request stage that would hand more rows than the key cap to the code stage fails the
/// query instead of truncating it, a caller whose scope keeps it under the cap gets the exact
/// page past row 200 (with the one extra row that tells pagination a next page exists), and a
/// caller who reads nothing gets an empty page. Adds 1,500 merge requests, so it
/// runs last on its context.
pub(super) async fn a_deep_page_fails_past_the_stage_cap_instead_of_losing_rows(ctx: &TestContext) {
    for (offset, count, project, path) in
        [(0, 1200, 42, "1/100/42/"), (1200, 300, 1000, "1/100/1000/")]
    {
        ctx.execute(&format!(
            "INSERT INTO {} (id, iid, title, state, source_branch, target_branch, project_id, traversal_path) SELECT number + {}, number + {}, 'bulk', 'opened', 'feature-a', 'main', {project}, '{path}' FROM numbers({count})",
            t("gl_merge_request"),
            100_000 + offset,
            100_000 + offset,
        ))
        .await;
    }
    let checks = AllowedKeys::checks("project", [Key::Int(1000), Key::Int(42)]);
    let (_, wide) = run_with(ctx, dag_packages(), DEEP, &["1/100/"], &checks).await;
    let error = wide
        .err()
        .map(|error| error.to_string())
        .unwrap_or_default();
    assert_eq!(
        error,
        "limit exceeded: stage 0 returned more than 1000 rows; a stage hands at most 1000 rows to the next"
    );

    let (_, narrow) = run(ctx, DEEP, &["1/100/1000/"], &[1000, 42]).await;
    let page = ints(&narrow, "mr_iid");
    assert_eq!(page, (101_400..101_451).collect::<Vec<i64>>());

    let (_, none) = run(ctx, DEEP, &["1/101/"], &[1000, 42]).await;
    assert_eq!(ints(&none, "mr_iid"), Vec::<i64>::new());
}
