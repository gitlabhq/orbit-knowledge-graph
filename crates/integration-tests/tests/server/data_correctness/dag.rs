//! ADR 024's DAG queries with ClickHouse on the test container: relational stages and scope
//! lookups run real SQL through the server's executor, Orbit Next and the Data Extraction
//! Service run behind their seeded fakes, and the engine joins every side.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use compiler::dag::Rendered;
use compiler::orbit_authorization::{Grants, Key};
use integration_testkit::dag_fakes::{AllowedKeys, dag_packages, run_dag};
use integration_testkit::{TestContext, t};
use orbit_server::pipeline::graph_executors;
use query_engine::pipeline::{AuthorizedPath, SecurityContext};
use query_engine::shared::dag::{Event, Executor, Outcome};

const QUERY_4: &str = "MATCH (mr:MergeRequest {state: 'opened'})-[:SOURCE_HEAD]->(h:Head)-[:HEAD_FILE]->(f:File) WHERE f.content MATCHES ast('todo!($$$MSG)', 'rust') RETURN mr.iid, h.branch, f.path, match.MSG ORDER BY mr.iid LIMIT 20";

const DISCUSSED: &str = "MATCH (mr:MergeRequest {state: 'opened'})<-[:DISCUSSES]-(m:SlackMessage) RETURN mr.iid, m.text ORDER BY mr.iid LIMIT 10";

const BOUNDED: &str = "MATCH (f:File) WHERE f.project_id IN [42, 77] AND f.content MATCHES ast('compile($$$ARGS)', 'rust') RETURN f.project_id, f.path, match.ARGS LIMIT 20";

/// The rows the DAG scenarios add to the data-correctness seed: the Slack workspace table the
/// DAG model adds, with T01 in group 100 and T02 in group 101; projects 42 and 77, which the code
/// fake indexes; an open merge request on project 42's indexed head, and one on project 43,
/// which the graph does not hold.
pub(super) async fn seed(ctx: &TestContext) {
    let schema = integration_testkit::graph_schema(&dag_packages().fragments().unwrap());
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

/// One run through the server's ClickHouse executor: the recorded events and the result rows.
async fn run(
    ctx: &TestContext,
    query: &str,
    paths: &[&str],
    allowed: &[i64],
) -> (Vec<Event>, Vec<RecordBatch>) {
    let fragments = dag_packages().fragments().unwrap();
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
    let checks = AllowedKeys::checks(allowed.iter().copied().map(Key::Int));
    let (events, outcome) = run_dag(&model, executor, query, &grants, "alice", &checks).await;
    match outcome {
        Ok(Outcome { executed, .. }) => (events, executed.batches),
        Err(error) => panic!("{query} failed for {paths:?}: {error}"),
    }
}

fn grants(model: &query_data_model::Model, paths: &[&str]) -> Grants {
    let paths = paths
        .iter()
        .map(|path| AuthorizedPath::new(*path, 20))
        .collect();
    let session = SecurityContext::new_with_roles(1, paths).unwrap();
    let config = orbit_server_config::AppConfig::embedded_defaults().authorization;
    orbit_server::authorization::gitlab::session_grants(&config, model, session).unwrap()
}

fn strings(batches: &[RecordBatch], name: &str) -> Vec<String> {
    let mut values = Vec::new();
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let column = batch.column_by_name(name).unwrap();
        let column = column.as_any().downcast_ref::<StringArray>().unwrap();
        values.extend((0..column.len()).map(|row| column.value(row).to_string()));
    }
    values
}

fn ints(batches: &[RecordBatch], name: &str) -> Vec<i64> {
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
        Event::Keys { sample, .. } => Some(sample.clone()),
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
    let Some(Rendered::Code(code)) = remote_requests(&events, 1).pop() else {
        panic!("the code stage renders a request");
    };
    assert_eq!(code.repositories, [42, 1000]);
    assert_eq!(strings(&both, "f_path"), ["src/feature.rs", "src/auth.rs"]);

    let (_, denied) = run(ctx, QUERY_4, &["1/100/"], &[1000]).await;
    assert_eq!(strings(&denied, "f_path"), ["src/feature.rs"]);

    let (events, unindexed) = run(ctx, QUERY_4, &["1/100/43/"], &[1000, 42]).await;
    assert!(remote_requests(&events, 1).is_empty());
    assert_eq!(strings(&unindexed, "f_path"), Vec::<String>::new());
}

/// The workspace lookup joins SlackWorkspace to Group: its columns come back under the names
/// the stage reads, each caller reads the workspaces of its groups, and a caller who reads no
/// workspace gets no messages and sends no call.
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
    let Some(Rendered::Live(call)) = remote_requests(&events, 1).pop() else {
        panic!("the Slack stage renders a call");
    };
    assert_eq!(call.workspaces, ["T01", "T02"]);
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
    assert!(remote_requests(&events, 1).is_empty());
    assert_eq!(pairs(&none), []);
}

/// A code search names projects 42 and 77; each caller searches only the repositories its scope
/// holds, and a guest whose scope holds neither gets no matches and sends no search.
pub(super) async fn a_code_search_reads_only_the_repositories_the_caller_can_read(
    ctx: &TestContext,
) {
    let (events, both) = run(ctx, BOUNDED, &["1/100/", "1/555/"], &[42, 77]).await;
    let Some(Rendered::Code(code)) = remote_requests(&events, 0).pop() else {
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
    assert!(remote_requests(&events, 0).is_empty());
    assert_eq!(strings(&guest, "f_path"), Vec::<String>::new());
}
