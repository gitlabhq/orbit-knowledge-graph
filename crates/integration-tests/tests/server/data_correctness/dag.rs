//! ADR 024's DAG queries with ClickHouse on the test container: relational stages and scope
//! lookups run real SQL, Orbit Next and the Data Extraction Service run behind their seeded fakes,
//! and the engine joins every side.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{Array, StringArray};
use arrow::record_batch::RecordBatch;
use compiler::dag::Rendered;
use compiler::orbit_authorization::{Grants, Key};
use integration_testkit::TestContext;
use integration_testkit::dag_fakes::{AllowedKeys, dag_packages, run_dag};
use orbit_server::pipeline::graph_executors;
use query_engine::pipeline::{AuthorizedPath, SecurityContext};
use query_engine::shared::dag::{Event, Executor, Outcome};

const QUERY_4: &str = "MATCH (mr:MergeRequest {state: 'opened'})-[:SOURCE_HEAD]->(h:Head)-[:HEAD_FILE]->(f:File) WHERE f.content MATCHES ast('todo!($$$MSG)', 'rust') RETURN mr.iid, h.branch, f.path, match.MSG ORDER BY mr.iid LIMIT 20";

const DISCUSSED: &str = "MATCH (mr:MergeRequest {state: 'opened'})<-[:DISCUSSES]-(m:SlackMessage) RETURN mr.iid, m.text ORDER BY mr.iid LIMIT 10";

/// The DAG model on the container's graph connection, and the executor the server registers.
fn graph(ctx: &TestContext) -> (Arc<query_data_model::Model>, Arc<dyn Executor>) {
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
    let executor = Arc::clone(executors.get(graph.connection()).unwrap());
    (model, executor)
}

fn grants(model: &query_data_model::Model, path: &str) -> Grants {
    let session = SecurityContext::new_with_roles(1, vec![AuthorizedPath::new(path, 20)]).unwrap();
    let config = orbit_server_config::AppConfig::embedded_defaults().authorization;
    orbit_server::authorization::gitlab::session_grants(&config, model, session).unwrap()
}

/// Creates the Slack workspace table the DAG model adds and installs workspace T01 in group 100.
async fn install_workspace(ctx: &TestContext) {
    let schema = integration_testkit::graph_schema(&dag_packages().fragments().unwrap());
    let table = schema
        .tables
        .iter()
        .find(|table| table.name == "gl_slack_workspace")
        .expect("the DAG model stores Slack workspaces");
    ctx.execute(&table.to_create_sql(&integration_testkit::TABLE_PREFIX))
        .await;
    ctx.execute(&format!(
        "INSERT INTO {} (id, name, group_id, traversal_path) VALUES ('T01', 'Orbit', 100, '1/100/')",
        integration_testkit::t("gl_slack_workspace")
    ))
    .await;
}

fn column(batches: &[RecordBatch], name: &str) -> Vec<String> {
    let mut values = Vec::new();
    for batch in batches {
        let column = batch.column_by_name(name).unwrap();
        let column = column.as_any().downcast_ref::<StringArray>().unwrap();
        values.extend((0..column.len()).map(|row| column.value(row).to_string()));
    }
    values
}

pub(super) async fn query_4_searches_the_heads_clickhouse_returns(ctx: &TestContext) {
    let (model, executor) = graph(ctx);
    let checks = AllowedKeys(BTreeSet::from([Key::Int(1000)]));
    let grants = grants(&model, "1/100/");
    let (events, outcome) = run_dag(&model, executor, QUERY_4, &grants, "alice", &checks).await;
    let Ok(Outcome { executed, .. }) = outcome else {
        panic!("query 4 failed: {outcome:?}");
    };
    let keys = events.iter().find_map(|event| match event {
        Event::Keys { sample, .. } => Some(sample.clone()),
        _ => None,
    });
    assert_eq!(
        keys,
        Some(vec![
            Key::Composite(vec![Key::Int(1000), Key::Text("feature-a".into())]),
            Key::Composite(vec![Key::Int(1000), Key::Text("fix-b".into())]),
        ])
    );
    let code = events
        .iter()
        .find_map(|event| match event {
            Event::Rendered { stage: 1, requests } => {
                requests.iter().find_map(|request| match request {
                    Rendered::Code(request) => Some(request.clone()),
                    _ => None,
                })
            }
            _ => None,
        })
        .expect("the code stage renders a request");
    assert_eq!(code.repositories, [1000]);
    assert_eq!(
        code.heads,
        [(1000, "feature-a".to_string()), (1000, "fix-b".to_string())]
    );
    assert_eq!(column(&executed.batches, "f_path"), ["src/feature.rs"]);
}

/// The workspace lookup joins SlackWorkspace to Group, so its columns must come back under the
/// names the stage reads, not ClickHouse's qualified ones.
pub(super) async fn merge_requests_join_the_slack_messages_that_discuss_them(ctx: &TestContext) {
    install_workspace(ctx).await;
    let (model, executor) = graph(ctx);
    let grants = grants(&model, "1/100/");
    let checks = AllowedKeys(BTreeSet::new());
    let (events, outcome) = run_dag(&model, executor, DISCUSSED, &grants, "alice", &checks).await;
    let Ok(Outcome { executed, .. }) = outcome else {
        panic!("the Slack join failed: {outcome:?}");
    };
    let workspaces = events.iter().find_map(|event| match event {
        Event::Rendered { stage: 1, requests } => {
            requests.iter().find_map(|request| match request {
                Rendered::Live(call) => Some(call.workspaces.clone()),
                _ => None,
            })
        }
        _ => None,
    });
    assert_eq!(workspaces, Some(vec!["T01".to_string()]));
    assert_eq!(
        column(&executed.batches, "m_text"),
        ["deploy to staging at 5pm", "deploy memes"]
    );
}

/// A caller who reads the merge requests but no workspace gets an empty join, not an error.
pub(super) async fn a_caller_who_reads_no_workspace_gets_no_messages(ctx: &TestContext) {
    let (model, executor) = graph(ctx);
    let grants = grants(&model, "1/100/1000/");
    let checks = AllowedKeys(BTreeSet::new());
    let (_, outcome) = run_dag(&model, executor, DISCUSSED, &grants, "alice", &checks).await;
    let Ok(Outcome { executed, .. }) = outcome else {
        panic!("the Slack join failed: {outcome:?}");
    };
    assert_eq!(column(&executed.batches, "m_text"), Vec::<String>::new());
}
