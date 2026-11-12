//! ADR 024's query 4 with ClickHouse on the test container: the merge request stage and the scope
//! lookup run real SQL, Orbit Next runs behind its seeded fake, and the engine joins both.

use std::collections::BTreeSet;
use std::sync::Arc;

use compiler::dag::Rendered;
use compiler::orbit_authorization::Key;
use integration_testkit::TestContext;
use integration_testkit::dag_fakes::{AllowedKeys, dag_packages, run_dag};
use orbit_server::pipeline::graph_executors;
use query_engine::pipeline::{AuthorizedPath, SecurityContext};
use query_engine::shared::dag::{Event, Outcome};

const QUERY_4: &str = "MATCH (mr:MergeRequest {state: 'opened'})-[:SOURCE_HEAD]->(h:Head)-[:HEAD_FILE]->(f:File) WHERE f.content MATCHES ast('todo!($$$MSG)', 'rust') RETURN mr.iid, h.branch, f.path, match.MSG ORDER BY mr.iid LIMIT 20";

pub(super) async fn query_4_searches_the_heads_clickhouse_returns(ctx: &TestContext) {
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
    let session =
        SecurityContext::new_with_roles(1, vec![AuthorizedPath::new("1/100/", 20)]).unwrap();
    let config = orbit_server_config::AppConfig::embedded_defaults().authorization;
    let grants =
        orbit_server::authorization::gitlab::session_grants(&config, &model, session).unwrap();
    let checks = AllowedKeys(BTreeSet::from([Key::Int(1000)]));
    let (events, outcome) = run_dag(&model, executor, QUERY_4, &grants, "alice", &checks).await;
    if let Err(error) = &outcome {
        panic!("query 4 failed: {error}");
    }
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
    let Ok(Outcome::Rows { batches, .. }) = outcome else {
        panic!("query 4 failed: {outcome:?}");
    };
    let rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(rows, 1);
    let paths = batches[0].column_by_name("f_path").unwrap();
    let paths = paths
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!(paths.value(0), "src/feature.rs");
}
