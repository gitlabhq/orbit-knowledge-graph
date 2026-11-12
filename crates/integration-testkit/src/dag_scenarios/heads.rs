//! ADR 024's query 4: open merge requests read in ClickHouse hand their (project, source branch)
//! heads to Orbit Next, which searches only those heads; the engine joins both sides. `Head` has
//! a composite identity, so the key set that crosses the stage is composite.

use compiler::dag::{Backend, Input, KeyInput, Rendered};
use compiler::orbit_authorization::Key;
use query_engine::shared::dag::Event;
use serde_json::json;

use super::single::ops;
use super::{caller, row, run};

const QUERY_4: &str = "MATCH (mr:MergeRequest {state: 'opened'})-[:SOURCE_HEAD]->(h:Head)-[:HEAD_FILE]->(f:File) WHERE f.content MATCHES ast('todo!($$$MSG)', 'rust') RETURN mr.iid, h.branch, f.path, match.MSG ORDER BY mr.iid LIMIT 20";

fn open_merge_requests() -> Vec<query_data_model::catalog::Row> {
    vec![
        row(
            json!({"mr_iid": 1, "mr_project_id": 42, "mr_source_branch": "feature-a", "_orbit_mr_id": 2000, "_orbit_mr_type": "MergeRequest"}),
        ),
        row(
            json!({"mr_iid": 2, "mr_project_id": 77, "mr_source_branch": "fix-b", "_orbit_mr_id": 2001, "_orbit_mr_type": "MergeRequest"}),
        ),
    ]
}

#[tokio::test]
async fn open_merge_requests_search_their_branch_heads_in_orbit_next() {
    let ran = run(QUERY_4, &caller("1/9970/"), open_merge_requests()).await;
    if let Err(error) = &ran.outcome {
        panic!("query 4 failed: {error}");
    }
    let logical = ran
        .events
        .iter()
        .find_map(|event| match event {
            Event::Logical { plan } => Some(plan.clone()),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        ops(&logical),
        [
            "Entities",
            "Filter",
            "Follow",
            "Follow",
            "Structural",
            "Order",
            "Select",
            "Limit"
        ]
    );
    let split = ran
        .events
        .iter()
        .find_map(|event| match event {
            Event::Split { stages } => Some(stages.clone()),
            _ => None,
        })
        .unwrap();
    let backends: Vec<Backend> = split.iter().map(|stage| stage.backend).collect();
    assert_eq!(
        backends,
        [Backend::ClickHouse, Backend::OrbitCode, Backend::Engine]
    );
    assert_eq!(
        split[1].inputs,
        [Input::Keys(KeyInput {
            from: 0,
            columns: vec!["mr_project_id".into(), "mr_source_branch".into()],
            binding: "h".into(),
            properties: vec!["project_id".into(), "branch".into()],
        })]
    );
    let keys = ran.events.iter().find_map(|event| match event {
        Event::Keys { sample, .. } => Some(sample.clone()),
        _ => None,
    });
    assert_eq!(
        keys,
        Some(vec![
            Key::Composite(vec![Key::Int(42), Key::Text("feature-a".into())]),
            Key::Composite(vec![Key::Int(77), Key::Text("fix-b".into())]),
        ])
    );
    let Rendered::Sql { sql, .. } = &ran.rendered(0)[0] else {
        panic!("the merge request stage renders SQL");
    };
    assert!(
        sql.contains("mr.project_id AS mr_project_id, mr.source_branch AS mr_source_branch"),
        "join keys are never excerpted: {sql}"
    );
    assert_eq!(
        ops(&ran.plan(1, false)),
        [
            "Repositories",
            "Branches",
            "Languages",
            "Prefilter",
            "Structural",
            "Top",
            "Select"
        ]
    );
    assert_eq!(
        ops(&ran.plan(1, true))[..3],
        ["Repositories", "Restrict", "Branches"]
    );
    let request = ran
        .rendered(1)
        .into_iter()
        .find_map(|request| match request {
            Rendered::Code(request) => Some(request),
            _ => None,
        })
        .unwrap();
    assert_eq!(request.repositories, [42]);
    assert_eq!(request.heads, [(42, "feature-a".to_string())]);
    assert_eq!(
        serde_json::to_value(&request).unwrap()["heads"],
        json!([[42, "feature-a"]])
    );
    assert_eq!(
        ran.rows(),
        [row(json!({
            "mr_iid": 1,
            "h_branch": "feature-a",
            "f_path": "src/auth.rs",
            "match.MSG": "\"check the token scope\"",
            "_orbit_mr_id": 2000,
            "_orbit_mr_type": "MergeRequest",
        }))]
    );
}
