use crate::compiler::setup::{compile_pair, embedded_ontology, test_ctx};
use compiler::HydrationPlan;

#[test]
fn fk_traversal_projects_columns_inline_and_skips_hydration() {
    let json = r#"{"query_type":"traversal","nodes":[{"id":"n","entity":"Note","node_ids":[1],"columns":["confidential"]},{"id":"u","entity":"User","columns":["username"]}],"relationships":[{"type":"AUTHORED","from":"u","to":"n"}]}"#;
    let orbit_query =
        "MATCH (n:Note {id: 1})<-[:AUTHORED]-(u:User) RETURN n.confidential, u.username";
    let compiled = compile_pair(json, orbit_query, &embedded_ontology(), &test_ctx()).unwrap();
    let sql = &compiled.base.sql;
    assert!(sql.contains("AS n_confidential"), "{sql}");
    assert!(sql.contains("AS u_username"), "{sql}");
    assert_eq!(compiled.hydration, HydrationPlan::None, "{sql}");
}

#[test]
fn edge_chain_traversal_keeps_hydration_for_lazy_nodes() {
    let json = r#"{"query_type":"traversal","nodes":[{"id":"mr","entity":"MergeRequest","node_ids":[1],"columns":["title"]},{"id":"wi","entity":"WorkItem","columns":["title"]}],"relationships":[{"type":"CLOSES","from":"mr","to":"wi"}]}"#;
    let orbit_query =
        "MATCH (mr:MergeRequest {id: 1})-[:CLOSES]->(wi:WorkItem) RETURN mr.title, wi.title";
    let compiled = compile_pair(json, orbit_query, &embedded_ontology(), &test_ctx()).unwrap();
    let sql = &compiled.base.sql;
    assert!(!sql.contains("AS wi_title"), "{sql}");
    match &compiled.hydration {
        HydrationPlan::Static(templates) => {
            let wi = templates.iter().find(|t| t.node_alias == "wi").unwrap();
            assert_eq!(wi.columns, vec!["title".to_string()]);
        }
        other => panic!("expected Static, got {other:?}"),
    }
}
