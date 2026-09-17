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

#[test]
fn filter_only_node_projects_columns_inline_and_drops_its_template() {
    let json = r#"{"query_type":"traversal","nodes":[{"id":"mr","entity":"MergeRequest","node_ids":[1]},{"id":"wi","entity":"WorkItem","filters":{"title":{"contains":"flaky"}},"columns":["title"]}],"relationships":[{"type":"CLOSES","from":"mr","to":"wi"}]}"#;
    let orbit_query = "MATCH (mr:MergeRequest {id: 1})-[:CLOSES]->(wi:WorkItem) WHERE wi.title CONTAINS 'flaky' RETURN mr, wi.title";
    let compiled = compile_pair(json, orbit_query, &embedded_ontology(), &test_ctx()).unwrap();
    let sql = &compiled.base.sql;
    assert!(sql.contains("AS wi_title"), "{sql}");
    match &compiled.hydration {
        HydrationPlan::Static(templates) => {
            assert!(
                templates.iter().all(|t| t.node_alias != "wi"),
                "{templates:?}"
            );
        }
        HydrationPlan::None => {}
        other => panic!("expected Static or None, got {other:?}"),
    }
}

#[test]
fn search_with_virtual_column_hydrates_only_its_dependencies() {
    let json = r#"{"query_type":"traversal","nodes":[{"id":"f","entity":"File","node_ids":[1],"columns":["name","content"]}],"limit":5}"#;
    let orbit_query = "MATCH (f:File {id: 1}) RETURN f.name, f.content LIMIT 5";
    let compiled = compile_pair(json, orbit_query, &embedded_ontology(), &test_ctx()).unwrap();
    assert!(
        compiled.base.sql.contains("AS f_name"),
        "{}",
        compiled.base.sql
    );
    match &compiled.hydration {
        HydrationPlan::Static(templates) => {
            let f = &templates[0];
            assert!(!f.columns.contains(&"name".to_string()), "{:?}", f.columns);
            assert!(f.virtual_columns.iter().any(|v| v.column_name == "content"));
        }
        other => panic!("expected Static, got {other:?}"),
    }
}
