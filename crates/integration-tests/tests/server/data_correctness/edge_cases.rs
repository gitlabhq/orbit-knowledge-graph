use super::helpers::*;

pub(super) async fn traversal_referential_integrity_on_complex_query(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User"},
                {"id": "g", "entity": "Group"},
                {"id": "p", "entity": "Project"}
            ],
            "relationships": [
                {"type": "MEMBER_OF", "from": "u", "to": "g"},
                {"type": "CONTAINS", "from": "g", "to": "p"}
            ],
            "limit": 50
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(14);
    resp.assert_referential_integrity();

    let member_of = resp.edges_of_type("MEMBER_OF");
    assert!(!member_of.is_empty(), "should have MEMBER_OF edges");
    let contains = resp.edges_of_type("CONTAINS");
    assert!(!contains.is_empty(), "should have CONTAINS edges");
}

pub(super) async fn giant_string_survives_pipeline(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "n", "entity": "Note", "columns": ["note"], "node_ids": [3002]},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(1);
    resp.assert_node_ids("Note", &[3002]);
    resp.assert_node("Note", 3002, |n| {
        n.prop_str("note")
            .is_some_and(|s| s.len() == 10_000 && s.chars().all(|c| c == 'x'))
    });
}

pub(super) async fn sql_injection_string_preserved(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "n", "entity": "Note", "columns": ["note"], "node_ids": [3003]},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(1);
    resp.assert_node_ids("Note", &[3003]);
    resp.assert_node("Note", 3003, |n| {
        n.prop_str("note").is_some_and(|s| s.contains("DROP TABLE"))
    });
}

pub(super) async fn empty_result_has_valid_schema(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username"], "node_ids": [99999]},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.skip_requirement(Requirement::NodeIds);
    resp.assert_node_count(0);
    assert_eq!(resp.edge_count(), 0);
}
