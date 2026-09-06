use super::helpers::*;
use query_engine::compiler::TokenScope;
use std::collections::HashMap;

fn token_context() -> SecurityContext {
    let mut security = SecurityContext::new(1, vec!["1/".into()]).unwrap();
    security.token_scopes = Some(HashMap::from([
        ("User".into(), TokenScope::All),
        (
            "Group".into(),
            TokenScope::Namespaces(vec!["1/100/".into()].into()),
        ),
        (
            "Project".into(),
            TokenScope::Namespaces(vec!["1/100/1000/".into()].into()),
        ),
        (
            "MergeRequest".into(),
            TokenScope::Namespaces(vec!["1/100/1000/".into()].into()),
        ),
    ]));
    security
}

pub(super) async fn exact_leaf_and_empty_grants(ctx: &TestContext) {
    let json = r#"{"query_type":"traversal","nodes":[{"id":"p","entity":"Project","id_range":{"start":1,"end":10000}}],"limit":20}"#;
    let response = run_query_with_security(ctx, json, &allow_all(), token_context()).await;
    response.assert_node_count(1);
    response.assert_node_ids("Project", &[1000]);
    let mut security = token_context();
    security.token_scopes = Some(HashMap::new());
    let response = run_query_with_security(ctx, json, &allow_all(), security).await;
    response.assert_node_count(0);
}

pub(super) async fn aggregate_filters_unreturned_input(ctx: &TestContext) {
    let json = r#"{"query_type":"aggregation","nodes":[{"id":"u","entity":"User","node_ids":[1]},{"id":"mr","entity":"MergeRequest"}],"relationships":[{"type":"AUTHORED","from":"u","to":"mr"}],"group_by":["u"],"aggregations":[{"count":"u","as":"total"}]}"#;
    let response = run_query_with_security(ctx, json, &allow_all(), token_context()).await;
    response.assert_group_node_ids("u", "User", &[1]);
    response.assert_group_row_value_i64("u", "User", 1, "total", 2);
    let mut security = token_context();
    security
        .token_scopes
        .as_mut()
        .unwrap()
        .remove("MergeRequest");
    let response = run_query_with_security(ctx, json, &allow_all(), security).await;
    response.assert_empty_aggregation();
}

pub(super) async fn dynamic_neighbors_filter_each_endpoint(ctx: &TestContext) {
    let response = run_query_with_security(ctx,
        r#"{"query_type":"neighbors","nodes":[{"id":"g","entity":"Group","node_ids":[100]}],"neighbors":{"direction":"outgoing","rel_types":["CONTAINS"]}}"#,
        &allow_all(), token_context()).await;
    response.assert_node_count(2);
    response.assert_node_ids("Group", &[100]);
    response.assert_node_ids("Project", &[1000]);
    response.assert_edge_set("CONTAINS", &[(100, 1000)]);
}

pub(super) async fn path_requires_intermediate_permission(ctx: &TestContext) {
    let json = r#"{"query_type":"path_finding","nodes":[{"id":"start","entity":"User","node_ids":[1]},{"id":"end","entity":"Project","node_ids":[1000]}],"path":{"type":"shortest","from":"start","to":"end","max_depth":3,"rel_types":["MEMBER_OF","CONTAINS"]}}"#;
    let response = run_query_with_security(ctx, json, &allow_all(), token_context()).await;
    assert_eq!(response.path_ids().len(), 1);
    response.assert_node_count(3);
    let mut security = token_context();
    security.token_scopes.as_mut().unwrap().remove("Group");
    let response = run_query_with_security(ctx, json, &allow_all(), security).await;
    assert!(response.path_ids().is_empty());
    response.assert_node_count(0);
}

pub(super) async fn denied_page_omits_cursor(ctx: &TestContext) {
    let response = run_query_with_security(
        ctx,
        r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User","id_range":{"start":1,"end":10000}}],"cursor":{"page_size":2}}"#,
        &MockRedactionService::new(),
        token_context(),
    )
    .await;
    response.assert_node_count(0);
    let pagination = response.response.pagination.as_ref().unwrap();
    assert!(!pagination.has_more);
    assert!(pagination.next_cursor.is_none());
}
