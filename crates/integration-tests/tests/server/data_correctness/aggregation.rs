use super::helpers::*;

pub(super) async fn aggregation_count_returns_correct_values(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "u", "entity": "User", "columns": ["username"]},
                {"id": "mr", "entity": "MergeRequest"}
            ],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "mr"}],
            "aggregations": [{"function": "count", "target": "mr", "group_by": "u", "alias": "mr_count"}],
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node("User", 1, |n| {
        n.prop_str("username") == Some("alice") && n.prop_i64("mr_count") == Some(2)
    });

    resp.assert_node("User", 2, |n| {
        n.prop_str("username") == Some("bob") && n.prop_i64("mr_count") == Some(1)
    });

    resp.assert_node("User", 3, |n| {
        n.prop_str("username") == Some("charlie") && n.prop_i64("mr_count") == Some(1)
    });
}

pub(super) async fn aggregation_count_group_contains_projects(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "g", "entity": "Group", "columns": ["name"]},
                {"id": "p", "entity": "Project"}
            ],
            "relationships": [{"type": "CONTAINS", "from": "g", "to": "p"}],
            "aggregations": [{"function": "count", "target": "p", "group_by": "g", "alias": "project_count"}],
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node("Group", 100, |n| {
        n.prop_str("name") == Some("Public Group") && n.prop_i64("project_count") == Some(2)
    });
    resp.assert_node("Group", 101, |n| {
        n.prop_str("name") == Some("Private Group") && n.prop_i64("project_count") == Some(2)
    });
    resp.assert_node("Group", 102, |n| {
        n.prop_str("name") == Some("Internal Group") && n.prop_i64("project_count") == Some(1)
    });
}

pub(super) async fn aggregation_sort_orders_by_aggregate_value(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "g", "entity": "Group", "columns": ["name"]},
                {"id": "u", "entity": "User"}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "aggregations": [{"function": "count", "target": "u", "group_by": "g", "alias": "member_count"}],
            "aggregation_sort": {"agg_index": 0, "direction": "DESC"},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_order("Group", &[101, 100, 102]);

    resp.assert_node("Group", 101, |n| n.prop_i64("member_count") == Some(4));
    resp.assert_node("Group", 100, |n| n.prop_i64("member_count") == Some(3));
    resp.assert_node("Group", 102, |n| n.prop_i64("member_count") == Some(2));
}

pub(super) async fn aggregation_sum_produces_correct_totals(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "g", "entity": "Group", "columns": ["name"]},
                {"id": "u", "entity": "User"}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "aggregations": [{"function": "sum", "target": "u", "property": "id", "group_by": "g", "alias": "id_sum"}],
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node("Group", 100, |n| n.prop_i64("id_sum") == Some(1 + 2 + 6));
    resp.assert_node("Group", 101, |n| {
        n.prop_i64("id_sum") == Some(3 + 4 + 5 + 6)
    });
    resp.assert_node("Group", 102, |n| n.prop_i64("id_sum") == Some(1 + 4));
}

pub(super) async fn aggregation_redaction_excludes_unauthorized_from_counts(ctx: &TestContext) {
    let mut svc = MockRedactionService::new();
    svc.allow("user", &[1, 2]);
    svc.allow("group", &[100, 101, 102, 200, 300]);

    let resp = run_query(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "g", "entity": "Group", "columns": ["name"]},
                {"id": "u", "entity": "User"}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "aggregations": [{"function": "count", "target": "u", "group_by": "g", "alias": "member_count"}],
            "limit": 10
        }"#,
        &svc,
    )
    .await;

    // Aggregation counts are computed in ClickHouse SQL before redaction.
    // Redaction removes unauthorized *rows* (entity-level), not aggregated
    // values within surviving rows. Group 100 has 3 MEMBER_OF edges in the
    // DB so count stays 3 even though only users 1,2 are authorized.
    resp.assert_node("Group", 100, |n| n.prop_i64("member_count") == Some(3));
}

pub(super) async fn aggregation_avg_produces_correct_values(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "g", "entity": "Group", "columns": ["name"]},
                {"id": "u", "entity": "User"}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "aggregations": [{"function": "avg", "target": "u", "property": "id", "group_by": "g", "alias": "avg_id"}],
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    // Group 100: users 1,2,6 → avg = 3.0
    // Group 101: users 3,4,5,6 → avg = 4.5
    // Group 102: users 1,4 → avg = 2.5
    resp.assert_node("Group", 100, |n| n.prop_f64("avg_id") == Some(3.0));
    resp.assert_node("Group", 101, |n| n.prop_f64("avg_id") == Some(4.5));
    resp.assert_node("Group", 102, |n| n.prop_f64("avg_id") == Some(2.5));
}

pub(super) async fn aggregation_min_max_produce_correct_values(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "g", "entity": "Group", "columns": ["name"]},
                {"id": "u", "entity": "User"}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "aggregations": [
                {"function": "min", "target": "u", "property": "id", "group_by": "g", "alias": "min_id"},
                {"function": "max", "target": "u", "property": "id", "group_by": "g", "alias": "max_id"}
            ],
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    // Group 100: users 1,2,6 → min=1 max=6
    // Group 101: users 3,4,5,6 → min=3 max=6
    // Group 102: users 1,4 → min=1 max=4
    resp.assert_node("Group", 100, |n| {
        n.prop_i64("min_id") == Some(1) && n.prop_i64("max_id") == Some(6)
    });
    resp.assert_node("Group", 101, |n| {
        n.prop_i64("min_id") == Some(3) && n.prop_i64("max_id") == Some(6)
    });
    resp.assert_node("Group", 102, |n| {
        n.prop_i64("min_id") == Some(1) && n.prop_i64("max_id") == Some(4)
    });
}

pub(super) async fn aggregation_min_on_string_column(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "g", "entity": "Group", "columns": ["name"]},
                {"id": "u", "entity": "User"}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "aggregations": [{"function": "min", "target": "u", "property": "username", "group_by": "g", "alias": "first_username"}],
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    // Lexicographic min: Group 100 (alice,bob,用户) → alice
    // Group 101 (charlie,diana,eve,用户) → charlie
    // Group 102 (alice,diana) → alice
    resp.assert_node("Group", 100, |n| {
        n.prop_str("first_username") == Some("alice")
    });
    resp.assert_node("Group", 101, |n| {
        n.prop_str("first_username") == Some("charlie")
    });
    resp.assert_node("Group", 102, |n| {
        n.prop_str("first_username") == Some("alice")
    });
}

pub(super) async fn aggregation_multiple_functions_in_one_query(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "g", "entity": "Group", "columns": ["name"]},
                {"id": "u", "entity": "User"}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "aggregations": [
                {"function": "count", "target": "u", "group_by": "g", "alias": "cnt"},
                {"function": "avg", "target": "u", "property": "id", "group_by": "g", "alias": "avg_id"},
                {"function": "min", "target": "u", "property": "id", "group_by": "g", "alias": "min_id"}
            ],
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    // Group 100: 3 members, avg=3.0, min=1
    resp.assert_node("Group", 100, |n| {
        n.prop_i64("cnt") == Some(3)
            && n.prop_f64("avg_id") == Some(3.0)
            && n.prop_i64("min_id") == Some(1)
    });
    // Group 101: 4 members, avg=4.5, min=3
    resp.assert_node("Group", 101, |n| {
        n.prop_i64("cnt") == Some(4)
            && n.prop_f64("avg_id") == Some(4.5)
            && n.prop_i64("min_id") == Some(3)
    });
}
