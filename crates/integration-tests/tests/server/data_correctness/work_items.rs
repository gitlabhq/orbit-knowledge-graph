use super::helpers::*;

pub(super) async fn search_returns_correct_work_item_properties(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "node": {"id": "w", "entity": "WorkItem", "id_range": {"start": 1, "end": 10000},
                     "columns": ["title", "state", "work_item_type", "confidential", "weight"]},
            "order_by": {"node": "w", "property": "id", "direction": "ASC"},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(5);
    resp.assert_node_order("WorkItem", &[4000, 4001, 4002, 4003, 4010]);

    let login = resp.find_node("WorkItem", 4000).unwrap();
    login.assert_str("title", "Implement login page");
    login.assert_str("state", "opened");
    login.assert_str("work_item_type", "issue");
    assert_eq!(login.prop_bool("confidential"), Some(false));
    login.assert_i64("weight", 3);

    let auth_bug = resp.find_node("WorkItem", 4001).unwrap();
    auth_bug.assert_str("state", "closed");
    auth_bug.assert_str("work_item_type", "incident");
    assert_eq!(auth_bug.prop_bool("confidential"), Some(true));
    auth_bug.assert_i64("weight", 8);

    let tests = resp.find_node("WorkItem", 4002).unwrap();
    tests.assert_str("work_item_type", "task");
    assert!(tests.prop("weight").is_none(), "weight should be null");

    let objective = resp.find_node("WorkItem", 4003).unwrap();
    objective.assert_str("work_item_type", "epic");
    objective.assert_i64("weight", 13);
}

pub(super) async fn search_filter_work_item_type_returns_matching_rows(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "node": {"id": "w", "entity": "WorkItem", "id_range": {"start": 1, "end": 10000},
                     "columns": ["title", "work_item_type"],
                     "filters": {"work_item_type": {"op": "in", "value": ["issue", "task"]}}},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(3);
    resp.assert_node_ids("WorkItem", &[4000, 4002, 4010]);
    resp.assert_filter("WorkItem", "work_item_type", |n| {
        let t = n.prop_str("work_item_type").unwrap_or("");
        t == "issue" || t == "task"
    });
}

pub(super) async fn traversal_user_authored_work_item_returns_correct_edges(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
                {"id": "w", "entity": "WorkItem", "columns": ["title", "state"]}
            ],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "w"}],
            "limit": 20
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(9);
    resp.assert_referential_integrity();

    resp.assert_edge_exists("User", 1, "WorkItem", 4000, "AUTHORED");
    resp.assert_edge_exists("User", 2, "WorkItem", 4001, "AUTHORED");
    resp.assert_edge_exists("User", 1, "WorkItem", 4002, "AUTHORED");
    resp.assert_edge_exists("User", 3, "WorkItem", 4003, "AUTHORED");
    resp.assert_edge_exists("User", 7, "WorkItem", 4010, "AUTHORED");

    resp.assert_node("WorkItem", 4000, |n| {
        n.prop_str("title") == Some("Implement login page") && n.prop_str("state") == Some("opened")
    });
    resp.assert_node("WorkItem", 4001, |n| {
        n.prop_str("title") == Some("Fix auth bug") && n.prop_str("state") == Some("closed")
    });
}

pub(super) async fn traversal_work_item_in_group_returns_correct_edges(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "w", "entity": "WorkItem", "id_range": {"start": 1, "end": 10000}, "columns": ["title"]},
                {"id": "g", "entity": "Group", "columns": ["name"]}
            ],
            "relationships": [{"type": "IN_GROUP", "from": "w", "to": "g"}],
            "limit": 20
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(7);
    resp.assert_referential_integrity();

    resp.assert_edge_exists("WorkItem", 4000, "Group", 100, "IN_GROUP");
    resp.assert_edge_exists("WorkItem", 4001, "Group", 100, "IN_GROUP");
    resp.assert_edge_exists("WorkItem", 4002, "Group", 101, "IN_GROUP");
    resp.assert_edge_exists("WorkItem", 4003, "Group", 102, "IN_GROUP");

    resp.assert_node("Group", 100, |n| n.prop_str("name") == Some("Public Group"));
}

pub(super) async fn traversal_work_item_in_milestone_returns_correct_edges(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "w", "entity": "WorkItem", "id_range": {"start": 1, "end": 10000}, "columns": ["title"]},
                {"id": "m", "entity": "Milestone", "columns": ["title", "state"]}
            ],
            "relationships": [{"type": "IN_MILESTONE", "from": "w", "to": "m"}],
            "limit": 20
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(3);
    resp.assert_referential_integrity();

    resp.assert_edge_exists("WorkItem", 4000, "Milestone", 6000, "IN_MILESTONE");
    resp.assert_edge_exists("WorkItem", 4001, "Milestone", 6000, "IN_MILESTONE");

    resp.assert_node("Milestone", 6000, |n| {
        n.prop_str("title") == Some("Sprint 1") && n.prop_str("state") == Some("active")
    });
}

pub(super) async fn traversal_user_assigned_work_item_returns_correct_edges(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
                {"id": "w", "entity": "WorkItem", "columns": ["title"]}
            ],
            "relationships": [{"type": "ASSIGNED", "from": "u", "to": "w"}],
            "limit": 20
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(5);
    resp.assert_referential_integrity();

    resp.assert_edge_exists("User", 1, "WorkItem", 4000, "ASSIGNED");
    resp.assert_edge_exists("User", 2, "WorkItem", 4000, "ASSIGNED");
    resp.assert_edge_exists("User", 3, "WorkItem", 4001, "ASSIGNED");

    resp.assert_node("User", 1, |n| n.prop_str("username") == Some("alice"));
    resp.assert_node("WorkItem", 4000, |n| {
        n.prop_str("title") == Some("Implement login page")
    });
}

pub(super) async fn traversal_work_item_in_project_returns_correct_edges(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "w", "entity": "WorkItem", "id_range": {"start": 1, "end": 10000}, "columns": ["title"]},
                {"id": "p", "entity": "Project", "columns": ["name"]}
            ],
            "relationships": [{"type": "IN_PROJECT", "from": "w", "to": "p"}],
            "limit": 20
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(5);
    resp.assert_referential_integrity();

    resp.assert_edge_exists("WorkItem", 4000, "Project", 1000, "IN_PROJECT");
    resp.assert_edge_exists("WorkItem", 4001, "Project", 1000, "IN_PROJECT");
    resp.assert_edge_exists("WorkItem", 4010, "Project", 1010, "IN_PROJECT");

    resp.assert_node("Project", 1000, |n| {
        n.prop_str("name") == Some("Public Project")
    });
}

pub(super) async fn traversal_user_closed_work_item_returns_correct_edges(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
                {"id": "w", "entity": "WorkItem", "columns": ["title", "state"]}
            ],
            "relationships": [{"type": "CLOSED", "from": "u", "to": "w"}],
            "limit": 20
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(2);
    resp.assert_referential_integrity();

    resp.assert_edge_exists("User", 2, "WorkItem", 4001, "CLOSED");

    resp.assert_node("User", 2, |n| n.prop_str("username") == Some("bob"));
    resp.assert_node("WorkItem", 4001, |n| {
        n.prop_str("title") == Some("Fix auth bug") && n.prop_str("state") == Some("closed")
    });
}

pub(super) async fn traversal_work_item_has_label_returns_correct_edges(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "w", "entity": "WorkItem", "id_range": {"start": 1, "end": 10000}, "columns": ["title"]},
                {"id": "l", "entity": "Label", "columns": ["title", "color"]}
            ],
            "relationships": [{"type": "HAS_LABEL", "from": "w", "to": "l"}],
            "limit": 20
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(5);
    resp.assert_referential_integrity();

    resp.assert_edge_exists("WorkItem", 4000, "Label", 7000, "HAS_LABEL");
    resp.assert_edge_exists("WorkItem", 4000, "Label", 7001, "HAS_LABEL");
    resp.assert_edge_exists("WorkItem", 4001, "Label", 7002, "HAS_LABEL");

    resp.assert_node("Label", 7000, |n| {
        n.prop_str("title") == Some("bug") && n.prop_str("color") == Some("#d73a4a")
    });
    resp.assert_node("Label", 7001, |n| n.prop_str("title") == Some("feature"));
}
