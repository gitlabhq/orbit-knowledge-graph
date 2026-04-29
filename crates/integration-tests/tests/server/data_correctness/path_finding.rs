use super::helpers::*;

pub(super) async fn path_finding_returns_valid_complete_paths(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "entity": "User", "node_ids": [1]},
                {"id": "end", "entity": "Project", "node_ids": [1000]}
            ],
            "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3,
                     "rel_types": ["MEMBER_OF", "CONTAINS"]}
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_referential_integrity();

    let pids = resp.path_ids();
    assert_eq!(
        pids.len(),
        1,
        "exactly one shortest path from User 1 to Project 1000"
    );

    for &pid in pids.iter() {
        let path = resp.path(pid);
        assert_eq!(path.len(), 2, "path {pid}: User→Group→Project = 2 edges");

        let first = path[0];
        assert_eq!(first.from, "User");
        assert_eq!(first.from_id, 1);
        assert_eq!(first.edge_type, "MEMBER_OF");
        assert_eq!(first.step, Some(0));

        let last = path.last().unwrap();
        assert_eq!(last.to, "Project");
        assert_eq!(last.to_id, 1000);
        assert_eq!(last.edge_type, "CONTAINS");

        for edge in &path {
            assert_eq!(edge.path_id, Some(pid), "edge should belong to path {pid}");
            assert!(edge.step.is_some(), "path_finding edges must have step");
        }
    }

    resp.assert_node_exists("User", 1);
    resp.assert_node_exists("Group", 100);
    resp.assert_node_exists("Project", 1000);
}

pub(super) async fn path_finding_filtered_start_endpoint_reaches_project(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "entity": "User", "filters": {"username": {"op": "eq", "value": "alice"}}},
                {"id": "end", "entity": "Project", "node_ids": [1004]}
            ],
            "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3,
                     "rel_types": ["MEMBER_OF", "CONTAINS"]}
        }"#,
        &allow_all(),
    )
    .await;

    let pids = resp.path_ids();
    assert_eq!(
        pids.len(),
        1,
        "filtered start endpoint should find alice's path to Project 1004"
    );

    let path = resp.path(pids[0]);
    assert_eq!(path.len(), 2);
    assert_eq!((path[0].from.as_str(), path[0].from_id), ("User", 1));
    assert_eq!(path[0].edge_type, "MEMBER_OF");
    assert_eq!((path[1].to.as_str(), path[1].to_id), ("Project", 1004));
    assert_eq!(path[1].edge_type, "CONTAINS");
    resp.assert_filter("User", "username", |n| {
        n.prop_str("username") == Some("alice")
    });
    resp.assert_referential_integrity();
}

pub(super) async fn path_finding_wildcard_keeps_intermediate_hops_unconstrained(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "entity": "User", "node_ids": [1]},
                {"id": "end", "entity": "Project", "node_ids": [1004]}
            ],
            "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3,
                     "rel_types": ["*"]}
        }"#,
        &allow_all(),
    )
    .await;

    let pids = resp.path_ids();
    assert_eq!(
        pids.len(),
        1,
        "wildcard path should traverse User -> Group -> Project"
    );

    let path = resp.path(pids[0]);
    assert_eq!(path.len(), 2);
    assert_eq!((path[0].from.as_str(), path[0].from_id), ("User", 1));
    assert_eq!((path[0].to.as_str(), path[0].to_id), ("Group", 102));
    assert_eq!(path[0].edge_type, "MEMBER_OF");
    assert_eq!((path[1].from.as_str(), path[1].from_id), ("Group", 102));
    assert_eq!((path[1].to.as_str(), path[1].to_id), ("Project", 1004));
    assert_eq!(path[1].edge_type, "CONTAINS");
    resp.assert_referential_integrity();
}

pub(super) async fn path_finding_multiple_destinations_returns_distinct_paths(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "entity": "User", "node_ids": [1]},
                {"id": "end", "entity": "Project", "node_ids": [1000, 1002, 1004]}
            ],
            "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3,
                     "rel_types": ["MEMBER_OF", "CONTAINS"]}
        }"#,
        &allow_all(),
    )
    .await;

    let pids = resp.path_ids();
    assert_eq!(
        pids.len(),
        3,
        "exactly 3 paths: to 1000 (via 100), 1002 (via 100), 1004 (via 102)"
    );

    let destinations: HashSet<i64> = pids
        .iter()
        .filter_map(|&pid| resp.path(pid).last().map(|e| e.to_id))
        .collect();
    assert_eq!(
        destinations,
        HashSet::from([1000, 1002, 1004]),
        "each path should reach exactly one of the requested projects"
    );

    resp.assert_referential_integrity();
}

pub(super) async fn path_finding_consecutive_edges_connect(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "entity": "User", "node_ids": [1]},
                {"id": "end", "entity": "Project", "node_ids": [1000, 1004]}
            ],
            "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3,
                     "rel_types": ["MEMBER_OF", "CONTAINS"]}
        }"#,
        &allow_all(),
    )
    .await;

    let pids = resp.path_ids();
    assert_eq!(
        pids.len(),
        2,
        "exactly 2 paths: to 1000 (via 100) and 1004 (via 102)"
    );

    for &pid in pids.iter() {
        let path = resp.path(pid);
        assert_eq!(path.len(), 2, "path {pid}: User→Group→Project = 2 edges");
        for window in path.windows(2) {
            let prev = window[0];
            let next = window[1];
            assert_eq!(
                (prev.to.as_str(), prev.to_id),
                (next.from.as_str(), next.from_id),
                "consecutive path edges must connect: {prev:?} → {next:?}",
            );
        }
    }
}

pub(super) async fn path_finding_max_depth_too_shallow_returns_empty(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "entity": "User", "node_ids": [1]},
                {"id": "end", "entity": "Project", "node_ids": [1000]}
            ],
            "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 1}
        }"#,
        &allow_all(),
    )
    .await;

    let pids = resp.path_ids();
    assert!(
        pids.is_empty(),
        "max_depth=1 cannot reach User→Group→Project (needs 2 hops)"
    );
}

pub(super) async fn path_finding_redaction_blocks_intermediate_node(ctx: &TestContext) {
    let mut svc = MockRedactionService::new();
    svc.allow("user", &[1]);
    svc.allow("project", &[1000]);

    let resp = run_query(
        ctx,
        r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "entity": "User", "node_ids": [1]},
                {"id": "end", "entity": "Project", "node_ids": [1000]}
            ],
            "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3}
        }"#,
        &svc,
    )
    .await;

    let pids = resp.path_ids();
    assert!(
        pids.is_empty(),
        "path through unauthorized Group 100 should be blocked by redaction"
    );
}

pub(super) async fn path_finding_all_shortest_returns_valid_paths(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "entity": "User", "node_ids": [1]},
                {"id": "end", "entity": "Project", "node_ids": [1000]}
            ],
            "path": {"type": "all_shortest", "from": "start", "to": "end", "max_depth": 3,
                     "rel_types": ["MEMBER_OF", "CONTAINS"]}
        }"#,
        &allow_all(),
    )
    .await;

    let pids = resp.path_ids();
    assert!(
        !pids.is_empty(),
        "all_shortest should find at least one path from User 1 to Project 1000"
    );

    for &pid in pids.iter() {
        let path = resp.path(pid);
        assert_eq!(path.len(), 2, "path {pid}: User→Group→Project = 2 edges");

        let first = path[0];
        assert_eq!(first.from, "User");
        assert_eq!(first.from_id, 1);

        let last = path.last().unwrap();
        assert_eq!(last.to, "Project");
        assert_eq!(last.to_id, 1000);
    }

    resp.assert_referential_integrity();
}

pub(super) async fn path_finding_any_returns_at_least_one_path(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "entity": "User", "node_ids": [1]},
                {"id": "end", "entity": "Project", "node_ids": [1000]}
            ],
            "path": {"type": "any", "from": "start", "to": "end", "max_depth": 3}
        }"#,
        &allow_all(),
    )
    .await;

    let pids = resp.path_ids();
    assert!(
        !pids.is_empty(),
        "any should find at least one path from User 1 to Project 1000"
    );

    for &pid in pids.iter() {
        let path = resp.path(pid);
        assert!(!path.is_empty(), "path {pid} should have at least one edge");
        assert_eq!(path[0].from, "User");
        assert_eq!(path[0].from_id, 1);
        assert_eq!(path.last().unwrap().to, "Project");
        assert_eq!(path.last().unwrap().to_id, 1000);
    }
    resp.assert_referential_integrity();
}

pub(super) async fn path_finding_rel_types_restricts_traversal(ctx: &TestContext) {
    // Only allow MEMBER_OF edges. The path User→Group→Project requires
    // a CONTAINS edge for the second hop, so no path should be found.
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "entity": "User", "node_ids": [1]},
                {"id": "end", "entity": "Project", "node_ids": [1000]}
            ],
            "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3,
                     "rel_types": ["MEMBER_OF"]}
        }"#,
        &allow_all(),
    )
    .await;

    let pids = resp.path_ids();
    assert!(
        pids.is_empty(),
        "MEMBER_OF-only path cannot reach Project from User (needs CONTAINS)"
    );
}

pub(super) async fn path_finding_step_indices_are_sequential(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "entity": "User", "node_ids": [1]},
                {"id": "end", "entity": "Project", "node_ids": [1000]}
            ],
            "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3,
                     "rel_types": ["MEMBER_OF", "CONTAINS"]}
        }"#,
        &allow_all(),
    )
    .await;

    let pids = resp.path_ids();
    assert!(!pids.is_empty());

    for &pid in pids.iter() {
        let path = resp.path(pid);
        for (i, edge) in path.iter().enumerate() {
            assert_eq!(
                edge.step,
                Some(i),
                "edge {i} in path {pid} should have step={i}, got {:?}",
                edge.step
            );
        }
    }
}

pub(super) async fn path_finding_target_entity_constrains_results(ctx: &TestContext) {
    // User 1 has AUTHORED edges to MR 2000, MR 2001, Note 3000, and WorkItems.
    // Finding paths User(1) -> MergeRequest without rel_types should only return
    // paths ending at MergeRequest nodes, not Notes or WorkItems.
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "entity": "User", "node_ids": [1]},
                {"id": "end", "entity": "MergeRequest", "node_ids": [2000, 2001]}
            ],
            "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 1,
                     "rel_types": ["AUTHORED"]}
        }"#,
        &allow_all(),
    )
    .await;

    let pids = resp.path_ids();
    assert_eq!(
        pids.len(),
        2,
        "should find exactly 2 paths: User 1 -> MR 2000 and User 1 -> MR 2001"
    );

    for &pid in pids.iter() {
        let path = resp.path(pid);
        assert_eq!(path.len(), 1, "depth-1 path should have 1 edge");

        let edge = path[0];
        assert_eq!(edge.from, "User");
        assert_eq!(edge.from_id, 1);
        assert_eq!(
            edge.to, "MergeRequest",
            "path endpoint must be MergeRequest, not Note or WorkItem"
        );
        assert!(
            [2000, 2001].contains(&edge.to_id),
            "path should reach MR 2000 or 2001, got {}",
            edge.to_id
        );
    }

    resp.assert_referential_integrity();
    resp.assert_node_absent("Note", 3000);
}

pub(super) async fn path_finding_entity_filter_excludes_wrong_types(ctx: &TestContext) {
    // Find all paths from User(1) to any MergeRequest in the seed range.
    // Uses id_range instead of node_ids to exercise the filtered-endpoint
    // path. The frontier should only include edges where the target_kind
    // matches MergeRequest.
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "entity": "User", "node_ids": [1]},
                {"id": "end", "entity": "MergeRequest", "id_range": {"start": 2000, "end": 2003}}
            ],
            "path": {"type": "any", "from": "start", "to": "end", "max_depth": 1,
                     "rel_types": ["AUTHORED"]}
        }"#,
        &allow_all(),
    )
    .await;

    let pids = resp.path_ids();
    assert!(
        !pids.is_empty(),
        "should find at least one path from User 1 to MergeRequest"
    );

    for &pid in pids.iter() {
        let path = resp.path(pid);
        let last = path.last().unwrap();
        assert_eq!(
            last.to, "MergeRequest",
            "every path endpoint must be MergeRequest, got {} (id {})",
            last.to, last.to_id
        );
    }

    // Verify no Note or WorkItem appears as a path endpoint
    for edge in resp.edges() {
        if edge.path_id.is_some() {
            assert_ne!(edge.to, "Note", "path should not include Note endpoints");
            assert_ne!(
                edge.to, "WorkItem",
                "path should not include WorkItem endpoints"
            );
        }
    }
}
