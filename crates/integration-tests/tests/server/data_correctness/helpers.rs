//! Data correctness integration tests.
//!
//! Seeds known data into ClickHouse, runs the full query pipeline
//! (compile → execute → redact → hydrate → format), and asserts that
//! returned values exactly match expectations. Every response is validated
//! against `query_response.json` and deserialized into typed [`GraphResponse`]
//! structs for type-safe assertions via [`ResponseView`].
//!
//! What these tests verify:
//! - Specific property values match seeded data (e.g. alice.username == "alice")
//! - Exact node counts, edge endpoints, and edge types per query
//! - Result ordering when `order_by` is specified
//! - Aggregation results are numerically correct against known inputs
//! - Path finding returns complete, connected paths with exact hop counts
//! - Redaction removes exactly the unauthorized nodes/edges
//! - Referential integrity: every edge endpoint exists in the nodes array

pub(super) use std::collections::HashSet;
pub(super) use std::sync::Arc;

pub(super) use crate::common::{
    GRAPH_SCHEMA_SQL, MockRedactionService, SIPHON_SCHEMA_SQL, TestContext, load_ontology,
    run_redaction, test_security_context,
};
pub(super) use gkg_server::query_pipeline::{
    Extensions, GraphFormatter, HydrationStage, NoOpObserver, PipelineStage, QueryPipelineContext,
    RedactionOutput, ResultFormatter,
};
pub(super) use gkg_server::redaction::QueryResult;
pub(super) use integration_testkit::visitor::{NodeExt, Requirement, ResponseView};
pub(super) use query_engine::{SecurityContext, compile};
pub(super) use serde_json::Value;

pub(super) static RESPONSE_SCHEMA: std::sync::LazyLock<jsonschema::Validator> =
    std::sync::LazyLock::new(|| {
        let schema: Value = serde_json::from_str(include_str!(concat!(
            env!("GKG_SERVER_SCHEMAS_DIR"),
            "/query_response.json"
        )))
        .unwrap();
        jsonschema::validator_for(&schema).unwrap()
    });

pub(super) fn assert_valid(value: &Value) {
    let errors: Vec<_> = RESPONSE_SCHEMA.iter_errors(value).collect();
    assert!(errors.is_empty(), "Schema validation failed: {errors:?}");
}

pub(super) async fn run_query(
    ctx: &TestContext,
    json: &str,
    svc: &MockRedactionService,
) -> ResponseView {
    run_query_with_security(ctx, json, svc, test_security_context()).await
}

pub(super) async fn run_query_with_security(
    ctx: &TestContext,
    json: &str,
    svc: &MockRedactionService,
    security_ctx: SecurityContext,
) -> ResponseView {
    let ontology = Arc::new(load_ontology());
    let client = Arc::new(ctx.create_client());
    let compiled = Arc::new(compile(json, &ontology, &security_ctx).unwrap());

    let batches = ctx.query_parameterized(&compiled.base).await;

    let mut result = QueryResult::from_batches(&batches, &compiled.base.result_context);
    let redacted_count = run_redaction(&mut result, svc);

    let mut extensions = Extensions::default();
    extensions.insert(client);
    let mut pipeline_ctx = QueryPipelineContext {
        query_json: String::new(),
        compiled: Some(Arc::clone(&compiled)),
        ontology: Arc::clone(&ontology),
        security_context: Some(security_ctx),
        extensions,
    };
    let mut obs = NoOpObserver;

    let output = HydrationStage
        .execute(
            RedactionOutput {
                query_result: result,
                redacted_count,
            },
            &mut pipeline_ctx,
            &mut obs,
        )
        .await
        .expect("pipeline should succeed");

    let value = GraphFormatter.format(&output.query_result, &output.result_context, &pipeline_ctx);
    assert_valid(&value);
    let response =
        serde_json::from_value(value).expect("response should deserialize to GraphResponse");
    ResponseView::for_query(&compiled.input, response)
}

pub(super) fn allow_all() -> MockRedactionService {
    let mut svc = MockRedactionService::new();
    svc.allow("user", &[1, 2, 3, 4, 5, 6]);
    svc.allow("group", &[100, 101, 102, 200, 300]);
    svc.allow("project", &[1000, 1001, 1002, 1003, 1004]);
    svc.allow("merge_request", &[2000, 2001, 2002, 2003]);
    svc.allow("note", &[3000, 3001, 3002, 3003]);
    svc
}

// Topology:
//
//   Users:
//     1 alice   (active,  human)
//     2 bob     (active,  human)
//     3 charlie (active,  human)
//     4 diana   (active,  project_bot)
//     5 eve     (blocked, service_account)
//     6 用户_émoji_🎉 (active, human) — unicode stress test
//
//   Groups:
//     100 Public Group   (public,   path 1/100/)
//     101 Private Group  (private,  path 1/101/)
//     102 Internal Group (internal, path 1/102/)
//     200 Deep Group A   (public,   path 1/100/200/)
//     300 Deep Group B   (public,   path 1/100/200/300/)
//
//   Projects:
//     1000 Public Project   (public,   path 1/100/1000/)
//     1001 Private Project  (private,  path 1/101/1001/)
//     1002 Internal Project (internal, path 1/100/1002/)
//     1003 Secret Project   (private,  path 1/101/1003/)
//     1004 Shared Project   (public,   path 1/102/1004/)
//
//   MergeRequests:
//     2000 Add feature A (opened, path 1/100/1000/)
//     2001 Fix bug B     (opened, path 1/100/1000/)
//     2002 Refactor C    (merged, path 1/101/1001/)
//     2003 Update D      (closed, path 1/102/1004/)
//
//   Notes:
//     3000 Normal note           (MR 2000, not confidential, not internal, created_at=2024-01-15T10:30:00)
//     3001 Confidential note     (MR 2001, confidential=true, created_at=2024-02-20T14:45:00)
//     3002 Giant string note     (MR 2000, 10000 chars, created_at=NULL)
//     3003 SQL injection note    (MR 2000, DROP TABLE payload, created_at=NULL)
//
//   MEMBER_OF edges:
//     User 1 → Group 100, User 1 → Group 102
//     User 2 → Group 100, User 3 → Group 101
//     User 4 → Group 101, User 4 → Group 102, User 5 → Group 101
//     User 6 → Group 100, User 6 → Group 101
//
//   CONTAINS edges:
//     Group 100 → Project 1000, Group 100 → Project 1002
//     Group 100 → Group 200 (subgroup)
//     Group 200 → Group 300 (subgroup depth 2)
//     Group 101 → Project 1001, Group 101 → Project 1003
//     Group 102 → Project 1004
//
//   AUTHORED edges:
//     User 1 → MR 2000, User 1 → MR 2001
//     User 2 → MR 2002, User 3 → MR 2003
//     User 1 → Note 3000
//
//   HAS_NOTE edges:
//     MR 2000 → Note 3000, MR 2000 → Note 3002, MR 2000 → Note 3003
//     MR 2001 → Note 3001

pub(super) async fn seed(ctx: &TestContext) {
    ctx.execute(
        "INSERT INTO gl_user (id, username, name, state, user_type) VALUES
         (1, 'alice', 'Alice Admin', 'active', 'human'),
         (2, 'bob', 'Bob Builder', 'active', 'human'),
         (3, 'charlie', 'Charlie Private', 'active', 'human'),
         (4, 'diana', 'Diana Developer', 'active', 'project_bot'),
         (5, 'eve', 'Eve External', 'blocked', 'service_account'),
         (6, '用户_émoji_🎉', 'Ünïcödé Üser', 'active', 'human')",
    )
    .await;

    ctx.execute(
        "INSERT INTO gl_group (id, name, visibility_level, traversal_path) VALUES
         (100, 'Public Group', 'public', '1/100/'),
         (101, 'Private Group', 'private', '1/101/'),
         (102, 'Internal Group', 'internal', '1/102/'),
         (200, 'Deep Group A', 'public', '1/100/200/'),
         (300, 'Deep Group B', 'public', '1/100/200/300/')",
    )
    .await;

    ctx.execute(
        "INSERT INTO gl_project (id, name, visibility_level, traversal_path) VALUES
         (1000, 'Public Project', 'public', '1/100/1000/'),
         (1001, 'Private Project', 'private', '1/101/1001/'),
         (1002, 'Internal Project', 'internal', '1/100/1002/'),
         (1003, 'Secret Project', 'private', '1/101/1003/'),
         (1004, 'Shared Project', 'public', '1/102/1004/')",
    )
    .await;

    ctx.execute(
        "INSERT INTO gl_merge_request (id, iid, title, state, source_branch, target_branch, traversal_path) VALUES
         (2000, 1, 'Add feature A', 'opened', 'feature-a', 'main', '1/100/1000/'),
         (2001, 2, 'Fix bug B', 'opened', 'fix-b', 'main', '1/100/1000/'),
         (2002, 3, 'Refactor C', 'merged', 'refactor-c', 'main', '1/101/1001/'),
         (2003, 4, 'Update D', 'closed', 'update-d', 'main', '1/102/1004/')",
    )
    .await;

    let giant_string = "x".repeat(10_000);
    ctx.execute(&format!(
        "INSERT INTO gl_note (id, note, noteable_type, noteable_id, confidential, internal, created_at, traversal_path) VALUES
         (3000, 'Normal note on feature A', 'MergeRequest', 2000, false, false, '2024-01-15 10:30:00', '1/100/1000/'),
         (3001, 'Confidential feedback on bug B', 'MergeRequest', 2001, true, false, '2024-02-20 14:45:00', '1/100/1000/'),
         (3002, '{giant_string}', 'MergeRequest', 2000, false, false, NULL, '1/100/1000/'),
         (3003, 'Robert''); DROP TABLE gl_note;--', 'MergeRequest', 2000, false, false, NULL, '1/100/1000/')",
    ))
    .await;

    ctx.execute(
        "INSERT INTO gl_edge (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind) VALUES
         ('1/100/', 1, 'User', 'MEMBER_OF', 100, 'Group'),
         ('1/102/', 1, 'User', 'MEMBER_OF', 102, 'Group'),
         ('1/100/', 2, 'User', 'MEMBER_OF', 100, 'Group'),
         ('1/101/', 3, 'User', 'MEMBER_OF', 101, 'Group'),
         ('1/101/', 4, 'User', 'MEMBER_OF', 101, 'Group'),
         ('1/102/', 4, 'User', 'MEMBER_OF', 102, 'Group'),
         ('1/101/', 5, 'User', 'MEMBER_OF', 101, 'Group'),
         ('1/100/', 6, 'User', 'MEMBER_OF', 100, 'Group'),
         ('1/101/', 6, 'User', 'MEMBER_OF', 101, 'Group'),
         ('1/100/200/', 100, 'Group', 'CONTAINS', 200, 'Group'),
         ('1/100/200/300/', 200, 'Group', 'CONTAINS', 300, 'Group'),
         ('1/100/1000/', 100, 'Group', 'CONTAINS', 1000, 'Project'),
         ('1/100/1002/', 100, 'Group', 'CONTAINS', 1002, 'Project'),
         ('1/101/1001/', 101, 'Group', 'CONTAINS', 1001, 'Project'),
         ('1/101/1003/', 101, 'Group', 'CONTAINS', 1003, 'Project'),
         ('1/102/1004/', 102, 'Group', 'CONTAINS', 1004, 'Project'),
         ('1/100/1000/', 1, 'User', 'AUTHORED', 2000, 'MergeRequest'),
         ('1/100/1000/', 1, 'User', 'AUTHORED', 2001, 'MergeRequest'),
         ('1/101/1001/', 2, 'User', 'AUTHORED', 2002, 'MergeRequest'),
         ('1/102/1004/', 3, 'User', 'AUTHORED', 2003, 'MergeRequest'),
         ('1/100/1000/', 1, 'User', 'AUTHORED', 3000, 'Note'),
         ('1/100/1000/', 2000, 'MergeRequest', 'HAS_NOTE', 3000, 'Note'),
         ('1/100/1000/', 2000, 'MergeRequest', 'HAS_NOTE', 3002, 'Note'),
         ('1/100/1000/', 2000, 'MergeRequest', 'HAS_NOTE', 3003, 'Note'),
         ('1/100/1000/', 2001, 'MergeRequest', 'HAS_NOTE', 3001, 'Note')",
    )
    .await;

    ctx.optimize_all().await;
}
