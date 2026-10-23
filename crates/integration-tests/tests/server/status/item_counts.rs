use std::collections::HashMap;
use std::sync::Arc;

use integration_testkit::{load_ontology, run_subtests_shared, t};
use orbit_server::item_counts::ItemCountService;
use orbit_utils::traversal_path::TraversalPath;
use query_engine::compiler::{AuthorizedPath, SecurityContext};

use super::fixtures::{admin_context, query_cache_ttls, query_cache_usage, seed_namespaces};
use crate::common::{GRAPH_SCHEMA_SQL, TestContext};

#[tokio::test]
async fn item_counts() {
    let ctx = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;
    seed_namespaces(&ctx).await;

    run_subtests_shared!(
        &ctx,
        counts_entities_under_a_scope,
        scope_outside_the_callers_paths_counts_nothing,
        deleted_rows_are_excluded,
        duplicate_versions_count_once,
        reporter_gets_no_vulnerability_count,
        security_manager_gets_vulnerability_count,
        role_on_another_path_does_not_expose_the_entity,
        missing_table_degrades_to_zero,
        counts_written_within_the_ttl_stay_cached,
        each_scope_gets_its_own_cache_entry,
        counts_stay_cached_for_five_seconds,
        many_scopes_are_counted_in_one_query,
        nested_scopes_count_each_scope,
        scopes_are_answered_in_request_order,
        each_scope_needs_its_own_role,
        a_hundred_scopes_are_counted_in_one_query,
    );
}

async fn count(
    ctx: &TestContext,
    security_context: &SecurityContext,
    scope: &str,
) -> HashMap<String, i64> {
    count_many(ctx, security_context, &[scope]).await.remove(0)
}

async fn count_many(
    ctx: &TestContext,
    security_context: &SecurityContext,
    scopes: &[&str],
) -> Vec<HashMap<String, i64>> {
    let scopes: Vec<TraversalPath> = scopes
        .iter()
        .map(|s| TraversalPath::new_unchecked(*s))
        .collect();
    ItemCountService::new(Arc::new(ctx.create_client()))
        .count_items(&load_ontology(), security_context, &scopes)
        .await
}

fn role_on(paths: &[(&str, u32)]) -> SecurityContext {
    let paths = paths
        .iter()
        .map(|(path, role)| AuthorizedPath::new(*path, *role))
        .collect();
    SecurityContext::new_with_roles(1, paths).unwrap()
}

async fn counts_entities_under_a_scope(ctx: &TestContext) {
    let root = count(ctx, &admin_context(), "1/").await;
    assert_eq!(root["Project"], 5);
    assert_eq!(root["Group"], 2);
    assert_eq!(root["MergeRequest"], 2);

    let group = count(ctx, &admin_context(), "1/100/").await;
    assert_eq!(group["Project"], 2);
    assert_eq!(group["Group"], 1);
    assert_eq!(group["MergeRequest"], 1);
}

async fn scope_outside_the_callers_paths_counts_nothing(ctx: &TestContext) {
    let counts = count(ctx, &role_on(&[("1/100/", 50)]), "1/101/").await;

    assert!(counts.is_empty(), "counts: {counts:?}");
}

async fn deleted_rows_are_excluded(ctx: &TestContext) {
    let db = ctx.fork("item_counts_deleted_rows").await;
    db.execute(&format!(
        "INSERT INTO {} (id, name, visibility_level, traversal_path, _version, _deleted) VALUES
         (9100, 'Live Group', 'public', '1/900/9100/', '2024-01-01 00:00:00', false),
         (9101, 'Deleted Group', 'public', '1/900/9101/', '2024-06-01 00:00:00', true)",
        t("gl_group")
    ))
    .await;
    db.optimize_all().await;

    let counts = count(&db, &admin_context(), "1/900/").await;

    assert_eq!(counts["Group"], 1);
}

async fn duplicate_versions_count_once(ctx: &TestContext) {
    let db = ctx.fork("item_counts_duplicate_versions").await;
    db.execute(&format!(
        "INSERT INTO {} (id, traversal_path, project_id, branch, commit_sha, file_path, fqn, name, definition_type, start_line, end_line, start_byte, end_byte, start_char, end_char, _version, _deleted) VALUES
         (9001, '1/100/1000/', 1000, 'main', 'sha-a', 'a.rb', 'A#m', 'm', 'Method', 1, 2, 0, 10, 0, 10, '2024-01-01 00:00:00', false),
         (9002, '1/100/1000/', 1000, 'main', 'sha-c', 'b.rb', 'B#m', 'm', 'Method', 1, 2, 0, 10, 0, 10, '2024-01-01 00:00:00', false),
         (9002, '1/100/1000/', 1000, 'main', 'sha-c', 'b.rb', 'B#m', 'm', 'Method', 1, 2, 0, 10, 0, 10, '2024-06-01 00:00:00', false)",
        t("gl_definition")
    ))
    .await;
    db.optimize_all().await;

    let counts = count(&db, &admin_context(), "1/100/1000/").await;

    assert_eq!(counts["Definition"], 2);
}

async fn reporter_gets_no_vulnerability_count(ctx: &TestContext) {
    let counts = count(ctx, &role_on(&[("1/", 20)]), "1/").await;

    assert!(!counts.contains_key("Vulnerability"));
    assert_eq!(counts["Project"], 5);
}

async fn security_manager_gets_vulnerability_count(ctx: &TestContext) {
    let counts = count(ctx, &role_on(&[("1/", 25)]), "1/").await;

    assert_eq!(counts["Vulnerability"], 1);
}

async fn role_on_another_path_does_not_expose_the_entity(ctx: &TestContext) {
    let security_context = role_on(&[("1/100/", 25), ("1/101/", 20)]);

    let counts = count(ctx, &security_context, "1/101/").await;

    assert!(!counts.contains_key("Vulnerability"));
    assert_eq!(counts["MergeRequest"], 1);
}

async fn missing_table_degrades_to_zero(ctx: &TestContext) {
    let db = ctx.fork("item_counts_missing_table").await;
    db.execute(&format!("DROP TABLE {}", t("gl_merge_request")))
        .await;

    let counts = count(&db, &admin_context(), "1/100/").await;

    assert_eq!(counts["MergeRequest"], 0);
    assert_eq!(counts["Project"], 0);
}

async fn counts_written_within_the_ttl_stay_cached(ctx: &TestContext) {
    let db = ctx.fork("item_counts_rows_written_later").await;
    let insert_group = |id: i64| {
        format!(
            "INSERT INTO {} (id, name, visibility_level, traversal_path) VALUES
             ({id}, 'Group {id}', 'public', '1/950/{id}/')",
            t("gl_group")
        )
    };
    db.execute(&insert_group(9500)).await;

    let before = count(&db, &admin_context(), "1/950/").await;
    db.execute(&insert_group(9501)).await;
    let after = count(&db, &admin_context(), "1/950/").await;

    assert_eq!((before["Group"], after["Group"]), (1, 1));
    assert_eq!(query_cache_usage(&db).await, ["Write", "Read"]);
}

async fn each_scope_gets_its_own_cache_entry(ctx: &TestContext) {
    let db = ctx.fork("item_counts_cache_per_scope").await;

    let root = count(&db, &admin_context(), "1/").await;
    let group = count(&db, &admin_context(), "1/100/").await;

    assert_eq!((root["Project"], group["Project"]), (5, 2));
    assert_eq!(query_cache_usage(&db).await, ["Write", "Write"]);
}

async fn counts_stay_cached_for_five_seconds(ctx: &TestContext) {
    let db = ctx.fork("item_counts_cache_ttl").await;

    count(&db, &admin_context(), "1/").await;

    assert_eq!(query_cache_ttls(&db).await, ["5"]);
}

async fn many_scopes_are_counted_in_one_query(ctx: &TestContext) {
    let db = ctx.fork("item_counts_many_scopes").await;

    let counts = count_many(&db, &admin_context(), &["1/100/", "1/101/"]).await;

    let projects: Vec<i64> = counts.iter().map(|c| c["Project"]).collect();
    let merge_requests: Vec<i64> = counts.iter().map(|c| c["MergeRequest"]).collect();
    assert_eq!((projects, merge_requests), (vec![2, 1], vec![1, 1]));
    assert_eq!(query_cache_usage(&db).await, ["Write"]);
}

async fn nested_scopes_count_each_scope(ctx: &TestContext) {
    let counts = count_many(ctx, &admin_context(), &["1/", "1/100/", "1/100/1000/"]).await;

    let projects: Vec<i64> = counts.iter().map(|c| c["Project"]).collect();
    let groups: Vec<i64> = counts.iter().map(|c| c["Group"]).collect();
    assert_eq!((projects, groups), (vec![5, 2, 1], vec![2, 1, 0]));
}

async fn scopes_are_answered_in_request_order(ctx: &TestContext) {
    let counts = count_many(ctx, &admin_context(), &["1/101/", "1/", "1/101/", "1/107/"]).await;

    let projects: Vec<i64> = counts.iter().map(|c| c["Project"]).collect();
    assert_eq!(projects, [1, 5, 1, 1]);
}

async fn each_scope_needs_its_own_role(ctx: &TestContext) {
    let security_context = role_on(&[("1/100/", 25), ("1/101/", 20)]);

    let counts = count_many(ctx, &security_context, &["1/100/", "1/101/", "1/107/"]).await;

    let vulnerabilities: Vec<Option<i64>> = counts
        .iter()
        .map(|c| c.get("Vulnerability").copied())
        .collect();
    assert_eq!(vulnerabilities, [Some(0), None, None]);
    assert_eq!(counts[1]["MergeRequest"], 1);
    assert!(counts[2].is_empty(), "counts: {:?}", counts[2]);
}

async fn a_hundred_scopes_are_counted_in_one_query(ctx: &TestContext) {
    let db = ctx.fork("item_counts_hundred_scopes").await;
    let scopes: Vec<String> = (0..100).map(|id| format!("1/{}/", 100 + id)).collect();
    let scopes: Vec<&str> = scopes.iter().map(String::as_str).collect();

    let counts = count_many(&db, &admin_context(), &scopes).await;

    let projects: Vec<i64> = counts.iter().take(3).map(|c| c["Project"]).collect();
    assert_eq!((counts.len(), projects), (100, vec![2, 1, 0]));
    assert_eq!(query_cache_usage(&db).await, ["Write"]);
}
