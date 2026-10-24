use std::collections::HashMap;
use std::sync::Arc;

use integration_testkit::{load_ontology, run_subtests_shared, t};
use orbit_server::item_counts::ItemCountService;
use orbit_utils::traversal_path::TraversalPath;
use query_engine::compiler::{AuthorizedPath, SecurityContext};

use super::fixtures::{
    admin_context, count_items, query_cache_ttls, query_cache_usage, read_cached_query_log,
    seed_namespaces, use_production_query_settings,
};
use crate::common::{GRAPH_SCHEMA_SQL, TestContext};

#[tokio::test]
async fn item_counts() {
    use_production_query_settings();
    let ctx = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;
    seed_namespaces(&ctx).await;

    run_subtests_shared!(
        &ctx,
        counts_entities_under_a_scope,
        deleted_rows_are_excluded,
        duplicate_versions_count_once,
        reporter_gets_no_vulnerability_count,
        security_manager_gets_vulnerability_count,
        missing_table_fails_the_count_and_zeroes_the_graph_status,
        repeated_counts_read_the_cache_with_a_five_second_ttl,
        each_scope_gets_its_own_cache_entry,
        each_path_gets_one_plain_count_query,
        nested_scopes_count_rows_at_each_scope_once,
        each_scope_needs_its_own_role,
        a_hundred_scopes_get_a_hundred_queries,
    );
}

async fn count(
    ctx: &TestContext,
    security_context: &SecurityContext,
    scope: &str,
) -> HashMap<String, i64> {
    count_items(ctx, security_context, &[scope])
        .await
        .remove(0)
        .counts
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

async fn missing_table_fails_the_count_and_zeroes_the_graph_status(ctx: &TestContext) {
    let db = ctx.fork("item_counts_missing_table").await;
    db.execute(&format!("DROP TABLE {}", t("gl_merge_request")))
        .await;
    let service = ItemCountService::new(Arc::new(db.create_client()));
    let scopes = [TraversalPath::new_unchecked("1/100/")];

    let failed = service
        .count_items(&load_ontology(), &admin_context(), &scopes)
        .await;
    let zeros = service
        .count_items_or_zeros(&load_ontology(), &admin_context(), &scopes)
        .await
        .remove(0)
        .counts;

    assert!(failed.is_err());
    assert_eq!((zeros["MergeRequest"], zeros["Project"]), (0, 0));
}

async fn repeated_counts_read_the_cache_with_a_five_second_ttl(ctx: &TestContext) {
    let db = ctx.fork("item_counts_cache_ttl").await;

    count(&db, &admin_context(), "1/100/").await;
    count(&db, &admin_context(), "1/100/").await;

    assert_eq!(query_cache_usage(&db).await, ["Write", "Read"]);
    assert_eq!(query_cache_ttls(&db).await, ["5", "5"]);
}

async fn each_scope_gets_its_own_cache_entry(ctx: &TestContext) {
    let db = ctx.fork("item_counts_cache_per_scope").await;

    let root = count(&db, &admin_context(), "1/").await;
    let group = count(&db, &admin_context(), "1/100/").await;

    assert_eq!((root["Project"], group["Project"]), (5, 2));
    assert_eq!(query_cache_usage(&db).await, ["Write", "Write"]);
}

async fn each_path_gets_one_plain_count_query(ctx: &TestContext) {
    let db = ctx.fork("item_counts_query_shape").await;

    count_items(&db, &admin_context(), &["1/100/", "1/101/"]).await;

    let shapes = read_cached_query_log(
        &db,
        "QueryStart",
        "concat(toString(position(query, 'ARRAY JOIN') > 0), Settings['enable_parallel_replicas'])",
    )
    .await;
    assert_eq!(shapes, ["01", "01"]);
}

async fn nested_scopes_count_rows_at_each_scope_once(ctx: &TestContext) {
    let counts = count_items(ctx, &admin_context(), &["1/", "1/100/", "1/100/1000/"]).await;

    let projects: Vec<i64> = counts.iter().map(|c| c.counts["Project"]).collect();
    let groups: Vec<i64> = counts.iter().map(|c| c.counts["Group"]).collect();
    assert_eq!((projects, groups), (vec![5, 2, 1], vec![2, 1, 0]));
}

async fn each_scope_needs_its_own_role(ctx: &TestContext) {
    let security_context = role_on(&[("1/100/", 25), ("1/101/", 20)]);

    let counts = count_items(ctx, &security_context, &["1/100/", "1/101/", "1/107/"]).await;

    let vulnerabilities: Vec<Option<i64>> = counts
        .iter()
        .map(|c| c.counts.get("Vulnerability").copied())
        .collect();
    assert_eq!(vulnerabilities, [Some(0), None, None]);
    assert_eq!(counts[1].counts["MergeRequest"], 1);
    assert!(counts[2].counts.is_empty());
}

async fn a_hundred_scopes_get_a_hundred_queries(ctx: &TestContext) {
    let db = ctx.fork("item_counts_hundred_scopes").await;
    let scopes: Vec<String> = (0..100).map(|id| format!("1/{}/", 100 + id)).collect();
    let scopes: Vec<&str> = scopes.iter().map(String::as_str).collect();

    let counts = count_items(&db, &admin_context(), &scopes).await;

    let projects: Vec<i64> = counts.iter().take(3).map(|c| c.counts["Project"]).collect();
    assert_eq!((counts.len(), projects), (100, vec![2, 1, 0]));
    assert_eq!(query_cache_usage(&db).await, vec!["Write"; 100]);
}
