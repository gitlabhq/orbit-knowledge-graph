use std::sync::Arc;

use crate::common::{GRAPH_SCHEMA_SQL, TestContext};
use gkg_server::graph_stats::GraphStatsService;
use integration_testkit::{load_ontology, run_subtests_shared, t};

async fn setup(ctx: &TestContext) {
    ctx.execute(&format!(
        "INSERT INTO {} (id, username, name, state, user_type) VALUES
         (1, 'alice', 'Alice Admin', 'active', 'human'),
         (2, 'bob', 'Bob Builder', 'active', 'human')",
        t("gl_user")
    ))
    .await;

    ctx.execute(&format!(
        "INSERT INTO {} (id, name, visibility_level, traversal_path) VALUES
         (100, 'Public Group', 'public', '1/100/'),
         (101, 'Private Group', 'private', '1/101/')",
        t("gl_group")
    ))
    .await;

    ctx.execute(&format!(
        "INSERT INTO {} (id, name, visibility_level, traversal_path) VALUES
         (1000, 'Public Project', 'public', '1/100/1000/'),
         (1001, 'Private Project', 'private', '1/101/1001/'),
         (1002, 'Internal Project', 'internal', '1/100/1002/')",
        t("gl_project")
    ))
    .await;

    ctx.execute(&format!(
        "INSERT INTO {} (id, iid, title, state, source_branch, target_branch, traversal_path) VALUES
         (2000, 1, 'Add feature A', 'opened', 'feature-a', 'main', '1/100/1000/'),
         (2001, 2, 'Fix bug B', 'opened', 'fix-b', 'main', '1/101/1001/')",
        t("gl_merge_request")
    ))
    .await;

    ctx.optimize_all().await;
}

fn build_service(ctx: &TestContext) -> GraphStatsService {
    let client = Arc::new(ctx.create_client());
    let ontology = Arc::new(load_ontology());
    GraphStatsService::new(client, ontology)
}

fn find_domain<'a>(
    domains: &'a [gkg_server::proto::GraphStatsDomain],
    name: &str,
) -> &'a gkg_server::proto::GraphStatsDomain {
    domains
        .iter()
        .find(|d| d.name == name)
        .unwrap_or_else(|| panic!("domain '{name}' not found"))
}

fn find_item(domain: &gkg_server::proto::GraphStatsDomain, name: &str) -> i64 {
    domain
        .items
        .iter()
        .find(|i| i.name == name)
        .unwrap_or_else(|| panic!("item '{name}' not found in domain '{}'", domain.name))
        .count
}

#[tokio::test]
async fn graph_stats() {
    let ctx = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;
    setup(&ctx).await;

    run_subtests_shared!(
        &ctx,
        root_traversal_path_returns_all_entity_counts,
        scoped_by_traversal_path_filters_counts,
        empty_traversal_path_rejected,
        non_matching_traversal_path_returns_zeros,
        all_domains_present_in_response,
    );
}

async fn root_traversal_path_returns_all_entity_counts(ctx: &TestContext) {
    let service = build_service(ctx);
    let response = service.get_stats("1/").await.expect("should succeed");

    let core = find_domain(&response.domains, "core");
    assert_eq!(find_item(core, "Project"), 3);
    assert_eq!(find_item(core, "Group"), 2);

    let code = find_domain(&response.domains, "code_review");
    assert_eq!(find_item(code, "MergeRequest"), 2);
}

async fn scoped_by_traversal_path_filters_counts(ctx: &TestContext) {
    let service = build_service(ctx);

    let response = service.get_stats("1/100/").await.expect("should succeed");

    let core = find_domain(&response.domains, "core");
    assert_eq!(find_item(core, "Project"), 2, "projects under 1/100/");
    assert_eq!(find_item(core, "Group"), 1, "groups under 1/100/");

    let code = find_domain(&response.domains, "code_review");
    assert_eq!(find_item(code, "MergeRequest"), 1, "MRs under 1/100/");
}

async fn empty_traversal_path_rejected(ctx: &TestContext) {
    let service = build_service(ctx);

    let result = service.get_stats("").await;

    assert!(result.is_err());
    let status = result.unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

async fn non_matching_traversal_path_returns_zeros(ctx: &TestContext) {
    let service = build_service(ctx);

    let response = service.get_stats("999/").await.expect("should succeed");

    let core = find_domain(&response.domains, "core");
    assert_eq!(find_item(core, "Project"), 0);
    assert_eq!(find_item(core, "Group"), 0);
}

async fn all_domains_present_in_response(ctx: &TestContext) {
    let service = build_service(ctx);
    let ontology = load_ontology();

    let response = service.get_stats("1/").await.expect("should succeed");

    let expected_domains: Vec<String> = ontology.domains().map(|d| d.name.clone()).collect();
    let actual_domains: Vec<String> = response.domains.iter().map(|d| d.name.clone()).collect();

    assert_eq!(actual_domains.len(), expected_domains.len());
    for expected in &expected_domains {
        assert!(
            actual_domains.contains(expected),
            "missing domain: {expected}"
        );
    }
}
