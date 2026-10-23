use integration_testkit::run_subtests_shared;
use orbit_server::item_counts::build_item_counts_response;
use orbit_server::proto::{
    DomainItemCount, GetItemCountsRequest, GetItemCountsResponse, NamespaceItemCounts,
};
use query_engine::compiler::{AuthorizedPath, SecurityContext};

use super::fixtures::{admin_context, count_items, pinned_schema, seed_namespaces};
use crate::common::{GRAPH_SCHEMA_SQL, TestContext};

#[tokio::test]
async fn item_counts_response() {
    let ctx = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;
    seed_namespaces(&ctx).await;

    run_subtests_shared!(
        &ctx,
        entities_are_grouped_under_their_domains,
        project_path_counts_its_own_entities,
        reporter_gets_no_security_domain,
        namespaces_keep_the_request_order,
    );
}

async fn respond(
    ctx: &TestContext,
    security_context: &SecurityContext,
    path: &str,
) -> Vec<DomainItemCount> {
    let request = GetItemCountsRequest {
        traversal_path: path.to_string(),
        ..Default::default()
    };
    let response = respond_to(ctx, security_context, &[path], &request).await;
    assert!(response.namespaces.is_empty());
    response.domains
}

async fn respond_many(
    ctx: &TestContext,
    security_context: &SecurityContext,
    paths: &[&str],
) -> Vec<NamespaceItemCounts> {
    let request = GetItemCountsRequest {
        traversal_paths: paths.iter().map(|path| path.to_string()).collect(),
        ..Default::default()
    };
    let response = respond_to(ctx, security_context, paths, &request).await;
    assert!(response.domains.is_empty());
    response.namespaces
}

async fn respond_to(
    ctx: &TestContext,
    security_context: &SecurityContext,
    paths: &[&str],
    request: &GetItemCountsRequest,
) -> GetItemCountsResponse {
    let counts = count_items(ctx, security_context, paths).await;
    build_item_counts_response(&pinned_schema().ontology, request, &counts)
}

fn count_of(domains: &[DomainItemCount], domain: &str, entity: &str) -> i64 {
    domains
        .iter()
        .find(|d| d.name == domain)
        .unwrap_or_else(|| panic!("domain '{domain}' not found"))
        .entities
        .iter()
        .find(|e| e.name == entity)
        .unwrap_or_else(|| panic!("entity '{entity}' not found in domain '{domain}'"))
        .count
}

async fn entities_are_grouped_under_their_domains(ctx: &TestContext) {
    let domains = respond(ctx, &admin_context(), "1/101/").await;

    assert_eq!(count_of(&domains, "core", "Group"), 1);
    assert_eq!(count_of(&domains, "core", "Project"), 1);
    assert_eq!(count_of(&domains, "code_review", "MergeRequest"), 1);
    assert_eq!(count_of(&domains, "security", "Vulnerability"), 1);
}

async fn project_path_counts_its_own_entities(ctx: &TestContext) {
    let domains = respond(ctx, &admin_context(), "1/100/1000/").await;

    assert_eq!(count_of(&domains, "core", "Group"), 0);
    assert_eq!(count_of(&domains, "core", "Project"), 1);
    assert_eq!(count_of(&domains, "code_review", "MergeRequest"), 1);
}

async fn reporter_gets_no_security_domain(ctx: &TestContext) {
    let reporter = SecurityContext::new_with_roles(1, vec![AuthorizedPath::new("1/", 20)]).unwrap();

    let domains = respond(ctx, &reporter, "1/101/").await;

    assert!(domains.iter().all(|d| d.name != "security"));
    assert_eq!(count_of(&domains, "core", "Project"), 1);
}

async fn namespaces_keep_the_request_order(ctx: &TestContext) {
    let reporter =
        SecurityContext::new_with_roles(1, vec![AuthorizedPath::new("1/101/", 20)]).unwrap();

    let namespaces = respond_many(
        ctx,
        &reporter,
        &["1/101/", "1/100/", "1/101/1001/", "1/101/"],
    )
    .await;

    let paths: Vec<&str> = namespaces
        .iter()
        .map(|n| n.traversal_path.as_str())
        .collect();
    assert_eq!(paths, ["1/101/", "1/100/", "1/101/1001/", "1/101/"]);
    assert_eq!(namespaces[3].domains, namespaces[0].domains);
    assert_eq!(
        count_of(&namespaces[2].domains, "code_review", "MergeRequest"),
        1
    );
}
