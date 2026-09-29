use std::sync::Arc;

use integration_testkit::run_subtests_shared;
use orbit_server::item_counts::{ItemCountService, build_item_counts_response};
use orbit_server::proto::{DomainItemCount, NamespaceItemCounts};
use orbit_utils::traversal_path::TraversalPath;
use query_engine::compiler::{AuthorizedPath, SecurityContext};

use super::fixtures::{admin_context, pinned_schema, seed_namespaces};
use crate::common::{GRAPH_SCHEMA_SQL, TestContext};

#[tokio::test]
async fn item_counts_response() {
    let ctx = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;
    seed_namespaces(&ctx).await;

    run_subtests_shared!(
        &ctx,
        each_path_gets_an_entry_in_request_order,
        entities_are_grouped_under_their_domains,
        reporter_gets_no_security_domain,
        path_with_nothing_visible_gets_empty_domains,
    );
}

async fn respond(
    ctx: &TestContext,
    security_context: &SecurityContext,
    paths: &[&str],
) -> Vec<NamespaceItemCounts> {
    let schema = pinned_schema();
    let paths: Vec<TraversalPath> = paths
        .iter()
        .map(|path| TraversalPath::new_unchecked(*path))
        .collect();
    let scope_counts = ItemCountService::new(Arc::new(ctx.create_client()))
        .count_items(&schema.ontology, security_context, &paths)
        .await;
    build_item_counts_response(&schema.ontology, &scope_counts).counts
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

async fn each_path_gets_an_entry_in_request_order(ctx: &TestContext) {
    let counts = respond(ctx, &admin_context(), &["1/101/", "1/100/"]).await;

    let paths: Vec<&str> = counts.iter().map(|c| c.traversal_path.as_str()).collect();
    assert_eq!(paths, ["1/101/", "1/100/"]);
}

async fn entities_are_grouped_under_their_domains(ctx: &TestContext) {
    let counts = respond(ctx, &admin_context(), &["1/100/", "1/101/"]).await;

    let (public_group, private_group) = (&counts[0].domains, &counts[1].domains);
    assert_eq!(count_of(public_group, "core", "Group"), 1);
    assert_eq!(count_of(public_group, "core", "Project"), 2);
    assert_eq!(count_of(public_group, "code_review", "MergeRequest"), 1);
    assert_eq!(count_of(public_group, "security", "Vulnerability"), 0);
    assert_eq!(count_of(private_group, "core", "Project"), 1);
    assert_eq!(count_of(private_group, "security", "Vulnerability"), 1);
}

async fn reporter_gets_no_security_domain(ctx: &TestContext) {
    let reporter = SecurityContext::new_with_roles(1, vec![AuthorizedPath::new("1/", 20)]).unwrap();

    let counts = respond(ctx, &reporter, &["1/101/"]).await;

    let domains = &counts[0].domains;
    assert!(domains.iter().all(|d| d.name != "security"));
    assert_eq!(count_of(domains, "core", "Project"), 1);
}

async fn path_with_nothing_visible_gets_empty_domains(ctx: &TestContext) {
    let reporter =
        SecurityContext::new_with_roles(1, vec![AuthorizedPath::new("1/100/", 20)]).unwrap();

    let counts = respond(ctx, &reporter, &["1/100/", "1/101/"]).await;

    assert!(!counts[0].domains.is_empty());
    assert_eq!(counts[1].traversal_path, "1/101/");
    assert!(
        counts[1].domains.is_empty(),
        "domains: {:?}",
        counts[1].domains
    );
}
