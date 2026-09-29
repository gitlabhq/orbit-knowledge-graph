use std::sync::Arc;

use integration_testkit::run_subtests_shared;
use orbit_server::item_counts::{ItemCountService, build_item_counts_response};
use orbit_server::proto::DomainItemCount;
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
        entities_are_grouped_under_their_domains,
        project_path_counts_its_own_entities,
        reporter_gets_no_security_domain,
    );
}

async fn respond(
    ctx: &TestContext,
    security_context: &SecurityContext,
    paths: &[&str],
) -> Vec<DomainItemCount> {
    let schema = pinned_schema();
    let paths: Vec<TraversalPath> = paths
        .iter()
        .map(|path| TraversalPath::new_unchecked(*path))
        .collect();
    let counts = ItemCountService::new(Arc::new(ctx.create_client()))
        .count_items(&schema.ontology, security_context, &paths)
        .await;
    build_item_counts_response(&schema.ontology, &counts).domains
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
    let domains = respond(ctx, &admin_context(), &["1/101/"]).await;

    assert_eq!(count_of(&domains, "core", "Group"), 1);
    assert_eq!(count_of(&domains, "core", "Project"), 1);
    assert_eq!(count_of(&domains, "code_review", "MergeRequest"), 1);
    assert_eq!(count_of(&domains, "security", "Vulnerability"), 1);
}

async fn project_path_counts_its_own_entities(ctx: &TestContext) {
    let domains = respond(ctx, &admin_context(), &["1/100/1000/"]).await;

    assert_eq!(count_of(&domains, "core", "Group"), 0);
    assert_eq!(count_of(&domains, "core", "Project"), 1);
    assert_eq!(count_of(&domains, "code_review", "MergeRequest"), 1);
}

async fn reporter_gets_no_security_domain(ctx: &TestContext) {
    let reporter = SecurityContext::new_with_roles(1, vec![AuthorizedPath::new("1/", 20)]).unwrap();

    let domains = respond(ctx, &reporter, &["1/101/"]).await;

    assert!(domains.iter().all(|d| d.name != "security"));
    assert_eq!(count_of(&domains, "core", "Project"), 1);
}
