use std::sync::Arc;

use integration_testkit::run_subtests_shared;
use orbit_server::graph_status::GraphStatusService;
use orbit_server::proto::{
    GetGraphStatusResponse, GraphStatusItem, IndexingState, IndexingStatus, ResponseFormat,
    StructuredGraphStatus, get_graph_status_response,
};
use orbit_utils::traversal_path::TraversalPath;
use query_engine::compiler::{AuthorizedPath, SecurityContext};

use super::fixtures::{admin_context, pinned_schema, seed_namespaces};
use crate::common::{GRAPH_SCHEMA_SQL, TestContext};

#[tokio::test]
async fn graph_status() {
    let ctx = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;
    seed_namespaces(&ctx).await;

    run_subtests_shared!(
        &ctx,
        indexed_when_every_plan_and_project_completed,
        backfilling_while_a_first_pass_pages,
        code_indexing_omitted_without_projects,
        items_carry_count_and_state,
        reporter_does_not_see_the_security_domain,
        projects_status_under_the_scope,
        empty_traversal_path_rejected,
        toon_renders_states_and_counts,
    );
}

async fn get_status(
    ctx: &TestContext,
    traversal_path: &str,
    format: ResponseFormat,
    security_context: &SecurityContext,
) -> Result<GetGraphStatusResponse, tonic::Status> {
    GraphStatusService::new(Arc::new(ctx.create_client()))
        .get_status(
            &pinned_schema(),
            &TraversalPath::new_unchecked(traversal_path),
            format as i32,
            security_context,
        )
        .await
}

async fn fetch_status(ctx: &TestContext, traversal_path: &str) -> StructuredGraphStatus {
    let response = get_status(ctx, traversal_path, ResponseFormat::Raw, &admin_context())
        .await
        .expect("should succeed");
    let Some(get_graph_status_response::Content::Structured(status)) = response.content else {
        panic!("expected structured response");
    };
    status
}

fn state_of(status: Option<&IndexingStatus>) -> IndexingState {
    let status = status.expect("indexing status should be present");
    IndexingState::try_from(status.state).expect("known state")
}

fn find_item<'a>(
    status: &'a StructuredGraphStatus,
    domain: &str,
    item: &str,
) -> &'a GraphStatusItem {
    status
        .domains
        .iter()
        .find(|d| d.name == domain)
        .unwrap_or_else(|| panic!("domain '{domain}' not found"))
        .items
        .iter()
        .find(|i| i.name == item)
        .unwrap_or_else(|| panic!("item '{item}' not found in domain '{domain}'"))
}

async fn indexed_when_every_plan_and_project_completed(ctx: &TestContext) {
    let status = fetch_status(ctx, "1/112/").await;

    assert_eq!(state_of(status.indexing.as_ref()), IndexingState::Indexed);
    assert_eq!(
        state_of(status.sdlc_indexing.as_ref()),
        IndexingState::Indexed
    );
    assert_eq!(
        state_of(status.code_indexing.as_ref()),
        IndexingState::Indexed
    );
    assert!(status.indexing.expect("present").last_started_at.is_none());
}

async fn backfilling_while_a_first_pass_pages(ctx: &TestContext) {
    let status = fetch_status(ctx, "1/101/").await;

    assert_eq!(
        state_of(status.indexing.as_ref()),
        IndexingState::Backfilling
    );
    assert_eq!(
        state_of(status.sdlc_indexing.as_ref()),
        IndexingState::Backfilling
    );
    assert_eq!(
        state_of(status.code_indexing.as_ref()),
        IndexingState::Indexed
    );
}

async fn code_indexing_omitted_without_projects(ctx: &TestContext) {
    let status = fetch_status(ctx, "1/102/").await;

    assert!(status.code_indexing.is_none());
    assert_eq!(
        state_of(status.indexing.as_ref()),
        IndexingState::NotIndexed
    );
}

async fn items_carry_count_and_state(ctx: &TestContext) {
    let status = fetch_status(ctx, "1/101/").await;

    let merge_requests = find_item(&status, "code_review", "MergeRequest");
    assert_eq!(merge_requests.count, 1);
    assert_eq!(
        merge_requests.state,
        Some(IndexingState::Backfilling as i32)
    );

    let definitions = find_item(&status, "source_code", "Definition");
    assert_eq!(definitions.state, Some(IndexingState::Indexed as i32));
}

async fn reporter_does_not_see_the_security_domain(ctx: &TestContext) {
    let reporter = SecurityContext::new_with_roles(1, vec![AuthorizedPath::new("1/", 20)]).unwrap();
    let response = get_status(ctx, "1/101/", ResponseFormat::Raw, &reporter)
        .await
        .expect("should succeed");
    let Some(get_graph_status_response::Content::Structured(status)) = response.content else {
        panic!("expected structured response");
    };

    assert!(status.domains.iter().all(|d| d.name != "security"));
    assert_eq!(find_item(&status, "core", "Project").count, 1);
}

async fn projects_status_under_the_scope(ctx: &TestContext) {
    let status = fetch_status(ctx, "1/100/").await;

    let projects = status.projects.expect("projects should be present");
    assert_eq!(projects.total_known, 2);
    assert_eq!(projects.indexed, 1);
}

async fn empty_traversal_path_rejected(ctx: &TestContext) {
    let result = get_status(ctx, "", ResponseFormat::Raw, &admin_context()).await;

    let status = result.expect_err("empty traversal_path must be rejected");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

async fn toon_renders_states_and_counts(ctx: &TestContext) {
    let response = get_status(ctx, "1/101/", ResponseFormat::Llm, &admin_context())
        .await
        .expect("should succeed");
    let Some(get_graph_status_response::Content::FormattedText(text)) = response.content else {
        panic!("expected formatted text response");
    };

    assert!(text.contains("backfilling"), "TOON output: {text}");
    assert!(text.contains("code_review"), "TOON output: {text}");
    assert!(text.contains("count"), "TOON output: {text}");
}
