use std::sync::Arc;

use orbit_utils::traversal_path::TraversalPath;
use query_engine::compiler::{AuthorizedPath, SecurityContext};

use crate::common::{GRAPH_SCHEMA_SQL, TestContext};
use integration_testkit::{
    FINISHED_CURSOR, INCREMENTAL_CURSOR, load_ontology, run_subtests_shared, t,
};
use orbit_server::active_schema::ActiveSchema;
use orbit_server::graph_status::GraphStatusService;
use orbit_server::proto::{
    GetGraphStatusResponse, GraphStatusDomain, IndexingPhase, IndexingState, ResponseFormat,
    StructuredGraphStatus, get_graph_status_response,
};

const FIRST_PASS_CURSOR: &str = r#"{"c":["1/100/","42"]}"#;

fn admin_context() -> SecurityContext {
    SecurityContext::new_with_roles(1, vec![AuthorizedPath::new("1/", 50)])
        .unwrap()
        .with_role(true, Some(50))
}

async fn setup(ctx: &TestContext) {
    ctx.execute(&format!(
        "INSERT INTO {} (id, name, visibility_level, traversal_path) VALUES
         (1000, 'Public Project', 'public', '1/100/1000/'),
         (1001, 'Private Project', 'private', '1/101/1001/'),
         (1002, 'Internal Project', 'internal', '1/100/1002/'),
         (1070, 'Code Indexed Project', 'public', '1/107/1070/')",
        t("gl_project")
    ))
    .await;

    ctx.execute(&format!(
        "INSERT INTO {} (traversal_path, project_id, branch, last_task_id, indexed_at) VALUES
         ('1/100/1000/', 1000, 'main', 1, now()),
         ('1/101/1001/', 1001, 'main', 2, now()),
         ('1/100/1999/', 1999, 'main', 3, now()),
         ('1/107/1070/', 1070, 'main', 4, now())",
        t("code_indexing_checkpoint")
    ))
    .await;

    seed_checkpoints(ctx, 100, &[("Note", INCREMENTAL_CURSOR)]).await;
    seed_checkpoints(ctx, 101, &[("MergeRequest", FIRST_PASS_CURSOR)]).await;
    seed_checkpoints(ctx, 104, &[("Job.p1of5", FIRST_PASS_CURSOR)]).await;
    seed_checkpoints(ctx, 105, &[("MEMBER_OF_siphon_members", FIRST_PASS_CURSOR)]).await;
    seed_checkpoints(ctx, 107, &[("Commit", FIRST_PASS_CURSOR)]).await;
    seed_checkpoints(ctx, 108, &[("SystemNote", FIRST_PASS_CURSOR)]).await;
    ctx.execute(&format!(
        "INSERT INTO {} (key, watermark, cursor_values) VALUES ('ns.106.Project', now(), '{FINISHED_CURSOR}')",
        t("checkpoint")
    ))
    .await;

    ctx.optimize_all().await;
}

async fn seed_checkpoints(ctx: &TestContext, root: i64, cursor_overrides: &[(&str, &str)]) {
    let overridden = |plan: &str| cursor_overrides.iter().any(|(key, _)| *key == plan);
    let finished_plans = load_ontology()
        .namespaced_pipeline_descriptors()
        .into_iter()
        .filter(|plan| !overridden(&plan.name))
        .map(|plan| (plan.name, FINISHED_CURSOR));
    let overrides = cursor_overrides
        .iter()
        .map(|(key, cursor)| (key.to_string(), *cursor));

    let values: Vec<String> = finished_plans
        .chain(overrides)
        .map(|(key, cursor)| format!("('ns.{root}.{key}', now(), '{cursor}')"))
        .collect();
    ctx.execute(&format!(
        "INSERT INTO {} (key, watermark, cursor_values) VALUES {}",
        t("checkpoint"),
        values.join(", ")
    ))
    .await;
}

async fn get_status(
    ctx: &TestContext,
    traversal_path: &str,
    format: ResponseFormat,
    security_context: &SecurityContext,
) -> Result<GetGraphStatusResponse, tonic::Status> {
    let schema = ActiveSchema::pinned(load_ontology())
        .snapshot()
        .expect("pinned schema is installed");
    GraphStatusService::new(Arc::new(ctx.create_client()))
        .get_status(
            &schema,
            &TraversalPath::new_unchecked(traversal_path),
            format as i32,
            security_context,
        )
        .await
}

async fn fetch_status(
    ctx: &TestContext,
    traversal_path: &str,
    security_context: &SecurityContext,
) -> StructuredGraphStatus {
    let response = get_status(ctx, traversal_path, ResponseFormat::Raw, security_context)
        .await
        .expect("should succeed");
    let Some(get_graph_status_response::Content::Structured(status)) = response.content else {
        panic!("expected structured response");
    };
    status
}

fn reported_phase(status: &StructuredGraphStatus) -> IndexingPhase {
    let progress = status
        .progress
        .as_ref()
        .expect("progress should be present");
    IndexingPhase::try_from(progress.phase).expect("known phase")
}

fn reported_indexing_state(status: &StructuredGraphStatus) -> IndexingState {
    let indexing = status
        .indexing
        .as_ref()
        .expect("indexing should be present");
    IndexingState::try_from(indexing.state).expect("known state")
}

fn find_domain<'a>(status: &'a StructuredGraphStatus, name: &str) -> &'a GraphStatusDomain {
    status
        .domains
        .iter()
        .find(|d| d.name == name)
        .unwrap_or_else(|| panic!("domain '{name}' not found"))
}

fn reported_domain_phase(status: &StructuredGraphStatus, name: &str) -> IndexingPhase {
    IndexingPhase::try_from(find_domain(status, name).phase).expect("known phase")
}

fn reported_item_state(
    status: &StructuredGraphStatus,
    domain: &str,
    item: &str,
) -> Option<IndexingState> {
    find_domain(status, domain)
        .items
        .iter()
        .find(|i| i.name == item)
        .unwrap_or_else(|| panic!("item '{item}' not found in domain '{domain}'"))
        .state
        .map(|state| IndexingState::try_from(state).expect("known state"))
}

#[tokio::test]
async fn graph_status() {
    let ctx = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;
    setup(&ctx).await;

    run_subtests_shared!(
        &ctx,
        ready_when_every_plan_finished_its_first_pass,
        ready_survives_an_incremental_cursor,
        sdlc_indexing_mirrors_indexing,
        syncing_while_a_first_pass_still_has_a_cursor,
        syncing_when_only_some_plans_have_checkpointed,
        not_started_when_no_plan_has_checkpointed,
        ready_ignores_partition_checkpoints,
        domain_syncs_while_an_edge_plan_feeding_it_is_unfinished,
        domain_syncs_while_a_derived_plan_feeding_it_is_unfinished,
        project_scope_reports_its_top_level_phase,
        organization_path_has_unknown_phase,
        code_domain_follows_project_coverage,
        code_domain_waits_for_its_sdlc_plans,
        empty_traversal_path_rejected,
        projects_status_at_root,
        projects_status_scoped_by_traversal_path,
        code_indexing_omitted_when_no_projects_known,
        reporter_does_not_see_the_security_domain,
        security_manager_sees_the_security_domain,
        toon_renders_phase_per_domain,
        projects_total_known_counts_distinct_ids,
        phase_unknown_when_checkpoints_cannot_be_read,
    );
}

async fn ready_when_every_plan_finished_its_first_pass(ctx: &TestContext) {
    let status = fetch_status(ctx, "1/100/", &admin_context()).await;

    assert_eq!(reported_phase(&status), IndexingPhase::Ready);
    assert_eq!(reported_indexing_state(&status), IndexingState::Indexed);
    assert_eq!(
        reported_domain_phase(&status, "code_review"),
        IndexingPhase::Ready
    );
    assert_eq!(
        reported_item_state(&status, "code_review", "MergeRequest"),
        Some(IndexingState::Indexed)
    );
    assert!(
        status
            .domains
            .iter()
            .flat_map(|d| &d.items)
            .all(|i| i.count.is_none())
    );
}

async fn ready_survives_an_incremental_cursor(ctx: &TestContext) {
    let status = fetch_status(ctx, "1/100/", &admin_context()).await;

    assert_eq!(
        reported_domain_phase(&status, "core"),
        IndexingPhase::Ready,
        "Note is mid-incremental with a floor, which is not a first pass"
    );
    assert_eq!(
        reported_item_state(&status, "core", "Note"),
        Some(IndexingState::Indexed)
    );
}

async fn sdlc_indexing_mirrors_indexing(ctx: &TestContext) {
    let status = fetch_status(ctx, "1/101/", &admin_context()).await;

    assert_eq!(status.sdlc_indexing, status.indexing);
}

async fn syncing_while_a_first_pass_still_has_a_cursor(ctx: &TestContext) {
    let status = fetch_status(ctx, "1/101/", &admin_context()).await;

    assert_eq!(reported_phase(&status), IndexingPhase::Syncing);
    assert_eq!(reported_indexing_state(&status), IndexingState::Backfilling);
    assert_eq!(
        reported_domain_phase(&status, "code_review"),
        IndexingPhase::Syncing
    );
    assert_eq!(reported_domain_phase(&status, "plan"), IndexingPhase::Ready);
    assert_eq!(
        reported_item_state(&status, "code_review", "MergeRequest"),
        Some(IndexingState::Backfilling)
    );
    assert_eq!(
        reported_item_state(&status, "plan", "WorkItem"),
        Some(IndexingState::Indexed)
    );
}

async fn syncing_when_only_some_plans_have_checkpointed(ctx: &TestContext) {
    let status = fetch_status(ctx, "1/106/", &admin_context()).await;

    assert_eq!(reported_phase(&status), IndexingPhase::Syncing);
    assert_eq!(
        reported_domain_phase(&status, "core"),
        IndexingPhase::Syncing
    );
    assert_eq!(
        reported_domain_phase(&status, "plan"),
        IndexingPhase::NotStarted
    );
}

async fn not_started_when_no_plan_has_checkpointed(ctx: &TestContext) {
    let status = fetch_status(ctx, "1/102/", &admin_context()).await;

    assert_eq!(reported_phase(&status), IndexingPhase::NotStarted);
    assert_eq!(reported_indexing_state(&status), IndexingState::NotIndexed);
    assert_eq!(
        reported_domain_phase(&status, "code_review"),
        IndexingPhase::NotStarted
    );
    assert_eq!(
        reported_item_state(&status, "code_review", "MergeRequest"),
        Some(IndexingState::NotIndexed)
    );
}

async fn ready_ignores_partition_checkpoints(ctx: &TestContext) {
    let status = fetch_status(ctx, "1/104/", &admin_context()).await;

    assert_eq!(reported_phase(&status), IndexingPhase::Ready);
    assert_eq!(reported_domain_phase(&status, "ci"), IndexingPhase::Ready);
}

async fn domain_syncs_while_an_edge_plan_feeding_it_is_unfinished(ctx: &TestContext) {
    let status = fetch_status(ctx, "1/105/", &admin_context()).await;

    assert_eq!(
        reported_domain_phase(&status, "core"),
        IndexingPhase::Syncing,
        "MEMBER_OF points at Group and Project"
    );
    assert_eq!(
        reported_item_state(&status, "core", "Project"),
        Some(IndexingState::Indexed)
    );
    assert_eq!(reported_domain_phase(&status, "ci"), IndexingPhase::Ready);
}

async fn domain_syncs_while_a_derived_plan_feeding_it_is_unfinished(ctx: &TestContext) {
    let status = fetch_status(ctx, "1/108/", &admin_context()).await;

    assert_eq!(
        reported_domain_phase(&status, "plan"),
        IndexingPhase::Syncing,
        "SystemNote emits MENTIONS from WorkItem"
    );
    assert_eq!(
        reported_item_state(&status, "plan", "WorkItem"),
        Some(IndexingState::Indexed)
    );
    assert_eq!(reported_domain_phase(&status, "ci"), IndexingPhase::Ready);
}

async fn project_scope_reports_its_top_level_phase(ctx: &TestContext) {
    let status = fetch_status(ctx, "1/100/1000/", &admin_context()).await;

    assert_eq!(reported_phase(&status), IndexingPhase::Ready);
}

async fn organization_path_has_unknown_phase(ctx: &TestContext) {
    let status = fetch_status(ctx, "1/", &admin_context()).await;

    assert_eq!(reported_phase(&status), IndexingPhase::Unknown);
    assert_eq!(reported_indexing_state(&status), IndexingState::Unknown);
    assert_eq!(
        reported_domain_phase(&status, "code_review"),
        IndexingPhase::Unknown
    );
}

async fn code_domain_follows_project_coverage(ctx: &TestContext) {
    let partial = fetch_status(ctx, "1/100/", &admin_context()).await;
    assert_eq!(
        reported_domain_phase(&partial, "source_code"),
        IndexingPhase::Syncing,
        "1 of 2 projects under 1/100/ is code-indexed"
    );
    assert_eq!(
        reported_item_state(&partial, "source_code", "Definition"),
        Some(IndexingState::Backfilling)
    );

    let complete = fetch_status(ctx, "1/101/", &admin_context()).await;
    assert_eq!(
        reported_domain_phase(&complete, "source_code"),
        IndexingPhase::Ready
    );
}

async fn code_domain_waits_for_its_sdlc_plans(ctx: &TestContext) {
    let status = fetch_status(ctx, "1/107/", &admin_context()).await;

    assert_eq!(
        reported_domain_phase(&status, "source_code"),
        IndexingPhase::Syncing,
        "code is fully indexed but the Commit plan is mid first pass"
    );
    assert_eq!(
        reported_item_state(&status, "source_code", "Definition"),
        Some(IndexingState::Indexed)
    );
    assert_eq!(
        reported_item_state(&status, "source_code", "Commit"),
        Some(IndexingState::Backfilling)
    );
}

async fn empty_traversal_path_rejected(ctx: &TestContext) {
    let result = get_status(ctx, "", ResponseFormat::Raw, &admin_context()).await;

    let status = result.expect_err("empty traversal_path must be rejected");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

async fn projects_status_at_root(ctx: &TestContext) {
    let status = fetch_status(ctx, "1/", &admin_context()).await;

    let projects = status.projects.expect("projects should be present");
    assert_eq!(projects.total_known, 4);
    assert_eq!(projects.indexed, 3);
}

async fn projects_status_scoped_by_traversal_path(ctx: &TestContext) {
    let status = fetch_status(ctx, "1/100/", &admin_context()).await;

    let projects = status.projects.expect("projects should be present");
    assert_eq!(projects.total_known, 2);
    assert_eq!(projects.indexed, 1);
}

async fn code_indexing_omitted_when_no_projects_known(ctx: &TestContext) {
    let status = fetch_status(ctx, "1/102/", &admin_context()).await;

    assert!(
        status.code_indexing.is_none(),
        "a scope with no known projects has no code coverage to claim"
    );
    assert_eq!(
        reported_domain_phase(&status, "source_code"),
        IndexingPhase::Unknown,
        "no projects means no code coverage to derive a phase from"
    );
}

async fn reporter_does_not_see_the_security_domain(ctx: &TestContext) {
    let reporter = SecurityContext::new_with_roles(1, vec![AuthorizedPath::new("1/", 20)]).unwrap();
    let status = fetch_status(ctx, "1/100/", &reporter).await;

    assert!(status.domains.iter().all(|d| d.name != "security"));
    assert!(
        find_domain(&status, "core")
            .items
            .iter()
            .any(|i| i.name == "Project")
    );
}

async fn security_manager_sees_the_security_domain(ctx: &TestContext) {
    let security_manager =
        SecurityContext::new_with_roles(1, vec![AuthorizedPath::new("1/", 25)]).unwrap();
    let status = fetch_status(ctx, "1/100/", &security_manager).await;

    assert!(
        find_domain(&status, "security")
            .items
            .iter()
            .any(|i| i.name == "Vulnerability")
    );
}

async fn toon_renders_phase_per_domain(ctx: &TestContext) {
    let response = get_status(ctx, "1/101/", ResponseFormat::Llm, &admin_context())
        .await
        .expect("should succeed");
    let Some(get_graph_status_response::Content::FormattedText(text)) = response.content else {
        panic!("expected formatted text response");
    };

    assert!(text.contains("phase: syncing"), "TOON output: {text}");
    assert!(text.contains("ready"), "TOON output: {text}");
    assert!(text.contains("code_review"), "TOON output: {text}");
    assert!(!text.contains("count"), "TOON output: {text}");
}

async fn projects_total_known_counts_distinct_ids(ctx: &TestContext) {
    let db = ctx.fork("graph_status_projects_distinct_ids").await;

    db.execute(&format!(
        "INSERT INTO {} (id, name, visibility_level, traversal_path, _version, _deleted) VALUES
         (9500, 'Dup Project', 'public', '1/100/9500/', '2024-01-01 00:00:00', false)",
        t("gl_project")
    ))
    .await;
    db.execute(&format!(
        "INSERT INTO {} (id, name, visibility_level, traversal_path, _version, _deleted) VALUES
         (9500, 'Dup Project Renamed', 'public', '1/100/9500/', '2024-06-01 00:00:00', false)",
        t("gl_project")
    ))
    .await;

    let status = fetch_status(&db, "1/", &admin_context()).await;
    let projects = status.projects.expect("projects should be present");
    assert_eq!(
        projects.total_known, 5,
        "duplicate-version project rows count as one distinct id"
    );
}

async fn phase_unknown_when_checkpoints_cannot_be_read(ctx: &TestContext) {
    let db = ctx.fork("graph_status_checkpoints_unreadable").await;
    db.execute(&format!("DROP TABLE {}", t("checkpoint"))).await;

    let status = fetch_status(&db, "1/100/", &admin_context()).await;

    assert_eq!(reported_phase(&status), IndexingPhase::Unknown);
    assert_eq!(reported_indexing_state(&status), IndexingState::Unknown);
    assert_eq!(
        reported_domain_phase(&status, "code_review"),
        IndexingPhase::Unknown
    );
    assert_eq!(
        reported_domain_phase(&status, "source_code"),
        IndexingPhase::Unknown
    );
    let projects = status.projects.expect("projects should be present");
    assert_eq!(projects.total_known, 2);
}
