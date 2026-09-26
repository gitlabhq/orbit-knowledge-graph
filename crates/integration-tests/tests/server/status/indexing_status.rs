use std::sync::Arc;

use integration_testkit::{run_subtests_shared, t};
use orbit_server::indexing_status::{IndexingStatusService, Phase, ProjectCoverage, ScopeStatus};
use orbit_utils::traversal_path::TraversalPath;

use super::fixtures::{pinned_schema, seed_namespaces, seed_plans};
use crate::common::{GRAPH_SCHEMA_SQL, TestContext};

#[tokio::test]
async fn indexing_status() {
    let ctx = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;
    seed_namespaces(&ctx).await;

    run_subtests_shared!(
        &ctx,
        ready_when_every_plan_and_project_completed,
        ready_survives_an_incremental_cursor,
        syncing_while_a_first_pass_pages,
        syncing_while_a_started_first_pass_has_no_page_yet,
        syncing_when_only_some_plans_have_checkpoints,
        not_started_without_checkpoints,
        partition_checkpoints_are_ignored,
        domain_syncs_while_an_edge_plan_feeding_it_pages,
        domain_syncs_while_a_derived_plan_feeding_it_pages,
        source_code_follows_project_coverage,
        source_code_waits_for_its_sdlc_plans,
        organization_path_is_unknown,
        unreadable_checkpoints_are_unknown,
        late_page_write_keeps_the_completion,
        tombstone_clears_the_completion,
        many_scopes_in_request_order,
        projects_count_distinct_ids,
    );
}

async fn read(ctx: &TestContext, scopes: &[&str]) -> Vec<ScopeStatus> {
    let scopes: Vec<TraversalPath> = scopes
        .iter()
        .map(|s| TraversalPath::new_unchecked(*s))
        .collect();
    IndexingStatusService::new(Arc::new(ctx.create_client()))
        .read_scope_statuses(&pinned_schema(), &scopes)
        .await
}

async fn read_one(ctx: &TestContext, scope: &str) -> ScopeStatus {
    read(ctx, &[scope]).await.remove(0)
}

fn domain_phase(status: &ScopeStatus, domain: &str) -> Phase {
    status
        .domains
        .iter()
        .find(|d| d.name == domain)
        .unwrap_or_else(|| panic!("domain '{domain}' not found"))
        .phase
}

fn entity_phase(status: &ScopeStatus, entity: &str) -> Option<Phase> {
    status
        .domains
        .iter()
        .flat_map(|d| &d.entities)
        .find(|e| e.name == entity)
        .unwrap_or_else(|| panic!("entity '{entity}' not found"))
        .phase
}

async fn ready_when_every_plan_and_project_completed(ctx: &TestContext) {
    let status = read_one(ctx, "1/112/").await;

    assert_eq!(status.phase, Phase::Ready);
    assert_eq!(status.sdlc_phase, Phase::Ready);
    assert_eq!(status.code_phase, Some(Phase::Ready));
    assert!(status.domains.iter().all(|d| d.phase == Phase::Ready));
}

async fn ready_survives_an_incremental_cursor(ctx: &TestContext) {
    let status = read_one(ctx, "1/100/").await;

    assert_eq!(status.sdlc_phase, Phase::Ready);
    assert_eq!(entity_phase(&status, "Note"), Some(Phase::Ready));
}

async fn syncing_while_a_first_pass_pages(ctx: &TestContext) {
    let status = read_one(ctx, "1/101/").await;

    assert_eq!(status.phase, Phase::Syncing);
    assert_eq!(status.sdlc_phase, Phase::Syncing);
    assert_eq!(domain_phase(&status, "code_review"), Phase::Syncing);
    assert_eq!(domain_phase(&status, "plan"), Phase::Ready);
    assert_eq!(entity_phase(&status, "MergeRequest"), Some(Phase::Syncing));
    assert_eq!(entity_phase(&status, "WorkItem"), Some(Phase::Ready));
}

async fn syncing_while_a_started_first_pass_has_no_page_yet(ctx: &TestContext) {
    let status = read_one(ctx, "1/109/").await;

    assert_eq!(status.sdlc_phase, Phase::Syncing);
    assert_eq!(entity_phase(&status, "MergeRequest"), Some(Phase::Syncing));
}

async fn syncing_when_only_some_plans_have_checkpoints(ctx: &TestContext) {
    let status = read_one(ctx, "1/106/").await;

    assert_eq!(status.sdlc_phase, Phase::Syncing);
    assert_eq!(domain_phase(&status, "core"), Phase::Syncing);
    assert_eq!(domain_phase(&status, "plan"), Phase::NotStarted);
    assert_eq!(entity_phase(&status, "Project"), Some(Phase::Ready));
}

async fn not_started_without_checkpoints(ctx: &TestContext) {
    let status = read_one(ctx, "1/102/").await;

    assert_eq!(status.phase, Phase::NotStarted);
    assert_eq!(status.sdlc_phase, Phase::NotStarted);
    assert_eq!(status.code_phase, None);
    assert_eq!(
        entity_phase(&status, "MergeRequest"),
        Some(Phase::NotStarted)
    );
}

async fn partition_checkpoints_are_ignored(ctx: &TestContext) {
    let status = read_one(ctx, "1/104/").await;

    assert_eq!(status.sdlc_phase, Phase::Ready);
    assert_eq!(domain_phase(&status, "ci"), Phase::Ready);
}

async fn domain_syncs_while_an_edge_plan_feeding_it_pages(ctx: &TestContext) {
    let status = read_one(ctx, "1/105/").await;

    assert_eq!(
        domain_phase(&status, "core"),
        Phase::Syncing,
        "MEMBER_OF points at Group and Project"
    );
    assert_eq!(entity_phase(&status, "Project"), Some(Phase::Ready));
    assert_eq!(domain_phase(&status, "ci"), Phase::Ready);
}

async fn domain_syncs_while_a_derived_plan_feeding_it_pages(ctx: &TestContext) {
    let status = read_one(ctx, "1/108/").await;

    assert_eq!(
        domain_phase(&status, "plan"),
        Phase::Syncing,
        "SystemNote emits MENTIONS from WorkItem"
    );
    assert_eq!(entity_phase(&status, "WorkItem"), Some(Phase::Ready));
    assert_eq!(domain_phase(&status, "ci"), Phase::Ready);
}

async fn source_code_follows_project_coverage(ctx: &TestContext) {
    let partial = read_one(ctx, "1/100/").await;
    assert_eq!(partial.code_phase, Some(Phase::Syncing));
    assert_eq!(domain_phase(&partial, "source_code"), Phase::Syncing);
    assert_eq!(entity_phase(&partial, "Definition"), Some(Phase::Syncing));

    let complete = read_one(ctx, "1/101/").await;
    assert_eq!(complete.code_phase, Some(Phase::Ready));
    assert_eq!(domain_phase(&complete, "source_code"), Phase::Ready);
}

async fn source_code_waits_for_its_sdlc_plans(ctx: &TestContext) {
    let status = read_one(ctx, "1/107/").await;

    assert_eq!(status.code_phase, Some(Phase::Ready));
    assert_eq!(domain_phase(&status, "source_code"), Phase::Syncing);
    assert_eq!(entity_phase(&status, "Definition"), Some(Phase::Ready));
    assert_eq!(entity_phase(&status, "Commit"), Some(Phase::Syncing));
}

async fn organization_path_is_unknown(ctx: &TestContext) {
    let status = read_one(ctx, "1/").await;

    assert_eq!(status.phase, Phase::Unknown);
    assert_eq!(status.sdlc_phase, Phase::Unknown);
    assert_eq!(domain_phase(&status, "code_review"), Phase::Unknown);
}

async fn unreadable_checkpoints_are_unknown(ctx: &TestContext) {
    let db = ctx.fork("indexing_status_checkpoints_unreadable").await;
    db.execute(&format!("DROP TABLE {}", t("checkpoint"))).await;

    let status = read_one(&db, "1/100/").await;

    assert_eq!(status.phase, Phase::Unknown);
    assert_eq!(status.sdlc_phase, Phase::Unknown);
    assert_eq!(domain_phase(&status, "source_code"), Phase::Unknown);
    assert_eq!(status.projects.total_known, 2);
}

async fn late_page_write_keeps_the_completion(ctx: &TestContext) {
    let db = ctx.fork("indexing_status_late_page_write").await;
    seed_plans(&db, 120, &[]).await;
    db.execute(&format!(
        "INSERT INTO {} (key, watermark, cursor_values, indexed_at, _version) VALUES
         ('ns.120.MergeRequest', now(), '{{\"c\":[\"1/120/\",\"7\"]}}', NULL, now64(6) + INTERVAL 1 SECOND)",
        t("checkpoint")
    ))
    .await;

    let status = read_one(&db, "1/120/").await;

    assert_eq!(entity_phase(&status, "MergeRequest"), Some(Phase::Ready));
}

async fn tombstone_clears_the_completion(ctx: &TestContext) {
    let db = ctx.fork("indexing_status_tombstone").await;
    seed_plans(&db, 121, &[]).await;
    db.execute(&format!(
        "INSERT INTO {} (key, watermark, cursor_values, _version, _deleted) VALUES
         ('ns.121.MergeRequest', now(), '', now64(6) + INTERVAL 1 SECOND, true)",
        t("checkpoint")
    ))
    .await;

    let status = read_one(&db, "1/121/").await;

    assert_eq!(
        entity_phase(&status, "MergeRequest"),
        Some(Phase::NotStarted)
    );
    assert_eq!(status.sdlc_phase, Phase::Syncing);
}

async fn many_scopes_in_request_order(ctx: &TestContext) {
    let statuses = read(ctx, &["1/101/", "1/100/", "1/100/1000/"]).await;

    let scopes: Vec<&str> = statuses.iter().map(|s| s.scope.as_str()).collect();
    assert_eq!(scopes, ["1/101/", "1/100/", "1/100/1000/"]);
    assert_eq!(statuses[0].sdlc_phase, Phase::Syncing);
    assert_eq!(statuses[1].sdlc_phase, Phase::Ready);
    assert_eq!(statuses[2].sdlc_phase, Phase::Ready);
    assert_eq!(
        statuses[1].projects,
        ProjectCoverage {
            indexed: 1,
            total_known: 2
        }
    );
    assert_eq!(
        statuses[2].projects,
        ProjectCoverage {
            indexed: 1,
            total_known: 1
        }
    );
}

async fn projects_count_distinct_ids(ctx: &TestContext) {
    let db = ctx.fork("indexing_status_projects_distinct_ids").await;
    db.execute(&format!(
        "INSERT INTO {} (id, name, visibility_level, traversal_path, _version, _deleted) VALUES
         (9500, 'Dup Project', 'public', '1/100/9500/', '2024-01-01 00:00:00', false),
         (9500, 'Dup Project Renamed', 'public', '1/100/9500/', '2024-06-01 00:00:00', false)",
        t("gl_project")
    ))
    .await;

    let status = read_one(&db, "1/").await;

    assert_eq!(
        status.projects,
        ProjectCoverage {
            indexed: 4,
            total_known: 6
        }
    );
}
