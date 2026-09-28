use std::sync::Arc;

use integration_testkit::run_subtests_shared;
use orbit_server::indexing_status::{IndexingStatusService, build_indexing_status_response};
use orbit_server::proto::{IndexingPhase, NamespaceIndexingStatus, ProjectsStatus};
use orbit_utils::traversal_path::TraversalPath;

use super::fixtures::{pinned_schema, seed_namespaces};
use crate::common::{GRAPH_SCHEMA_SQL, TestContext};

#[tokio::test]
async fn indexing_status_response() {
    let ctx = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;
    seed_namespaces(&ctx).await;

    run_subtests_shared!(
        &ctx,
        ready_at_every_level_when_everything_completed,
        not_started_at_every_level_without_checkpoints,
        project_path_gets_its_root_phases_and_its_own_projects,
        organization_path_is_unknown,
    );
}

async fn respond(ctx: &TestContext, paths: &[&str]) -> Vec<NamespaceIndexingStatus> {
    let paths: Vec<TraversalPath> = paths
        .iter()
        .map(|path| TraversalPath::new_unchecked(*path))
        .collect();
    let statuses = IndexingStatusService::new(Arc::new(ctx.create_client()))
        .read_scope_statuses(&pinned_schema(), &paths)
        .await;
    build_indexing_status_response(&statuses).statuses
}

fn phase_of(phase: i32) -> IndexingPhase {
    IndexingPhase::try_from(phase).expect("known phase")
}

fn projects_by_domain(status: &NamespaceIndexingStatus) -> Vec<(&str, Option<ProjectsStatus>)> {
    status
        .domains
        .iter()
        .map(|domain| (domain.name.as_str(), domain.projects))
        .filter(|(_, projects)| projects.is_some())
        .collect()
}

async fn ready_at_every_level_when_everything_completed(ctx: &TestContext) {
    let status = respond(ctx, &["1/112/"]).await.remove(0);

    assert_eq!(phase_of(status.phase), IndexingPhase::Ready);
    assert_eq!(status.domains.len(), 8);
    assert!(
        status
            .domains
            .iter()
            .all(|domain| phase_of(domain.phase) == IndexingPhase::Ready)
    );
    assert_eq!(
        projects_by_domain(&status),
        [(
            "source_code",
            Some(ProjectsStatus {
                indexed: 1,
                total_known: 1
            })
        )]
    );
}

async fn not_started_at_every_level_without_checkpoints(ctx: &TestContext) {
    let status = respond(ctx, &["1/102/"]).await.remove(0);

    assert_eq!(phase_of(status.phase), IndexingPhase::NotStarted);
    assert!(
        status
            .domains
            .iter()
            .all(|domain| phase_of(domain.phase) == IndexingPhase::NotStarted)
    );
}

async fn project_path_gets_its_root_phases_and_its_own_projects(ctx: &TestContext) {
    let statuses = respond(ctx, &["1/100/1000/", "1/100/"]).await;

    let paths: Vec<&str> = statuses
        .iter()
        .map(|status| status.traversal_path.as_str())
        .collect();
    assert_eq!(paths, ["1/100/1000/", "1/100/"]);

    let sdlc_phases = |status: &NamespaceIndexingStatus| -> Vec<i32> {
        status
            .domains
            .iter()
            .filter(|domain| domain.projects.is_none())
            .map(|domain| domain.phase)
            .collect()
    };
    assert_eq!(
        sdlc_phases(&statuses[0]),
        sdlc_phases(&statuses[1]),
        "SDLC plans run per root, so a project reports the SDLC phases of its root"
    );

    assert_eq!(
        projects_by_domain(&statuses[0]),
        [(
            "source_code",
            Some(ProjectsStatus {
                indexed: 1,
                total_known: 1
            })
        )]
    );
    assert_eq!(
        projects_by_domain(&statuses[1]),
        [(
            "source_code",
            Some(ProjectsStatus {
                indexed: 1,
                total_known: 2
            })
        )]
    );
}

async fn organization_path_is_unknown(ctx: &TestContext) {
    let status = respond(ctx, &["1/"]).await.remove(0);

    assert_eq!(phase_of(status.phase), IndexingPhase::Unknown);
}
