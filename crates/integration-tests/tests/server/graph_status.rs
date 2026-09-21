use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use futures::future::join_all;
use indexer::modules::sdlc::jobs::namespace_data_job;
use jobs::{JobLedger, JobState, JobTransition};
use orbit_utils::traversal_path::TraversalPath;
use query_engine::compiler::{AuthorizedPath, SecurityContext};
use uuid::Uuid;

use crate::common::{GRAPH_SCHEMA_SQL, TestContext};
use clickhouse_client::ClickHouseConfigurationExt;
use integration_testkit::{PERSISTENT_SCHEMA_SQL, load_ontology, run_subtests_shared, t};
use orbit_server::graph_status::GraphStatusService;
use orbit_server::proto::{
    GetGraphStatusResponse, IndexingState, ResponseFormat, StructuredGraphStatus,
    get_graph_status_response,
};

fn admin_context() -> SecurityContext {
    SecurityContext::new_with_roles(1, vec![AuthorizedPath::new("1/", 50)])
        .unwrap()
        .with_role(true, Some(50))
}

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
         (101, 'Private Group', 'private', '1/101/'),
         (300, 'Other Org Group', 'public', '2/300/')",
        t("gl_group")
    ))
    .await;

    ctx.execute(&format!(
        "INSERT INTO {} (id, name, visibility_level, traversal_path) VALUES
         (1000, 'Public Project', 'public', '1/100/1000/'),
         (1001, 'Private Project', 'private', '1/101/1001/'),
         (1002, 'Internal Project', 'internal', '1/100/1002/'),
         (3000, 'Indexed Other Org Project', 'public', '2/300/3000/'),
         (3002, 'Unindexed Other Org Project', 'public', '2/300/3002/')",
        t("gl_project")
    ))
    .await;

    ctx.execute(&format!(
        "INSERT INTO {} (traversal_path, project_id, branch, last_task_id, indexed_at) VALUES
         ('1/100/1000/', 1000, 'main', 1, now()),
         ('1/101/1001/', 1001, 'main', 2, now()),
         ('1/100/1999/', 1999, 'main', 3, now()),
         ('2/300/3000/', 3000, 'main', 4, now())",
        t("code_indexing_checkpoint")
    ))
    .await;

    ctx.execute(&format!(
        "INSERT INTO {} (id, iid, title, state, source_branch, target_branch, traversal_path) VALUES
         (2000, 1, 'Add feature A', 'opened', 'feature-a', 'main', '1/100/1000/'),
         (2001, 2, 'Fix bug B', 'opened', 'fix-b', 'main', '1/101/1001/')",
        t("gl_merge_request")
    ))
    .await;

    ctx.execute(&format!(
        "INSERT INTO {} (id, title, state, severity, report_type, traversal_path) VALUES
         (5000, 'SQL Injection', 'detected', 'critical', 'sast', '1/100/1000/')",
        t("gl_vulnerability")
    ))
    .await;

    ctx.execute(&format!(
        "INSERT INTO {} (id, name, version, package_type, status, project_id, traversal_path) VALUES
         (6000, '@gitlab/ui', '1.0.0', 'npm', 'default', 1000, '1/100/1000/'),
         (6001, 'rails', '7.1.0', 'rubygems', 'default', 1000, '1/100/1000/'),
         (6002, 'lodash', '4.17.21', 'npm', 'default', 1001, '1/101/1001/')",
        t("gl_package")
    ))
    .await;

    ctx.execute(&format!(
        "INSERT INTO {} (id, name, status, project_id, traversal_path) VALUES
         (7000, 'gitlab-org/gitlab/web', '', 1000, '1/100/1000/'),
         (7001, 'gitlab-org/gitlab/api', '', 1001, '1/101/1001/')",
        t("gl_container_repository")
    ))
    .await;

    seed_all_pipelines(ctx, "1/100/", &completed_run()).await;
    ctx.optimize_all().await;
}

fn build_service(ctx: &TestContext) -> GraphStatusService {
    let client = Arc::new(ctx.create_client());
    GraphStatusService::new(client)
}

async fn record_run(
    ledger: &JobLedger,
    path: &str,
    plan: &str,
    state: JobState,
    started_at: DateTime<Utc>,
    at: DateTime<Utc>,
    rows: (u64, u64),
) {
    let traversal_path = TraversalPath::new_unchecked(path);
    let namespace_id = traversal_path.top_level_namespace_id().unwrap_or(0);
    let transition = JobTransition {
        job: namespace_data_job(plan, &traversal_path, namespace_id),
        dispatch_id: Uuid::new_v4(),
        attempt: 1,
        state,
        reason: (state == JobState::Failed).then(|| "scan failure".to_string()),
        rows_read: rows.0,
        rows_written: rows.1,
        started_at,
        recorded_at: at,
    };
    ledger
        .record(&transition)
        .await
        .expect("record job transition");
}

async fn seed_pipeline(
    ctx: &TestContext,
    path: &str,
    plan: &str,
    steps: &[(JobState, DateTime<Utc>)],
) {
    seed_pipelines(ctx, path, &[plan.to_string()], steps).await;
}

async fn seed_all_pipelines(ctx: &TestContext, path: &str, steps: &[(JobState, DateTime<Utc>)]) {
    seed_pipelines(ctx, path, &namespaced_pipeline_names(), steps).await;
}

async fn seed_pipelines(
    ctx: &TestContext,
    path: &str,
    plans: &[String],
    steps: &[(JobState, DateTime<Utc>)],
) {
    let ledger = JobLedger::new(Arc::new(ctx.create_client()));
    let mut started_at = steps.first().map_or_else(Utc::now, |(_, at)| *at);
    for (state, at) in steps {
        if *state == JobState::Running {
            started_at = *at;
        }
        join_all(
            plans
                .iter()
                .map(|plan| record_run(&ledger, path, plan, *state, started_at, *at, (0, 0))),
        )
        .await;
    }
    ctx.flush_async_inserts().await;
}

fn completed_run() -> [(JobState, DateTime<Utc>); 2] {
    let started = Utc::now() - Duration::seconds(30);
    [
        (JobState::Running, started),
        (JobState::Succeeded, started + Duration::seconds(5)),
    ]
}

fn failed_run() -> [(JobState, DateTime<Utc>); 2] {
    let started = Utc::now() - Duration::seconds(10);
    [
        (JobState::Running, started),
        (JobState::Failed, started + Duration::seconds(2)),
    ]
}

fn extract_structured(response: GetGraphStatusResponse) -> StructuredGraphStatus {
    match response.content {
        Some(get_graph_status_response::Content::Structured(s)) => s,
        _ => panic!("Expected structured response"),
    }
}

fn find_domain<'a>(
    domains: &'a [orbit_server::proto::GraphStatusDomain],
    name: &str,
) -> &'a orbit_server::proto::GraphStatusDomain {
    domains
        .iter()
        .find(|d| d.name == name)
        .unwrap_or_else(|| panic!("domain '{name}' not found"))
}

fn find_item(domain: &orbit_server::proto::GraphStatusDomain, name: &str) -> i64 {
    domain
        .items
        .iter()
        .find(|i| i.name == name)
        .unwrap_or_else(|| panic!("item '{name}' not found in domain '{}'", domain.name))
        .count
}

#[tokio::test]
async fn graph_status() {
    let ctx = TestContext::new(&[*GRAPH_SCHEMA_SQL, *PERSISTENT_SCHEMA_SQL]).await;
    setup(&ctx).await;

    run_subtests_shared!(
        &ctx,
        root_traversal_path_returns_all_entity_counts,
        scoped_by_traversal_path_filters_counts,
        empty_traversal_path_rejected,
        non_matching_traversal_path_returns_zeros,
        all_domains_present_in_response,
        projects_status_at_root,
        projects_status_scoped_by_traversal_path,
        indexing_status_indexed_for_group,
        indexing_status_backfilling_for_project,
        indexing_status_not_indexed_when_no_runs,
        indexing_status_indexing_when_reindex_in_flight,
        indexing_status_error_state,
        indexing_status_unknown_when_job_table_missing,
        indexing_status_per_entity_worst_state_wins,
        indexing_status_per_entity_missing_run_treated_as_not_indexed,
        code_not_indexed_dominates_when_no_project_checkpointed,
        code_indexing_omitted_when_no_projects_known,
        edge_pipeline_error_surfaces_in_sdlc_state,
        items_carry_per_entity_state,
        indexing_status_reports_last_run_rows,
        toon_renders_split_indexing_blocks,
        reporter_excludes_security_entity_counts,
        security_manager_includes_security_entity_counts,
        definition_count_counts_distinct_ids,
        group_count_excludes_deleted,
        projects_total_known_counts_distinct_ids,
        get_status_degrades_when_entity_count_table_missing,
    );
}

async fn root_traversal_path_returns_all_entity_counts(ctx: &TestContext) {
    let service = build_service(ctx);
    let response = service
        .get_status(
            &load_ontology(),
            &TraversalPath::new_unchecked("1/"),
            ResponseFormat::Raw as i32,
            &admin_context(),
        )
        .await
        .expect("should succeed");
    let status = extract_structured(response);

    let core = find_domain(&status.domains, "core");
    assert_eq!(find_item(core, "Project"), 3);
    assert_eq!(find_item(core, "Group"), 2);

    let code = find_domain(&status.domains, "code_review");
    assert_eq!(find_item(code, "MergeRequest"), 2);

    let packages = find_domain(&status.domains, "packages");
    assert_eq!(find_item(packages, "Package"), 3);

    let container = find_domain(&status.domains, "container_registry");
    assert_eq!(find_item(container, "ContainerRepository"), 2);
}

async fn scoped_by_traversal_path_filters_counts(ctx: &TestContext) {
    let service = build_service(ctx);

    let response = service
        .get_status(
            &load_ontology(),
            &TraversalPath::new_unchecked("1/100/"),
            ResponseFormat::Raw as i32,
            &admin_context(),
        )
        .await
        .expect("should succeed");
    let status = extract_structured(response);

    let core = find_domain(&status.domains, "core");
    assert_eq!(find_item(core, "Project"), 2, "projects under 1/100/");
    assert_eq!(find_item(core, "Group"), 1, "groups under 1/100/");

    let code = find_domain(&status.domains, "code_review");
    assert_eq!(find_item(code, "MergeRequest"), 1, "MRs under 1/100/");

    let packages = find_domain(&status.domains, "packages");
    assert_eq!(find_item(packages, "Package"), 2, "packages under 1/100/");

    let container = find_domain(&status.domains, "container_registry");
    assert_eq!(
        find_item(container, "ContainerRepository"),
        1,
        "container repos under 1/100/"
    );
}

async fn empty_traversal_path_rejected(ctx: &TestContext) {
    let service = build_service(ctx);

    let result = service
        .get_status(
            &load_ontology(),
            &TraversalPath::new_unchecked(""),
            ResponseFormat::Raw as i32,
            &admin_context(),
        )
        .await;

    assert!(result.is_err());
    let status = result.unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

async fn non_matching_traversal_path_returns_zeros(ctx: &TestContext) {
    let service = build_service(ctx);

    let response = service
        .get_status(
            &load_ontology(),
            &TraversalPath::new_unchecked("999/"),
            ResponseFormat::Raw as i32,
            &admin_context(),
        )
        .await
        .expect("should succeed");
    let status = extract_structured(response);

    let core = find_domain(&status.domains, "core");
    assert_eq!(find_item(core, "Project"), 0);
    assert_eq!(find_item(core, "Group"), 0);
}

async fn all_domains_present_in_response(ctx: &TestContext) {
    let service = build_service(ctx);
    let ontology = load_ontology();

    let response = service
        .get_status(
            &load_ontology(),
            &TraversalPath::new_unchecked("1/"),
            ResponseFormat::Raw as i32,
            &admin_context(),
        )
        .await
        .expect("should succeed");
    let status = extract_structured(response);

    let expected_domains: Vec<String> = ontology.domains().map(|d| d.name.clone()).collect();
    let actual_domains: Vec<String> = status.domains.iter().map(|d| d.name.clone()).collect();

    assert_eq!(actual_domains.len(), expected_domains.len());
    for expected in &expected_domains {
        assert!(
            actual_domains.contains(expected),
            "missing domain: {expected}"
        );
    }
}

async fn projects_status_at_root(ctx: &TestContext) {
    let service = build_service(ctx);
    let response = service
        .get_status(
            &load_ontology(),
            &TraversalPath::new_unchecked("1/"),
            ResponseFormat::Raw as i32,
            &admin_context(),
        )
        .await
        .expect("should succeed");
    let status = extract_structured(response);

    let projects = status.projects.expect("projects should be present");
    assert_eq!(projects.total_known, 3, "3 projects under 1/");
    assert_eq!(projects.indexed, 2, "2 projects with checkpoints");
}

async fn projects_status_scoped_by_traversal_path(ctx: &TestContext) {
    let service = build_service(ctx);
    let response = service
        .get_status(
            &load_ontology(),
            &TraversalPath::new_unchecked("1/100/"),
            ResponseFormat::Raw as i32,
            &admin_context(),
        )
        .await
        .expect("should succeed");
    let status = extract_structured(response);

    let projects = status.projects.expect("projects should be present");
    assert_eq!(projects.total_known, 2, "2 projects under 1/100/");
    assert_eq!(
        projects.indexed, 1,
        "1 project with checkpoint under 1/100/"
    );
}

async fn indexing_status_indexed_for_group(ctx: &TestContext) {
    let service = build_service(ctx);
    let response = service
        .get_status(
            &load_ontology(),
            &TraversalPath::new_unchecked("1/100/"),
            ResponseFormat::Raw as i32,
            &admin_context(),
        )
        .await
        .expect("should succeed");
    let status = extract_structured(response);

    let sdlc = status
        .sdlc_indexing
        .expect("sdlc_indexing should be present");
    assert_eq!(sdlc.state, IndexingState::Indexed as i32);
    assert!(sdlc.last_started_at.is_some());
    assert!(sdlc.last_completed_at.is_some());
    assert_eq!(sdlc.last_duration_ms, Some(5000));
    assert!(sdlc.last_error.is_none());

    let indexing = status.indexing.expect("indexing should be present");
    assert_eq!(
        indexing.state,
        IndexingState::Backfilling as i32,
        "combined state reflects code coverage (1 of 2 projects) even though SDLC completed"
    );
}

async fn indexing_status_backfilling_for_project(ctx: &TestContext) {
    seed_all_pipelines(ctx, "2/300/3000/", &[(JobState::Running, Utc::now())]).await;

    let service = build_service(ctx);
    let response = service
        .get_status(
            &load_ontology(),
            &TraversalPath::new_unchecked("2/300/3000/"),
            ResponseFormat::Raw as i32,
            &admin_context(),
        )
        .await
        .expect("should succeed");
    let status = extract_structured(response);

    let indexing = status.indexing.expect("indexing should be present");
    assert_eq!(indexing.state, IndexingState::Backfilling as i32);
    assert!(indexing.last_started_at.is_some());
    assert!(indexing.last_completed_at.is_none());

    let code = status
        .code_indexing
        .expect("code_indexing should be present");
    assert_eq!(
        code.state,
        IndexingState::Indexed as i32,
        "the project itself is checkpointed"
    );
}

async fn indexing_status_indexing_when_reindex_in_flight(ctx: &TestContext) {
    let previous_completion = Utc::now() - Duration::seconds(60);
    seed_all_pipelines(
        ctx,
        "1/201/",
        &[
            (
                JobState::Running,
                previous_completion - Duration::seconds(5),
            ),
            (JobState::Succeeded, previous_completion),
            (JobState::Running, Utc::now()),
        ],
    )
    .await;

    let service = build_service(ctx);
    let response = service
        .get_status(
            &load_ontology(),
            &TraversalPath::new_unchecked("1/201/"),
            ResponseFormat::Raw as i32,
            &admin_context(),
        )
        .await
        .expect("should succeed");
    let status = extract_structured(response);

    let sdlc = status
        .sdlc_indexing
        .expect("sdlc_indexing should be present");
    assert_eq!(sdlc.state, IndexingState::Indexing as i32);
}

async fn indexing_status_error_state(ctx: &TestContext) {
    seed_all_pipelines(ctx, "1/202/", &failed_run()).await;

    let service = build_service(ctx);
    let response = service
        .get_status(
            &load_ontology(),
            &TraversalPath::new_unchecked("1/202/"),
            ResponseFormat::Raw as i32,
            &admin_context(),
        )
        .await
        .expect("should succeed");
    let status = extract_structured(response);

    let sdlc = status
        .sdlc_indexing
        .expect("sdlc_indexing should be present");
    assert_eq!(sdlc.state, IndexingState::Error as i32);
    assert_eq!(
        sdlc.last_error.as_deref(),
        Some("Something went wrong during indexing.")
    );
}

async fn indexing_status_not_indexed_when_no_runs(ctx: &TestContext) {
    let service = build_service(ctx);
    let response = service
        .get_status(
            &load_ontology(),
            &TraversalPath::new_unchecked("1/101/"),
            ResponseFormat::Raw as i32,
            &admin_context(),
        )
        .await
        .expect("should succeed");
    let status = extract_structured(response);

    let indexing = status.indexing.expect("indexing should be present");
    assert_eq!(indexing.state, IndexingState::NotIndexed as i32);
    assert!(indexing.last_started_at.is_none());
}

async fn indexing_status_unknown_when_job_table_missing(ctx: &TestContext) {
    let mut empty_database = ctx.config.clone();
    empty_database.database = "default".to_string();
    let service = GraphStatusService::new(Arc::new(empty_database.build_client()));
    let response = service
        .get_status(
            &load_ontology(),
            &TraversalPath::new_unchecked("1/100/"),
            ResponseFormat::Raw as i32,
            &admin_context(),
        )
        .await
        .expect("should succeed");
    let status = extract_structured(response);

    let indexing = status.indexing.expect("indexing should be present");
    assert_eq!(indexing.state, IndexingState::Unknown as i32);
}

async fn indexing_status_per_entity_missing_run_treated_as_not_indexed(ctx: &TestContext) {
    seed_pipeline(ctx, "1/204/", "MergeRequest", &completed_run()).await;

    let service = build_service(ctx);
    let response = service
        .get_status(
            &load_ontology(),
            &TraversalPath::new_unchecked("1/204/"),
            ResponseFormat::Raw as i32,
            &admin_context(),
        )
        .await
        .expect("should succeed");
    let status = extract_structured(response);

    let indexing = status.indexing.expect("indexing should be present");
    assert_eq!(indexing.state, IndexingState::NotIndexed as i32);
}

async fn indexing_status_per_entity_worst_state_wins(ctx: &TestContext) {
    for name in namespaced_pipeline_names() {
        let steps = if name == "WorkItem" {
            failed_run()
        } else {
            completed_run()
        };
        seed_pipeline(ctx, "1/203/", &name, &steps).await;
    }

    let service = build_service(ctx);
    let response = service
        .get_status(
            &load_ontology(),
            &TraversalPath::new_unchecked("1/203/"),
            ResponseFormat::Raw as i32,
            &admin_context(),
        )
        .await
        .expect("should succeed");
    let status = extract_structured(response);

    let sdlc = status
        .sdlc_indexing
        .expect("sdlc_indexing should be present");
    assert_eq!(sdlc.state, IndexingState::Error as i32);
    assert_eq!(
        sdlc.last_error.as_deref(),
        Some("Something went wrong during indexing.")
    );
}

async fn reporter_excludes_security_entity_counts(ctx: &TestContext) {
    let service = build_service(ctx);
    let reporter_context =
        SecurityContext::new_with_roles(1, vec![AuthorizedPath::new("1/", 20)]).unwrap();

    let response = service
        .get_status(
            &load_ontology(),
            &TraversalPath::new_unchecked("1/"),
            ResponseFormat::Raw as i32,
            &reporter_context,
        )
        .await
        .expect("should succeed");
    let status = extract_structured(response);

    let security = status.domains.iter().find(|d| d.name == "security");
    assert!(
        security.is_none(),
        "Reporter should not see security domain at all"
    );

    let core = find_domain(&status.domains, "core");
    assert!(
        find_item(core, "Project") > 0,
        "Reporter should still see project counts"
    );
}

async fn security_manager_includes_security_entity_counts(ctx: &TestContext) {
    let service = build_service(ctx);
    let sm_context =
        SecurityContext::new_with_roles(1, vec![AuthorizedPath::new("1/", 25)]).unwrap();

    let response = service
        .get_status(
            &load_ontology(),
            &TraversalPath::new_unchecked("1/"),
            ResponseFormat::Raw as i32,
            &sm_context,
        )
        .await
        .expect("should succeed");
    let status = extract_structured(response);

    let security = find_domain(&status.domains, "security");
    assert_eq!(
        find_item(security, "Vulnerability"),
        1,
        "SecurityManager should see vulnerability counts"
    );
}

fn namespaced_pipeline_names() -> Vec<String> {
    load_ontology()
        .pipeline_descriptors()
        .into_iter()
        .filter(|descriptor| descriptor.scope == ontology::EtlScope::Namespaced)
        .map(|descriptor| descriptor.name)
        .collect()
}

async fn code_not_indexed_dominates_when_no_project_checkpointed(ctx: &TestContext) {
    seed_all_pipelines(ctx, "2/300/3002/", &completed_run()).await;

    let service = build_service(ctx);
    let response = service
        .get_status(
            &load_ontology(),
            &TraversalPath::new_unchecked("2/300/3002/"),
            ResponseFormat::Raw as i32,
            &admin_context(),
        )
        .await
        .expect("should succeed");
    let status = extract_structured(response);

    let sdlc = status
        .sdlc_indexing
        .expect("sdlc_indexing should be present");
    assert_eq!(sdlc.state, IndexingState::Indexed as i32);

    let code = status
        .code_indexing
        .expect("code_indexing should be present");
    assert_eq!(
        code.state,
        IndexingState::NotIndexed as i32,
        "project 3002 has no checkpoint"
    );

    let indexing = status.indexing.expect("indexing should be present");
    assert_eq!(
        indexing.state,
        IndexingState::NotIndexed as i32,
        "an un-code-indexed scope must not report plain indexed"
    );
}

async fn code_indexing_omitted_when_no_projects_known(ctx: &TestContext) {
    seed_all_pipelines(ctx, "999/1/", &completed_run()).await;

    let service = build_service(ctx);
    let response = service
        .get_status(
            &load_ontology(),
            &TraversalPath::new_unchecked("999/1/"),
            ResponseFormat::Raw as i32,
            &admin_context(),
        )
        .await
        .expect("should succeed");
    let status = extract_structured(response);

    assert!(
        status.code_indexing.is_none(),
        "a scope with no known projects has no code coverage to claim"
    );
    let indexing = status.indexing.expect("indexing should be present");
    assert_eq!(indexing.state, IndexingState::Indexed as i32);
}

async fn edge_pipeline_error_surfaces_in_sdlc_state(ctx: &TestContext) {
    seed_all_pipelines(ctx, "1/205/", &completed_run()).await;
    seed_pipeline(ctx, "1/205/", "MEMBER_OF_siphon_members", &failed_run()).await;

    let service = build_service(ctx);
    let response = service
        .get_status(
            &load_ontology(),
            &TraversalPath::new_unchecked("1/205/"),
            ResponseFormat::Raw as i32,
            &admin_context(),
        )
        .await
        .expect("should succeed");
    let status = extract_structured(response);

    let sdlc = status
        .sdlc_indexing
        .expect("sdlc_indexing should be present");
    assert_eq!(sdlc.state, IndexingState::Error as i32);
    assert_eq!(
        sdlc.last_error.as_deref(),
        Some("Something went wrong during indexing.")
    );
}

async fn items_carry_per_entity_state(ctx: &TestContext) {
    let service = build_service(ctx);
    let response = service
        .get_status(
            &load_ontology(),
            &TraversalPath::new_unchecked("1/100/"),
            ResponseFormat::Raw as i32,
            &admin_context(),
        )
        .await
        .expect("should succeed");
    let status = extract_structured(response);

    let find_state = |domain: &str, item: &str| {
        find_domain(&status.domains, domain)
            .items
            .iter()
            .find(|i| i.name == item)
            .unwrap_or_else(|| panic!("item {item} not found"))
            .state
    };

    assert_eq!(
        find_state("code_review", "MergeRequest"),
        Some(IndexingState::Indexed as i32)
    );
    assert_eq!(
        find_state("source_code", "Definition"),
        Some(IndexingState::Backfilling as i32),
        "code entities carry the code coverage state (1 of 2 projects under 1/100/)"
    );
}

async fn indexing_status_reports_last_run_rows(ctx: &TestContext) {
    let ledger = JobLedger::new(Arc::new(ctx.create_client()));
    let [(_, started), (_, completed)] = completed_run();
    let plans = namespaced_pipeline_names();
    join_all(plans.iter().map(|plan| async {
        record_run(
            &ledger,
            "1/206/",
            plan,
            JobState::Running,
            started,
            started,
            (0, 0),
        )
        .await;
        record_run(
            &ledger,
            "1/206/",
            plan,
            JobState::Succeeded,
            started,
            completed,
            (307, 465),
        )
        .await;
    }))
    .await;
    ctx.flush_async_inserts().await;

    let service = build_service(ctx);
    let response = service
        .get_status(
            &load_ontology(),
            &TraversalPath::new_unchecked("1/206/"),
            ResponseFormat::Raw as i32,
            &admin_context(),
        )
        .await
        .expect("should succeed");
    let status = extract_structured(response);

    let sdlc = status
        .sdlc_indexing
        .expect("sdlc_indexing should be present");
    assert_eq!(sdlc.last_rows_read, Some(307));
    assert_eq!(sdlc.last_rows_written, Some(465));
}

async fn toon_renders_split_indexing_blocks(ctx: &TestContext) {
    let service = build_service(ctx);
    let response = service
        .get_status(
            &load_ontology(),
            &TraversalPath::new_unchecked("1/100/"),
            ResponseFormat::Llm as i32,
            &admin_context(),
        )
        .await
        .expect("should succeed");
    let text = match response.content {
        Some(get_graph_status_response::Content::FormattedText(t)) => t,
        _ => panic!("Expected formatted text response"),
    };

    assert!(text.contains("sdlc_indexing"), "TOON output: {text}");
    assert!(text.contains("code_indexing"), "TOON output: {text}");
    assert!(
        text.contains("backfilling"),
        "code coverage 1/2 renders as backfilling: {text}"
    );
}

async fn definition_count_counts_distinct_ids(ctx: &TestContext) {
    let db = ctx.fork("graph_status_definition_distinct_ids").await;

    db.execute(&format!(
        "INSERT INTO {} (id, traversal_path, project_id, branch, commit_sha, file_path, fqn, name, definition_type, start_line, end_line, start_byte, end_byte, start_char, end_char, _version, _deleted) VALUES
         (9001, '1/100/1000/', 1000, 'main', 'sha-a', 'a.rb', 'A#m', 'm', 'Method', 1, 2, 0, 10, 0, 10, '2024-01-01 00:00:00', false),
         (9002, '1/100/1000/', 1000, 'main', 'sha-c', 'b.rb', 'B#m', 'm', 'Method', 1, 2, 0, 10, 0, 10, '2024-01-01 00:00:00', false),
         (9002, '1/100/1000/', 1000, 'main', 'sha-c', 'b.rb', 'B#m', 'm', 'Method', 1, 2, 0, 10, 0, 10, '2024-06-01 00:00:00', false)",
        t("gl_definition")
    ))
    .await;
    db.optimize_all().await;

    let service = build_service(&db);
    let response = service
        .get_status(
            &load_ontology(),
            &TraversalPath::new_unchecked("1/100/1000/"),
            ResponseFormat::Raw as i32,
            &admin_context(),
        )
        .await
        .expect("should succeed");
    let status = extract_structured(response);
    let source_code = find_domain(&status.domains, "source_code");
    assert_eq!(
        find_item(source_code, "Definition"),
        2,
        "two distinct ids count as two; 9002's duplicate version is deduped by uniq(id)"
    );
}

async fn group_count_excludes_deleted(ctx: &TestContext) {
    let db = ctx.fork("graph_status_group_excludes_deleted").await;

    db.execute(&format!(
        "INSERT INTO {} (id, name, visibility_level, traversal_path, _version, _deleted) VALUES
         (9100, 'Live Group', 'public', '1/900/9100/', '2024-01-01 00:00:00', false),
         (9101, 'Deleted Group', 'public', '1/900/9101/', '2024-06-01 00:00:00', true)",
        t("gl_group")
    ))
    .await;
    db.optimize_all().await;

    let service = build_service(&db);
    let response = service
        .get_status(
            &load_ontology(),
            &TraversalPath::new_unchecked("1/900/"),
            ResponseFormat::Raw as i32,
            &admin_context(),
        )
        .await
        .expect("should succeed");
    let status = extract_structured(response);
    let core = find_domain(&status.domains, "core");
    assert_eq!(find_item(core, "Group"), 1, "tombstoned group is excluded");
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

    let service = build_service(&db);
    let response = service
        .get_status(
            &load_ontology(),
            &TraversalPath::new_unchecked("1/"),
            ResponseFormat::Raw as i32,
            &admin_context(),
        )
        .await
        .expect("should succeed");
    let projects = extract_structured(response)
        .projects
        .expect("projects should be present");
    assert_eq!(
        projects.total_known, 4,
        "duplicate-version project rows count as one distinct id"
    );
}

async fn get_status_degrades_when_entity_count_table_missing(ctx: &TestContext) {
    let db = ctx.fork("graph_status_degrade_missing_table").await;
    db.execute(&format!("DROP TABLE {}", t("gl_merge_request")))
        .await;

    db.execute("TRUNCATE TABLE job").await;
    seed_all_pipelines(&db, "1/100/", &completed_run()).await;
    let service = build_service(&db);

    let response = service
        .get_status(
            &load_ontology(),
            &TraversalPath::new_unchecked("1/"),
            ResponseFormat::Raw as i32,
            &admin_context(),
        )
        .await
        .expect("a failed entity-count branch must not fail the whole request");
    let status = extract_structured(response);

    let projects = status.projects.expect("projects should be present");
    assert_eq!(projects.total_known, 3);

    let sdlc = status
        .sdlc_indexing
        .expect("sdlc_indexing should be present");
    assert_eq!(sdlc.state, IndexingState::Indexed as i32);

    let core = find_domain(&status.domains, "core");
    assert_eq!(
        find_item(core, "Project"),
        0,
        "entity counts degrade to empty when their query fails"
    );
}
