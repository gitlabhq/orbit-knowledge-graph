use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use nats_client::error::NatsError;
use nats_client::kv_types::{KvEntry, KvPutOptions, KvPutResult};
use orbit_utils::traversal_path::TraversalPath;
use query_engine::compiler::{AuthorizedPath, SecurityContext};

use crate::common::{GRAPH_SCHEMA_SQL, TestContext};
use indexer::indexing_status::{IndexingStatusStore, InitialBackfill, InitialBackfillState};
use integration_testkit::{load_ontology, run_subtests_shared, t};
use nats_client::testkit::MockKvServices;
use orbit_server::graph_status::GraphStatusService;
use orbit_server::proto::{
    BackfillState, BackfillStatus, GetGraphStatusResponse, ResponseFormat, StructuredGraphStatus,
    get_graph_status_response,
};

mod backfill;

fn admin_context() -> SecurityContext {
    SecurityContext::new_with_roles(1, vec![AuthorizedPath::new("1/", 50)])
        .unwrap()
        .with_role(true, Some(50))
}

struct FailingKvServices;

#[async_trait]
impl nats_client::KvServices for FailingKvServices {
    async fn kv_get(&self, bucket: &str, key: &str) -> Result<Option<KvEntry>, NatsError> {
        Err(NatsError::KvGet {
            bucket: bucket.to_string(),
            key: key.to_string(),
            message: "connection refused".to_string(),
        })
    }

    async fn kv_put(
        &self,
        bucket: &str,
        key: &str,
        _value: Bytes,
        _options: KvPutOptions,
    ) -> Result<KvPutResult, NatsError> {
        Err(NatsError::KvPut {
            bucket: bucket.to_string(),
            key: key.to_string(),
            message: "connection refused".to_string(),
        })
    }

    async fn kv_delete(&self, bucket: &str, key: &str) -> Result<(), NatsError> {
        Err(NatsError::KvDelete {
            bucket: bucket.to_string(),
            key: key.to_string(),
            message: "connection refused".to_string(),
        })
    }

    async fn kv_keys(&self, bucket: &str) -> Result<Vec<String>, NatsError> {
        Err(NatsError::KvKeys {
            bucket: bucket.to_string(),
            message: "connection refused".to_string(),
        })
    }
}

struct KvFailingOnKey {
    inner: MockKvServices,
    fail_key: String,
}

#[async_trait]
impl nats_client::KvServices for KvFailingOnKey {
    async fn kv_get(&self, bucket: &str, key: &str) -> Result<Option<KvEntry>, NatsError> {
        if key == self.fail_key {
            return Err(NatsError::KvGet {
                bucket: bucket.to_string(),
                key: key.to_string(),
                message: "connection refused".to_string(),
            });
        }
        self.inner.kv_get(bucket, key).await
    }

    async fn kv_put(
        &self,
        bucket: &str,
        key: &str,
        value: Bytes,
        options: KvPutOptions,
    ) -> Result<KvPutResult, NatsError> {
        self.inner.kv_put(bucket, key, value, options).await
    }

    async fn kv_delete(&self, bucket: &str, key: &str) -> Result<(), NatsError> {
        self.inner.kv_delete(bucket, key).await
    }

    async fn kv_keys(&self, bucket: &str) -> Result<Vec<String>, NatsError> {
        self.inner.kv_keys(bucket).await
    }
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
        "INSERT INTO {} (traversal_path, project_id, branch, last_task_id, indexed_at) VALUES
         ('1/100/1000/', 1000, 'main', 1, now()),
         ('1/101/1001/', 1001, 'main', 2, now()),
         ('1/100/1999/', 1999, 'main', 3, now())",
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

    ctx.optimize_all().await;
}

fn build_service(ctx: &TestContext) -> GraphStatusService {
    let client = Arc::new(ctx.create_client());
    GraphStatusService::new(client)
}

fn build_service_with_indexing_status(
    ctx: &TestContext,
    mock_kv: MockKvServices,
) -> GraphStatusService {
    let client = Arc::new(ctx.create_client());
    let store = IndexingStatusStore::new(Arc::new(mock_kv));
    GraphStatusService::new(client).with_indexing_status(store)
}

fn extract_structured(response: GetGraphStatusResponse) -> StructuredGraphStatus {
    match response.content {
        Some(get_graph_status_response::Content::Structured(s)) => s,
        _ => panic!("Expected structured response"),
    }
}

async fn backfill_status(service: &GraphStatusService, path: &str) -> BackfillStatus {
    extract_structured(
        service
            .get_status(
                &load_ontology(),
                &TraversalPath::new_unchecked(path),
                ResponseFormat::Raw as i32,
                &admin_context(),
            )
            .await
            .unwrap(),
    )
    .backfill
    .unwrap()
}

async fn unavailable_status_preserves_inventory(ctx: &TestContext) {
    let services = [
        build_service(ctx),
        build_service(ctx)
            .with_indexing_status(IndexingStatusStore::new(Arc::new(FailingKvServices))),
        build_service(ctx).with_indexing_status(IndexingStatusStore::new(Arc::new(
            KvFailingOnKey {
                inner: MockKvServices::new(),
                fail_key: "backfill.100".into(),
            },
        ))),
    ];
    for service in services {
        let response = service
            .get_status(
                &load_ontology(),
                &TraversalPath::new_unchecked("1/100/"),
                ResponseFormat::Raw as i32,
                &admin_context(),
            )
            .await
            .unwrap();
        let status = extract_structured(response);
        assert_eq!(status.projects.unwrap().total_known, 2);
        let backfill = status.backfill.unwrap();
        assert_eq!(backfill.state(), BackfillState::Unknown);
        assert_eq!(backfill.last_progress_at, None);
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
    let ctx = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;
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
        unavailable_status_preserves_inventory,
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

    let mock_kv = MockKvServices::new();
    IndexingStatusStore::new(Arc::new(mock_kv.clone()))
        .put_initial_backfill(
            &TraversalPath::new_unchecked("1/100/"),
            &InitialBackfill {
                state: InitialBackfillState::Running,
                completed_pipelines: 0,
                total_pipelines: 1,
                completed_projects: 0,
                last_progress_at: None,
            },
        )
        .await
        .unwrap();
    let service = build_service_with_indexing_status(&db, mock_kv);

    let response = service
        .get_status(
            &load_ontology(),
            &TraversalPath::new_unchecked("1/100/1000/"),
            ResponseFormat::Raw as i32,
            &admin_context(),
        )
        .await
        .expect("a failed entity-count branch must not fail the whole request");
    let status = extract_structured(response);

    let projects = status.projects.expect("projects should be present");
    assert_eq!(projects.total_known, 1);
    assert_eq!(status.backfill.unwrap().state(), BackfillState::Running);

    let core = find_domain(&status.domains, "core");
    assert_eq!(
        find_item(core, "Project"),
        0,
        "entity counts degrade to empty when their query fails"
    );
}
