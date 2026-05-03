use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{Duration, Utc};
use nats_client::error::NatsError;
use nats_client::kv_types::{KvEntry, KvPutOptions, KvPutResult};
use query_engine::compiler::{SecurityContext, TraversalPath};

use crate::common::{GRAPH_SCHEMA_SQL, TestContext};
use gkg_server::graph_status::GraphStatusService;
use gkg_server::proto::{
    GetGraphStatusResponse, IndexingState, ResponseFormat, StructuredGraphStatus,
    get_graph_status_response,
};
use indexer::indexing_status::{INDEXING_PROGRESS_BUCKET, IndexingProgress, IndexingStatusStore};
use integration_testkit::{load_ontology, run_subtests_shared, t};
use nats_client::testkit::MockKvServices;

fn admin_context() -> SecurityContext {
    SecurityContext::new_with_roles(1, vec![TraversalPath::new("1/", 50)])
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

    ctx.optimize_all().await;
}

fn build_service(ctx: &TestContext) -> GraphStatusService {
    let client = Arc::new(ctx.create_client());
    let ontology = Arc::new(load_ontology());
    GraphStatusService::new(client, ontology)
}

fn build_service_with_indexing_status(
    ctx: &TestContext,
    mock_kv: MockKvServices,
) -> GraphStatusService {
    let client = Arc::new(ctx.create_client());
    let ontology = Arc::new(load_ontology());
    let store = IndexingStatusStore::new(Arc::new(mock_kv));
    GraphStatusService::new(client, ontology).with_indexing_status(store)
}

fn seed_indexing_progress(
    mock_kv: &MockKvServices,
    traversal_path: &str,
    progress: &IndexingProgress,
) {
    let key = traversal_path
        .split('/')
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join(".");
    let key = format!("status.{key}");
    let payload = serde_json::to_vec(progress).expect("serialize progress");
    mock_kv.set(INDEXING_PROGRESS_BUCKET, &key, Bytes::from(payload));
}

fn extract_structured(response: GetGraphStatusResponse) -> StructuredGraphStatus {
    match response.content {
        Some(get_graph_status_response::Content::Structured(s)) => s,
        _ => panic!("Expected structured response"),
    }
}

fn find_domain<'a>(
    domains: &'a [gkg_server::proto::GraphStatusDomain],
    name: &str,
) -> &'a gkg_server::proto::GraphStatusDomain {
    domains
        .iter()
        .find(|d| d.name == name)
        .unwrap_or_else(|| panic!("domain '{name}' not found"))
}

fn find_item(domain: &gkg_server::proto::GraphStatusDomain, name: &str) -> i64 {
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
        indexing_status_absent_without_store,
        indexing_status_indexed_for_group,
        indexing_status_backfilling_for_project,
        indexing_status_not_indexed_when_no_kv_entry,
        indexing_status_indexing_when_reindex_in_flight,
        indexing_status_error_state,
        indexing_status_unknown_when_nats_unreachable,
        reporter_excludes_security_entity_counts,
        security_manager_includes_security_entity_counts,
    );
}

async fn root_traversal_path_returns_all_entity_counts(ctx: &TestContext) {
    let service = build_service(ctx);
    let response = service
        .get_status("1/", ResponseFormat::Raw as i32, &admin_context())
        .await
        .expect("should succeed");
    let status = extract_structured(response);

    let core = find_domain(&status.domains, "core");
    assert_eq!(find_item(core, "Project"), 3);
    assert_eq!(find_item(core, "Group"), 2);

    let code = find_domain(&status.domains, "code_review");
    assert_eq!(find_item(code, "MergeRequest"), 2);
}

async fn scoped_by_traversal_path_filters_counts(ctx: &TestContext) {
    let service = build_service(ctx);

    let response = service
        .get_status("1/100/", ResponseFormat::Raw as i32, &admin_context())
        .await
        .expect("should succeed");
    let status = extract_structured(response);

    let core = find_domain(&status.domains, "core");
    assert_eq!(find_item(core, "Project"), 2, "projects under 1/100/");
    assert_eq!(find_item(core, "Group"), 1, "groups under 1/100/");

    let code = find_domain(&status.domains, "code_review");
    assert_eq!(find_item(code, "MergeRequest"), 1, "MRs under 1/100/");
}

async fn empty_traversal_path_rejected(ctx: &TestContext) {
    let service = build_service(ctx);

    let result = service
        .get_status("", ResponseFormat::Raw as i32, &admin_context())
        .await;

    assert!(result.is_err());
    let status = result.unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

async fn non_matching_traversal_path_returns_zeros(ctx: &TestContext) {
    let service = build_service(ctx);

    let response = service
        .get_status("999/", ResponseFormat::Raw as i32, &admin_context())
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
        .get_status("1/", ResponseFormat::Raw as i32, &admin_context())
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
        .get_status("1/", ResponseFormat::Raw as i32, &admin_context())
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
        .get_status("1/100/", ResponseFormat::Raw as i32, &admin_context())
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

async fn indexing_status_absent_without_store(ctx: &TestContext) {
    let service = build_service(ctx);
    let response = service
        .get_status("1/", ResponseFormat::Raw as i32, &admin_context())
        .await
        .expect("should succeed");
    let status = extract_structured(response);

    assert!(
        status.indexing.is_none(),
        "indexing field should be absent when no store is configured"
    );
}

async fn indexing_status_indexed_for_group(ctx: &TestContext) {
    let mock_kv = MockKvServices::new();
    let started = Utc::now() - Duration::seconds(30);
    let completed = Utc::now() - Duration::seconds(25);
    seed_indexing_progress(
        &mock_kv,
        "1/100/",
        &IndexingProgress {
            last_started_at: started,
            last_completed_at: Some(completed),
            last_duration_ms: Some(5000),
            last_error: None,
        },
    );

    let service = build_service_with_indexing_status(ctx, mock_kv);
    let response = service
        .get_status("1/100/", ResponseFormat::Raw as i32, &admin_context())
        .await
        .expect("should succeed");
    let status = extract_structured(response);

    let indexing = status.indexing.expect("indexing should be present");
    assert_eq!(indexing.state, IndexingState::Indexed as i32);
    assert!(indexing.last_started_at.is_some());
    assert!(indexing.last_completed_at.is_some());
    assert_eq!(indexing.last_duration_ms, Some(5000));
    assert!(indexing.last_error.is_none());
}

async fn indexing_status_backfilling_for_project(ctx: &TestContext) {
    let mock_kv = MockKvServices::new();
    seed_indexing_progress(
        &mock_kv,
        "1/100/1000/",
        &IndexingProgress {
            last_started_at: Utc::now(),
            last_completed_at: None,
            last_duration_ms: None,
            last_error: None,
        },
    );

    let service = build_service_with_indexing_status(ctx, mock_kv);
    let response = service
        .get_status("1/100/1000/", ResponseFormat::Raw as i32, &admin_context())
        .await
        .expect("should succeed");
    let status = extract_structured(response);

    let indexing = status.indexing.expect("indexing should be present");
    assert_eq!(indexing.state, IndexingState::Backfilling as i32);
    assert!(indexing.last_started_at.is_some());
    assert!(indexing.last_completed_at.is_none());
}

async fn indexing_status_indexing_when_reindex_in_flight(ctx: &TestContext) {
    let mock_kv = MockKvServices::new();
    let previous_completion = Utc::now() - Duration::seconds(60);
    seed_indexing_progress(
        &mock_kv,
        "1/100/",
        &IndexingProgress {
            last_started_at: Utc::now(),
            last_completed_at: Some(previous_completion),
            last_duration_ms: Some(5000),
            last_error: None,
        },
    );

    let service = build_service_with_indexing_status(ctx, mock_kv);
    let response = service
        .get_status("1/100/", ResponseFormat::Raw as i32, &admin_context())
        .await
        .expect("should succeed");
    let status = extract_structured(response);

    let indexing = status.indexing.expect("indexing should be present");
    assert_eq!(indexing.state, IndexingState::Indexing as i32);
}

async fn indexing_status_not_indexed_when_no_kv_entry(ctx: &TestContext) {
    let mock_kv = MockKvServices::new();
    let service = build_service_with_indexing_status(ctx, mock_kv);
    let response = service
        .get_status("1/101/", ResponseFormat::Raw as i32, &admin_context())
        .await
        .expect("should succeed");
    let status = extract_structured(response);

    let indexing = status.indexing.expect("indexing should be present");
    assert_eq!(indexing.state, IndexingState::NotIndexed as i32);
    assert!(indexing.last_started_at.is_none());
}

async fn indexing_status_error_state(ctx: &TestContext) {
    let mock_kv = MockKvServices::new();
    let started = Utc::now() - Duration::seconds(10);
    seed_indexing_progress(
        &mock_kv,
        "1/100/",
        &IndexingProgress {
            last_started_at: started,
            last_completed_at: Some(started + Duration::seconds(2)),
            last_duration_ms: Some(2000),
            last_error: Some("deadline exceeded".to_string()),
        },
    );

    let service = build_service_with_indexing_status(ctx, mock_kv);
    let response = service
        .get_status("1/100/", ResponseFormat::Raw as i32, &admin_context())
        .await
        .expect("should succeed");
    let status = extract_structured(response);

    let indexing = status.indexing.expect("indexing should be present");
    assert_eq!(indexing.state, IndexingState::Error as i32);
    assert_eq!(indexing.last_error.as_deref(), Some("deadline exceeded"));
}

async fn indexing_status_unknown_when_nats_unreachable(ctx: &TestContext) {
    let store = IndexingStatusStore::new(Arc::new(FailingKvServices));
    let client = Arc::new(ctx.create_client());
    let ontology = Arc::new(load_ontology());
    let service = GraphStatusService::new(client, ontology).with_indexing_status(store);

    let response = service
        .get_status("1/100/", ResponseFormat::Raw as i32, &admin_context())
        .await
        .expect("should succeed");
    let status = extract_structured(response);

    let indexing = status.indexing.expect("indexing should be present");
    assert_eq!(indexing.state, IndexingState::Unknown as i32);
}

async fn reporter_excludes_security_entity_counts(ctx: &TestContext) {
    let service = build_service(ctx);
    let reporter_context =
        SecurityContext::new_with_roles(1, vec![TraversalPath::new("1/", 20)]).unwrap();

    let response = service
        .get_status("1/", ResponseFormat::Raw as i32, &reporter_context)
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
        SecurityContext::new_with_roles(1, vec![TraversalPath::new("1/", 25)]).unwrap();

    let response = service
        .get_status("1/", ResponseFormat::Raw as i32, &sm_context)
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
