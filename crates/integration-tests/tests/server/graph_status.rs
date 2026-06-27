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

fn dotted_traversal(traversal_path: &str) -> String {
    traversal_path
        .split('/')
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join(".")
}

fn seed_indexing_progress(
    mock_kv: &MockKvServices,
    traversal_path: &str,
    progress: &IndexingProgress,
) {
    let key = format!("status.{}", dotted_traversal(traversal_path));
    let payload = serde_json::to_vec(progress).expect("serialize progress");
    mock_kv.set(INDEXING_PROGRESS_BUCKET, &key, Bytes::from(payload));
}

fn seed_entity_progress(
    mock_kv: &MockKvServices,
    traversal_path: &str,
    entity_kind: &str,
    progress: &IndexingProgress,
) {
    let key = format!("status.{}.{entity_kind}", dotted_traversal(traversal_path));
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
        indexing_status_per_entity_worst_state_wins,
        indexing_status_per_entity_missing_key_treated_as_not_indexed,
        indexing_status_falls_back_to_legacy_key_during_rollout,
        indexing_status_survives_single_entity_read_failure,
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
        .get_status("1/", ResponseFormat::Raw as i32, &admin_context())
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
        .get_status("1/100/", ResponseFormat::Raw as i32, &admin_context())
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
    assert_eq!(
        indexing.last_error.as_deref(),
        Some("Something went wrong during indexing.")
    );
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

async fn indexing_status_per_entity_worst_state_wins(ctx: &TestContext) {
    let mock_kv = MockKvServices::new();
    let started = Utc::now() - Duration::seconds(30);
    let completed = started + Duration::seconds(5);
    let indexed = IndexingProgress {
        last_started_at: started,
        last_completed_at: Some(completed),
        last_duration_ms: Some(5000),
        last_error: None,
    };
    let errored = IndexingProgress {
        last_started_at: started,
        last_completed_at: Some(completed),
        last_duration_ms: Some(5000),
        last_error: Some("scan failure".to_string()),
    };

    let ontology = load_ontology();
    for node in ontology.nodes() {
        let Some(etl) = node.etl.as_ref() else {
            continue;
        };
        if etl.scope() != ontology::EtlScope::Namespaced {
            continue;
        }
        let progress = if node.name == "WorkItem" {
            &errored
        } else {
            &indexed
        };
        seed_entity_progress(&mock_kv, "1/100/", &node.name, progress);
    }

    let service = build_service_with_indexing_status(ctx, mock_kv);
    let response = service
        .get_status("1/100/", ResponseFormat::Raw as i32, &admin_context())
        .await
        .expect("should succeed");
    let status = extract_structured(response);

    let indexing = status.indexing.expect("indexing should be present");
    assert_eq!(indexing.state, IndexingState::Error as i32);
    assert_eq!(
        indexing.last_error.as_deref(),
        Some("Something went wrong during indexing.")
    );
}

async fn indexing_status_per_entity_missing_key_treated_as_not_indexed(ctx: &TestContext) {
    let mock_kv = MockKvServices::new();
    let progress = IndexingProgress {
        last_started_at: Utc::now() - Duration::seconds(30),
        last_completed_at: Some(Utc::now() - Duration::seconds(25)),
        last_duration_ms: Some(5000),
        last_error: None,
    };
    seed_entity_progress(&mock_kv, "1/100/", "MergeRequest", &progress);

    let service = build_service_with_indexing_status(ctx, mock_kv);
    let response = service
        .get_status("1/100/", ResponseFormat::Raw as i32, &admin_context())
        .await
        .expect("should succeed");
    let status = extract_structured(response);

    let indexing = status.indexing.expect("indexing should be present");
    assert_eq!(indexing.state, IndexingState::NotIndexed as i32);
}

async fn indexing_status_falls_back_to_legacy_key_during_rollout(ctx: &TestContext) {
    let mock_kv = MockKvServices::new();
    let started = Utc::now() - Duration::seconds(30);
    let completed = started + Duration::seconds(5);
    let legacy = IndexingProgress {
        last_started_at: started,
        last_completed_at: Some(completed),
        last_duration_ms: Some(5000),
        last_error: None,
    };
    seed_indexing_progress(&mock_kv, "1/100/", &legacy);

    let service = build_service_with_indexing_status(ctx, mock_kv);
    let response = service
        .get_status("1/100/", ResponseFormat::Raw as i32, &admin_context())
        .await
        .expect("should succeed");
    let status = extract_structured(response);

    let indexing = status.indexing.expect("indexing should be present");
    assert_eq!(indexing.state, IndexingState::Indexed as i32);
    assert_eq!(indexing.last_duration_ms, Some(5000));
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

fn seed_namespaced_entities(
    mock_kv: &MockKvServices,
    traversal_path: &str,
    progress: &IndexingProgress,
) {
    let ontology = load_ontology();
    for node in ontology.nodes() {
        let Some(etl) = node.etl.as_ref() else {
            continue;
        };
        if etl.scope() != ontology::EtlScope::Namespaced {
            continue;
        }
        seed_entity_progress(mock_kv, traversal_path, &node.name, progress);
    }
}

async fn indexing_status_survives_single_entity_read_failure(ctx: &TestContext) {
    let mock_kv = MockKvServices::new();
    let started = Utc::now() - Duration::seconds(30);
    let indexed = IndexingProgress {
        last_started_at: started,
        last_completed_at: Some(started + Duration::seconds(5)),
        last_duration_ms: Some(5000),
        last_error: None,
    };
    seed_namespaced_entities(&mock_kv, "1/100/", &indexed);

    let store = IndexingStatusStore::new(Arc::new(KvFailingOnKey {
        inner: mock_kv,
        fail_key: format!("status.1.100.{}", "MergeRequest"),
    }));
    let client = Arc::new(ctx.create_client());
    let ontology = Arc::new(load_ontology());
    let service = GraphStatusService::new(client, ontology).with_indexing_status(store);

    let response = service
        .get_status("1/100/", ResponseFormat::Raw as i32, &admin_context())
        .await
        .expect("should succeed");
    let status = extract_structured(response);

    let indexing = status.indexing.expect("indexing should be present");
    assert_ne!(indexing.state, IndexingState::Unknown as i32);
    assert_eq!(indexing.state, IndexingState::Indexed as i32);
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
        .get_status("1/100/1000/", ResponseFormat::Raw as i32, &admin_context())
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
        .get_status("1/900/", ResponseFormat::Raw as i32, &admin_context())
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
        .get_status("1/", ResponseFormat::Raw as i32, &admin_context())
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
    let started = Utc::now() - Duration::seconds(30);
    seed_indexing_progress(
        &mock_kv,
        "1/",
        &IndexingProgress {
            last_started_at: started,
            last_completed_at: Some(started + Duration::seconds(5)),
            last_duration_ms: Some(5000),
            last_error: None,
        },
    );
    let service = build_service_with_indexing_status(&db, mock_kv);

    let response = service
        .get_status("1/", ResponseFormat::Raw as i32, &admin_context())
        .await
        .expect("a failed entity-count branch must not fail the whole request");
    let status = extract_structured(response);

    let projects = status.projects.expect("projects should be present");
    assert_eq!(projects.total_known, 3);

    let indexing = status.indexing.expect("indexing should be present");
    assert_eq!(indexing.state, IndexingState::Indexed as i32);

    let core = find_domain(&status.domains, "core");
    assert_eq!(
        find_item(core, "Project"),
        0,
        "entity counts degrade to empty when their query fails"
    );
}
