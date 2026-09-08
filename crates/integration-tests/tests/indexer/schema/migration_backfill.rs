//! Integration test: migration-triggered code backfill.
//!
//! When a schema migration is in progress (`gkg_schema_version` has a
//! `migrating` row), the shared `CodeBackfill` active sweep must dispatch
//! code indexing tasks for **all** enabled namespaces, not only for
//! newly-enabled ones arriving via CDC events.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use clickhouse_client::{ClickHouseConfigurationExt, FromArrowColumn};
use indexer::config::{DispatcherConfig, DispatcherError};
use indexer::nats::versioning::NATS_VERSIONER;
use indexer::orchestrator::dispatch::CodeBackfill;
use indexer::orchestrator::scheduled::{
    MigrationCompletionChecker, ScheduledTask, ScheduledTaskMetrics,
};
use indexer::schema::version::{
    SCHEMA_VERSION, ensure_version_table, mark_version_active, mark_version_migrating,
    prefixed_table_name,
};
use indexer::topic::{CODE_INDEXING_TASK_SUBJECT_PATTERN, INDEXER_STREAM};
use nats_client::{KvPutOptions, NatsClient};
use ontology::archive::OntologyArchive;
use ontology::migrations::embedded_sources;
use orbit_migrations::catalog::OntologyCatalog;
use orbit_server_config::NatsConfiguration;
use serde::Deserialize;
use testcontainers::ImageExt;
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::nats::{Nats, NatsServerCmd};
use tokio_util::sync::CancellationToken;

use super::super::common;
use common::TestContext as ClickHouseContext;

#[derive(Deserialize)]
struct CodeIndexingRequest {
    task_id: i64,
    project_id: i64,
    #[serde(default)]
    campaign_id: Option<String>,
}

struct TestContext {
    clickhouse: ClickHouseContext,
    _nats: testcontainers::ContainerAsync<Nats>,
    nats_url: String,
}

impl TestContext {
    async fn new() -> Self {
        let clickhouse =
            ClickHouseContext::new(&[common::SIPHON_SCHEMA_SQL, *common::GRAPH_SCHEMA_SQL]).await;
        let (nats, nats_url) = Self::start_nats().await;
        Self::create_streams(&nats_url).await;
        Self {
            clickhouse,
            _nats: nats,
            nats_url,
        }
    }

    fn nats_config(&self) -> NatsConfiguration {
        NatsConfiguration {
            url: self.nats_url.clone(),
            ..orbit_server_config::AppConfig::embedded_defaults().nats
        }
    }

    async fn ontology_catalog(&self, client: Arc<NatsClient>) -> OntologyCatalog {
        OntologyCatalog::open(client, &self.clickhouse.config.database)
            .await
            .unwrap()
    }

    async fn given_enabled_namespaces(&self, namespace_ids: impl IntoIterator<Item = i64>) {
        for (i, ns_id) in namespace_ids.into_iter().enumerate() {
            self.clickhouse
                .execute(&format!(
                    "INSERT INTO siphon_knowledge_graph_enabled_namespaces \
                     (id, root_namespace_id, traversal_path, created_at, updated_at) \
                     VALUES ({}, {ns_id}, '1/{ns_id}/', now(), now())",
                    i + 1
                ))
                .await;
            self.clickhouse
                .execute(&format!(
                    "INSERT INTO namespace_traversal_paths (id, traversal_path) \
                     VALUES ({ns_id}, '1/{ns_id}/')"
                ))
                .await;
        }
    }

    async fn consume_code_indexing_requests(&self) -> Vec<CodeIndexingRequest> {
        use futures::StreamExt;

        let client = async_nats::connect(format!("nats://{}", self.nats_url))
            .await
            .unwrap();
        let jetstream = async_nats::jetstream::new(client);

        let consumer = jetstream
            .create_consumer_on_stream(
                async_nats::jetstream::consumer::pull::Config {
                    filter_subject: NATS_VERSIONER.subject(CODE_INDEXING_TASK_SUBJECT_PATTERN),
                    ..Default::default()
                },
                &NATS_VERSIONER.stream(INDEXER_STREAM),
            )
            .await
            .unwrap();

        let mut messages = consumer.fetch().max_messages(100).messages().await.unwrap();
        let mut results = Vec::new();

        while let Some(Ok(msg)) = messages.next().await {
            results.push(serde_json::from_slice(&msg.payload).unwrap());
            msg.ack().await.unwrap();
        }

        results
    }

    async fn start_nats() -> (testcontainers::ContainerAsync<Nats>, String) {
        let container = Nats::default()
            .with_cmd(&NatsServerCmd::default().with_jetstream())
            .with_tag("2.11-alpine")
            .with_mapped_port(0, ContainerPort::Tcp(4222))
            .with_ready_conditions(vec![WaitFor::seconds(3)])
            .start()
            .await
            .unwrap();

        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(4222).await.unwrap();

        (container, format!("{host}:{port}"))
    }

    async fn create_streams(url: &str) {
        let client = async_nats::connect(format!("nats://{url}")).await.unwrap();
        let jetstream = async_nats::jetstream::new(client);

        jetstream
            .create_stream(async_nats::jetstream::stream::Config {
                name: NATS_VERSIONER.stream(INDEXER_STREAM),
                subjects: vec![NATS_VERSIONER.subject(CODE_INDEXING_TASK_SUBJECT_PATTERN)],
                retention: async_nats::jetstream::stream::RetentionPolicy::WorkQueue,
                max_messages_per_subject: 1,
                discard: async_nats::jetstream::stream::DiscardPolicy::New,
                discard_new_per_subject: true,
                ..Default::default()
            })
            .await
            .unwrap();

        // Siphon stream consumed by the CDC path of the backfill dispatcher.
        jetstream
            .create_stream(async_nats::jetstream::stream::Config {
                name: "siphon_stream_main_db".into(),
                subjects: vec!["siphon_stream_main_db.>".into()],
                ..Default::default()
            })
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn migration_triggers_backfill_for_all_enabled_namespaces() {
    let context = TestContext::new().await;

    common::create_namespace(&context.clickhouse, 100, None, 20, "1/100/").await;
    common::create_namespace(&context.clickhouse, 200, None, 20, "1/200/").await;
    common::create_project(&context.clickhouse, 10, 100, 1, 20, "1/100/10/").await;
    common::create_project(&context.clickhouse, 20, 200, 1, 20, "1/200/20/").await;
    common::create_project(&context.clickhouse, 21, 200, 1, 20, "1/200/21/").await;
    context.given_enabled_namespaces([100, 200]).await;

    let graph = context.clickhouse.create_client();
    ensure_version_table(&graph).await.unwrap();
    mark_version_active(&graph, 0).await.unwrap();
    mark_version_migrating(&graph, 1).await.unwrap();

    let services = indexer::orchestrator::scheduled::connect(&context.nats_config())
        .await
        .unwrap();

    let campaign = std::sync::Arc::new(indexer::campaign::CampaignState::new());
    campaign.set(indexer::campaign::campaign_id_for_version(1));

    let backfill = CodeBackfill::new(
        services.nats.clone(),
        context.clickhouse.create_client(),
        context.clickhouse.config.build_client(),
        ScheduledTaskMetrics::new(),
        campaign,
        orbit_server_config::AppConfig::embedded_defaults()
            .schedule
            .tasks
            .code_backfill
            .publish_window,
    );

    backfill
        .dispatch_enabled(uuid::Uuid::new_v4())
        .await
        .unwrap();

    let requests = context.consume_code_indexing_requests().await;
    let project_ids: HashSet<i64> = requests.iter().map(|r| r.project_id).collect();

    assert_eq!(
        project_ids,
        HashSet::from([10, 20, 21]),
        "expected backfill for all projects in enabled namespaces"
    );

    assert!(
        requests.iter().all(|r| r.task_id == 0),
        "migration backfill requests should use task_id=0"
    );

    assert!(
        requests
            .iter()
            .all(|r| r.campaign_id.as_deref() == Some("migration-v1")),
        "migration backfill requests should carry the campaign id"
    );
}

/// Coverage-driven backfill: projects that already have a checkpoint row for
/// the indexer's current schema version should be filtered out. Without this,
/// each tick re-dispatches the entire project list and relies on NATS
/// per-subject dedup, which wedges as soon as any message hits max_deliver.
#[tokio::test]
async fn backfill_skips_projects_with_existing_checkpoints() {
    let context = TestContext::new().await;

    common::create_namespace(&context.clickhouse, 100, None, 20, "1/100/").await;
    common::create_project(&context.clickhouse, 10, 100, 1, 20, "1/100/10/").await;
    common::create_project(&context.clickhouse, 11, 100, 1, 20, "1/100/11/").await;
    common::create_project(&context.clickhouse, 12, 100, 1, 20, "1/100/12/").await;
    context.given_enabled_namespaces([100]).await;

    let graph = context.clickhouse.create_client();
    ensure_version_table(&graph).await.unwrap();
    mark_version_active(&graph, 0).await.unwrap();
    mark_version_migrating(&graph, *SCHEMA_VERSION)
        .await
        .unwrap();

    let table = prefixed_table_name("code_indexing_checkpoint", *SCHEMA_VERSION);
    context
        .clickhouse
        .execute(&format!(
            "INSERT INTO {table} \
             (traversal_path, project_id, branch, last_task_id, last_commit, indexed_at) \
             VALUES ('1/100/11/', 11, 'main', 0, 'sha', now())"
        ))
        .await;

    let services = indexer::orchestrator::scheduled::connect(&context.nats_config())
        .await
        .unwrap();

    let backfill = CodeBackfill::new(
        services.nats.clone(),
        context.clickhouse.create_client(),
        context.clickhouse.config.build_client(),
        ScheduledTaskMetrics::new(),
        std::sync::Arc::new(indexer::campaign::CampaignState::new()),
        orbit_server_config::AppConfig::embedded_defaults()
            .schedule
            .tasks
            .code_backfill
            .publish_window,
    );

    backfill
        .dispatch_enabled(uuid::Uuid::new_v4())
        .await
        .unwrap();

    let requests = context.consume_code_indexing_requests().await;
    let project_ids: HashSet<i64> = requests.iter().map(|r| r.project_id).collect();
    assert_eq!(
        project_ids,
        HashSet::from([10, 12]),
        "checkpointed project 11 must not be re-dispatched"
    );
}

#[tokio::test]
async fn migration_completion_checker_promotes_rebuilt_rollback_version() {
    let context = TestContext::new().await;

    common::create_namespace(&context.clickhouse, 100, None, 20, "1/100/").await;
    context.given_enabled_namespaces([100]).await;

    let graph = context.clickhouse.create_client();
    ensure_version_table(&graph).await.unwrap();
    mark_version_active(&graph, *SCHEMA_VERSION + 1)
        .await
        .unwrap();
    mark_version_migrating(&graph, *SCHEMA_VERSION)
        .await
        .unwrap();

    let checkpoint_table = prefixed_table_name("checkpoint", *SCHEMA_VERSION);
    let ontology = ontology::Ontology::load_embedded().unwrap();
    let invalidated = orbit_migrations::scope::find_invalidated_pipelines(
        &ontology,
        &orbit_migrations::scope::MigrationScope::Full,
    );
    for plan in &invalidated.namespaced {
        context
            .clickhouse
            .execute(&format!(
                "INSERT INTO {checkpoint_table} (key, watermark, cursor_values) \
                 VALUES ('ns.100.{plan}', now(), 'null')"
            ))
            .await;
    }
    for plan in &invalidated.global {
        context
            .clickhouse
            .execute(&format!(
                "INSERT INTO {checkpoint_table} (key, watermark, cursor_values) \
                 VALUES ('global.{plan}', now(), 'null')"
            ))
            .await;
    }

    let services = indexer::orchestrator::scheduled::connect(&context.nats_config())
        .await
        .unwrap();
    let catalog = context.ontology_catalog(services.nats_client.clone()).await;
    catalog
        .publish(&embedded_archive(*SCHEMA_VERSION))
        .await
        .unwrap();

    let checker = MigrationCompletionChecker::new(
        context.clickhouse.create_client(),
        context.clickhouse.create_client(),
        std::sync::Arc::new(indexer::testkit::MockLockService::new()),
        std::sync::Arc::new(ontology::Ontology::load_embedded().unwrap()),
        orbit_server_config::AppConfig::embedded_defaults().schema,
        orbit_server_config::AppConfig::embedded_defaults()
            .schedule
            .tasks
            .migration_completion,
        ScheduledTaskMetrics::new(),
        std::sync::Arc::new(indexer::campaign::CampaignState::new()),
        services.nats_connection.clone(),
        catalog,
    );

    checker.run().await.unwrap();

    let result = context
        .clickhouse
        .query(&format!(
            "SELECT CAST(status AS String) AS status \
             FROM gkg_schema_version FINAL WHERE version = {}",
            *SCHEMA_VERSION
        ))
        .await;
    let statuses = String::extract_column(&result, 0).unwrap();
    assert_eq!(
        statuses,
        vec!["active"],
        "the rebuilt version must be promoted once its checkpoint covers all enabled namespaces"
    );

    let result = context
        .clickhouse
        .query(&format!(
            "SELECT CAST(status AS String) AS status \
             FROM gkg_schema_version FINAL WHERE version = {}",
            *SCHEMA_VERSION + 1
        ))
        .await;
    let statuses = String::extract_column(&result, 0).unwrap();
    assert_eq!(
        statuses,
        vec!["retired"],
        "the version that was active before promotion must be retired"
    );
}

#[tokio::test]
async fn migration_completion_checker_promotes_when_no_namespaces_are_enabled() {
    let context = TestContext::new().await;

    let graph = context.clickhouse.create_client();
    ensure_version_table(&graph).await.unwrap();
    mark_version_active(&graph, 0).await.unwrap();
    mark_version_migrating(&graph, *SCHEMA_VERSION)
        .await
        .unwrap();

    let checkpoint_table = prefixed_table_name("checkpoint", *SCHEMA_VERSION);
    let ontology = ontology::Ontology::load_embedded().unwrap();
    let invalidated = orbit_migrations::scope::find_invalidated_pipelines(
        &ontology,
        &orbit_migrations::scope::MigrationScope::Full,
    );
    for plan in &invalidated.global {
        context
            .clickhouse
            .execute(&format!(
                "INSERT INTO {checkpoint_table} (key, watermark, cursor_values) \
                 VALUES ('global.{plan}', now(), 'null')"
            ))
            .await;
    }

    let services = indexer::orchestrator::scheduled::connect(&context.nats_config())
        .await
        .unwrap();
    let catalog = context.ontology_catalog(services.nats_client.clone()).await;
    catalog
        .publish(&embedded_archive(*SCHEMA_VERSION))
        .await
        .unwrap();

    let checker = MigrationCompletionChecker::new(
        context.clickhouse.create_client(),
        context.clickhouse.create_client(),
        std::sync::Arc::new(indexer::testkit::MockLockService::new()),
        std::sync::Arc::new(ontology::Ontology::load_embedded().unwrap()),
        orbit_server_config::AppConfig::embedded_defaults().schema,
        orbit_server_config::AppConfig::embedded_defaults()
            .schedule
            .tasks
            .migration_completion,
        ScheduledTaskMetrics::new(),
        std::sync::Arc::new(indexer::campaign::CampaignState::new()),
        services.nats_connection.clone(),
        catalog,
    );

    checker.run().await.unwrap();

    let result = context
        .clickhouse
        .query(&format!(
            "SELECT CAST(status AS String) AS status \
             FROM gkg_schema_version FINAL WHERE version = {}",
            *SCHEMA_VERSION
        ))
        .await;
    let statuses = String::extract_column(&result, 0).unwrap();
    assert_eq!(
        statuses,
        vec!["active"],
        "an empty enabled set has nothing to reindex, so the migration must promote"
    );
}

#[tokio::test]
async fn migration_completion_checker_does_not_promote_version_it_does_not_embed() {
    let context = TestContext::new().await;

    common::create_namespace(&context.clickhouse, 100, None, 20, "1/100/").await;
    context.given_enabled_namespaces([100]).await;

    let graph = context.clickhouse.create_client();
    ensure_version_table(&graph).await.unwrap();
    mark_version_active(&graph, *SCHEMA_VERSION).await.unwrap();
    mark_version_migrating(&graph, *SCHEMA_VERSION + 1)
        .await
        .unwrap();

    let sdlc = prefixed_table_name("checkpoint", *SCHEMA_VERSION + 1);
    let sdlc_src = prefixed_table_name("checkpoint", *SCHEMA_VERSION);
    let code = prefixed_table_name("code_indexing_checkpoint", *SCHEMA_VERSION + 1);
    let code_src = prefixed_table_name("code_indexing_checkpoint", *SCHEMA_VERSION);
    context
        .clickhouse
        .execute(&format!("CREATE TABLE {sdlc} AS {sdlc_src}"))
        .await;
    context
        .clickhouse
        .execute(&format!("CREATE TABLE {code} AS {code_src}"))
        .await;
    context
        .clickhouse
        .execute(&format!(
            "INSERT INTO {sdlc} (key, watermark) VALUES ('ns.100.sdlc', now())"
        ))
        .await;

    let services = indexer::orchestrator::scheduled::connect(&context.nats_config())
        .await
        .unwrap();
    let catalog = context.ontology_catalog(services.nats_client.clone()).await;

    let checker = MigrationCompletionChecker::new(
        context.clickhouse.create_client(),
        context.clickhouse.create_client(),
        std::sync::Arc::new(indexer::testkit::MockLockService::new()),
        std::sync::Arc::new(ontology::Ontology::load_embedded().unwrap()),
        orbit_server_config::AppConfig::embedded_defaults().schema,
        orbit_server_config::AppConfig::embedded_defaults()
            .schedule
            .tasks
            .migration_completion,
        ScheduledTaskMetrics::new(),
        std::sync::Arc::new(indexer::campaign::CampaignState::new()),
        services.nats_connection.clone(),
        catalog,
    );

    checker.run().await.unwrap();

    let result = context
        .clickhouse
        .query(&format!(
            "SELECT CAST(status AS String) AS status \
             FROM gkg_schema_version FINAL WHERE version = {}",
            *SCHEMA_VERSION + 1
        ))
        .await;
    let statuses = String::extract_column(&result, 0).unwrap();
    assert_eq!(
        statuses,
        vec!["migrating"],
        "a migrating version this binary does not embed must not be promoted"
    );
}

#[tokio::test]
async fn migration_completion_checker_guards_against_two_migrating_versions() {
    let context = TestContext::new().await;

    common::create_namespace(&context.clickhouse, 100, None, 20, "1/100/").await;
    context.given_enabled_namespaces([100]).await;

    let graph = context.clickhouse.create_client();
    ensure_version_table(&graph).await.unwrap();
    mark_version_active(&graph, *SCHEMA_VERSION + 1)
        .await
        .unwrap();
    mark_version_migrating(&graph, *SCHEMA_VERSION)
        .await
        .unwrap();
    // created_at is second-precision; +1s prevents a same-second tie with the embedded row.
    context
        .clickhouse
        .execute(&format!(
            "INSERT INTO gkg_schema_version (version, status, created_at) \
             VALUES ({}, 'migrating', now() + INTERVAL 1 SECOND)",
            *SCHEMA_VERSION + 2
        ))
        .await;

    let checkpoint_table = prefixed_table_name("checkpoint", *SCHEMA_VERSION);
    context
        .clickhouse
        .execute(&format!(
            "INSERT INTO {checkpoint_table} (key, watermark) \
             VALUES ('ns.100.sdlc', now())"
        ))
        .await;

    let services = indexer::orchestrator::scheduled::connect(&context.nats_config())
        .await
        .unwrap();
    let catalog = context.ontology_catalog(services.nats_client.clone()).await;
    catalog
        .publish(&embedded_archive(*SCHEMA_VERSION))
        .await
        .unwrap();

    let checker = MigrationCompletionChecker::new(
        context.clickhouse.create_client(),
        context.clickhouse.create_client(),
        std::sync::Arc::new(indexer::testkit::MockLockService::new()),
        std::sync::Arc::new(ontology::Ontology::load_embedded().unwrap()),
        orbit_server_config::AppConfig::embedded_defaults().schema,
        orbit_server_config::AppConfig::embedded_defaults()
            .schedule
            .tasks
            .migration_completion,
        ScheduledTaskMetrics::new(),
        std::sync::Arc::new(indexer::campaign::CampaignState::new()),
        services.nats_connection.clone(),
        catalog,
    );

    checker.run().await.unwrap();

    let result = context
        .clickhouse
        .query(&format!(
            "SELECT CAST(status AS String) AS status \
             FROM gkg_schema_version FINAL WHERE version = {}",
            *SCHEMA_VERSION
        ))
        .await;
    let statuses = String::extract_column(&result, 0).unwrap();
    assert_eq!(
        statuses,
        vec!["migrating"],
        "embedded rebuild-in-flight version must not be promoted while a newer version is migrating"
    );

    let result = context
        .clickhouse
        .query(&format!(
            "SELECT CAST(status AS String) AS status \
             FROM gkg_schema_version FINAL WHERE version = {}",
            *SCHEMA_VERSION + 2
        ))
        .await;
    let statuses = String::extract_column(&result, 0).unwrap();
    assert_eq!(
        statuses,
        vec!["migrating"],
        "the foreign migrating version this binary does not embed must not be promoted either"
    );

    let result = context
        .clickhouse
        .query(&format!(
            "SELECT CAST(status AS String) AS status \
             FROM gkg_schema_version FINAL WHERE version = {}",
            *SCHEMA_VERSION + 1
        ))
        .await;
    let statuses = String::extract_column(&result, 0).unwrap();
    assert_eq!(
        statuses,
        vec!["active"],
        "the active version must remain unchanged"
    );
}

#[tokio::test]
async fn migration_completion_preserves_state_until_target_archive_is_usable() {
    let context = TestContext::new().await;

    let graph = context.clickhouse.create_client();
    ensure_version_table(&graph).await.unwrap();
    mark_version_active(&graph, 0).await.unwrap();
    mark_version_migrating(&graph, *SCHEMA_VERSION)
        .await
        .unwrap();

    let checkpoint_table = prefixed_table_name("checkpoint", *SCHEMA_VERSION);
    let ontology = ontology::Ontology::load_embedded().unwrap();
    let view = ontology
        .refreshable_materialized_views()
        .iter()
        .find(|view| view.versioned)
        .unwrap();
    let view_name = prefixed_table_name(&view.name, *SCHEMA_VERSION);
    context
        .clickhouse
        .execute(&format!("DROP VIEW IF EXISTS {view_name}"))
        .await;
    let invalidated = orbit_migrations::scope::find_invalidated_pipelines(
        &ontology,
        &orbit_migrations::scope::MigrationScope::Full,
    );
    for plan in &invalidated.global {
        context
            .clickhouse
            .execute(&format!(
                "INSERT INTO {checkpoint_table} (key, watermark, cursor_values) \
                 VALUES ('global.{plan}', now(), 'null')"
            ))
            .await;
    }

    let services = indexer::orchestrator::scheduled::connect(&context.nats_config())
        .await
        .unwrap();
    let catalog = context.ontology_catalog(services.nats_client.clone()).await;
    let campaign = Arc::new(indexer::campaign::CampaignState::new());
    let campaign_id = indexer::campaign::campaign_id_for_version(*SCHEMA_VERSION);
    campaign.set(campaign_id.clone());
    let checker = MigrationCompletionChecker::new(
        context.clickhouse.create_client(),
        context.clickhouse.create_client(),
        Arc::new(indexer::testkit::MockLockService::new()),
        Arc::new(ontology),
        orbit_server_config::SchemaConfig::default(),
        orbit_server_config::MigrationCompletionConfig::default(),
        ScheduledTaskMetrics::new(),
        campaign.clone(),
        services.nats_connection.clone(),
        catalog.clone(),
    );

    let mut invalid_sources = embedded_sources();
    invalid_sources.insert("schema.yaml".into(), "not: [valid yaml".into());
    let invalid_ontology =
        OntologyArchive::from_sources(*SCHEMA_VERSION, &invalid_sources).unwrap();
    let bucket = archive_bucket(&context.clickhouse.config.database);
    let archive_key = SCHEMA_VERSION.to_string();

    for (payload, expected_error) in [
        (None, "load ontology archive before promotion"),
        (
            Some(Bytes::from_static(b"not an ontology archive")),
            "load ontology archive before promotion",
        ),
        (
            Some(Bytes::copy_from_slice(invalid_ontology.bytes())),
            "validate ontology archive before promotion",
        ),
    ] {
        if let Some(payload) = payload {
            services
                .nats_client
                .kv_put(&bucket, &archive_key, payload, KvPutOptions::default())
                .await
                .unwrap();
        }

        let error = checker.run().await.unwrap_err();
        assert!(error.to_string().contains(expected_error), "{error}");
        assert_eq!(
            orbit_migrations::version::read_active_version(&graph)
                .await
                .unwrap(),
            Some(0)
        );
        assert_eq!(
            orbit_migrations::version::read_migrating_version(&graph)
                .await
                .unwrap(),
            Some(*SCHEMA_VERSION)
        );
        assert_eq!(campaign.current(), Some(campaign_id.clone()));
        assert!(
            orbit_migrations::version::list_version_entities(&graph, *SCHEMA_VERSION)
                .await
                .unwrap()
                .iter()
                .all(|entity| entity.name != view_name)
        );
    }

    services
        .nats_client
        .kv_delete(&bucket, &archive_key)
        .await
        .unwrap();
    catalog
        .publish(&embedded_archive(*SCHEMA_VERSION))
        .await
        .unwrap();

    checker.run().await.unwrap();

    let versions = orbit_migrations::version::read_all_versions(&graph)
        .await
        .unwrap();
    assert_eq!(versions.len(), 2);
    assert!(
        versions
            .iter()
            .any(|entry| entry.version == 0 && entry.status == "retired")
    );
    assert!(
        versions
            .iter()
            .any(|entry| entry.version == *SCHEMA_VERSION && entry.status == "active")
    );
    assert_eq!(campaign.current(), None);
    assert!(
        orbit_migrations::version::list_version_entities(&graph, *SCHEMA_VERSION)
            .await
            .unwrap()
            .iter()
            .any(|entity| entity.name == view_name)
    );
}

#[tokio::test]
async fn dispatcher_rejects_missing_active_archive_before_migration() {
    let context = TestContext::new().await;

    let graph = context.clickhouse.create_client();
    ensure_version_table(&graph).await.unwrap();
    mark_version_active(&graph, *SCHEMA_VERSION - 1)
        .await
        .unwrap();

    let archive = embedded_archive(*SCHEMA_VERSION);
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        indexer::run_dispatcher(
            &dispatcher_config(&context),
            &archive,
            CancellationToken::new(),
        ),
    )
    .await
    .expect("dispatcher must reject the missing archive before starting");

    assert!(matches!(
        result,
        Err(DispatcherError::Archive(orbit_migrations::catalog::CatalogError::Missing(version)))
        if version == *SCHEMA_VERSION - 1
    ));

    let services = indexer::orchestrator::scheduled::connect(&context.nats_config())
        .await
        .unwrap();
    let catalog = context.ontology_catalog(services.nats_client.clone()).await;
    assert_eq!(
        catalog.load(*SCHEMA_VERSION).await.unwrap().bytes(),
        archive.bytes()
    );
    assert_eq!(
        orbit_migrations::version::read_active_version(&graph)
            .await
            .unwrap(),
        Some(*SCHEMA_VERSION - 1)
    );
    assert_eq!(
        orbit_migrations::version::read_migrating_version(&graph)
            .await
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn dispatcher_rejects_corrupt_active_archive_before_migration() {
    let context = TestContext::new().await;

    let graph = context.clickhouse.create_client();
    ensure_version_table(&graph).await.unwrap();
    mark_version_active(&graph, *SCHEMA_VERSION - 1)
        .await
        .unwrap();

    let services = indexer::orchestrator::scheduled::connect(&context.nats_config())
        .await
        .unwrap();
    let catalog = context.ontology_catalog(services.nats_client.clone()).await;
    let bucket = archive_bucket(&context.clickhouse.config.database);
    services
        .nats_client
        .kv_put(
            &bucket,
            &(*SCHEMA_VERSION - 1).to_string(),
            Bytes::from_static(b"not an ontology archive"),
            KvPutOptions::create_only(),
        )
        .await
        .unwrap();

    let archive = embedded_archive(*SCHEMA_VERSION);
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        indexer::run_dispatcher(
            &dispatcher_config(&context),
            &archive,
            CancellationToken::new(),
        ),
    )
    .await
    .expect("dispatcher must reject the corrupt archive before starting");

    assert!(matches!(
        result,
        Err(DispatcherError::Archive(
            orbit_migrations::catalog::CatalogError::Archive(_)
        ))
    ));
    assert_eq!(
        catalog.load(*SCHEMA_VERSION).await.unwrap().bytes(),
        archive.bytes()
    );
    assert_eq!(
        orbit_migrations::version::read_active_version(&graph)
            .await
            .unwrap(),
        Some(*SCHEMA_VERSION - 1)
    );
    assert_eq!(
        orbit_migrations::version::read_migrating_version(&graph)
            .await
            .unwrap(),
        None
    );
}

fn embedded_archive(version: u32) -> OntologyArchive {
    OntologyArchive::from_sources(version, &embedded_sources()).unwrap()
}

fn dispatcher_config(context: &TestContext) -> DispatcherConfig {
    DispatcherConfig {
        nats: context.nats_config(),
        graph: context.clickhouse.config.clone(),
        datalake: context.clickhouse.config.clone(),
        schedule: orbit_server_config::AppConfig::embedded_defaults().schedule,
        schema: orbit_server_config::AppConfig::embedded_defaults().schema,
        health_bind_address: "127.0.0.1:0".parse().unwrap(),
    }
}

fn archive_bucket(graph_database: &str) -> String {
    format!(
        "ontology_archives_{}",
        ontology::migrations::sha256_hex(graph_database)
    )
}
