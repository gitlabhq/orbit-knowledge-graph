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
use clickhouse_client::ClickHouseConfigurationExt;
use indexer::campaign::{CampaignState, campaign_id_for_version};
use indexer::config::{DispatcherConfig, DispatcherError};
use indexer::nats::versioning::NATS_VERSIONER;
use indexer::orchestrator::dispatch::CodeBackfill;
use indexer::orchestrator::scheduled::{
    MigrationCompletionChecker, ScheduledTask, ScheduledTaskMetrics, SchedulerServices,
};
use indexer::schema::version::{
    SCHEMA_VERSION, VersionEntry, ensure_version_table, list_version_entities, mark_version_active,
    mark_version_migrating, prefixed_table_name, read_active_version, read_all_versions,
    read_migrating_version,
};
use indexer::topic::{CODE_INDEXING_TASK_SUBJECT_PATTERN, INDEXER_STREAM};
use nats_client::KvPutOptions;
use ontology::archive::OntologyArchive;
use ontology::migrations::embedded_sources;
use orbit_migrations::catalog::{ONTOLOGY_ARCHIVES_BUCKET, OntologyCatalog};
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
    scheduler_services: SchedulerServices,
    ontology: Arc<ontology::Ontology>,
    catalog: OntologyCatalog,
    campaign: Arc<CampaignState>,
}

impl TestContext {
    async fn new() -> Self {
        let clickhouse =
            ClickHouseContext::new(&[common::SIPHON_SCHEMA_SQL, *common::GRAPH_SCHEMA_SQL]).await;
        let (nats, nats_url) = Self::start_nats().await;
        Self::create_streams(&nats_url).await;
        let scheduler_services = indexer::orchestrator::scheduled::connect(&NatsConfiguration {
            url: nats_url.clone(),
            ..orbit_server_config::AppConfig::embedded_defaults().nats
        })
        .await
        .unwrap();
        let catalog = OntologyCatalog::open(scheduler_services.nats_client.clone())
            .await
            .unwrap();
        Self {
            clickhouse,
            _nats: nats,
            nats_url,
            scheduler_services,
            ontology: Arc::new(ontology::Ontology::load_embedded().unwrap()),
            catalog,
            campaign: Arc::new(CampaignState::new()),
        }
    }

    fn nats_config(&self) -> NatsConfiguration {
        NatsConfiguration {
            url: self.nats_url.clone(),
            ..orbit_server_config::AppConfig::embedded_defaults().nats
        }
    }

    fn completion_checker(&self) -> MigrationCompletionChecker {
        MigrationCompletionChecker::new(
            self.clickhouse.create_client(),
            self.clickhouse.create_client(),
            Arc::new(indexer::testkit::MockLockService::new()),
            self.ontology.clone(),
            orbit_server_config::AppConfig::embedded_defaults().schema,
            orbit_server_config::AppConfig::embedded_defaults()
                .schedule
                .tasks
                .migration_completion,
            ScheduledTaskMetrics::new(),
            self.campaign.clone(),
            self.scheduler_services.nats_connection.clone(),
            self.catalog.clone(),
        )
    }

    async fn given_migration(&self, active_version: u32, migrating_version: u32) {
        let graph = self.clickhouse.create_client();
        ensure_version_table(&graph).await.unwrap();
        mark_version_active(&graph, active_version).await.unwrap();
        mark_version_migrating(&graph, migrating_version)
            .await
            .unwrap();
        self.campaign
            .set(campaign_id_for_version(migrating_version));
    }

    async fn complete_required_pipelines(&self, namespace_ids: &[i64]) {
        let invalidated = orbit_migrations::scope::find_invalidated_pipelines(
            &self.ontology,
            &orbit_migrations::scope::MigrationScope::Full,
        );
        let global_keys = invalidated
            .global
            .iter()
            .map(|pipeline_name| format!("global.{pipeline_name}"));
        let namespace_keys = namespace_ids.iter().flat_map(|namespace_id| {
            invalidated
                .namespaced
                .iter()
                .map(move |pipeline_name| format!("ns.{namespace_id}.{pipeline_name}"))
        });
        self.insert_completed_checkpoints(
            &prefixed_table_name("checkpoint", *SCHEMA_VERSION),
            global_keys.chain(namespace_keys),
        )
        .await;
    }

    fn target_view_name(&self) -> String {
        let view = self
            .ontology
            .refreshable_materialized_views()
            .iter()
            .find(|view| view.versioned)
            .unwrap();
        prefixed_table_name(&view.name, *SCHEMA_VERSION)
    }

    async fn remove_target_view(&self) {
        self.clickhouse
            .execute(&format!("DROP VIEW IF EXISTS {}", self.target_view_name()))
            .await;
    }

    async fn promotion_state(&self) -> PromotionState {
        let graph = self.clickhouse.create_client();
        let target_view_name = self.target_view_name();
        PromotionState {
            versions: read_all_versions(&graph).await.unwrap(),
            campaign: self.campaign.current(),
            target_view_exists: list_version_entities(&graph, *SCHEMA_VERSION)
                .await
                .unwrap()
                .iter()
                .any(|entity| entity.name == target_view_name),
        }
    }

    async fn restore_archive(&self, version: u32) {
        self.scheduler_services
            .nats_client
            .kv_delete(ONTOLOGY_ARCHIVES_BUCKET, &version.to_string())
            .await
            .unwrap();
        self.catalog
            .publish(&embedded_archive(version))
            .await
            .unwrap();
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

    async fn insert_completed_checkpoints(
        &self,
        checkpoint_table: &str,
        checkpoint_keys: impl IntoIterator<Item = String>,
    ) {
        for checkpoint_key in checkpoint_keys {
            self.clickhouse
                .execute(&format!(
                    "INSERT INTO {checkpoint_table} (key, watermark, cursor_values) \
                     VALUES ('{checkpoint_key}', now(), 'null')"
                ))
                .await;
        }
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

fn version_entry(version: u32, status: &str) -> VersionEntry {
    VersionEntry {
        version,
        status: status.to_owned(),
    }
}

#[derive(Debug, PartialEq, Eq)]
struct PromotionState {
    versions: Vec<VersionEntry>,
    campaign: Option<String>,
    target_view_exists: bool,
}

#[derive(Clone, Copy)]
enum ArchiveCase {
    Missing,
    BadGzip,
    InvalidYaml,
}

impl ArchiveCase {
    fn expected_error(self) -> &'static str {
        match self {
            ArchiveCase::Missing | ArchiveCase::BadGzip => "load ontology archive before promotion",
            ArchiveCase::InvalidYaml => "validate ontology archive before promotion",
        }
    }

    async fn inject(self, context: &TestContext, version: u32) {
        let archive_key = version.to_string();
        let client = &context.scheduler_services.nats_client;
        client
            .kv_delete(ONTOLOGY_ARCHIVES_BUCKET, &archive_key)
            .await
            .unwrap();

        let payload = match self {
            ArchiveCase::Missing => None,
            ArchiveCase::BadGzip => Some(Bytes::from_static(b"not an ontology archive")),
            ArchiveCase::InvalidYaml => {
                let mut sources = embedded_sources();
                sources.insert("schema.yaml".into(), "not: [valid yaml".into());
                let archive = OntologyArchive::from_sources(version, &sources).unwrap();
                Some(Bytes::copy_from_slice(archive.bytes()))
            }
        };
        if let Some(payload) = payload {
            client
                .kv_put(
                    ONTOLOGY_ARCHIVES_BUCKET,
                    &archive_key,
                    payload,
                    KvPutOptions::default(),
                )
                .await
                .unwrap();
        }
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

    let campaign = std::sync::Arc::new(indexer::campaign::CampaignState::new());
    campaign.set(indexer::campaign::campaign_id_for_version(1));

    let backfill = CodeBackfill::new(
        context.scheduler_services.nats.clone(),
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

    let backfill = CodeBackfill::new(
        context.scheduler_services.nats.clone(),
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
    context
        .given_migration(*SCHEMA_VERSION + 1, *SCHEMA_VERSION)
        .await;
    context.complete_required_pipelines(&[100]).await;
    context
        .catalog
        .publish(&embedded_archive(*SCHEMA_VERSION))
        .await
        .unwrap();
    let checker = context.completion_checker();

    checker.run().await.unwrap();

    assert_eq!(
        read_all_versions(&graph).await.unwrap(),
        vec![
            version_entry(*SCHEMA_VERSION + 1, "retired"),
            version_entry(*SCHEMA_VERSION, "active"),
        ],
        "the rebuilt version must be promoted once its checkpoint covers all enabled namespaces"
    );
}

#[tokio::test]
async fn migration_completion_checker_promotes_when_no_namespaces_are_enabled() {
    let context = TestContext::new().await;

    let graph = context.clickhouse.create_client();
    context.given_migration(0, *SCHEMA_VERSION).await;
    context.complete_required_pipelines(&[]).await;
    context
        .catalog
        .publish(&embedded_archive(*SCHEMA_VERSION))
        .await
        .unwrap();
    let checker = context.completion_checker();

    checker.run().await.unwrap();

    assert_eq!(
        read_all_versions(&graph).await.unwrap(),
        vec![
            version_entry(*SCHEMA_VERSION, "active"),
            version_entry(0, "retired"),
        ]
    );
}

#[tokio::test]
async fn migration_completion_checker_does_not_promote_version_it_does_not_embed() {
    let context = TestContext::new().await;
    common::create_namespace(&context.clickhouse, 100, None, 20, "1/100/").await;
    context.given_enabled_namespaces([100]).await;
    context
        .given_migration(*SCHEMA_VERSION, *SCHEMA_VERSION + 1)
        .await;

    for table in ["checkpoint", "code_indexing_checkpoint"] {
        let source_table = prefixed_table_name(table, *SCHEMA_VERSION);
        let migrating_table = prefixed_table_name(table, *SCHEMA_VERSION + 1);
        context
            .clickhouse
            .execute(&format!("CREATE TABLE {migrating_table} AS {source_table}"))
            .await;
    }
    context
        .insert_completed_checkpoints(
            &prefixed_table_name("checkpoint", *SCHEMA_VERSION + 1),
            ["ns.100.sdlc".to_owned()],
        )
        .await;
    let checker = context.completion_checker();

    checker.run().await.unwrap();

    assert_eq!(
        read_all_versions(&context.clickhouse.create_client())
            .await
            .unwrap(),
        vec![
            version_entry(*SCHEMA_VERSION + 1, "migrating"),
            version_entry(*SCHEMA_VERSION, "active"),
        ]
    );
}

#[tokio::test]
async fn migration_completion_checker_guards_against_two_migrating_versions() {
    let context = TestContext::new().await;
    common::create_namespace(&context.clickhouse, 100, None, 20, "1/100/").await;
    context.given_enabled_namespaces([100]).await;
    context
        .given_migration(*SCHEMA_VERSION + 1, *SCHEMA_VERSION)
        .await;
    // created_at is second-precision; +1s prevents a same-second tie with the embedded row.
    context
        .clickhouse
        .execute(&format!(
            "INSERT INTO gkg_schema_version (version, status, created_at) \
         VALUES ({}, 'migrating', now() + INTERVAL 1 SECOND)",
            *SCHEMA_VERSION + 2
        ))
        .await;
    context
        .insert_completed_checkpoints(
            &prefixed_table_name("checkpoint", *SCHEMA_VERSION),
            ["ns.100.sdlc".to_owned()],
        )
        .await;
    context
        .catalog
        .publish(&embedded_archive(*SCHEMA_VERSION))
        .await
        .unwrap();
    let checker = context.completion_checker();

    checker.run().await.unwrap();

    assert_eq!(
        read_all_versions(&context.clickhouse.create_client())
            .await
            .unwrap(),
        vec![
            version_entry(*SCHEMA_VERSION + 2, "migrating"),
            version_entry(*SCHEMA_VERSION + 1, "active"),
            version_entry(*SCHEMA_VERSION, "migrating"),
        ]
    );
}

#[tokio::test]
async fn migration_completion_preserves_state_until_target_archive_is_usable() {
    let context = TestContext::new().await;
    context.given_migration(0, *SCHEMA_VERSION).await;
    context.complete_required_pipelines(&[]).await;
    context.remove_target_view().await;
    let checker = context.completion_checker();

    let preserved_state = PromotionState {
        versions: vec![
            version_entry(*SCHEMA_VERSION, "migrating"),
            version_entry(0, "active"),
        ],
        campaign: Some(campaign_id_for_version(*SCHEMA_VERSION)),
        target_view_exists: false,
    };

    for archive_case in [
        ArchiveCase::Missing,
        ArchiveCase::BadGzip,
        ArchiveCase::InvalidYaml,
    ] {
        archive_case.inject(&context, *SCHEMA_VERSION).await;

        let error = checker.run().await.unwrap_err();
        assert!(
            error.to_string().contains(archive_case.expected_error()),
            "{error}"
        );
        assert_eq!(context.promotion_state().await, preserved_state);
    }

    context.restore_archive(*SCHEMA_VERSION).await;
    checker.run().await.unwrap();

    assert_eq!(
        context.promotion_state().await,
        PromotionState {
            versions: vec![
                version_entry(*SCHEMA_VERSION, "active"),
                version_entry(0, "retired"),
            ],
            campaign: None,
            target_view_exists: true,
        }
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

    assert_eq!(
        context.catalog.load(*SCHEMA_VERSION).await.unwrap().bytes(),
        archive.bytes()
    );
    assert_eq!(
        read_active_version(&graph).await.unwrap(),
        Some(*SCHEMA_VERSION - 1)
    );
    assert_eq!(read_migrating_version(&graph).await.unwrap(), None);
}

#[tokio::test]
async fn dispatcher_rejects_corrupt_active_archive_before_migration() {
    let context = TestContext::new().await;

    let graph = context.clickhouse.create_client();
    ensure_version_table(&graph).await.unwrap();
    mark_version_active(&graph, *SCHEMA_VERSION - 1)
        .await
        .unwrap();

    ArchiveCase::BadGzip
        .inject(&context, *SCHEMA_VERSION - 1)
        .await;

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
        context.catalog.load(*SCHEMA_VERSION).await.unwrap().bytes(),
        archive.bytes()
    );
    assert_eq!(
        read_active_version(&graph).await.unwrap(),
        Some(*SCHEMA_VERSION - 1)
    );
    assert_eq!(read_migrating_version(&graph).await.unwrap(), None);
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
