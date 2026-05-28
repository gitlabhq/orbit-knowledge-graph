//! Integration test: migration-triggered code backfill.
//!
//! When a schema migration is in progress (`gkg_schema_version` has a
//! `migrating` row), `NamespaceCodeBackfillDispatcher` must dispatch
//! code indexing tasks for **all** enabled namespaces, not only for
//! newly-enabled ones arriving via CDC events.

use std::collections::HashSet;

use clickhouse_client::ClickHouseConfigurationExt;
use gkg_server_config::{
    NamespaceCodeBackfillDispatcherConfig, NatsConfiguration, ScheduleConfiguration,
};
use indexer::modules::code::NamespaceCodeBackfillDispatcher;
use indexer::scheduler::{ScheduledTask, ScheduledTaskMetrics};
use indexer::schema::version::{
    SCHEMA_VERSION, ensure_version_table, prefixed_table_name, write_migrating_version,
    write_schema_version,
};
use indexer::topic::{CODE_INDEXING_TASK_SUBJECT_PATTERN, INDEXER_STREAM};
use serde::Deserialize;
use testcontainers::ImageExt;
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::nats::{Nats, NatsServerCmd};

use super::super::common;
use common::TestContext as ClickHouseContext;

#[derive(Deserialize)]
struct CodeIndexingRequest {
    task_id: i64,
    project_id: i64,
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
            ..Default::default()
        }
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
                    filter_subject: CODE_INDEXING_TASK_SUBJECT_PATTERN.into(),
                    ..Default::default()
                },
                INDEXER_STREAM,
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

        // Stream for dispatched code indexing tasks.
        jetstream
            .create_stream(async_nats::jetstream::stream::Config {
                name: INDEXER_STREAM.into(),
                subjects: vec![CODE_INDEXING_TASK_SUBJECT_PATTERN.into()],
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

    // Seed: two namespaces with projects.
    common::create_namespace(&context.clickhouse, 100, None, 20, "1/100/").await;
    common::create_namespace(&context.clickhouse, 200, None, 20, "1/200/").await;
    common::create_project(&context.clickhouse, 10, 100, 1, 20, "1/100/10/").await;
    common::create_project(&context.clickhouse, 20, 200, 1, 20, "1/200/20/").await;
    common::create_project(&context.clickhouse, 21, 200, 1, 20, "1/200/21/").await;
    context.given_enabled_namespaces([100, 200]).await;

    // Put a migrating version into gkg_schema_version.
    let graph = context.clickhouse.create_client();
    ensure_version_table(&graph).await.unwrap();
    write_schema_version(&graph, 0).await.unwrap();
    write_migrating_version(&graph, 1).await.unwrap();

    // Build and run the dispatcher once.
    let services = indexer::scheduler::connect(&context.nats_config())
        .await
        .unwrap();

    let task: Box<dyn ScheduledTask> = Box::new(NamespaceCodeBackfillDispatcher::new(
        services.nats.clone(),
        context.clickhouse.create_client(),
        context.clickhouse.config.build_client(),
        ScheduledTaskMetrics::new(),
        NamespaceCodeBackfillDispatcherConfig {
            schedule: ScheduleConfiguration::default(),
            ..Default::default()
        },
    ));

    indexer::scheduler::run_once(&[task], &*services.lock_service)
        .await
        .unwrap();

    // Verify: code indexing tasks dispatched for all 3 projects.
    let requests = context.consume_code_indexing_requests().await;
    let project_ids: HashSet<i64> = requests.iter().map(|r| r.project_id).collect();

    assert_eq!(
        project_ids,
        HashSet::from([10, 20, 21]),
        "expected backfill for all projects in enabled namespaces"
    );

    // All backfill requests should have task_id=0 (backfill marker).
    assert!(
        requests.iter().all(|r| r.task_id == 0),
        "migration backfill requests should use task_id=0"
    );
}

/// Coverage-driven backfill: projects that already have a checkpoint row for
/// the indexer's current schema version should be filtered out. Without this,
/// each tick re-dispatches the entire project list and relies on NATS
/// per-subject dedup, which wedges as soon as any message hits max_deliver.
#[tokio::test]
async fn backfill_skips_projects_with_existing_checkpoints() {
    let context = TestContext::new().await;

    // Seed: one namespace with three projects (10, 11, 12).
    common::create_namespace(&context.clickhouse, 100, None, 20, "1/100/").await;
    common::create_project(&context.clickhouse, 10, 100, 1, 20, "1/100/10/").await;
    common::create_project(&context.clickhouse, 11, 100, 1, 20, "1/100/11/").await;
    common::create_project(&context.clickhouse, 12, 100, 1, 20, "1/100/12/").await;
    context.given_enabled_namespaces([100]).await;

    let graph = context.clickhouse.create_client();
    ensure_version_table(&graph).await.unwrap();
    write_schema_version(&graph, 0).await.unwrap();
    write_migrating_version(&graph, *SCHEMA_VERSION)
        .await
        .unwrap();

    // Insert a checkpoint row for project 11 in the current-version table:
    // simulates a prior successful indexing run that the dispatcher must not
    // re-dispatch.
    let table = prefixed_table_name("code_indexing_checkpoint", *SCHEMA_VERSION);
    context
        .clickhouse
        .execute(&format!(
            "INSERT INTO {table} \
             (traversal_path, project_id, branch, last_task_id, last_commit, indexed_at) \
             VALUES ('1/100/11/', 11, 'main', 0, 'sha', now())"
        ))
        .await;

    let services = indexer::scheduler::connect(&context.nats_config())
        .await
        .unwrap();

    let task: Box<dyn ScheduledTask> = Box::new(NamespaceCodeBackfillDispatcher::new(
        services.nats.clone(),
        context.clickhouse.create_client(),
        context.clickhouse.config.build_client(),
        ScheduledTaskMetrics::new(),
        NamespaceCodeBackfillDispatcherConfig {
            schedule: ScheduleConfiguration::default(),
            ..Default::default()
        },
    ));

    indexer::scheduler::run_once(&[task], &*services.lock_service)
        .await
        .unwrap();

    // Project 11 was already checkpointed, so only 10 and 12 should be
    // dispatched.
    let requests = context.consume_code_indexing_requests().await;
    let project_ids: HashSet<i64> = requests.iter().map(|r| r.project_id).collect();
    assert_eq!(
        project_ids,
        HashSet::from([10, 12]),
        "checkpointed project 11 must not be re-dispatched"
    );
}
