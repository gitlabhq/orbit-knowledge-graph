//! Integration tests for the dispatcher.

use super::common;

use std::collections::HashSet;

use chrono::{DateTime, Utc};
use clickhouse_client::ClickHouseConfigurationExt;
use common::TestContext as ClickHouseContext;
use futures::StreamExt;
use gkg_server_config::{
    GlobalDispatcherConfig, NamespaceDispatcherConfig, NatsConfiguration, ScheduleConfiguration,
    SweepConfig,
};
use indexer::nats::versioning::NATS_VERSIONER;
use indexer::orchestrator::scheduled::{GlobalDispatcher, NamespaceDispatcher};
use indexer::orchestrator::scheduled::{ScheduledTask, ScheduledTaskMetrics};
use indexer::topic::{GLOBAL_INDEXING_SUBJECT, INDEXER_STREAM, NAMESPACE_INDEXING_SUBJECT_PATTERN};
use serde::Deserialize;
use testcontainers::ImageExt;
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::nats::{Nats, NatsServerCmd};

// --- Test Infrastructure ---

struct Namespace {
    id: i64,
    traversal_path: String,
}

#[derive(Debug, Deserialize)]
struct GlobalRequest {
    watermark: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
struct NamespaceRequest {
    namespace: i64,
    traversal_path: String,
    watermark: DateTime<Utc>,
}

struct TestContext {
    clickhouse: ClickHouseContext,
    _nats: testcontainers::ContainerAsync<Nats>,
    nats_url: String,
    /// Monotonically increasing counter to generate unique consumer names.
    consumer_seq: std::sync::atomic::AtomicU64,
}

impl TestContext {
    async fn new() -> Self {
        let clickhouse =
            ClickHouseContext::new(&[common::SIPHON_SCHEMA_SQL, *common::GRAPH_SCHEMA_SQL]).await;
        let (nats, nats_url) = Self::start_nats().await;
        Self::create_stream(&nats_url).await;
        Self {
            clickhouse,
            _nats: nats,
            nats_url,
            consumer_seq: std::sync::atomic::AtomicU64::new(0),
        }
    }

    fn nats_config(&self) -> NatsConfiguration {
        NatsConfiguration {
            url: self.nats_url.clone(),
            ..Default::default()
        }
    }

    async fn given_enabled_namespaces(&self, namespaces: impl IntoIterator<Item = Namespace>) {
        for (i, ns) in namespaces.into_iter().enumerate() {
            self.clickhouse
                .execute(&format!(
                    "INSERT INTO siphon_knowledge_graph_enabled_namespaces \
                     (id, root_namespace_id, traversal_path, created_at, updated_at) \
                     VALUES ({}, {}, '{}', now(), now())",
                    i + 1,
                    ns.id,
                    ns.traversal_path
                ))
                .await;
        }
    }

    async fn consume_global_requests(&self) -> Vec<GlobalRequest> {
        self.consume_messages(GLOBAL_INDEXING_SUBJECT).await
    }

    async fn consume_namespace_requests(&self) -> Vec<NamespaceRequest> {
        self.consume_messages(NAMESPACE_INDEXING_SUBJECT_PATTERN)
            .await
    }

    /// Drains all messages matching `subject` from the JetStream stream.
    /// Uses a unique durable consumer name per invocation and deletes it
    /// after draining, avoiding WorkQueue "filtered consumer not unique" errors.
    async fn consume_messages<T: for<'de> Deserialize<'de>>(&self, subject: &str) -> Vec<T> {
        let seq = self
            .consumer_seq
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let consumer_name = format!("test-drain-{seq}");
        let client = async_nats::connect(format!("nats://{}", self.nats_url))
            .await
            .unwrap();
        let jetstream = async_nats::jetstream::new(client);
        let stream_name = NATS_VERSIONER.stream(INDEXER_STREAM);

        let consumer = jetstream
            .create_consumer_on_stream(
                async_nats::jetstream::consumer::pull::Config {
                    durable_name: Some(consumer_name.clone()),
                    filter_subject: NATS_VERSIONER.subject(subject),
                    ..Default::default()
                },
                &stream_name,
            )
            .await
            .unwrap();

        let mut messages = consumer.fetch().max_messages(100).messages().await.unwrap();
        let mut results = Vec::new();

        while let Some(Ok(msg)) = messages.next().await {
            results.push(serde_json::from_slice(&msg.payload).unwrap());
            msg.ack().await.unwrap();
        }

        // Clean up to avoid collision on subsequent calls.
        let stream = jetstream.get_stream(&stream_name).await.unwrap();
        let _ = stream.delete_consumer(&consumer_name).await;

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

    async fn create_stream(url: &str) {
        let client = async_nats::connect(format!("nats://{url}")).await.unwrap();
        let jetstream = async_nats::jetstream::new(client);

        jetstream
            .create_stream(async_nats::jetstream::stream::Config {
                name: NATS_VERSIONER.stream(INDEXER_STREAM),
                subjects: vec![
                    NATS_VERSIONER.subject(GLOBAL_INDEXING_SUBJECT),
                    NATS_VERSIONER.subject(NAMESPACE_INDEXING_SUBJECT_PATTERN),
                ],
                retention: async_nats::jetstream::stream::RetentionPolicy::WorkQueue,
                max_messages_per_subject: 1,
                discard: async_nats::jetstream::stream::DiscardPolicy::New,
                discard_new_per_subject: true,
                ..Default::default()
            })
            .await
            .unwrap();
    }

    /// Purge all messages from the stream so subjects can accept new publishes.
    async fn purge_stream(url: &str) {
        let client = async_nats::connect(format!("nats://{url}")).await.unwrap();
        let jetstream = async_nats::jetstream::new(client);
        let stream = jetstream
            .get_stream(&NATS_VERSIONER.stream(INDEXER_STREAM))
            .await
            .unwrap();
        stream.purge().await.unwrap();
    }
}

// --- Tests ---

#[tokio::test]
async fn dispatcher_publishes_global_and_namespace_requests() {
    let context = TestContext::new().await;

    context
        .given_enabled_namespaces([
            Namespace {
                id: 100,
                traversal_path: "1/100/".to_string(),
            },
            Namespace {
                id: 200,
                traversal_path: "2/200/".to_string(),
            },
            Namespace {
                id: 300,
                traversal_path: "3/300/".to_string(),
            },
        ])
        .await;

    let services = indexer::orchestrator::scheduled::connect(&context.nats_config())
        .await
        .unwrap();
    let datalake = context.clickhouse.config.build_client();
    let metrics = ScheduledTaskMetrics::new();
    let lock_service = services.lock_service.clone();
    let tasks: Vec<Box<dyn ScheduledTask>> = vec![
        Box::new(GlobalDispatcher::new(
            services.nats.clone(),
            metrics.clone(),
            GlobalDispatcherConfig::default(),
            std::sync::Arc::new(indexer::campaign::CampaignState::new()),
        )),
        Box::new(NamespaceDispatcher::new(
            services.nats,
            datalake,
            metrics,
            NamespaceDispatcherConfig::default(),
            std::sync::Arc::new(indexer::campaign::CampaignState::new()),
        )),
    ];

    let before = Utc::now();
    indexer::orchestrator::scheduled::run_once(&tasks, &*lock_service)
        .await
        .unwrap();
    let after = Utc::now();

    let global = context.consume_global_requests().await;
    assert_eq!(global.len(), 1);
    assert!(global[0].watermark >= before && global[0].watermark <= after);

    let namespaces = context.consume_namespace_requests().await;
    assert_eq!(namespaces.len(), 3);

    let actual: HashSet<_> = namespaces
        .iter()
        .map(|r| (r.namespace, r.traversal_path.as_str()))
        .collect();
    let expected: HashSet<_> = [(100, "1/100/"), (200, "2/200/"), (300, "3/300/")].into();
    assert_eq!(actual, expected);

    assert!(
        namespaces
            .iter()
            .all(|r| r.watermark >= before && r.watermark <= after)
    );
}

// --- Dirty-namespace detection tests ---

/// Config that makes the first run a sweep (default), then subsequent runs
/// use dirty-detection because the sweep cron is far in the future.
fn dirty_detection_config() -> NamespaceDispatcherConfig {
    NamespaceDispatcherConfig {
        schedule: ScheduleConfiguration {
            cron: Some("*/1 * * * * *".to_string()),
        },
        sweep: SweepConfig {
            cron: "0 0 1 1 1 *".to_string(),
            slack_secs: 0,
        },
    }
}

#[tokio::test]
async fn dirty_detection_dispatches_only_changed_namespaces() {
    let context = TestContext::new().await;

    context
        .given_enabled_namespaces([
            Namespace {
                id: 100,
                traversal_path: "1/100/".to_string(),
            },
            Namespace {
                id: 200,
                traversal_path: "2/200/".to_string(),
            },
            Namespace {
                id: 300,
                traversal_path: "3/300/".to_string(),
            },
        ])
        .await;

    let services = indexer::orchestrator::scheduled::connect(&context.nats_config())
        .await
        .unwrap();
    let datalake = context.clickhouse.config.build_client();
    let metrics = ScheduledTaskMetrics::new();
    let campaign = std::sync::Arc::new(indexer::campaign::CampaignState::new());
    let dispatcher = NamespaceDispatcher::new(
        services.nats.clone(),
        datalake.clone(),
        metrics.clone(),
        dirty_detection_config(),
        campaign.clone(),
    );

    // First run is always a sweep (last_sweep = None) — dispatches all 3.
    dispatcher.run().await.unwrap();
    let first_batch = context.consume_namespace_requests().await;
    assert_eq!(first_batch.len(), 3, "sweep should dispatch all namespaces");

    // Seed a row with a recent watermark in namespace 1/100/.
    context
        .clickhouse
        .execute(
            "INSERT INTO work_items (id, iid, title, work_item_type_id, namespace_id, traversal_path, _siphon_watermark) \
             VALUES (1, 1, 'test', 1, 100, '1/100/', now64(6))",
        )
        .await;

    TestContext::purge_stream(&context.nats_url).await;

    // Second run: dirty-detection mode. Only namespace 1/100/ changed.
    dispatcher.run().await.unwrap();
    let second_batch = context.consume_namespace_requests().await;
    let dispatched_paths: HashSet<_> = second_batch
        .iter()
        .map(|r| r.traversal_path.as_str())
        .collect();
    assert!(
        dispatched_paths.contains("1/100/"),
        "namespace with changed row should be dispatched"
    );
    assert!(
        !dispatched_paths.contains("2/200/"),
        "unchanged namespace should not be dispatched"
    );
    assert!(
        !dispatched_paths.contains("3/300/"),
        "unchanged namespace should not be dispatched"
    );
}

#[tokio::test]
async fn dirty_detection_catches_descendant_path() {
    let context = TestContext::new().await;

    context
        .given_enabled_namespaces([Namespace {
            id: 100,
            traversal_path: "1/100/".to_string(),
        }])
        .await;

    let services = indexer::orchestrator::scheduled::connect(&context.nats_config())
        .await
        .unwrap();
    let datalake = context.clickhouse.config.build_client();
    let metrics = ScheduledTaskMetrics::new();
    let campaign = std::sync::Arc::new(indexer::campaign::CampaignState::new());
    let dispatcher = NamespaceDispatcher::new(
        services.nats.clone(),
        datalake.clone(),
        metrics.clone(),
        dirty_detection_config(),
        campaign.clone(),
    );

    dispatcher.run().await.unwrap();
    context.consume_namespace_requests().await;

    // Seed a row under a deeper sub-group path within the enabled namespace.
    context
        .clickhouse
        .execute(
            "INSERT INTO work_items (id, iid, title, work_item_type_id, namespace_id, traversal_path, _siphon_watermark) \
             VALUES (10, 10, 'subgroup', 1, 200, '1/100/200/', now64(6))",
        )
        .await;

    TestContext::purge_stream(&context.nats_url).await;
    dispatcher.run().await.unwrap();
    let batch = context.consume_namespace_requests().await;
    assert_eq!(
        batch.len(),
        1,
        "descendant path should trigger parent namespace dispatch"
    );
    assert_eq!(batch[0].traversal_path, "1/100/");
}

#[tokio::test]
async fn dirty_detection_catches_watermark_update() {
    let context = TestContext::new().await;

    context
        .given_enabled_namespaces([Namespace {
            id: 100,
            traversal_path: "1/100/".to_string(),
        }])
        .await;

    let services = indexer::orchestrator::scheduled::connect(&context.nats_config())
        .await
        .unwrap();
    let datalake = context.clickhouse.config.build_client();
    let metrics = ScheduledTaskMetrics::new();
    let campaign = std::sync::Arc::new(indexer::campaign::CampaignState::new());
    let dispatcher = NamespaceDispatcher::new(
        services.nats.clone(),
        datalake.clone(),
        metrics.clone(),
        dirty_detection_config(),
        campaign.clone(),
    );

    dispatcher.run().await.unwrap();
    context.consume_namespace_requests().await;

    context
        .clickhouse
        .execute(
            "INSERT INTO work_items (id, iid, title, work_item_type_id, namespace_id, traversal_path, _siphon_watermark) \
             VALUES (2, 2, 'updated', 1, 100, '1/100/', now64(6))",
        )
        .await;

    TestContext::purge_stream(&context.nats_url).await;
    dispatcher.run().await.unwrap();
    let batch = context.consume_namespace_requests().await;
    assert_eq!(batch.len(), 1, "updated watermark should trigger dispatch");
    assert_eq!(batch[0].traversal_path, "1/100/");
}

#[tokio::test]
async fn dirty_detection_catches_deleted_tombstone() {
    let context = TestContext::new().await;

    context
        .given_enabled_namespaces([Namespace {
            id: 100,
            traversal_path: "1/100/".to_string(),
        }])
        .await;

    let services = indexer::orchestrator::scheduled::connect(&context.nats_config())
        .await
        .unwrap();
    let datalake = context.clickhouse.config.build_client();
    let metrics = ScheduledTaskMetrics::new();
    let campaign = std::sync::Arc::new(indexer::campaign::CampaignState::new());
    let dispatcher = NamespaceDispatcher::new(
        services.nats.clone(),
        datalake.clone(),
        metrics.clone(),
        dirty_detection_config(),
        campaign.clone(),
    );

    dispatcher.run().await.unwrap();
    context.consume_namespace_requests().await;

    // _siphon_deleted=true tombstone with an advanced watermark.
    context
        .clickhouse
        .execute(
            "INSERT INTO work_items (id, iid, title, work_item_type_id, namespace_id, traversal_path, _siphon_watermark, _siphon_deleted) \
             VALUES (3, 3, 'deleted', 1, 100, '1/100/', now64(6), true)",
        )
        .await;

    TestContext::purge_stream(&context.nats_url).await;
    dispatcher.run().await.unwrap();
    let batch = context.consume_namespace_requests().await;
    assert_eq!(
        batch.len(),
        1,
        "deleted tombstone with advanced watermark should trigger dispatch"
    );
    assert_eq!(batch[0].traversal_path, "1/100/");
}

#[tokio::test]
async fn dirty_detection_covers_merge_request_table() {
    let context = TestContext::new().await;

    context
        .given_enabled_namespaces([
            Namespace {
                id: 100,
                traversal_path: "1/100/".to_string(),
            },
            Namespace {
                id: 200,
                traversal_path: "2/200/".to_string(),
            },
        ])
        .await;

    let services = indexer::orchestrator::scheduled::connect(&context.nats_config())
        .await
        .unwrap();
    let datalake = context.clickhouse.config.build_client();
    let metrics = ScheduledTaskMetrics::new();
    let campaign = std::sync::Arc::new(indexer::campaign::CampaignState::new());
    let dispatcher = NamespaceDispatcher::new(
        services.nats.clone(),
        datalake.clone(),
        metrics.clone(),
        dirty_detection_config(),
        campaign.clone(),
    );

    dispatcher.run().await.unwrap();
    context.consume_namespace_requests().await;

    // MR is a high-churn entity that must be covered by dirty-detection.
    context
        .clickhouse
        .execute(
            "INSERT INTO merge_requests \
             (id, iid, title, state_id, target_project_id, target_branch, source_branch, \
              traversal_path, _siphon_watermark) \
             VALUES (1, 1, 'test MR', 1, 1, 'main', 'feat', '1/100/', now64(6))",
        )
        .await;

    TestContext::purge_stream(&context.nats_url).await;
    dispatcher.run().await.unwrap();
    let batch = context.consume_namespace_requests().await;

    let dispatched: HashSet<_> = batch.iter().map(|r| r.traversal_path.as_str()).collect();
    assert!(
        dispatched.contains("1/100/"),
        "MR change should trigger dirty-detection for its namespace"
    );
    assert!(
        !dispatched.contains("2/200/"),
        "unchanged namespace should not be dispatched"
    );
}
