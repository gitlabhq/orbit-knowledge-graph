//! Integration tests for the dispatcher.

use super::common;

use std::collections::HashSet;

use chrono::{DateTime, Utc};
use clickhouse_client::ClickHouseConfigurationExt;
use common::TestContext as ClickHouseContext;
use futures::StreamExt;
use gkg_server_config::{GlobalDispatcherConfig, NamespaceDispatcherConfig, NatsConfiguration};
use indexer::modules::sdlc::dispatch::{GlobalDispatcher, NamespaceDispatcher};
use indexer::scheduler::{ScheduledTask, ScheduledTaskMetrics};
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

    async fn consume_messages<T: for<'de> Deserialize<'de>>(&self, subject: &str) -> Vec<T> {
        let client = async_nats::connect(format!("nats://{}", self.nats_url))
            .await
            .unwrap();
        let jetstream = async_nats::jetstream::new(client);

        let consumer = jetstream
            .create_consumer_on_stream(
                async_nats::jetstream::consumer::pull::Config {
                    filter_subject: subject.into(),
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

    async fn create_stream(url: &str) {
        let client = async_nats::connect(format!("nats://{url}")).await.unwrap();
        let jetstream = async_nats::jetstream::new(client);

        jetstream
            .create_stream(async_nats::jetstream::stream::Config {
                name: INDEXER_STREAM.into(),
                subjects: vec![
                    GLOBAL_INDEXING_SUBJECT.into(),
                    NAMESPACE_INDEXING_SUBJECT_PATTERN.into(),
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

    let services = indexer::scheduler::connect(&context.nats_config())
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
        )),
        Box::new(NamespaceDispatcher::new(
            services.nats,
            datalake,
            metrics,
            NamespaceDispatcherConfig::default(),
        )),
    ];

    let before = Utc::now();
    indexer::scheduler::run_once(&tasks, &*lock_service)
        .await
        .unwrap();
    let after = Utc::now();

    // Global indexing request
    let global = context.consume_global_requests().await;
    assert_eq!(global.len(), 1);
    assert!(global[0].watermark >= before && global[0].watermark <= after);

    // Namespace indexing requests
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
