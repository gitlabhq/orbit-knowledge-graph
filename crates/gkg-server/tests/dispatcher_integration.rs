//! Integration tests for the dispatcher.

mod common;

use std::collections::HashSet;
use std::net::SocketAddr;

use chrono::{DateTime, Utc};
use common::TestContext as ClickHouseContext;
use etl_engine::nats::NatsConfiguration;
use futures::StreamExt;
use gkg_server::config::AppConfig;
use gkg_server::dispatcher;
use gkg_server::indexer::topic::{
    GLOBAL_INDEXING_SUBJECT, INDEXER_STREAM, NAMESPACE_INDEXING_SUBJECT,
};
use serde::Deserialize;
use serial_test::serial;
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::nats::{Nats, NatsServerCmd};

// --- Test Infrastructure ---

struct Namespace {
    id: i64,
    organization_id: i64,
}

#[derive(Debug, Deserialize)]
struct GlobalRequest {
    watermark: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
struct NamespaceRequest {
    organization: i64,
    namespace: i64,
    watermark: DateTime<Utc>,
}

struct TestContext {
    clickhouse: ClickHouseContext,
    _nats: testcontainers::ContainerAsync<Nats>,
    nats_url: String,
}

impl TestContext {
    async fn new() -> Self {
        let clickhouse = ClickHouseContext::new().await;
        let (nats, nats_url) = Self::start_nats().await;
        Self::create_stream(&nats_url).await;
        Self {
            clickhouse,
            _nats: nats,
            nats_url,
        }
    }

    fn app_config(&self) -> AppConfig {
        AppConfig {
            bind_address: SocketAddr::from(([127, 0, 0, 1], 0)),
            grpc_bind_address: SocketAddr::from(([127, 0, 0, 1], 0)),
            jwt_secret: Some("test".into()),
            jwt_clock_skew_secs: 0,
            health_check_url: None,
            nats: NatsConfiguration {
                url: self.nats_url.clone(),
                ..Default::default()
            },
            datalake: self.clickhouse.config.clone(),
            graph: self.clickhouse.config.clone(),
            engine: Default::default(),
        }
    }

    async fn given_namespaces(&self, namespaces: impl IntoIterator<Item = Namespace>) {
        for ns in namespaces {
            self.clickhouse
                .execute(&format!(
                    "INSERT INTO siphon_namespaces (id, name, path, organization_id) \
                     VALUES ({}, 'ns-{}', 'path-{}', {})",
                    ns.id, ns.id, ns.id, ns.organization_id
                ))
                .await;
        }
    }

    async fn given_enabled_namespaces(&self, namespace_ids: impl IntoIterator<Item = i64>) {
        for (i, ns_id) in namespace_ids.into_iter().enumerate() {
            self.clickhouse
                .execute(&format!(
                    "INSERT INTO siphon_knowledge_graph_enabled_namespaces \
                     (id, root_namespace_id, created_at, updated_at) \
                     VALUES ({}, {ns_id}, now(), now())",
                    i + 1
                ))
                .await;
        }
    }

    async fn consume_global_requests(&self) -> Vec<GlobalRequest> {
        self.consume_messages(GLOBAL_INDEXING_SUBJECT).await
    }

    async fn consume_namespace_requests(&self) -> Vec<NamespaceRequest> {
        self.consume_messages(NAMESPACE_INDEXING_SUBJECT).await
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
                    NAMESPACE_INDEXING_SUBJECT.into(),
                ],
                ..Default::default()
            })
            .await
            .unwrap();
    }
}

// --- Tests ---

#[tokio::test]
#[serial]
async fn dispatcher_publishes_global_and_namespace_requests() {
    let context = TestContext::new().await;

    context
        .given_namespaces([
            Namespace {
                id: 100,
                organization_id: 1,
            },
            Namespace {
                id: 200,
                organization_id: 2,
            },
            Namespace {
                id: 300,
                organization_id: 3,
            },
        ])
        .await;

    context.given_enabled_namespaces([100, 200, 300]).await;

    let before = Utc::now();
    dispatcher::run(&context.app_config()).await.unwrap();
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
        .map(|r| (r.namespace, r.organization))
        .collect();
    let expected: HashSet<_> = [(100, 1), (200, 2), (300, 3)].into();
    assert_eq!(actual, expected);

    assert!(
        namespaces
            .iter()
            .all(|r| r.watermark >= before && r.watermark <= after)
    );
}
