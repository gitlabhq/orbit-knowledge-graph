//! Integration tests for the dispatcher.

use super::common;

use std::collections::HashSet;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use clickhouse_client::ClickHouseConfigurationExt;
use common::TestContext as ClickHouseContext;
use futures::StreamExt;
use gkg_server_config::{GlobalDispatcherConfig, NamespaceDispatcherConfig, NatsConfiguration};
use indexer::nats::versioning::NATS_VERSIONER;
use indexer::orchestrator::dispatch::{CodeBackfill, NamespaceIndexingDispatch};
use indexer::orchestrator::scheduled::{GlobalDispatcher, NamespaceDispatcher};
use indexer::orchestrator::scheduled::{ScheduledTask, ScheduledTaskMetrics};
use indexer::orchestrator::siphon::{CdcContext, EnabledNamespacesRoute, Route};
use indexer::topic::{
    CODE_INDEXING_TASK_SUBJECT_PATTERN, CodeIndexingTaskRequest, GLOBAL_INDEXING_SUBJECT,
    INDEXER_STREAM, NAMESPACE_INDEXING_SUBJECT_PATTERN,
};
use serde::Deserialize;
use siphon_proto::replication_event::{Column, Operation};
use siphon_proto::{LogicalReplicationEvents, ReplicationEvent, Value, value};
use testcontainers::ImageExt;
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::nats::{Nats, NatsServerCmd};

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

    async fn given_project_path(&self, project_id: i64, traversal_path: &str) {
        self.clickhouse
            .execute(&format!(
                "INSERT INTO project_namespace_traversal_paths (id, traversal_path) \
                 VALUES ({project_id}, '{traversal_path}')"
            ))
            .await;
    }

    async fn dispatch_enabled_namespace_cdc(
        &self,
        namespace: Namespace,
    ) -> (Vec<NamespaceRequest>, Vec<CodeIndexingTaskRequest>) {
        let services = indexer::orchestrator::scheduled::connect(&self.nats_config())
            .await
            .unwrap();
        let backfill = Arc::new(CodeBackfill::new(
            services.nats.clone(),
            self.clickhouse.config.build_client(),
            self.clickhouse.config.build_client(),
            ScheduledTaskMetrics::new(),
            Arc::new(indexer::campaign::CampaignState::new()),
        ));
        let route =
            EnabledNamespacesRoute::new(NamespaceIndexingDispatch::new(services.nats), backfill);

        route
            .dispatch(
                &CdcContext {
                    dispatch_id: uuid::Uuid::new_v4(),
                    campaign_id: None,
                },
                &[enabled_namespace_insert(namespace)],
            )
            .await
            .unwrap();

        (
            self.consume_namespace_requests().await,
            self.consume_code_indexing_requests().await,
        )
    }

    async fn consume_global_requests(&self) -> Vec<GlobalRequest> {
        self.consume_messages(GLOBAL_INDEXING_SUBJECT).await
    }

    async fn consume_namespace_requests(&self) -> Vec<NamespaceRequest> {
        self.consume_messages(NAMESPACE_INDEXING_SUBJECT_PATTERN)
            .await
    }

    async fn consume_code_indexing_requests(&self) -> Vec<CodeIndexingTaskRequest> {
        self.consume_messages(CODE_INDEXING_TASK_SUBJECT_PATTERN)
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
                    filter_subject: NATS_VERSIONER.subject(subject),
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

    async fn create_stream(url: &str) {
        let client = async_nats::connect(format!("nats://{url}")).await.unwrap();
        let jetstream = async_nats::jetstream::new(client);

        jetstream
            .create_stream(async_nats::jetstream::stream::Config {
                name: NATS_VERSIONER.stream(INDEXER_STREAM),
                subjects: vec![
                    NATS_VERSIONER.subject(GLOBAL_INDEXING_SUBJECT),
                    NATS_VERSIONER.subject(NAMESPACE_INDEXING_SUBJECT_PATTERN),
                    NATS_VERSIONER.subject(CODE_INDEXING_TASK_SUBJECT_PATTERN),
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

fn namespace(id: i64, traversal_path: &str) -> Namespace {
    Namespace {
        id,
        traversal_path: traversal_path.to_string(),
    }
}

fn enabled_namespace_insert(namespace: Namespace) -> LogicalReplicationEvents {
    LogicalReplicationEvents {
        event: 1,
        table: "knowledge_graph_enabled_namespaces".to_string(),
        schema: "public".to_string(),
        application_identifier: "test".to_string(),
        columns: vec![
            "root_namespace_id".to_string(),
            "traversal_path".to_string(),
        ],
        events: vec![ReplicationEvent {
            operation: Operation::Insert as i32,
            columns: vec![
                Column {
                    column_index: 0,
                    value: Some(Value {
                        value: Some(value::Value::Int64Value(namespace.id)),
                    }),
                },
                Column {
                    column_index: 1,
                    value: Some(Value {
                        value: Some(value::Value::StringValue(namespace.traversal_path)),
                    }),
                },
            ],
        }],
        version_hash: 0,
    }
}

#[tokio::test]
async fn dispatcher_publishes_global_and_namespace_requests() {
    let context = TestContext::new().await;

    context
        .given_enabled_namespaces([
            namespace(100, "1/100/"),
            namespace(200, "2/200/"),
            namespace(300, "3/300/"),
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

#[tokio::test]
async fn enabled_namespace_cdc_dispatches_sdlc_and_code_requests() {
    let context = TestContext::new().await;

    context.given_project_path(7000, "7/700/7000/").await;

    let (namespaces, code) = context
        .dispatch_enabled_namespace_cdc(namespace(700, "7/700/"))
        .await;

    assert_eq!(namespaces.len(), 1);
    assert_eq!(namespaces[0].namespace, 700);
    assert_eq!(namespaces[0].traversal_path, "7/700/");

    assert_eq!(code.len(), 1);
    assert_eq!(code[0].project_id, 7000);
    assert_eq!(code[0].traversal_path, "7/700/7000/");
}
