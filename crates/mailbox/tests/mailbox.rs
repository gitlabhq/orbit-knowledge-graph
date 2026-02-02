//! Integration tests for the mailbox module.
//!
//! These tests verify the full message flow: NATS -> MailboxHandler -> ClickHouse.
//!
//! ## Requirements
//!
//! - Docker-compatible runtime (Docker Desktop, Colima, OrbStack, etc.)
//! - Docker socket accessible at `/var/run/docker.sock` (or configure via `DOCKER_HOST`)
//!
//! ## Running the tests
//!
//! ```sh
//! cargo test -p mailbox --test mailbox -- --test-threads=1
//! ```
//!
//! Tests run sequentially via `--test-threads=1` and `#[serial]` to avoid port conflicts.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, BooleanArray, Int64Array, StringArray};
use etl_engine::clickhouse::{
    ArrowClickHouseClient, ClickHouseConfiguration, ClickHouseDestination,
};
use etl_engine::configuration::EngineConfiguration;
use etl_engine::engine::{Engine, EngineBuilder};
use etl_engine::module::{Module, ModuleRegistry};
use etl_engine::nats::{NatsBroker, NatsConfiguration};
use etl_engine::types::{Envelope, Event};
use mailbox::MailboxModule;
use mailbox::handler::MAILBOX_STREAM;
use mailbox::storage::{PluginStore, TraversalPathResolver};
use mailbox::types::{
    EdgeDefinition, EdgePayload, MailboxMessage, NodeDefinition, NodePayload, NodeReference,
    Plugin, PluginSchema, PropertyDefinition, PropertyType,
};
use serde_json::json;
use serial_test::serial;
use testcontainers::GenericImage;
use testcontainers::core::{ContainerPort, ImageExt};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::nats::{Nats, NatsServerCmd};

const MAILBOX_SUBJECT: &str = "mailbox.messages";
const CLICKHOUSE_IMAGE: &str = "clickhouse/clickhouse-server";
const CLICKHOUSE_TAG: &str = "25.12";
const USERNAME: &str = "default";
const PASSWORD: &str = "testpass";
const DATABASE: &str = "test";

const PLUGIN_ID: &str = "security-scanner";
const NAMESPACE_ID: i64 = 42;
const ORGANIZATION_ID: i64 = 1;

struct TestContext {
    _nats_container: testcontainers::ContainerAsync<Nats>,
    _clickhouse_container: testcontainers::ContainerAsync<GenericImage>,
    nats_url: String,
    clickhouse_endpoint: String,
    clickhouse_client: Arc<ArrowClickHouseClient>,
}

impl TestContext {
    async fn new() -> Self {
        let (nats_container, nats_url) = Self::start_nats().await;
        let (clickhouse_container, clickhouse_endpoint) = Self::start_clickhouse().await;
        let clickhouse_client = Self::create_clickhouse_client(&clickhouse_endpoint);

        Self::create_nats_stream(&nats_url).await;
        Self::setup_database_tables(&clickhouse_client).await;

        Self {
            _nats_container: nats_container,
            _clickhouse_container: clickhouse_container,
            nats_url,
            clickhouse_endpoint,
            clickhouse_client,
        }
    }

    async fn start_nats() -> (testcontainers::ContainerAsync<Nats>, String) {
        let container = Nats::default()
            .with_cmd(&NatsServerCmd::default().with_jetstream())
            .start()
            .await
            .expect("failed to start NATS container");

        let host = container.get_host().await.expect("failed to get host");
        let port = container
            .get_host_port_ipv4(4222)
            .await
            .expect("failed to get port");

        (container, format!("{host}:{port}"))
    }

    async fn start_clickhouse() -> (testcontainers::ContainerAsync<GenericImage>, String) {
        let http_port = ContainerPort::Tcp(8123);

        let container = GenericImage::new(CLICKHOUSE_IMAGE, CLICKHOUSE_TAG)
            .with_exposed_port(http_port)
            .with_env_var("CLICKHOUSE_USER", USERNAME)
            .with_env_var("CLICKHOUSE_PASSWORD", PASSWORD)
            .with_env_var("CLICKHOUSE_DB", DATABASE)
            .start()
            .await
            .expect("failed to start ClickHouse container");

        let host = container.get_host().await.expect("failed to get host");
        let port = container
            .get_host_port_ipv4(http_port)
            .await
            .expect("failed to get port");

        let host = if host.to_string() == "localhost" {
            "127.0.0.1".to_string()
        } else {
            host.to_string()
        };

        (container, format!("http://{host}:{port}"))
    }

    fn create_clickhouse_client(endpoint: &str) -> Arc<ArrowClickHouseClient> {
        Arc::new(ArrowClickHouseClient::new(
            endpoint,
            DATABASE,
            USERNAME,
            Some(PASSWORD),
        ))
    }

    async fn create_nats_stream(url: &str) {
        let client = async_nats::connect(format!("nats://{url}"))
            .await
            .expect("failed to connect to NATS");

        async_nats::jetstream::new(client)
            .create_stream(async_nats::jetstream::stream::Config {
                name: MAILBOX_STREAM.to_string(),
                subjects: vec![format!("{MAILBOX_SUBJECT}.>"), MAILBOX_SUBJECT.to_string()],
                ..Default::default()
            })
            .await
            .expect("failed to create stream");
    }

    async fn setup_database_tables(client: &ArrowClickHouseClient) {
        Self::wait_for_clickhouse(client).await;

        client
            .execute(mailbox::storage::plugins_table_ddl())
            .await
            .expect("failed to create plugins table");

        client
            .execute(mailbox::storage::migrations_table_ddl())
            .await
            .expect("failed to create migrations table");

        client
            .execute(
                r#"CREATE TABLE IF NOT EXISTS gl_groups (
                    id Int64,
                    organization_id Int64,
                    name String,
                    _version DateTime64(6, 'UTC') DEFAULT now64(6),
                    _deleted Bool DEFAULT false
                ) ENGINE = ReplacingMergeTree(_version, _deleted)
                ORDER BY id"#,
            )
            .await
            .expect("failed to create gl_groups table");

        client
            .execute(
                r#"CREATE TABLE IF NOT EXISTS gl_edges (
                    id Int64,
                    relationship_kind String,
                    source_id Int64,
                    source_kind String,
                    target_id Int64,
                    target_kind String,
                    traversal_path String,
                    _version DateTime64(6, 'UTC') DEFAULT now64(6),
                    _deleted Bool DEFAULT false
                ) ENGINE = ReplacingMergeTree(_version, _deleted)
                ORDER BY (traversal_path, id)"#,
            )
            .await
            .expect("failed to create gl_edges table");

        client
            .execute(&format!(
                "INSERT INTO gl_groups (id, organization_id, name) VALUES ({}, {}, 'test-namespace')",
                NAMESPACE_ID, ORGANIZATION_ID
            ))
            .await
            .expect("failed to insert test namespace");
    }

    async fn wait_for_clickhouse(client: &ArrowClickHouseClient) {
        for attempt in 1..=30 {
            match client.execute("SELECT 1").await {
                Ok(_) => return,
                Err(error) if attempt == 30 => {
                    panic!("failed to connect to ClickHouse after 30 attempts: {error}")
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(500)).await,
            }
        }
    }

    async fn create_broker(&self) -> Arc<NatsBroker> {
        Arc::new(
            NatsBroker::connect(&NatsConfiguration {
                url: self.nats_url.clone(),
                ..Default::default()
            })
            .await
            .expect("failed to connect to NATS"),
        )
    }

    fn create_destination(&self) -> Arc<ClickHouseDestination> {
        Arc::new(
            ClickHouseDestination::new(ClickHouseConfiguration {
                database: DATABASE.to_string(),
                url: self.clickhouse_endpoint.clone(),
                username: USERNAME.to_string(),
                password: Some(PASSWORD.to_string()),
            })
            .expect("failed to create destination"),
        )
    }

    fn create_plugin_store(&self) -> Arc<PluginStore> {
        Arc::new(PluginStore::new(self.clickhouse_client.clone()))
    }

    fn create_traversal_resolver(&self) -> Arc<TraversalPathResolver> {
        Arc::new(TraversalPathResolver::new(self.clickhouse_client.clone()))
    }

    async fn setup_plugin_with_table(&self) -> Plugin {
        let schema = PluginSchema::new()
            .with_node(
                NodeDefinition::new("security_scanner_Vulnerability")
                    .with_property(PropertyDefinition::new("severity", PropertyType::String))
                    .with_property(PropertyDefinition::new("score", PropertyType::Float).nullable())
                    .with_property(
                        PropertyDefinition::new("cve_id", PropertyType::String).nullable(),
                    ),
            )
            .with_edge(
                EdgeDefinition::new("security_scanner_AFFECTS")
                    .from_kinds(vec!["security_scanner_Vulnerability".into()])
                    .to_kinds(vec!["Project".into()]),
            );

        let plugin = Plugin::new(PLUGIN_ID, NAMESPACE_ID, "test-hash", schema);

        self.create_plugin_store()
            .insert(&plugin)
            .await
            .expect("failed to insert plugin");

        let node = plugin
            .schema
            .get_node("security_scanner_Vulnerability")
            .unwrap();
        let ddl = mailbox::schema_generator::generate_create_table_ddl(&plugin, node);
        self.clickhouse_client
            .execute(&ddl)
            .await
            .expect("failed to create plugin node table");

        plugin
    }

    async fn publish_message(&self, broker: &NatsBroker, message: &MailboxMessage) {
        let envelope = Envelope::new(message).expect("failed to create envelope");
        broker
            .publish(&mailbox::types::MailboxMessage::topic(), &envelope)
            .await
            .expect("failed to publish");
    }

    async fn query_node_count(&self, table_name: &str) -> u64 {
        let batches = self
            .clickhouse_client
            .query_arrow(&format!(
                "SELECT count() FROM {} FINAL WHERE NOT _deleted",
                table_name
            ))
            .await
            .expect("query failed");

        if batches.is_empty() || batches[0].num_rows() == 0 {
            return 0;
        }

        use arrow::array::UInt64Array;
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("expected UInt64Array")
            .value(0)
    }

    async fn query_nodes(&self, table_name: &str) -> Vec<NodeRow> {
        let batches = self
            .clickhouse_client
            .query_arrow(&format!(
                "SELECT id, traversal_path, severity, score, cve_id, _deleted FROM {} FINAL ORDER BY id",
                table_name
            ))
            .await
            .expect("query failed");

        let mut nodes = Vec::new();
        for batch in batches {
            for row in 0..batch.num_rows() {
                let id = batch
                    .column_by_name("id")
                    .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
                    .map(|a| a.value(row))
                    .expect("missing id column");

                let traversal_path = batch
                    .column_by_name("traversal_path")
                    .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                    .map(|a| a.value(row).to_string())
                    .expect("missing traversal_path column");

                let severity = batch
                    .column_by_name("severity")
                    .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                    .map(|a| a.value(row).to_string())
                    .expect("missing severity column");

                let deleted = batch
                    .column_by_name("_deleted")
                    .and_then(|c| c.as_any().downcast_ref::<BooleanArray>())
                    .map(|a| a.value(row))
                    .expect("missing _deleted column");

                nodes.push(NodeRow {
                    id,
                    traversal_path,
                    severity,
                    deleted,
                });
            }
        }

        nodes
    }

    async fn query_edge_count(&self) -> u64 {
        let batches = self
            .clickhouse_client
            .query_arrow("SELECT count() FROM gl_edges FINAL WHERE NOT _deleted")
            .await
            .expect("query failed");

        if batches.is_empty() || batches[0].num_rows() == 0 {
            return 0;
        }

        use arrow::array::UInt64Array;
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("expected UInt64Array")
            .value(0)
    }

    async fn query_edges(&self) -> Vec<EdgeRow> {
        let batches = self
            .clickhouse_client
            .query_arrow(
                "SELECT id, relationship_kind, source_id, source_kind, target_id, target_kind, traversal_path FROM gl_edges FINAL ORDER BY id",
            )
            .await
            .expect("query failed");

        let mut edges = Vec::new();
        for batch in batches {
            for row in 0..batch.num_rows() {
                let id = batch
                    .column_by_name("id")
                    .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
                    .map(|a| a.value(row))
                    .expect("missing id column");

                let relationship_kind = batch
                    .column_by_name("relationship_kind")
                    .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                    .map(|a| a.value(row).to_string())
                    .expect("missing relationship_kind column");

                let source_kind = batch
                    .column_by_name("source_kind")
                    .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                    .map(|a| a.value(row).to_string())
                    .expect("missing source_kind column");

                let target_kind = batch
                    .column_by_name("target_kind")
                    .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                    .map(|a| a.value(row).to_string())
                    .expect("missing target_kind column");

                edges.push(EdgeRow {
                    id,
                    relationship_kind,
                    source_kind,
                    target_kind,
                });
            }
        }

        edges
    }
}

#[derive(Debug)]
#[allow(dead_code)]
struct NodeRow {
    id: i64,
    traversal_path: String,
    severity: String,
    deleted: bool,
}

#[derive(Debug)]
#[allow(dead_code)]
struct EdgeRow {
    id: i64,
    relationship_kind: String,
    source_kind: String,
    target_kind: String,
}

fn create_engine(
    broker: Arc<NatsBroker>,
    destination: Arc<ClickHouseDestination>,
    module: &dyn Module,
) -> Arc<Engine> {
    let registry = Arc::new(ModuleRegistry::default());
    registry.register_module(module);
    Arc::new(EngineBuilder::new(broker, registry, destination).build())
}

async fn run_engine_for(engine: Arc<Engine>, duration: Duration) {
    let engine_handle = engine.clone();
    let task = tokio::spawn(async move {
        engine_handle
            .run(&EngineConfiguration::default())
            .await
            .expect("engine failed");
    });

    tokio::time::sleep(duration).await;
    engine.stop();
    task.await.expect("engine task panicked");
}

#[tokio::test]
#[serial]
async fn single_node_ingestion() {
    let context = TestContext::new().await;
    let plugin = context.setup_plugin_with_table().await;
    let broker = context.create_broker().await;
    let destination = context.create_destination();

    let mailbox_module = MailboxModule::new(
        context.create_plugin_store(),
        context.create_traversal_resolver(),
    );

    let engine = create_engine(broker.clone(), destination, &mailbox_module);

    let message = MailboxMessage::new("msg-001", PLUGIN_ID).with_node(
        NodePayload::new("vuln-001", "security_scanner_Vulnerability")
            .with_properties(json!({"severity": "high", "score": 8.5, "cve_id": "CVE-2024-001"})),
    );

    context.publish_message(&broker, &message).await;
    run_engine_for(engine, Duration::from_secs(2)).await;

    let table_name = plugin.table_name_for_node("security_scanner_Vulnerability");
    assert_eq!(context.query_node_count(&table_name).await, 1);

    let nodes = context.query_nodes(&table_name).await;
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].severity, "high");
    assert_eq!(
        nodes[0].traversal_path,
        format!("{}/{}", ORGANIZATION_ID, NAMESPACE_ID)
    );
    assert!(!nodes[0].deleted);
}

#[tokio::test]
#[serial]
async fn multiple_nodes_ingestion() {
    let context = TestContext::new().await;
    let plugin = context.setup_plugin_with_table().await;
    let broker = context.create_broker().await;
    let destination = context.create_destination();

    let mailbox_module = MailboxModule::new(
        context.create_plugin_store(),
        context.create_traversal_resolver(),
    );

    let engine = create_engine(broker.clone(), destination, &mailbox_module);

    let message = MailboxMessage::new("msg-002", PLUGIN_ID)
        .with_node(
            NodePayload::new("vuln-001", "security_scanner_Vulnerability")
                .with_properties(json!({"severity": "high"})),
        )
        .with_node(
            NodePayload::new("vuln-002", "security_scanner_Vulnerability")
                .with_properties(json!({"severity": "medium"})),
        )
        .with_node(
            NodePayload::new("vuln-003", "security_scanner_Vulnerability")
                .with_properties(json!({"severity": "low"})),
        );

    context.publish_message(&broker, &message).await;
    run_engine_for(engine, Duration::from_secs(2)).await;

    let table_name = plugin.table_name_for_node("security_scanner_Vulnerability");
    assert_eq!(context.query_node_count(&table_name).await, 3);
}

#[tokio::test]
#[serial]
async fn edge_ingestion() {
    let context = TestContext::new().await;
    let plugin = context.setup_plugin_with_table().await;
    let broker = context.create_broker().await;
    let destination = context.create_destination();

    let mailbox_module = MailboxModule::new(
        context.create_plugin_store(),
        context.create_traversal_resolver(),
    );

    let engine = create_engine(broker.clone(), destination, &mailbox_module);

    let message = MailboxMessage::new("msg-003", PLUGIN_ID)
        .with_node(
            NodePayload::new("vuln-001", "security_scanner_Vulnerability")
                .with_properties(json!({"severity": "high"})),
        )
        .with_edge(EdgePayload::new(
            "edge-001",
            "security_scanner_AFFECTS",
            NodeReference::new("security_scanner_Vulnerability", "vuln-001"),
            NodeReference::new("Project", "100"),
        ));

    context.publish_message(&broker, &message).await;
    run_engine_for(engine, Duration::from_secs(2)).await;

    let table_name = plugin.table_name_for_node("security_scanner_Vulnerability");
    assert_eq!(context.query_node_count(&table_name).await, 1);
    assert_eq!(context.query_edge_count().await, 1);

    let edges = context.query_edges().await;
    assert_eq!(edges.len(), 1);
    assert_eq!(edges[0].relationship_kind, "security_scanner_AFFECTS");
    assert_eq!(edges[0].source_kind, "security_scanner_Vulnerability");
    assert_eq!(edges[0].target_kind, "Project");
}

#[tokio::test]
#[serial]
async fn multiple_messages_processed() {
    let context = TestContext::new().await;
    let plugin = context.setup_plugin_with_table().await;
    let broker = context.create_broker().await;
    let destination = context.create_destination();

    let mailbox_module = MailboxModule::new(
        context.create_plugin_store(),
        context.create_traversal_resolver(),
    );

    let engine = create_engine(broker.clone(), destination, &mailbox_module);

    for i in 1..=5 {
        let message = MailboxMessage::new(format!("msg-{:03}", i), PLUGIN_ID).with_node(
            NodePayload::new(format!("vuln-{:03}", i), "security_scanner_Vulnerability")
                .with_properties(json!({"severity": "medium"})),
        );
        context.publish_message(&broker, &message).await;
    }

    run_engine_for(engine, Duration::from_secs(3)).await;

    let table_name = plugin.table_name_for_node("security_scanner_Vulnerability");
    assert_eq!(context.query_node_count(&table_name).await, 5);
}

#[tokio::test]
#[serial]
async fn node_update_via_same_external_id() {
    let context = TestContext::new().await;
    let plugin = context.setup_plugin_with_table().await;
    let broker = context.create_broker().await;
    let destination = context.create_destination();

    let mailbox_module = MailboxModule::new(
        context.create_plugin_store(),
        context.create_traversal_resolver(),
    );

    let engine = create_engine(broker.clone(), destination, &mailbox_module);

    let message1 = MailboxMessage::new("msg-001", PLUGIN_ID).with_node(
        NodePayload::new("vuln-001", "security_scanner_Vulnerability")
            .with_properties(json!({"severity": "low"})),
    );

    let message2 = MailboxMessage::new("msg-002", PLUGIN_ID).with_node(
        NodePayload::new("vuln-001", "security_scanner_Vulnerability")
            .with_properties(json!({"severity": "critical"})),
    );

    context.publish_message(&broker, &message1).await;
    context.publish_message(&broker, &message2).await;
    run_engine_for(engine, Duration::from_secs(2)).await;

    let table_name = plugin.table_name_for_node("security_scanner_Vulnerability");

    let nodes = context.query_nodes(&table_name).await;
    let active_nodes: Vec<_> = nodes.iter().filter(|n| !n.deleted).collect();

    assert_eq!(active_nodes.len(), 1);
    assert_eq!(active_nodes[0].severity, "critical");
}
