use std::sync::Arc;

use async_trait::async_trait;
use clickhouse_client::ClickHouseConfigurationExt;
use futures::StreamExt;
use gkg_server_config::{GlobalDispatcherConfig, NamespaceDispatcherConfig, NatsConfiguration};
use indexer::campaign::CampaignState;
use indexer::checkpoint::ClickHouseCheckpointStore;
use indexer::nats::versioning::NATS_VERSIONER;
use indexer::orchestrator::dispatch::{CodeBackfill, NamespaceIndexingDispatch};
use indexer::orchestrator::scheduled::{
    GlobalDispatcher, NamespaceDispatcher, ScheduledTask, ScheduledTaskMetrics,
};
use indexer::orchestrator::siphon::{CdcContext, EnabledNamespacesRoute, Route};
use indexer::topic::{
    CODE_INDEXING_TASK_SUBJECT_PATTERN, GLOBAL_INDEXING_SUBJECT, INDEXER_STREAM,
    NAMESPACE_INDEXING_SUBJECT_PATTERN,
};
use integration_testkit::TestContext;
use integration_testkit::scenario::{
    CdcEvent, CdcOperation, DispatchedMessage, HandlerInput, ScenarioHandlers,
};
use siphon_proto::replication_event::{Column, Operation};
use siphon_proto::{LogicalReplicationEvents, ReplicationEvent, Value, value};
use testcontainers::ImageExt;
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::nats::{Nats, NatsServerCmd};

/// Dispatchers publish to a fixed versioned stream, so the shared NATS container
/// can't host concurrent scenarios: `serial` runs them one at a time and each run
/// recreates the stream for a clean slate.
pub struct DispatchScenarioHandlers {
    nats_url: String,
    serial: tokio::sync::Mutex<()>,
}

impl DispatchScenarioHandlers {
    pub fn new(nats_url: String) -> Self {
        Self {
            nats_url,
            serial: tokio::sync::Mutex::new(()),
        }
    }
}

#[async_trait]
impl ScenarioHandlers for DispatchScenarioHandlers {
    async fn run(
        &self,
        ctx: &TestContext,
        handler: &str,
        input: HandlerInput<'_>,
    ) -> Vec<DispatchedMessage> {
        let _serial = self.serial.lock().await;
        recreate_stream(&self.nats_url).await;
        match handler {
            "dispatch_namespace" => run_namespace_dispatcher(ctx, &self.nats_url).await,
            "dispatch_global" => run_global_dispatcher(&self.nats_url).await,
            "dispatch_enabled_namespace_cdc" => {
                dispatch_enabled_namespace_cdc(ctx, &self.nats_url, input.cdc).await
            }
            other => panic!("unknown dispatch scenario handler '{other}'"),
        }
    }
}

pub async fn start_nats() -> (testcontainers::ContainerAsync<Nats>, String) {
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

async fn run_namespace_dispatcher(ctx: &TestContext, nats_url: &str) -> Vec<DispatchedMessage> {
    let services = indexer::orchestrator::scheduled::connect(&nats_config(nats_url))
        .await
        .unwrap();
    let checkpoint_store = Arc::new(ClickHouseCheckpointStore::new(Arc::new(
        ctx.config.build_client(),
    )));
    let ontology = ontology::Ontology::load_embedded().unwrap();
    let dispatcher = NamespaceDispatcher::new(
        services.nats,
        ctx.config.build_client(),
        checkpoint_store,
        ScheduledTaskMetrics::new(),
        NamespaceDispatcherConfig::default(),
        Arc::new(indexer::campaign::CampaignState::new()),
        &ontology,
    );

    dispatcher.run().await.unwrap();

    drain(nats_url, NAMESPACE_INDEXING_SUBJECT_PATTERN, "namespace").await
}

async fn run_global_dispatcher(nats_url: &str) -> Vec<DispatchedMessage> {
    let services = indexer::orchestrator::scheduled::connect(&nats_config(nats_url))
        .await
        .unwrap();
    let dispatcher = GlobalDispatcher::new(
        services.nats,
        ScheduledTaskMetrics::new(),
        GlobalDispatcherConfig::default(),
        Arc::new(CampaignState::new()),
    );

    dispatcher.run().await.unwrap();

    drain(nats_url, GLOBAL_INDEXING_SUBJECT, "global").await
}

async fn dispatch_enabled_namespace_cdc(
    ctx: &TestContext,
    nats_url: &str,
    cdc: &[CdcEvent],
) -> Vec<DispatchedMessage> {
    let services = indexer::orchestrator::scheduled::connect(&nats_config(nats_url))
        .await
        .unwrap();
    let backfill = Arc::new(CodeBackfill::new(
        services.nats.clone(),
        ctx.config.build_client(),
        ctx.config.build_client(),
        ScheduledTaskMetrics::new(),
        Arc::new(CampaignState::new()),
    ));
    let route =
        EnabledNamespacesRoute::new(NamespaceIndexingDispatch::new(services.nats), backfill);
    let events: Vec<LogicalReplicationEvents> = cdc.iter().map(replication_event).collect();

    route
        .dispatch(
            &CdcContext {
                dispatch_id: uuid::Uuid::new_v4(),
                campaign_id: None,
            },
            &events,
        )
        .await
        .unwrap();

    let mut drained = drain(nats_url, NAMESPACE_INDEXING_SUBJECT_PATTERN, "namespace").await;
    drained.extend(drain(nats_url, CODE_INDEXING_TASK_SUBJECT_PATTERN, "code_task").await);
    drained
}

fn replication_event(event: &CdcEvent) -> LogicalReplicationEvents {
    let operation = match event.operation {
        CdcOperation::Insert => Operation::Insert,
        CdcOperation::Update => Operation::Update,
        CdcOperation::Delete => Operation::Delete,
    };
    let columns: Vec<String> = event
        .rows
        .first()
        .expect("cdc event has at least one row")
        .keys()
        .cloned()
        .collect();
    let events = event
        .rows
        .iter()
        .map(|row| ReplicationEvent {
            operation: operation as i32,
            columns: columns
                .iter()
                .enumerate()
                .map(|(index, name)| Column {
                    column_index: index as u32,
                    value: Some(proto_value(&row[name])),
                })
                .collect(),
        })
        .collect();

    LogicalReplicationEvents {
        event: 1,
        table: event.table.clone(),
        schema: "public".to_string(),
        application_identifier: "test".to_string(),
        columns,
        events,
        version_hash: 0,
    }
}

fn proto_value(value: &serde_yaml::Value) -> Value {
    let inner = match value {
        serde_yaml::Value::Number(n) if n.is_i64() => value::Value::Int64Value(n.as_i64().unwrap()),
        serde_yaml::Value::String(s) => value::Value::StringValue(s.clone()),
        other => panic!("cdc column value must be an integer or string, got {other:?}"),
    };
    Value { value: Some(inner) }
}

async fn recreate_stream(nats_url: &str) {
    let client = async_nats::connect(format!("nats://{nats_url}"))
        .await
        .unwrap();
    let jetstream = async_nats::jetstream::new(client);
    let stream = NATS_VERSIONER.stream(INDEXER_STREAM);
    let _ = jetstream.delete_stream(&stream).await;
    jetstream
        .create_stream(async_nats::jetstream::stream::Config {
            name: stream,
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

async fn drain(nats_url: &str, subject: &str, kind: &str) -> Vec<DispatchedMessage> {
    let client = async_nats::connect(format!("nats://{nats_url}"))
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
    let mut drained = Vec::new();
    while let Some(Ok(msg)) = messages.next().await {
        drained.push(DispatchedMessage {
            kind: kind.to_string(),
            payload: serde_json::from_slice(&msg.payload).unwrap(),
        });
        msg.ack().await.unwrap();
    }
    drained
}

fn nats_config(nats_url: &str) -> NatsConfiguration {
    NatsConfiguration {
        url: nats_url.to_string(),
        ..Default::default()
    }
}
