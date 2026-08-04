//! Validates the three assumptions the RA bench harness depends on:
//!
//! A. Rows seeded with a backdated `_siphon_watermark` are captured by the
//!    first SDLC backfill window (no checkpoint -> window = (epoch, now]).
//! B. Rows inserted later with a fresh `_siphon_watermark` are picked up by
//!    the NamespaceDispatcher's periodic sweep.
//! C. An enrollment event built with the promoted wire builders, published to
//!    a real NATS JetStream siphon stream, round-trips through the real
//!    Siphon router and dispatches a NamespaceIndexingRequest.

use std::sync::Arc;

use clickhouse_client::ClickHouseConfigurationExt;
use futures::StreamExt;
use gkg_server_config::{NatsConfiguration, NamespaceDispatcherConfig, SiphonRouterConfig};
use indexer::campaign::CampaignState;
use indexer::checkpoint::ClickHouseCheckpointStore;
use indexer::nats::versioning::NATS_VERSIONER;
use indexer::orchestrator::dispatch::{CodeBackfill, NamespaceIndexingDispatch};
use indexer::orchestrator::scheduled::{
    NamespaceDispatcher, ScheduledTask, ScheduledTaskMetrics,
};
use indexer::orchestrator::siphon::wire::{
    build_replication_events_for_table, enabled_namespace_columns,
};
use indexer::orchestrator::siphon::{EnabledNamespacesRoute, Route, Siphon};
use indexer::topic::{
    CODE_INDEXING_TASK_SUBJECT_PATTERN, INDEXER_STREAM, NAMESPACE_INDEXING_SUBJECT_PATTERN,
};
use testcontainers::ImageExt;
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::nats::{Nats, NatsServerCmd};

use super::common;
use common::TestContext as ClickHouseContext;

const SIPHON_STREAM: &str = "siphon_stream_main_db";

struct Ctx {
    ch: ClickHouseContext,
    _nats: testcontainers::ContainerAsync<Nats>,
    nats_url: String,
}

impl Ctx {
    async fn new() -> Self {
        let ch =
            ClickHouseContext::new(&[common::SIPHON_SCHEMA_SQL, *common::GRAPH_SCHEMA_SQL]).await;
        let (nats, nats_url) = start_nats().await;
        create_streams(&nats_url).await;
        Self {
            ch,
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
    let js = async_nats::jetstream::new(client);

    let stream = NATS_VERSIONER.stream(INDEXER_STREAM);
    let _ = js.delete_stream(&stream).await;
    js.create_stream(async_nats::jetstream::stream::Config {
        name: stream,
        subjects: vec![
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

    js.create_stream(async_nats::jetstream::stream::Config {
        name: SIPHON_STREAM.into(),
        subjects: vec![format!("{SIPHON_STREAM}.>")],
        ..Default::default()
    })
    .await
    .unwrap();
}

async fn drain_namespace_requests(nats_url: &str) -> Vec<serde_json::Value> {
    let client = async_nats::connect(format!("nats://{nats_url}"))
        .await
        .unwrap();
    let js = async_nats::jetstream::new(client);
    let consumer = js
        .create_consumer_on_stream(
            async_nats::jetstream::consumer::pull::Config {
                filter_subject: NATS_VERSIONER.subject(NAMESPACE_INDEXING_SUBJECT_PATTERN),
                ..Default::default()
            },
            &NATS_VERSIONER.stream(INDEXER_STREAM),
        )
        .await
        .unwrap();
    let mut msgs = consumer.fetch().max_messages(100).messages().await.unwrap();
    let mut out = Vec::new();
    while let Some(Ok(msg)) = msgs.next().await {
        out.push(serde_json::from_slice(&msg.payload).unwrap());
        msg.ack().await.unwrap();
    }
    out
}

/// Assertion A: backdated watermark rows are captured by the first enrollment
/// backfill window because no checkpoint exists (window lower bound = epoch).
#[tokio::test]
async fn backdated_watermark_captured_by_first_backfill() {
    let ctx = Ctx::new().await;

    common::create_namespace(&ctx.ch, 100, None, 20, "1/100/").await;
    common::create_project(&ctx.ch, 10, 100, 1, 20, "1/100/10/").await;

    ctx.ch
        .execute(
            "INSERT INTO siphon_knowledge_graph_enabled_namespaces \
             (id, root_namespace_id, traversal_path, created_at, updated_at) \
             VALUES (1, 100, '1/100/', now(), now())",
        )
        .await;

    // Seed project rows with a heavily backdated watermark.
    ctx.ch
        .execute(
            "INSERT INTO siphon_projects \
             (id, name, description, visibility_level, path, namespace_id, creator_id, \
              created_at, updated_at, archived, star_count, last_activity_at, \
              _siphon_replicated_at, _siphon_watermark) \
             VALUES (10, 'p', NULL, 20, 'p', 100, 1, \
                     '2023-01-01', '2023-01-01', false, 0, '2023-01-01', \
                     '2020-01-01 00:00:00', '2020-01-01 00:00:00')",
        )
        .await;

    let handler = common::namespace_handler(&ctx.ch).await;
    let envelope = common::namespace_envelope(1, 100);
    handler
        .handle(common::handler_context(), envelope)
        .await
        .unwrap();

    // The handler should have indexed the backdated row into the graph.
    common::assert_node_count(&ctx.ch, "gl_project", 1).await;
}

/// Assertion B: the NamespaceDispatcher picks up rows inserted with a fresh
/// _siphon_watermark via its periodic datalake sweep.
#[tokio::test]
async fn fresh_watermark_picked_up_by_dispatcher_sweep() {
    let ctx = Ctx::new().await;

    common::create_namespace(&ctx.ch, 200, None, 20, "1/200/").await;
    common::create_project(&ctx.ch, 20, 200, 1, 20, "1/200/20/").await;

    ctx.ch
        .execute(
            "INSERT INTO siphon_knowledge_graph_enabled_namespaces \
             (id, root_namespace_id, traversal_path, created_at, updated_at) \
             VALUES (2, 200, '1/200/', now(), now())",
        )
        .await;

    let services = indexer::orchestrator::scheduled::connect(&ctx.nats_config())
        .await
        .unwrap();
    let checkpoint_store = Arc::new(ClickHouseCheckpointStore::new(Arc::new(
        ctx.ch.config.build_client(),
    )));
    let ontology = ontology::Ontology::load_embedded().unwrap();
    let dispatcher = NamespaceDispatcher::new(
        services.nats,
        ctx.ch.config.build_client(),
        checkpoint_store,
        ScheduledTaskMetrics::new(),
        NamespaceDispatcherConfig::default(),
        Arc::new(CampaignState::new()),
        &ontology,
    );

    dispatcher.run().await.unwrap();
    let requests = drain_namespace_requests(&ctx.nats_url).await;

    assert!(
        !requests.is_empty(),
        "dispatcher should dispatch at least one request for namespace 200"
    );
    assert_eq!(requests[0]["namespace"], 200);
}

/// Assertion C: an enrollment event built with the promoted wire builders,
/// published to a real NATS siphon stream, is consumed by the real Siphon
/// router and produces a dispatched NamespaceIndexingRequest.
#[tokio::test]
async fn wire_builder_enrollment_roundtrips_through_real_siphon_router() {
    let ctx = Ctx::new().await;

    common::create_namespace(&ctx.ch, 300, None, 20, "1/300/").await;
    common::create_project(&ctx.ch, 30, 300, 1, 20, "1/300/30/").await;

    ctx.ch
        .execute(
            "INSERT INTO siphon_knowledge_graph_enabled_namespaces \
             (id, root_namespace_id, traversal_path, created_at, updated_at) \
             VALUES (3, 300, '1/300/', now(), now())",
        )
        .await;

    let services = indexer::orchestrator::scheduled::connect(&ctx.nats_config())
        .await
        .unwrap();

    let backfill = Arc::new(CodeBackfill::new(
        services.nats.clone(),
        ctx.ch.create_client(),
        ctx.ch.config.build_client(),
        ScheduledTaskMetrics::new(),
        Arc::new(CampaignState::new()),
    ));
    let route: Arc<dyn Route> =
        Arc::new(EnabledNamespacesRoute::new(
            NamespaceIndexingDispatch::new(services.nats.clone()),
            backfill,
        ));
    let siphon = Siphon::new(
        services.nats.clone(),
        ScheduledTaskMetrics::new(),
        SiphonRouterConfig {
            events_stream_name: SIPHON_STREAM.to_string(),
            ..Default::default()
        },
        Arc::new(CampaignState::new()),
        vec![route],
    );

    // First drain creates the durable consumer (DeliverPolicy::New).
    siphon.drain_once().await.unwrap();

    // Now publish the enrollment event using the promoted wire builders.
    let nc = async_nats::connect(format!("nats://{}", ctx.nats_url))
        .await
        .unwrap();
    let js = async_nats::jetstream::new(nc);
    let payload = build_replication_events_for_table(
        "knowledge_graph_enabled_namespaces",
        vec![enabled_namespace_columns(300, "1/300/").build()],
    );
    js.publish(
        format!("{SIPHON_STREAM}.knowledge_graph_enabled_namespaces"),
        payload,
    )
    .await
    .unwrap()
    .await
    .unwrap();

    // Drain again: the router should decode and dispatch.
    let outcome = siphon.drain_once().await.unwrap();
    assert!(
        outcome.dispatched > 0,
        "siphon router should dispatch the enrollment event"
    );

    let requests = drain_namespace_requests(&ctx.nats_url).await;
    assert!(
        !requests.is_empty(),
        "an enrollment request should be on the indexer stream"
    );
    assert_eq!(requests[0]["namespace"], 300);
}
