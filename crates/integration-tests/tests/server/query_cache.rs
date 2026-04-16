//! Integration tests for the NATS KV query result cache.
//!
//! Proves the full CachedExecutor stage behavior against real NATS and
//! ClickHouse containers:
//!
//! 1. Cache miss -> ClickHouse execution -> result stored in KV
//! 2. Data deleted from ClickHouse
//! 3. Cache hit -> stale result returned from KV (proving cache works)
//! 4. Cache disabled -> ClickHouse execution returns empty (proving bypass)

use std::sync::Arc;

use gkg_server::pipeline::stages::{CachedExecutor, ensure_query_cache_bucket};
use gkg_server_config::NatsConfiguration;
use indexer::nats::NatsBroker;
use ontology::Ontology;
use query_engine::compiler::{SecurityContext, compile};
use query_engine::pipeline::{NoOpObserver, PipelineStage, QueryPipelineContext, TypeMap};
use testcontainers::ImageExt;
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::nats::{Nats, NatsServerCmd};

use crate::common::{GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL, TestContext};

async fn start_nats() -> (testcontainers::ContainerAsync<Nats>, String) {
    let nats_cmd = NatsServerCmd::default().with_jetstream();
    let container = Nats::default()
        .with_cmd(&nats_cmd)
        .with_tag("2.11-alpine")
        .with_mapped_port(0, ContainerPort::Tcp(4222))
        .with_ready_conditions(vec![WaitFor::seconds(3)])
        .start()
        .await
        .expect("failed to start NATS container");

    let host = container
        .get_host()
        .await
        .expect("failed to get container host");
    let port = container
        .get_host_port_ipv4(4222)
        .await
        .expect("failed to get NATS port");

    (container, format!("{host}:{port}"))
}

async fn connect_broker(url: &str) -> NatsBroker {
    let config = NatsConfiguration {
        url: url.to_string(),
        ..Default::default()
    };
    NatsBroker::connect(&config)
        .await
        .expect("failed to connect broker")
}

fn load_ontology() -> Arc<Ontology> {
    Arc::new(integration_testkit::load_ontology())
}

fn test_security_context() -> SecurityContext {
    SecurityContext::new(1, vec!["1/".into()]).expect("valid security context")
}

const SIMPLE_TRAVERSAL: &str = r#"{
    "query_type": "traversal",
    "nodes": [
        {"id": "u", "entity": "User", "columns": ["username"]},
        {"id": "g", "entity": "Group", "columns": ["name"]}
    ],
    "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
    "limit": 20
}"#;

fn build_context(
    ontology: &Arc<Ontology>,
    compiled: Arc<query_engine::compiler::CompiledQueryContext>,
    client: Arc<clickhouse_client::ArrowClickHouseClient>,
    broker: Arc<NatsBroker>,
) -> QueryPipelineContext {
    let mut server_extensions = TypeMap::default();
    server_extensions.insert(client);
    server_extensions.insert(gkg_server_config::ProfilingConfig::default());
    server_extensions.insert(broker);

    QueryPipelineContext {
        query_json: String::new(),
        compiled: Some(compiled),
        ontology: Arc::clone(ontology),
        security_context: None,
        server_extensions,
        phases: TypeMap::default(),
    }
}

#[tokio::test]
async fn cached_executor_miss_then_hit() {
    let graph_sql: &str = &GRAPH_SCHEMA_SQL;
    let schema: &[&str] = &[SIPHON_SCHEMA_SQL, graph_sql];
    let (nats_future, ch_future) = tokio::join!(start_nats(), TestContext::new(schema));
    let (_nats_container, nats_url) = nats_future;
    let ch_ctx = ch_future;

    let broker = Arc::new(connect_broker(&nats_url).await);
    ensure_query_cache_bucket(&broker)
        .await
        .expect("failed to create cache bucket");

    integration_testkit::load_seed(&ch_ctx, "data_correctness").await;
    ch_ctx.optimize_all().await;

    let ontology = load_ontology();
    let security_ctx = test_security_context();
    let mut compiled =
        compile(SIMPLE_TRAVERSAL, &ontology, &security_ctx).expect("compilation should succeed");
    compiled.base.query_config.graph_query_cache_enabled = Some(true);
    compiled.base.query_config.graph_query_cache_ttl = Some(300);
    let compiled = Arc::new(compiled);

    let client = Arc::new(ch_ctx.create_client());

    // CALL 1: cache miss -> ClickHouse -> cache store
    let mut ctx1 = build_context(
        &ontology,
        Arc::clone(&compiled),
        Arc::clone(&client),
        Arc::clone(&broker),
    );
    let output1 = CachedExecutor
        .execute(&mut ctx1, &mut NoOpObserver)
        .await
        .expect("first execution should succeed");

    let first_row_count: usize = output1.batches.iter().map(|b| b.num_rows()).sum();
    assert!(first_row_count > 0, "seeded data should produce rows");

    // Wait for fire-and-forget KV write
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // DELETE all data from ClickHouse
    ch_ctx.truncate_all_tables().await;
    ch_ctx.optimize_all().await;

    // Verify ClickHouse is empty
    let direct_batches = ch_ctx.query_parameterized(&compiled.base).await;
    let direct_rows: usize = direct_batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        direct_rows, 0,
        "ClickHouse should be empty after truncation"
    );

    // CALL 2: cache hit -> stale data from KV
    let mut ctx2 = build_context(
        &ontology,
        Arc::clone(&compiled),
        Arc::clone(&client),
        Arc::clone(&broker),
    );
    let output2 = CachedExecutor
        .execute(&mut ctx2, &mut NoOpObserver)
        .await
        .expect("second execution should succeed (cache hit)");

    let second_row_count: usize = output2.batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        first_row_count, second_row_count,
        "cache hit should return the same row count as the original query \
         (ClickHouse is empty, so this data must come from cache)"
    );

    if !output1.batches.is_empty() && !output2.batches.is_empty() {
        assert_eq!(
            output1.batches[0].schema(),
            output2.batches[0].schema(),
            "cached result should preserve the Arrow schema"
        );
    }
}

#[tokio::test]
async fn cached_executor_bypass_when_disabled() {
    let graph_sql: &str = &GRAPH_SCHEMA_SQL;
    let schema: &[&str] = &[SIPHON_SCHEMA_SQL, graph_sql];
    let (nats_future, ch_future) = tokio::join!(start_nats(), TestContext::new(schema));
    let (_nats_container, nats_url) = nats_future;
    let ch_ctx = ch_future;

    let broker = Arc::new(connect_broker(&nats_url).await);
    ensure_query_cache_bucket(&broker)
        .await
        .expect("failed to create cache bucket");

    integration_testkit::load_seed(&ch_ctx, "data_correctness").await;
    ch_ctx.optimize_all().await;

    let ontology = load_ontology();
    let security_ctx = test_security_context();
    let compiled =
        compile(SIMPLE_TRAVERSAL, &ontology, &security_ctx).expect("compilation should succeed");
    assert!(
        compiled
            .base
            .query_config
            .graph_query_cache_enabled
            .is_none()
            || compiled.base.query_config.graph_query_cache_enabled == Some(false),
        "cache should be disabled by default"
    );
    let compiled = Arc::new(compiled);

    let client = Arc::new(ch_ctx.create_client());

    let mut ctx = build_context(
        &ontology,
        Arc::clone(&compiled),
        Arc::clone(&client),
        Arc::clone(&broker),
    );
    let output = CachedExecutor
        .execute(&mut ctx, &mut NoOpObserver)
        .await
        .expect("execution should succeed");

    let row_count: usize = output.batches.iter().map(|b| b.num_rows()).sum();
    assert!(row_count > 0, "should return rows from ClickHouse");

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    ch_ctx.truncate_all_tables().await;
    ch_ctx.optimize_all().await;

    // With cache disabled, second query should return empty (hit ClickHouse, not cache)
    let mut ctx2 = build_context(
        &ontology,
        Arc::clone(&compiled),
        Arc::clone(&client),
        Arc::clone(&broker),
    );
    let output2 = CachedExecutor
        .execute(&mut ctx2, &mut NoOpObserver)
        .await
        .expect("execution should succeed");

    let row_count2: usize = output2.batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        row_count2, 0,
        "with cache disabled, truncated ClickHouse should return 0 rows \
         (proving the cache was NOT used)"
    );
}

#[tokio::test]
async fn cached_executor_works_without_nats() {
    let graph_sql: &str = &GRAPH_SCHEMA_SQL;
    let ch_ctx = TestContext::new(&[SIPHON_SCHEMA_SQL, graph_sql]).await;

    integration_testkit::load_seed(&ch_ctx, "data_correctness").await;
    ch_ctx.optimize_all().await;

    let ontology = load_ontology();
    let security_ctx = test_security_context();
    let compiled = Arc::new(
        compile(SIMPLE_TRAVERSAL, &ontology, &security_ctx).expect("compilation should succeed"),
    );

    let client = Arc::new(ch_ctx.create_client());

    // Build context WITHOUT NatsBroker
    let mut server_extensions = TypeMap::default();
    server_extensions.insert(client);
    server_extensions.insert(gkg_server_config::ProfilingConfig::default());

    let mut ctx = QueryPipelineContext {
        query_json: String::new(),
        compiled: Some(compiled),
        ontology,
        security_context: None,
        server_extensions,
        phases: TypeMap::default(),
    };

    let output = CachedExecutor
        .execute(&mut ctx, &mut NoOpObserver)
        .await
        .expect("should succeed by falling through to ClickHouseExecutor");

    let row_count: usize = output.batches.iter().map(|b| b.num_rows()).sum();
    assert!(
        row_count > 0,
        "should return rows from ClickHouse when no cache is configured"
    );
}
