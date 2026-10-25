use std::sync::Arc;

use chrono::{TimeDelta, Utc};
use indexer::checkpoint::{
    Checkpoint, CheckpointStore, ClickHouseCheckpointStore, GLOBAL_SCOPE_KEY,
    namespace_position_key,
};
use indexer::durability::WriteDurability;
use indexer::orchestrator::scheduled::{NamespaceFreshness, ScheduledTask};
use indexer::schema::version::{
    SCHEMA_VERSION, ensure_version_table, mark_version_active, prefixed_table_name,
};
use ontology::EtlScope;
use ontology::archive::OntologyArchive;
use ontology::migrations::embedded_sources;
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData, ResourceMetrics};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
use orbit_migrations::catalog::OntologyCatalog;
use orbit_migrations::execute::CHECKPOINT_TABLE;
use orbit_server_config::{AppConfig, NatsConfiguration};

use super::common::siphon::create_namespace;
use super::common::{GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL, TestContext, start_nats};

const IN_SYNC: i64 = 100;
const PENDING: i64 = 200;
const STALE: i64 = 300;
const NEVER_SYNCED: i64 = 400;
const MOVED_PARENT: i64 = 500;
const MOVED: i64 = 600;

#[tokio::test]
async fn records_how_far_each_enabled_namespace_trails_the_datalake() {
    let exporter = InMemoryMetricExporter::default();
    let provider = SdkMeterProvider::builder()
        .with_reader(PeriodicReader::builder(exporter.clone()).build())
        .build();
    opentelemetry::global::set_meter_provider(provider.clone());

    let clickhouse = TestContext::new(&[SIPHON_SCHEMA_SQL, *GRAPH_SCHEMA_SQL]).await;
    let (_nats, nats_url) = start_nats().await;
    let services = indexer::orchestrator::scheduled::connect(&NatsConfiguration {
        url: nats_url,
        ..AppConfig::embedded_defaults().nats
    })
    .await
    .unwrap();
    let catalog = OntologyCatalog::open(services.nats_client.clone())
        .await
        .unwrap();
    // A served version that differs from this binary's proves the task reads what users query.
    let served = *SCHEMA_VERSION - 1;
    catalog
        .publish(&OntologyArchive::from_sources(served, &embedded_sources()).unwrap())
        .await
        .unwrap();

    let graph = clickhouse.create_client();
    ensure_version_table(&graph).await.unwrap();
    mark_version_active(&graph, served).await.unwrap();
    clickhouse
        .execute(&format!(
            "CREATE TABLE {} AS {}",
            prefixed_table_name(CHECKPOINT_TABLE, served),
            prefixed_table_name(CHECKPOINT_TABLE, *SCHEMA_VERSION)
        ))
        .await;

    for (id, enabled_minutes_ago) in [
        (IN_SYNC, 1440),
        (PENDING, 1440),
        (STALE, 1440),
        (NEVER_SYNCED, 90),
    ] {
        clickhouse
            .execute(&format!(
                "INSERT INTO siphon_knowledge_graph_enabled_namespaces \
                 (id, root_namespace_id, traversal_path, created_at, updated_at) \
                 VALUES ({id}, {id}, '1/{id}/', now() - INTERVAL {enabled_minutes_ago} MINUTE, now())"
            ))
            .await;
        create_namespace(&clickhouse, id, None, 20, &format!("1/{id}/")).await;
    }
    clickhouse
        .execute(&format!(
            "INSERT INTO siphon_knowledge_graph_enabled_namespaces \
             (id, root_namespace_id, traversal_path, created_at, updated_at) \
             VALUES ({MOVED}, {MOVED}, '1/{MOVED_PARENT}/{MOVED}/', now() - INTERVAL 1440 MINUTE, now())"
        ))
        .await;
    create_namespace(
        &clickhouse,
        MOVED,
        Some(MOVED_PARENT),
        20,
        &format!("1/{MOVED_PARENT}/{MOVED}/"),
    )
    .await;
    clickhouse
        .execute(&format!(
            "INSERT INTO work_items (id, traversal_path, _siphon_watermark) VALUES \
             (1, '1/{IN_SYNC}/7/', now64(6) - INTERVAL 20 MINUTE), \
             (2, '1/{PENDING}/7/', now64(6) - INTERVAL 3 MINUTE)"
        ))
        .await;

    let ontology = ontology::Ontology::load_embedded().unwrap();
    let plans_in = |scope: EtlScope| -> Vec<String> {
        ontology
            .pipeline_descriptors()
            .into_iter()
            .filter(|plan| plan.scope == scope)
            .map(|plan| plan.name)
            .collect()
    };
    let plans = plans_in(EtlScope::Namespaced);
    let global_plans = plans_in(EtlScope::Global);
    let (synced_global, never_synced_global) = global_plans.split_first().unwrap();
    let served_store = ClickHouseCheckpointStore::for_version(Arc::new(graph.clone()), served);
    let unserved_store = ClickHouseCheckpointStore::new(Arc::new(graph.clone()));
    let started = Utc::now();
    for plan in &plans {
        for (id, synced_minutes_ago) in [(IN_SYNC, 0), (PENDING, 10), (STALE, 180)] {
            complete(&served_store, &namespace_key(id, plan), synced_minutes_ago).await;
        }
        complete(&unserved_store, &namespace_key(NEVER_SYNCED, plan), 0).await;
    }
    complete(
        &served_store,
        &format!("{GLOBAL_SCOPE_KEY}.{synced_global}"),
        30,
    )
    .await;
    let seeded = Utc::now();
    let synced_while_seeding = |minutes_ago| {
        let ago = TimeDelta::minutes(minutes_ago);
        (started - ago).timestamp()..=(seeded - ago).timestamp()
    };

    let config = AppConfig::embedded_defaults().schedule.tasks;
    let task = NamespaceFreshness::new(
        graph,
        clickhouse.create_client(),
        catalog,
        config.namespace_freshness,
        config.namespace.sweep_interval_secs,
    );
    task.run().await.unwrap();
    provider.force_flush().unwrap();
    let metrics = exporter.get_finished_metrics().unwrap();

    assert_eq!(
        lag_buckets(&metrics, "WorkItem"),
        [1, 0, 1, 0, 0, 1, 1, 0, 0, 0],
        "in sync, pending since the sync 10m ago, never synced since enabled 90m ago, synced 3h ago"
    );
    assert_eq!(
        lag_buckets(&metrics, "MergeRequest"),
        [2, 0, 0, 0, 0, 1, 1, 0, 0, 0],
        "no merge request changed, so only enablement and stale syncs lag"
    );
    let observations: u64 = plans
        .iter()
        .map(|plan| lag_buckets(&metrics, plan).iter().sum::<u64>())
        .sum();
    assert_eq!(observations, 4 * plans.len() as u64);
    assert_eq!(
        task_runs(&metrics, "namespace_freshness", "success"),
        1,
        "a stopped or failing task must be visible without its samples"
    );

    for plan in &plans {
        let oldest = oldest_sync(&metrics, plan).unwrap();
        assert!(
            synced_while_seeding(180).contains(&oldest),
            "{plan} is only as fresh as its stalest namespace, synced 3h ago: {oldest}"
        );
    }
    let global = oldest_sync(&metrics, synced_global).unwrap();
    assert!(
        synced_while_seeding(30).contains(&global),
        "a global plan is as fresh as its own checkpoint: {global}"
    );
    for plan in never_synced_global {
        assert_eq!(oldest_sync(&metrics, plan), None, "{plan} has never synced");
    }
}

fn namespace_key(namespace: i64, plan: &str) -> String {
    format!("{}.{plan}", namespace_position_key(namespace))
}

async fn complete(store: &ClickHouseCheckpointStore, key: &str, synced_minutes_ago: i64) {
    let synced = Utc::now() - TimeDelta::minutes(synced_minutes_ago);
    let mut checkpoint = Checkpoint::new(synced);
    checkpoint.complete(synced);
    store
        .save(key, &checkpoint, WriteDurability::Durable)
        .await
        .unwrap();
}

fn lag_buckets(metrics: &[ResourceMetrics], entity: &str) -> Vec<u64> {
    metrics
        .iter()
        .flat_map(|resource| resource.scope_metrics())
        .flat_map(|scope| scope.metrics())
        .filter(|metric| metric.name() == "gkg.indexer.sdlc.freshness.lag")
        .flat_map(|metric| match metric.data() {
            AggregatedMetrics::F64(MetricData::Histogram(histogram)) => histogram
                .data_points()
                .filter(|point| {
                    point
                        .attributes()
                        .any(|kv| kv.key.as_str() == "entity" && kv.value.as_str() == entity)
                })
                .flat_map(|point| point.bucket_counts())
                .collect::<Vec<_>>(),
            _ => panic!("freshness lag is not an f64 histogram"),
        })
        .collect()
}

fn oldest_sync(metrics: &[ResourceMetrics], entity: &str) -> Option<i64> {
    metrics
        .last()?
        .scope_metrics()
        .flat_map(|scope| scope.metrics())
        .filter(|metric| metric.name() == "gkg.indexer.sdlc.freshness.oldest_sync_timestamp")
        .flat_map(|metric| match metric.data() {
            AggregatedMetrics::I64(MetricData::Gauge(gauge)) => gauge
                .data_points()
                .filter(|point| {
                    point
                        .attributes()
                        .any(|kv| kv.key.as_str() == "entity" && kv.value.as_str() == entity)
                })
                .map(|point| point.value())
                .collect::<Vec<_>>(),
            _ => panic!("oldest sync is not an i64 gauge"),
        })
        .next()
}

fn task_runs(metrics: &[ResourceMetrics], task: &str, outcome: &str) -> u64 {
    metrics
        .iter()
        .flat_map(|resource| resource.scope_metrics())
        .flat_map(|scope| scope.metrics())
        .filter(|metric| metric.name() == "gkg.scheduler.task.runs")
        .flat_map(|metric| match metric.data() {
            AggregatedMetrics::U64(MetricData::Sum(sum)) => sum
                .data_points()
                .filter(|point| {
                    let has = |key: &str, value: &str| {
                        point
                            .attributes()
                            .any(|kv| kv.key.as_str() == key && kv.value.as_str() == value)
                    };
                    has("task", task) && has("outcome", outcome)
                })
                .map(|point| point.value())
                .collect::<Vec<_>>(),
            _ => panic!("scheduler task runs is not a u64 sum"),
        })
        .sum()
}
