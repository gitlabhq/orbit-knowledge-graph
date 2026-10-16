use std::collections::BTreeMap;

use indexer::indexing_status::namespaced_plans;
use indexer::orchestrator::scheduled::{NamespaceFreshness, ScheduledTask};
use indexer::schema::version::{
    SCHEMA_VERSION, ensure_version_table, mark_version_active, mark_version_migrating,
    prefixed_table_name,
};
use ontology::archive::OntologyArchive;
use ontology::migrations::embedded_sources;
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData, ResourceMetrics};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
use orbit_migrations::catalog::OntologyCatalog;
use orbit_migrations::execute::CHECKPOINT_TABLE;
use orbit_server_config::NatsConfiguration;

use super::common::dispatch::start_nats;
use super::common::{GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL, TestContext};

const FRESH: i64 = 100;
const THREE_HOURS_OLD: i64 = 200;
const NOT_STARTED: i64 = 300;
const FAILING: i64 = 400;

#[tokio::test]
async fn exports_namespace_freshness_for_the_served_version() {
    let exporter = InMemoryMetricExporter::default();
    let provider = SdkMeterProvider::builder()
        .with_reader(PeriodicReader::builder(exporter.clone()).build())
        .build();
    opentelemetry::global::set_meter_provider(provider.clone());

    let clickhouse = TestContext::new(&[SIPHON_SCHEMA_SQL, *GRAPH_SCHEMA_SQL]).await;
    let (_nats, nats_url) = start_nats().await;
    let services = indexer::orchestrator::scheduled::connect(&NatsConfiguration {
        url: nats_url,
        ..orbit_server_config::AppConfig::embedded_defaults().nats
    })
    .await
    .unwrap();
    let catalog = OntologyCatalog::open(services.nats_client.clone())
        .await
        .unwrap();
    catalog
        .publish(&OntologyArchive::from_sources(*SCHEMA_VERSION, &embedded_sources()).unwrap())
        .await
        .unwrap();

    let graph = clickhouse.create_client();
    let served_table = prefixed_table_name(CHECKPOINT_TABLE, *SCHEMA_VERSION);
    let migrating_table = prefixed_table_name(CHECKPOINT_TABLE, *SCHEMA_VERSION + 1);
    ensure_version_table(&graph).await.unwrap();
    mark_version_active(&graph, *SCHEMA_VERSION).await.unwrap();
    mark_version_migrating(&graph, *SCHEMA_VERSION + 1)
        .await
        .unwrap();
    clickhouse
        .execute(&format!("CREATE TABLE {migrating_table} AS {served_table}"))
        .await;

    for (index, namespace) in [FRESH, THREE_HOURS_OLD, NOT_STARTED, FAILING]
        .into_iter()
        .enumerate()
    {
        clickhouse
            .execute(&format!(
                "INSERT INTO siphon_knowledge_graph_enabled_namespaces \
                 (id, root_namespace_id, traversal_path, created_at, updated_at) \
                 VALUES ({}, {namespace}, '1/{namespace}/', now(), now())",
                index + 1
            ))
            .await;
    }

    let plans: Vec<String> = namespaced_plans(&ontology::Ontology::load_embedded().unwrap())
        .into_iter()
        .map(|plan| plan.name)
        .collect();
    let mut rows = Vec::new();
    for plan in &plans {
        rows.push(format!("('ns.{FRESH}.{plan}', now(), 0, now())"));
        rows.push(format!(
            "('ns.{THREE_HOURS_OLD}.{plan}', now(), 0, now() - INTERVAL 3 HOUR)"
        ));
    }
    let (failing_plan, completed_plans) = plans.split_first().unwrap();
    rows.push(format!("('ns.{FAILING}.{failing_plan}', now(), 5, NULL)"));
    for plan in completed_plans {
        rows.push(format!("('ns.{FAILING}.{plan}', now(), 0, now())"));
    }
    clickhouse
        .execute(&format!(
            "INSERT INTO {served_table} (key, watermark, attempts, indexed_at) VALUES {}",
            rows.join(", ")
        ))
        .await;
    let migrating_rows: Vec<String> = plans
        .iter()
        .map(|plan| format!("('ns.{NOT_STARTED}.{plan}', now(), 0, now())"))
        .collect();
    clickhouse
        .execute(&format!(
            "INSERT INTO {migrating_table} (key, watermark, attempts, indexed_at) VALUES {}",
            migrating_rows.join(", ")
        ))
        .await;

    let task = NamespaceFreshness::new(
        graph,
        clickhouse.create_client(),
        catalog,
        orbit_server_config::AppConfig::embedded_defaults()
            .schedule
            .tasks
            .namespace_freshness,
    );
    task.run().await.unwrap();
    provider.force_flush().unwrap();
    let metrics = exporter.get_finished_metrics().unwrap();

    assert_eq!(
        gauge(&metrics, "gkg.indexer.freshness.namespaces", "phase"),
        BTreeMap::from([
            ("error".to_string(), 1),
            ("not_started".to_string(), 1),
            ("ready".to_string(), 2),
            ("syncing".to_string(), 0),
        ])
    );
    assert_eq!(
        gauge(
            &metrics,
            "gkg.indexer.freshness.ready.namespaces",
            "max_age"
        ),
        BTreeMap::from([
            ("1h".to_string(), 1),
            ("2h".to_string(), 1),
            ("4h".to_string(), 2),
            ("8h".to_string(), 2),
            ("16h".to_string(), 2),
        ])
    );

    let oldest_age = gauge(&metrics, "gkg.indexer.freshness.plan.oldest.age", "plan");
    assert_eq!(oldest_age.len(), plans.len());
    assert!(
        oldest_age
            .values()
            .all(|age| (3 * 3600 - 60..3 * 3600 + 60).contains(age)),
        "every plan's oldest completion is the three-hour-old namespace: {oldest_age:?}"
    );
}

fn gauge(metrics: &[ResourceMetrics], name: &str, label: &str) -> BTreeMap<String, i64> {
    metrics
        .iter()
        .flat_map(|resource| resource.scope_metrics())
        .flat_map(|scope| scope.metrics())
        .filter(|metric| metric.name() == name)
        .flat_map(|metric| match metric.data() {
            AggregatedMetrics::I64(MetricData::Gauge(gauge)) => gauge
                .data_points()
                .map(|point| {
                    let value = point
                        .attributes()
                        .find(|kv| kv.key.as_str() == label)
                        .map(|kv| kv.value.to_string())
                        .unwrap();
                    (value, point.value())
                })
                .collect::<Vec<_>>(),
            _ => panic!("{name} is not an i64 gauge"),
        })
        .collect()
}
