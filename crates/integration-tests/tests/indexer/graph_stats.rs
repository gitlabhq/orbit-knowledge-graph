use std::sync::Arc;

use indexer::analytics::GraphStatsReporter;
use indexer::orchestrator::scheduled::{GraphStatsSnapshot, ScheduledTask, ScheduledTaskMetrics};
use indexer::schema::version::{
    SCHEMA_VERSION, ensure_version_table, mark_version_active, prefixed_table_name,
};
use ontology::archive::OntologyArchive;
use ontology::migrations::embedded_sources;
use orbit_analytics::{
    GitlabInstanceWatch, InMemoryAnalyticsTracker, ORBIT_COMMON_SCHEMA, ORBIT_GRAPH_STATS_SCHEMA,
    ORBIT_NAMESPACE_GRAPH_STATS_SCHEMA,
};
use orbit_server_config::{AppConfig, NatsConfiguration};

use super::common::siphon::create_namespace;
use super::common::{GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL, TestContext, start_nats};

const ENABLED: i64 = 100;
const NOT_ENABLED: i64 = 900;

#[tokio::test]
async fn snapshot_counts_the_instance_and_each_enabled_namespace() {
    let clickhouse = TestContext::new(&[SIPHON_SCHEMA_SQL, *GRAPH_SCHEMA_SQL]).await;
    let (_nats, nats_url) = start_nats().await;
    let services = indexer::orchestrator::scheduled::connect(&NatsConfiguration {
        url: nats_url,
        ..AppConfig::embedded_defaults().nats
    })
    .await
    .unwrap();
    let catalog = orbit_migrations::catalog::OntologyCatalog::open(services.nats_client.clone())
        .await
        .unwrap();
    catalog
        .publish(&OntologyArchive::from_sources(*SCHEMA_VERSION, &embedded_sources()).unwrap())
        .await
        .unwrap();
    let graph = clickhouse.create_client();
    ensure_version_table(&graph).await.unwrap();
    mark_version_active(&graph, *SCHEMA_VERSION).await.unwrap();
    let schema = orbit_migrations::schema::GraphSchema::from_ontology(
        &ontology::Ontology::load_embedded().unwrap(),
    )
    .unwrap();
    orbit_migrations::execute::create_unversioned_definitions(&graph, &schema)
        .await
        .unwrap();
    let version = *SCHEMA_VERSION;
    clickhouse
        .execute(&format!(
            "INSERT INTO namespace_storage_snapshot \
             (snapshot_date, schema_version, logical_table, top_level_namespace, compressed_bytes) VALUES \
             (today(), {version}, 'gl_project', '1/{ENABLED}', 5000), \
             (today(), {version}, 'gl_edge', '1/{ENABLED}', 700), \
             (today() - 1, {version}, 'gl_project', '1/{ENABLED}', 1), \
             (today(), {version}, 'gl_project', '1/{NOT_ENABLED}', 900), \
             (today(), {version}, 'gl_user', '0/', 300)"
        ))
        .await;

    for id in [ENABLED, NOT_ENABLED] {
        create_namespace(&clickhouse, id, None, 20, &format!("1/{id}/")).await;
    }
    clickhouse
        .execute(&format!(
            "INSERT INTO siphon_knowledge_graph_enabled_namespaces \
             (id, root_namespace_id, traversal_path, created_at, updated_at) \
             VALUES ({ENABLED}, {ENABLED}, '1/{ENABLED}/', now(), now())"
        ))
        .await;
    clickhouse
        .execute(&format!(
            "INSERT INTO project_namespace_traversal_paths (id, traversal_path, version, deleted) VALUES \
             (1, '1/{ENABLED}/', now64(6), false), \
             (2, '1/{ENABLED}/7/', now64(6), false), \
             (3, '1/{ENABLED}/', now64(6), false), \
             (4, '1/{ENABLED}/', now64(6) - INTERVAL 1 HOUR, false), \
             (4, '1/{ENABLED}/', now64(6), true), \
             (5, '1/{NOT_ENABLED}/', now64(6), false)"
        ))
        .await;

    let checkpoint = prefixed_table_name("code_indexing_checkpoint", *SCHEMA_VERSION);
    clickhouse
        .execute(&format!(
            "INSERT INTO {checkpoint} \
             (traversal_path, project_id, branch, last_task_id, indexed_at, is_default_branch, _deleted) VALUES \
             ('1/{ENABLED}/', 1, 'main', 1, now64(6), true, false), \
             ('1/{ENABLED}/', 1, 'feature', 1, now64(6), false, false), \
             ('1/{ENABLED}/7/', 2, 'main', 1, now64(6), true, false), \
             ('1/{ENABLED}/', 3, 'main', 1, NULL, true, false), \
             ('1/{ENABLED}/', 4, 'main', 1, now64(6), true, false), \
             ('1/{NOT_ENABLED}/', 5, 'main', 1, now64(6), true, false)"
        ))
        .await;

    let projects = prefixed_table_name("gl_project", *SCHEMA_VERSION);
    clickhouse
        .execute(&format!(
            "INSERT INTO {projects} (id, traversal_path, created_at, updated_at, last_activity_at, _version, _deleted) VALUES \
             (1, '1/{ENABLED}/', now(), now(), now(), now64(6), false), \
             (2, '1/{ENABLED}/7/', now(), now(), now(), now64(6), false), \
             (2, '1/{ENABLED}/7/', now(), now(), now(), now64(6) - INTERVAL 1 HOUR, false), \
             (4, '1/{ENABLED}/', now(), now(), now(), now64(6) - INTERVAL 1 HOUR, false), \
             (4, '1/{ENABLED}/', now(), now(), now(), now64(6), true)"
        ))
        .await;

    let tracker = Arc::new(InMemoryAnalyticsTracker::new());
    let defaults = AppConfig::embedded_defaults();
    let reporter = GraphStatsReporter::new(
        tracker.clone(),
        Arc::new(defaults.analytics.clone()),
        GitlabInstanceWatch::default(),
    );
    let task = GraphStatsSnapshot::new(
        graph,
        clickhouse.create_client(),
        catalog,
        reporter,
        ScheduledTaskMetrics::new(),
        defaults.schedule.tasks.graph_stats,
    );
    task.run().await.unwrap();

    let events = tracker.drain();
    assert_eq!(
        events.len(),
        2,
        "one instance event and one event for the enabled namespace"
    );
    assert_eq!(events[0].action(), "orbit_graph_stats");
    let stats = context(&events[0], ORBIT_GRAPH_STATS_SCHEMA);

    assert_eq!(
        stats["entity_counts"]["Project"], 2,
        "duplicate and soft-deleted rows do not count: {stats}"
    );
    assert_eq!(stats["entity_counts"]["MergeRequest"], 0);
    let ontology = ontology::Ontology::load_embedded().unwrap();
    let keys = stats["entity_counts"].as_object().unwrap();
    assert_eq!(keys.len(), ontology.node_names().count());
    assert!(keys.keys().all(|key| ontology.get_node(key).is_some()));

    assert_eq!(
        stats["top_level_groups_indexed"], 1,
        "a namespace without Orbit enabled does not count"
    );
    assert_eq!(
        stats["projects_indexed"], 2,
        "an unindexed branch, a deleted project and another namespace do not count"
    );
    assert!(stats["graph_bytes"].as_i64().unwrap() > 0, "{stats}");

    assert_eq!(events[1].action(), "orbit_namespace_graph_stats");
    assert_eq!(
        context(&events[1], ORBIT_COMMON_SCHEMA)["root_namespace_ids"],
        serde_json::json!([ENABLED])
    );
    let namespace = context(&events[1], ORBIT_NAMESPACE_GRAPH_STATS_SCHEMA);
    assert_eq!(
        namespace["graph_bytes"], 5700,
        "the latest snapshot: {namespace}"
    );
    assert_eq!(namespace["entity_counts"]["Project"], 2, "{namespace}");
    assert!(
        namespace["entity_counts"].get("User").is_none(),
        "a node type without a traversal path is not listed: {namespace}"
    );
    assert_eq!(namespace["projects_indexed"], 2);
}

fn context<'a>(event: &'a labkit_events::StructuredEvent, schema: &str) -> &'a serde_json::Value {
    &event
        .contexts()
        .iter()
        .find(|context| context.schema == schema)
        .unwrap_or_else(|| panic!("{schema} context attached"))
        .data
}
