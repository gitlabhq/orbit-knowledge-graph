use std::sync::Arc;

use indexer::analytics::GraphStatsReporter;
use indexer::orchestrator::scheduled::{GraphStatsSnapshot, ScheduledTask, ScheduledTaskMetrics};
use indexer::schema::version::{
    SCHEMA_VERSION, ensure_version_table, mark_version_active, prefixed_table_name,
};
use labkit_events::StructuredEvent;
use ontology::archive::OntologyArchive;
use ontology::migrations::embedded_sources;
use orbit_analytics::{AnalyticsTracker, GitlabInstanceWatch, ORBIT_GRAPH_STATS_SCHEMA};
use orbit_server_config::{AppConfig, NatsConfiguration};
use parking_lot::Mutex;

use super::common::siphon::create_namespace;
use super::common::{GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL, TestContext, start_nats};

const ENABLED: i64 = 100;
const NOT_ENABLED: i64 = 900;

#[derive(Default)]
struct Recorder(Mutex<Vec<StructuredEvent>>);

impl AnalyticsTracker for Recorder {
    fn track(&self, event: StructuredEvent) {
        self.0.lock().push(event);
    }
}

#[tokio::test]
async fn snapshot_counts_live_rows_and_projects_of_enabled_namespaces() {
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
    // Projects 1 to 3 are eligible. Project 4 was deleted, and project 5 is not in an enabled namespace.
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
    // Indexed default branches: projects 1, 2, 4 and 5. Only 1 and 2 still count.
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

    let recorder = Arc::new(Recorder::default());
    let defaults = AppConfig::embedded_defaults();
    let reporter = GraphStatsReporter::new(
        recorder.clone(),
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

    let events = std::mem::take(&mut *recorder.0.lock());
    assert_eq!(events.len(), 1, "one snapshot per run");
    assert_eq!(events[0].action(), "gkg_graph_stats");
    let stats = &events[0]
        .contexts()
        .iter()
        .find(|context| context.schema == *ORBIT_GRAPH_STATS_SCHEMA)
        .expect("orbit_graph_stats context")
        .data;

    assert_eq!(
        stats["entity_counts"]["Project"], 2,
        "duplicate and soft-deleted rows do not count: {stats}"
    );
    assert_eq!(stats["entity_counts"]["MergeRequest"], 0);
    let ontology = ontology::Ontology::load_embedded().unwrap();
    let keys = stats["entity_counts"].as_object().unwrap();
    assert_eq!(keys.len(), ontology.node_names().count());
    assert!(keys.keys().all(|key| ontology.get_node(key).is_some()));

    assert_eq!(stats["projects_eligible"], 3);
    assert_eq!(stats["projects_indexed"], 2);
    assert!(stats["graph_bytes"].as_i64().unwrap() > 0, "{stats}");
}
