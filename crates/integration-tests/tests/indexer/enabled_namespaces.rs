use std::sync::Arc;

use indexer::modules::namespace_deletion::{
    ClickHouseNamespaceDeletionStore, NamespaceDeletionStore,
};
use orbit_migrations::completion::fetch_enabled_top_level_namespaces;
use orbit_utils::traversal_path::TraversalPath;

use super::common::{SIPHON_SCHEMA_SQL, TestContext};

const ENABLED: i64 = 100;
const DISABLED: i64 = 200;

#[tokio::test]
async fn migration_gate_skips_a_disabled_namespace_before_the_delete_merges() {
    let context = TestContext::new(&[SIPHON_SCHEMA_SQL]).await;
    disable_one_of_two_namespaces(&context).await;

    let split = fetch_enabled_top_level_namespaces(&context.create_client())
        .await
        .unwrap();

    assert_eq!(split.ids, [ENABLED]);
}

#[tokio::test]
async fn moved_entity_reconcile_skips_a_disabled_namespace_before_the_delete_merges() {
    let context = TestContext::new(&[SIPHON_SCHEMA_SQL]).await;
    disable_one_of_two_namespaces(&context).await;
    let ontology = ontology::Ontology::load_embedded().unwrap();
    let store = ClickHouseNamespaceDeletionStore::new(
        Arc::new(context.create_client()),
        Arc::new(context.create_client()),
        &ontology,
    );

    let roots = store.enabled_namespace_roots().await.unwrap();

    assert_eq!(
        roots,
        [TraversalPath::new_unchecked(format!("1/{ENABLED}/"))]
    );
}

async fn disable_one_of_two_namespaces(context: &TestContext) {
    context
        .execute("SYSTEM STOP MERGES siphon_knowledge_graph_enabled_namespaces")
        .await;
    context
        .execute(&format!(
            "INSERT INTO siphon_knowledge_graph_enabled_namespaces \
             (id, root_namespace_id, traversal_path, _siphon_replicated_at) VALUES \
             (1, {ENABLED}, '1/{ENABLED}/', now64(6) - INTERVAL 1 HOUR), \
             (2, {DISABLED}, '1/{DISABLED}/', now64(6) - INTERVAL 1 HOUR)"
        ))
        .await;
    context
        .execute(&format!(
            "INSERT INTO siphon_knowledge_graph_enabled_namespaces \
             (id, root_namespace_id, traversal_path, _siphon_deleted) VALUES \
             (2, {DISABLED}, '1/{DISABLED}/', true)"
        ))
        .await;
}
