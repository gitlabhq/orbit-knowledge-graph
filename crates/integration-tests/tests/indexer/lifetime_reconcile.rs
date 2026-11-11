use std::sync::Arc;

use clickhouse_client::ClickHouseConfigurationExt;
use integration_testkit::t;
use orbit_utils::traversal_path::TraversalPath;

use super::common::{GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL, TestContext};
use indexer::modules::namespace_deletion::{
    ClickHouseNamespaceDeletionStore, NamespaceDeletionStore,
};

const ROOT: &str = "1/100/";

fn store(context: &TestContext) -> ClickHouseNamespaceDeletionStore {
    let catalog = query_data_model::Catalog::<query_data_model::ClickHouse>::embedded()
        .expect("embedded catalog");
    ClickHouseNamespaceDeletionStore::new(
        Arc::new(context.config.build_client("graph")),
        Arc::new(context.config.build_client("graph")),
        &catalog,
        &indexer::testkit::siphon_source(),
    )
    .expect("deletion statements build")
}

async fn live(context: &TestContext, table: &str, predicate: &str) -> usize {
    context
        .query(&format!(
            "SELECT 1 FROM {} FINAL WHERE _deleted = false AND {predicate}",
            t(table)
        ))
        .await
        .iter()
        .map(|batch| batch.num_rows())
        .sum()
}

async fn member_of(context: &TestContext, group: i64) -> usize {
    live(
        context,
        "gl_edge",
        &format!(
            "relationship_kind = 'MEMBER_OF' AND target_kind = 'Group' AND target_id = {group}"
        ),
    )
    .await
}

/// Stale state written before the indexer applied both rules: a deleted group whose
/// membership edge lives on, a membership of a group no writer indexed yet, and a label
/// that moved to another project but kept its old row.
async fn seed_stale_state(context: &TestContext) {
    context
        .execute(&format!(
            "INSERT INTO {} (traversal_path, id, name, _version, _deleted) VALUES \
             ('1/100/', 100, 'parent', '2024-01-01 00:00:00.000000', false), \
             ('1/100/200/', 200, 'gone', '2024-01-01 00:00:00.000000', false), \
             ('1/100/200/', 200, '', '2024-02-01 00:00:00.000000', true)",
            t("gl_group")
        ))
        .await;
    context
        .execute(&format!(
            "INSERT INTO {} (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind, _version) VALUES \
             ('1/100/', 1, 'User', 'MEMBER_OF', 100, 'Group', '2024-01-01 00:00:00.000000'), \
             ('1/100/200/', 2, 'User', 'MEMBER_OF', 200, 'Group', '2024-01-01 00:00:00.000000'), \
             ('1/100/', 3, 'User', 'MEMBER_OF', 999, 'Group', '2024-01-01 00:00:00.000000')",
            t("gl_edge")
        ))
        .await;
    context
        .execute(&format!(
            "INSERT INTO {} (traversal_path, id, title, _version) VALUES \
             ('1/100/1000/', 5, 'bug', '2024-01-01 00:00:00.000000'), \
             ('1/100/2000/', 5, 'bug', '2024-03-01 00:00:00.000000')",
            t("gl_label")
        ))
        .await;
}

#[tokio::test]
async fn lifetime_reconcile_clears_dead_node_edges_and_old_paths_idempotently() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL]).await;
    seed_stale_state(&context).await;
    let store = store(&context);
    let root = TraversalPath::new_unchecked(ROOT);

    for _ in 0..2 {
        let failed: Vec<_> = store
            .reconcile_lifetime(&root)
            .await
            .into_iter()
            .filter_map(|outcome| outcome.error.map(|error| (outcome.table, error)))
            .collect();
        assert!(failed.is_empty(), "{failed:?}");

        assert_eq!(member_of(&context, 200).await, 0, "the dead group's edge");
        assert_eq!(member_of(&context, 100).await, 1, "a live group's edge");
        assert_eq!(
            member_of(&context, 999).await,
            1,
            "an edge to a node never indexed"
        );
        assert_eq!(live(&context, "gl_label", "id = 5").await, 1);
        assert_eq!(
            live(
                &context,
                "gl_label",
                "id = 5 AND traversal_path = '1/100/2000/'"
            )
            .await,
            1,
            "the label keeps only its newest path"
        );
    }
}

#[tokio::test]
async fn namespace_deletion_tombstones_edges_stored_under_another_namespace() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL]).await;
    context
        .execute(&format!(
            "INSERT INTO {} (traversal_path, id, _version) VALUES \
             ('1/100/', 1, '2024-01-01 00:00:00.000000'), \
             ('1/200/', 2, '2024-01-01 00:00:00.000000')",
            t("gl_project")
        ))
        .await;
    context
        .execute(&format!(
            "INSERT INTO {} (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind, _version) VALUES \
             ('1/200/', 7, 'MergeRequest', 'SOURCE_PROJECT', 1, 'Project', '2024-01-01 00:00:00.000000'), \
             ('1/200/', 8, 'MergeRequest', 'SOURCE_PROJECT', 2, 'Project', '2024-01-01 00:00:00.000000')",
            t("gl_edge")
        ))
        .await;

    let outcomes = store(&context)
        .delete_namespace_data(&TraversalPath::new_unchecked(ROOT))
        .await;
    assert!(outcomes.iter().all(|outcome| outcome.error.is_none()));

    let source_project =
        |project| format!("relationship_kind = 'SOURCE_PROJECT' AND target_id = {project}");
    assert_eq!(
        live(&context, "gl_edge", &source_project(1)).await,
        0,
        "the fork source in the deleted namespace"
    );
    assert_eq!(
        live(&context, "gl_edge", &source_project(2)).await,
        1,
        "a live project's edge"
    );
}
