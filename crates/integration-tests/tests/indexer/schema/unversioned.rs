use std::{collections::BTreeSet, time::Duration};

use clickhouse_client::FromArrowColumn;
use indexer::schema::migration::{
    create_unversioned_tables, drop_refreshable_views_for_version,
    replace_refreshable_views_for_version,
};
use indexer::schema::version::{SCHEMA_VERSION, ensure_version_table, write_schema_version};
use integration_testkit::{GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL, TestContext, t};

const GLOBAL_TOP_LEVEL_NAMESPACE: &str = "0/";

struct NamespaceStorageSnapshotScenario {
    context: TestContext,
    ontology: ontology::Ontology,
}

struct NamespaceStorageAttributionFixture {
    namespace_attributable_tables: Vec<String>,
    global_tables: Vec<String>,
    tables_with_rows_in_two_namespaces: [&'static str; 2],
}

#[tokio::test]
async fn creates_namespace_storage_table_and_refreshable_view() {
    let scenario = NamespaceStorageSnapshotScenario::new().await;
    scenario.create_schema().await;

    for name in namespace_storage_table_and_view_names() {
        assert_eq!(scenario.get_table_or_view_count(&name).await, 1, "{name}");
    }
}

#[tokio::test]
async fn replacing_refreshable_view_preserves_snapshot_rows() {
    let scenario = NamespaceStorageSnapshotScenario::new().await;
    scenario.create_schema().await;
    scenario.insert_snapshot_row().await;

    scenario.replace_view().await;

    scenario.assert_snapshot_row_exists().await;
}

#[tokio::test]
async fn drops_and_recreates_versioned_refreshable_view() {
    let scenario = NamespaceStorageSnapshotScenario::new().await;
    scenario.create_schema().await;

    scenario.drop_view().await;
    scenario.assert_view_count(0).await;

    scenario.replace_view().await;
    scenario.assert_view_count(1).await;
}

#[tokio::test]
async fn attributes_compressed_bytes_to_top_level_namespaces() {
    let scenario = NamespaceStorageSnapshotScenario::new().await;
    scenario.create_schema().await;
    let fixture = scenario.insert_storage_attribution_rows().await;

    scenario.optimize_graph_tables_and_refresh().await;

    scenario.assert_all_rows_have_compressed_bytes().await;
    scenario.assert_namespace_attribution(&fixture).await;
    scenario.assert_global_table_attribution(&fixture).await;
    scenario.assert_refresh_preserves_current_snapshot().await;
}

impl NamespaceStorageSnapshotScenario {
    async fn new() -> Self {
        let context = TestContext::new(&[SIPHON_SCHEMA_SQL, &GRAPH_SCHEMA_SQL]).await;
        let client = context.create_client();
        ensure_version_table(&client).await.unwrap();
        write_schema_version(&client, *SCHEMA_VERSION)
            .await
            .unwrap();
        Self {
            context,
            ontology: ontology::Ontology::load_embedded().unwrap(),
        }
    }

    async fn create_schema(&self) {
        let client = self.context.create_client();
        create_unversioned_tables(&client, &self.ontology)
            .await
            .unwrap();
        self.replace_view().await;
    }

    async fn replace_view(&self) {
        replace_refreshable_views_for_version(
            &self.context.create_client(),
            &self.ontology,
            *SCHEMA_VERSION,
        )
        .await
        .unwrap();
    }

    async fn drop_view(&self) {
        drop_refreshable_views_for_version(
            &self.context.create_client(),
            &self.ontology,
            *SCHEMA_VERSION,
        )
        .await
        .unwrap();
    }

    async fn assert_view_count(&self, expected_count: i64) {
        let view_name = t("namespace_storage_snapshot_refresh");
        assert_eq!(
            self.get_table_or_view_count(&view_name).await,
            expected_count
        );
    }

    async fn insert_snapshot_row(&self) {
        self.context
            .execute(
                "INSERT INTO namespace_storage_snapshot \
                 (snapshot_date, schema_version, logical_table, top_level_namespace, compressed_bytes) \
                 VALUES (today(), 1, 'gl_note', '1/111', 42)",
            )
            .await;
    }

    async fn assert_snapshot_row_exists(&self) {
        assert_eq!(
            self.query_first_i64_or_zero(
                "SELECT toInt64(compressed_bytes) FROM namespace_storage_snapshot \
                 WHERE compressed_bytes = 42",
            )
            .await,
            42
        );
    }

    async fn insert_storage_attribution_rows(&self) -> NamespaceStorageAttributionFixture {
        let namespace_attributable_tables = self
            .insert_rows_into_all_namespace_attributable_tables()
            .await;
        let global_tables = self.insert_rows_into_all_global_tables().await;
        let tables_with_rows_in_two_namespaces =
            self.insert_note_and_edge_rows_for_two_namespaces().await;
        NamespaceStorageAttributionFixture {
            namespace_attributable_tables,
            global_tables,
            tables_with_rows_in_two_namespaces,
        }
    }

    async fn optimize_graph_tables_and_refresh(&self) {
        self.context.optimize_all().await;
        self.refresh_and_wait().await;
    }

    async fn assert_all_rows_have_compressed_bytes(&self) {
        assert_eq!(
            self.query_first_i64_or_zero(
                "SELECT toInt64(count()) FROM namespace_storage_snapshot \
                 WHERE compressed_bytes = 0",
            )
            .await,
            0
        );
    }

    async fn assert_namespace_attribution(&self, fixture: &NamespaceStorageAttributionFixture) {
        for logical_table in &fixture.namespace_attributable_tables {
            let attributed_bytes = self
                .get_snapshot_compressed_bytes_for_table(logical_table)
                .await;
            let compressed_bytes = self.get_table_compressed_bytes(&t(logical_table)).await;
            let namespace_count = self
                .get_snapshot_namespace_count_for_table(logical_table)
                .await;
            assert!(
                attributed_bytes <= compressed_bytes
                    && compressed_bytes - attributed_bytes <= namespace_count,
                "{logical_table}: attributed {attributed_bytes} bytes, actual {compressed_bytes} bytes"
            );
            let expected_namespace_count = if fixture
                .tables_with_rows_in_two_namespaces
                .contains(&logical_table.as_str())
            {
                2
            } else {
                1
            };
            assert_eq!(namespace_count, expected_namespace_count, "{logical_table}");
        }
    }

    async fn assert_global_table_attribution(&self, fixture: &NamespaceStorageAttributionFixture) {
        for logical_table in &fixture.global_tables {
            assert_eq!(
                self.get_snapshot_compressed_bytes_for_table_and_namespace(
                    logical_table,
                    GLOBAL_TOP_LEVEL_NAMESPACE,
                )
                .await,
                self.get_table_compressed_bytes(&t(logical_table)).await,
                "{logical_table}"
            );
        }
    }

    async fn assert_refresh_preserves_current_snapshot(&self) {
        let rows_before_refresh = self.get_snapshot_row_count().await;
        let bytes_before_refresh = self.get_snapshot_compressed_bytes().await;
        self.context
            .execute("OPTIMIZE TABLE namespace_storage_snapshot FINAL")
            .await;
        self.refresh_and_wait().await;
        self.context
            .execute("OPTIMIZE TABLE namespace_storage_snapshot FINAL")
            .await;

        assert_eq!(self.get_snapshot_row_count().await, rows_before_refresh);
        assert_eq!(
            self.get_snapshot_compressed_bytes().await,
            bytes_before_refresh
        );
    }

    async fn insert_rows_into_all_namespace_attributable_tables(&self) -> Vec<String> {
        let mut logical_tables = self
            .ontology
            .nodes()
            .filter(|node| node.has_traversal_path)
            .map(|node| node.destination_table.clone())
            .collect::<BTreeSet<_>>();
        logical_tables.extend(
            self.ontology
                .edge_tables()
                .into_iter()
                .filter(|table| {
                    self.ontology
                        .edge_table_config(table)
                        .is_some_and(ontology::EdgeTableConfig::has_traversal_path)
                })
                .map(str::to_owned),
        );
        for logical_table in &logical_tables {
            let physical_table = t(logical_table);
            self.context
                .execute(&format!(
                    "INSERT INTO `{physical_table}` (traversal_path) SELECT '1/111/'"
                ))
                .await;
        }
        logical_tables.into_iter().collect()
    }

    async fn insert_rows_into_all_global_tables(&self) -> Vec<String> {
        let global_tables = self
            .ontology
            .global_tables()
            .into_iter()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        for logical_table in &global_tables {
            let physical_table = t(logical_table);
            self.context
                .execute(&format!(
                    "INSERT INTO `{physical_table}` (id) SELECT number FROM numbers(50)"
                ))
                .await;
        }
        global_tables
    }

    async fn insert_note_and_edge_rows_for_two_namespaces(&self) -> [&'static str; 2] {
        let note_table = t("gl_note");
        self.context
            .execute(&format!(
                "INSERT INTO {note_table} (traversal_path, id, note) \
                 SELECT '1/111/', number, concat('note-', toString(number)) FROM numbers(5000)"
            ))
            .await;
        self.context
            .execute(&format!(
                "INSERT INTO {note_table} (traversal_path, id, note) \
                 SELECT '1/222/', number, concat('note-', toString(number)) FROM numbers(5000, 5000)"
            ))
            .await;

        let edge_table = t("gl_edge");
        for traversal_path in ["1/111/", "1/222/"] {
            self.context
                .execute(&format!(
                    "INSERT INTO {edge_table} \
                     (traversal_path, relationship_kind, source_id, source_kind, target_id, target_kind) \
                     SELECT '{traversal_path}', 'MENTIONS', number, 'WorkItem', number + 1, 'User' \
                     FROM numbers(3000)"
                ))
                .await;
        }
        ["gl_note", "gl_edge"]
    }

    async fn refresh_and_wait(&self) {
        let view_name = t("namespace_storage_snapshot_refresh");
        self.context
            .execute(&format!("SYSTEM REFRESH VIEW {view_name}"))
            .await;

        for _ in 0..300 {
            let batches = self
                .context
                .query(&format!(
                    "SELECT status, exception FROM system.view_refreshes WHERE view = '{view_name}'"
                ))
                .await;
            let statuses = String::extract_column(&batches, 0).unwrap();
            let exceptions = String::extract_column(&batches, 1).unwrap();
            assert!(exceptions.iter().all(String::is_empty), "{exceptions:?}");
            if statuses.first().is_some_and(|status| status == "Scheduled") {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("{view_name} refresh did not finish");
    }

    async fn get_table_or_view_count(&self, name: &str) -> i64 {
        self.query_first_i64_or_zero(&format!(
            "SELECT toInt64(count()) FROM system.tables \
             WHERE database = currentDatabase() AND name = '{name}'"
        ))
        .await
    }

    async fn get_table_compressed_bytes(&self, physical_table: &str) -> i64 {
        self.query_first_i64_or_zero(&format!(
            "SELECT toInt64(sum(data_compressed_bytes)) FROM system.parts \
             WHERE database = currentDatabase() AND active AND table = '{physical_table}'"
        ))
        .await
    }

    async fn get_snapshot_row_count(&self) -> i64 {
        self.query_first_i64_or_zero("SELECT toInt64(count()) FROM namespace_storage_snapshot")
            .await
    }

    async fn get_snapshot_compressed_bytes(&self) -> i64 {
        self.query_first_i64_or_zero(
            "SELECT toInt64(sum(compressed_bytes)) FROM namespace_storage_snapshot",
        )
        .await
    }

    async fn get_snapshot_compressed_bytes_for_table(&self, logical_table: &str) -> i64 {
        self.query_first_i64_or_zero(&format!(
            "SELECT toInt64(sum(compressed_bytes)) FROM namespace_storage_snapshot \
             WHERE logical_table = '{logical_table}'"
        ))
        .await
    }

    async fn get_snapshot_compressed_bytes_for_table_and_namespace(
        &self,
        logical_table: &str,
        top_level_namespace: &str,
    ) -> i64 {
        self.query_first_i64_or_zero(&format!(
            "SELECT toInt64(sum(compressed_bytes)) FROM namespace_storage_snapshot \
             WHERE logical_table = '{logical_table}' \
               AND top_level_namespace = '{top_level_namespace}'"
        ))
        .await
    }

    async fn get_snapshot_namespace_count_for_table(&self, logical_table: &str) -> i64 {
        self.query_first_i64_or_zero(&format!(
            "SELECT toInt64(count(DISTINCT top_level_namespace)) \
             FROM namespace_storage_snapshot WHERE logical_table = '{logical_table}'"
        ))
        .await
    }

    async fn query_first_i64_or_zero(&self, sql: &str) -> i64 {
        let batches = self.context.query(sql).await;
        i64::extract_column(&batches, 0)
            .unwrap()
            .first()
            .copied()
            .unwrap_or(0)
    }
}

fn namespace_storage_table_and_view_names() -> [String; 2] {
    [
        "namespace_storage_snapshot".to_string(),
        t("namespace_storage_snapshot_refresh"),
    ]
}
