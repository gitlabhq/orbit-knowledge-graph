//! Schema version tracking with table prefix support.
//!
//! Loads the schema version from `config/SCHEMA_VERSION` (embedded at compile
//! time via `include_str!`) and persists it in the `gkg_schema_version`
//! ClickHouse control table.
//!
//! Table prefix derivation maps a version number to the string prepended to
//! graph table names. Version 0 uses no prefix (backward compatible); version N
//! uses `vN_`.

use std::sync::LazyLock;

use arrow::datatypes::UInt32Type;
use clickhouse_client::ArrowClickHouseClient;
use gkg_utils::arrow::ArrowUtils;
use query_engine::compiler::ast::ddl::{ColumnDef, ColumnType, CreateTable, Engine};
use query_engine::compiler::emit_create_table;
use query_engine::compiler::emit_simple_query;
use query_engine::compiler::{Expr, Insert, Node, OrderExpr, Query, SelectExpr, TableRef};
use thiserror::Error;
use tracing::info;

const VERSION_TABLE: &str = "gkg_schema_version";

/// Non-ontology graph tables that must always be prefixed alongside the
/// ontology-driven node and edge tables.
///
/// These tables are defined directly in `config/graph.sql` and are not
/// tracked in the ontology YAML. When adding a new non-ontology table to
/// `graph.sql`, add it here too — CI validates that this list is complete.
pub const NON_ONTOLOGY_GRAPH_TABLES: &[&str] = &[
    "checkpoint",
    "code_indexing_checkpoint",
    "namespace_deletion_schedule",
];

/// Returns all graph table names (unprefixed) that must be created for a
/// given schema version.
///
/// Combines the non-ontology tables from [`NON_ONTOLOGY_GRAPH_TABLES`] with
/// ontology-derived node and edge tables. The ontology is the source of truth
/// for node/edge table names; the constant list covers auxiliary tables.
pub fn all_graph_tables(ontology: &ontology::Ontology) -> Vec<String> {
    let mut tables: Vec<String> = NON_ONTOLOGY_GRAPH_TABLES
        .iter()
        .map(|s| (*s).to_string())
        .collect();

    for node in ontology.nodes() {
        tables.push(node.destination_table.clone());
    }

    for edge_table in ontology.edge_tables() {
        if !tables.contains(&edge_table.to_string()) {
            tables.push(edge_table.to_string());
        }
    }

    tables
}

/// Schema version loaded from `config/SCHEMA_VERSION`.
///
/// Bump this file whenever `config/graph.sql` or `config/ontology/` changes
/// in a way that requires a new table-set. The CI `schema-version-check` job
/// enforces this.
pub static SCHEMA_VERSION: LazyLock<u32> = LazyLock::new(|| {
    include_str!("../../../config/SCHEMA_VERSION")
        .trim()
        .parse()
        .expect("config/SCHEMA_VERSION must contain a valid u32")
});

fn version_table_ddl() -> CreateTable {
    CreateTable {
        name: VERSION_TABLE.into(),
        columns: vec![
            ColumnDef::new("version", ColumnType::UInt32),
            ColumnDef::new(
                "status",
                ColumnType::Enum8(vec![
                    ("active".into(), 1),
                    ("migrating".into(), 2),
                    ("retired".into(), 3),
                    ("dropped".into(), 4),
                ]),
            ),
            ColumnDef::new("created_at", ColumnType::DateTime).with_default("now()"),
        ],
        indexes: vec![],
        projections: vec![],
        engine: Engine::replacing_merge_tree_version_only("created_at"),
        order_by: vec!["version".into()],
        primary_key: None,
        settings: vec![],
    }
}

fn read_active_version_query() -> (
    String,
    std::collections::HashMap<String, gkg_utils::clickhouse::ParamValue>,
) {
    let query = Query {
        select: vec![SelectExpr {
            expr: Expr::col("t", "version"),
            alias: None,
        }],
        from: TableRef::scan_final(VERSION_TABLE, "t"),
        where_clause: Some(Expr::eq(Expr::col("t", "status"), Expr::string("active"))),
        order_by: vec![OrderExpr {
            expr: Expr::col("t", "created_at"),
            desc: true,
        }],
        limit: Some(1),
        ..Query::default()
    };
    emit_simple_query(&Node::Query(Box::new(query)))
        .expect("read_active_version query must be valid")
}

fn write_version_query(
    version: u32,
) -> (
    String,
    std::collections::HashMap<String, gkg_utils::clickhouse::ParamValue>,
) {
    let insert = Insert::new(
        VERSION_TABLE,
        vec!["version".into(), "status".into()],
        vec![vec![Expr::uint32(version), Expr::string("active")]],
    );
    emit_simple_query(&Node::Insert(Box::new(insert))).expect("write_version query must be valid")
}

fn write_migrating_version_query(
    version: u32,
) -> (
    String,
    std::collections::HashMap<String, gkg_utils::clickhouse::ParamValue>,
) {
    let insert = Insert::new(
        VERSION_TABLE,
        vec!["version".into(), "status".into()],
        vec![vec![Expr::uint32(version), Expr::string("migrating")]],
    );
    emit_simple_query(&Node::Insert(Box::new(insert)))
        .expect("write_migrating_version query must be valid")
}

#[derive(Debug, Error)]
pub enum SchemaVersionError {
    #[error("ClickHouse error: {0}")]
    ClickHouse(#[from] clickhouse_client::ClickHouseError),

    #[error("unexpected query result: {0}")]
    UnexpectedResult(String),
}

/// Returns the table prefix for a given schema version.
///
/// Version 0 → `""` (no prefix, backward compatible).
/// Version N → `"vN_"`.
pub fn table_prefix(schema_version: u32) -> String {
    if schema_version == 0 {
        String::new()
    } else {
        format!("v{schema_version}_")
    }
}

/// Returns the fully-qualified (prefixed) table name for the given schema version.
pub fn prefixed_table_name(table: &str, schema_version: u32) -> String {
    format!("{}{}", table_prefix(schema_version), table)
}

/// Creates the `gkg_schema_version` control table if it does not exist.
///
/// This table is never prefixed or dropped across schema versions — it is the
/// single source of truth for which version is active.
pub async fn ensure_version_table(graph: &ArrowClickHouseClient) -> Result<(), SchemaVersionError> {
    let ddl = emit_create_table(&version_table_ddl());
    graph.execute(&ddl).await?;
    Ok(())
}

/// Reads the active schema version from ClickHouse.
///
/// Returns `None` on a fresh install (no rows yet).
/// Uses `FINAL` to handle `ReplacingMergeTree` eventual consistency.
pub async fn read_active_version(
    graph: &ArrowClickHouseClient,
) -> Result<Option<u32>, SchemaVersionError> {
    let (sql, params) = read_active_version_query();
    let mut query = graph.query(&sql);
    for (name, param) in &params {
        query = query.param(name, &param.value);
    }
    let batches = query.fetch_arrow().await?;

    for batch in &batches {
        if batch.num_rows() == 0 {
            continue;
        }
        return Ok(ArrowUtils::get_column::<UInt32Type>(batch, "version", 0));
    }

    Ok(None)
}

/// Records the given version as the active version in ClickHouse.
pub async fn write_schema_version(
    graph: &ArrowClickHouseClient,
    version: u32,
) -> Result<(), SchemaVersionError> {
    let (sql, params) = write_version_query(version);
    let mut query = graph.query(&sql);
    for (name, param) in &params {
        query = query.param(name, &param.value);
    }
    query.execute().await?;
    Ok(())
}

/// Records a schema version as `migrating` in ClickHouse.
///
/// Used by the migration orchestrator to signal that new-prefix tables are
/// being populated. The version remains `migrating` until the Webserver
/// cutover (tracked in a subsequent issue).
pub async fn write_migrating_version(
    graph: &ArrowClickHouseClient,
    version: u32,
) -> Result<(), SchemaVersionError> {
    let (sql, params) = write_migrating_version_query(version);
    let mut query = graph.query(&sql);
    for (name, param) in &params {
        query = query.param(name, &param.value);
    }
    query.execute().await?;
    Ok(())
}

/// Ensures the `gkg_schema_version` table exists and, on a fresh install,
/// records version 0 as active.
///
/// Called by all service modes (Indexer, Webserver, DispatchIndexing) at
/// startup so the control table is always present.
pub async fn init(graph: &ArrowClickHouseClient) -> Result<(), SchemaVersionError> {
    ensure_version_table(graph).await?;

    let active = read_active_version(graph).await?;
    if active.is_none() {
        info!(
            version = *SCHEMA_VERSION,
            "fresh install — recording initial schema version"
        );
        write_schema_version(graph, *SCHEMA_VERSION).await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_version_is_valid_u32() {
        let _ = *SCHEMA_VERSION;
    }

    #[test]
    fn table_prefix_v0_is_empty() {
        assert_eq!(table_prefix(0), "");
    }

    #[test]
    fn table_prefix_v1() {
        assert_eq!(table_prefix(1), "v1_");
    }

    #[test]
    fn table_prefix_v2() {
        assert_eq!(table_prefix(2), "v2_");
    }

    #[test]
    fn prefixed_table_name_v0_no_change() {
        assert_eq!(prefixed_table_name("gl_user", 0), "gl_user");
    }

    #[test]
    fn prefixed_table_name_v1() {
        assert_eq!(prefixed_table_name("gl_user", 1), "v1_gl_user");
    }

    #[test]
    fn prefixed_table_name_v2() {
        assert_eq!(prefixed_table_name("gl_user", 2), "v2_gl_user");
    }

    #[test]
    fn create_table_ddl_uses_replacing_merge_tree() {
        let ddl = emit_create_table(&version_table_ddl());
        assert!(ddl.contains("ReplacingMergeTree"));
        assert!(ddl.contains("IF NOT EXISTS"));
        assert!(ddl.contains("gkg_schema_version"));
        assert!(ddl.contains("UInt32"));
        assert!(ddl.contains("Enum8("));
        assert!(ddl.contains("'active' = 1"));
        assert!(ddl.contains("DateTime"));
        assert!(ddl.contains("ORDER BY (version)"));
    }

    #[test]
    fn read_query_uses_final() {
        let (sql, _params) = read_active_version_query();
        assert!(
            sql.contains("FINAL"),
            "version query must use FINAL for ReplacingMergeTree consistency: {sql}"
        );
        assert!(sql.contains("gkg_schema_version"));
        assert!(sql.contains("ORDER BY"));
        assert!(sql.contains("LIMIT 1"));
    }

    #[test]
    fn table_prefix_large_version() {
        assert_eq!(table_prefix(99), "v99_");
        assert_eq!(prefixed_table_name("checkpoint", 99), "v99_checkpoint");
    }

    #[test]
    fn non_ontology_graph_tables_not_empty() {
        assert!(
            !NON_ONTOLOGY_GRAPH_TABLES.is_empty(),
            "NON_ONTOLOGY_GRAPH_TABLES must list at least checkpoint and code_indexing_checkpoint"
        );
        assert!(NON_ONTOLOGY_GRAPH_TABLES.contains(&"checkpoint"));
        assert!(NON_ONTOLOGY_GRAPH_TABLES.contains(&"code_indexing_checkpoint"));
        assert!(NON_ONTOLOGY_GRAPH_TABLES.contains(&"namespace_deletion_schedule"));
    }

    #[test]
    fn all_graph_tables_includes_non_ontology() {
        let ontology = ontology::Ontology::load_embedded().expect("ontology must load");
        let tables = all_graph_tables(&ontology);
        for non_ont in NON_ONTOLOGY_GRAPH_TABLES {
            assert!(
                tables.contains(&non_ont.to_string()),
                "all_graph_tables must include non-ontology table '{non_ont}'"
            );
        }
    }

    #[test]
    fn all_graph_tables_includes_ontology_nodes() {
        let ontology = ontology::Ontology::load_embedded().expect("ontology must load");
        let tables = all_graph_tables(&ontology);
        for node in ontology.nodes() {
            assert!(
                tables.contains(&node.destination_table),
                "all_graph_tables must include ontology node table '{}'",
                node.destination_table
            );
        }
    }

    #[test]
    fn all_graph_tables_no_duplicates() {
        let ontology = ontology::Ontology::load_embedded().expect("ontology must load");
        let tables = all_graph_tables(&ontology);
        let mut seen = std::collections::HashSet::new();
        for table in &tables {
            assert!(
                seen.insert(table.clone()),
                "duplicate table '{table}' in all_graph_tables"
            );
        }
    }

    #[test]
    fn migrating_query_uses_migrating_status() {
        let (sql, params) = write_migrating_version_query(1);
        assert!(
            sql.contains("gkg_schema_version"),
            "migrating query must target version table: {sql}"
        );
        assert!(
            !params.is_empty(),
            "migrating query must have parameters"
        );
    }
}
