//! DuckDB DDL code generation.
//!
//! Emits `CREATE TABLE IF NOT EXISTS` statements from the DDL AST,
//! targeting DuckDB SQL dialect. ClickHouse-specific features (codecs,
//! indexes, projections, engine, settings) are omitted.

use crate::ast::ddl::{ColumnType, CreateTable};

/// Recursively checks whether a column type is nullable, handling
/// wrappers like `LowCardinality(Nullable(String))`.
fn is_nullable(ct: &ColumnType) -> bool {
    match ct {
        ColumnType::Nullable(_) => true,
        ColumnType::LowCardinality(inner) => is_nullable(inner),
        _ => false,
    }
}

fn emit_column_type(ct: &ColumnType) -> String {
    match ct {
        ColumnType::Int64 => "BIGINT".into(),
        ColumnType::UInt64 => "UBIGINT".into(),
        ColumnType::UInt32 => "UINTEGER".into(),
        ColumnType::Bool => "BOOLEAN".into(),
        ColumnType::String => "VARCHAR".into(),
        ColumnType::Date32 => "DATE".into(),
        ColumnType::DateTime => "TIMESTAMP".into(),
        ColumnType::Timestamp { .. } => "TIMESTAMP".into(),
        ColumnType::Enum8(_) => "VARCHAR".into(),
        ColumnType::Nullable(inner) => emit_column_type(inner),
        ColumnType::LowCardinality(inner) => emit_column_type(inner),
        ColumnType::Array(inner) => format!("{}[]", emit_column_type(inner)),
    }
}

/// Emits a `CREATE TABLE IF NOT EXISTS` statement for DuckDB.
///
/// Strips ClickHouse-specific features: codecs, indexes, projections,
/// engine clauses, and settings. Defaults are preserved only when they
/// are plain literal values (not ClickHouse function calls).
pub fn emit_create_table(table: &CreateTable) -> String {
    let mut col_defs: Vec<String> = Vec::new();

    for col in &table.columns {
        let not_null = !is_nullable(&col.data_type);
        let mut parts = vec![format!(
            "    {} {}",
            &col.name,
            emit_column_type(&col.data_type)
        )];
        if not_null {
            parts.push("NOT NULL".into());
        }
        if let Some(default) = &col.default
            && is_duckdb_compatible_default(default)
        {
            parts.push(format!("DEFAULT {default}"));
        }
        col_defs.push(parts.join(" "));
    }

    format!(
        "CREATE TABLE IF NOT EXISTS {} (\n{}\n)",
        table.name,
        col_defs.join(",\n"),
    )
}

/// Returns true if a DEFAULT expression is valid in DuckDB.
///
/// ClickHouse-specific defaults like `now64(6)` are not valid.
/// String literals, numeric literals, and `false`/`true` are fine.
fn is_duckdb_compatible_default(default: &str) -> bool {
    let d = default.trim();
    if d.eq_ignore_ascii_case("false") || d.eq_ignore_ascii_case("true") {
        return true;
    }
    if d.starts_with('\'') && d.ends_with('\'') {
        return true;
    }
    d.parse::<f64>().is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::ddl::*;

    #[test]
    fn emit_simple_table() {
        let table = CreateTable {
            name: "gl_file".into(),
            columns: vec![
                ColumnDef::new("id", ColumnType::Int64),
                ColumnDef::new("project_id", ColumnType::Int64),
                ColumnDef::new("name", ColumnType::String),
            ],
            indexes: vec![],
            projections: vec![],
            engine: Engine {
                name: "ignored".into(),
                args: vec![],
            },
            order_by: vec![],
            primary_key: None,
            settings: vec![],
        };

        let sql = emit_create_table(&table);
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS gl_file"));
        assert!(sql.contains("id BIGINT NOT NULL"));
        assert!(sql.contains("project_id BIGINT NOT NULL"));
        assert!(sql.contains("name VARCHAR NOT NULL"));
        assert!(!sql.contains("ENGINE"));
        assert!(!sql.contains("ORDER BY"));
        assert!(!sql.contains("CODEC"));
    }

    #[test]
    fn nullable_columns_omit_not_null() {
        let table = CreateTable {
            name: "test".into(),
            columns: vec![
                ColumnDef::new("id", ColumnType::Int64),
                ColumnDef::new("alias", ColumnType::Nullable(Box::new(ColumnType::String))),
            ],
            indexes: vec![],
            projections: vec![],
            engine: Engine {
                name: "ignored".into(),
                args: vec![],
            },
            order_by: vec![],
            primary_key: None,
            settings: vec![],
        };

        let sql = emit_create_table(&table);
        assert!(sql.contains("id BIGINT NOT NULL"));
        assert!(sql.contains("alias VARCHAR,") || sql.contains("alias VARCHAR\n"));
        assert!(!sql.contains("alias VARCHAR NOT NULL"));
    }

    #[test]
    fn lowcardinality_unwrapped() {
        let table = CreateTable {
            name: "test".into(),
            columns: vec![ColumnDef::new(
                "lang",
                ColumnType::LowCardinality(Box::new(ColumnType::String)),
            )],
            indexes: vec![],
            projections: vec![],
            engine: Engine {
                name: "ignored".into(),
                args: vec![],
            },
            order_by: vec![],
            primary_key: None,
            settings: vec![],
        };

        let sql = emit_create_table(&table);
        assert!(
            sql.contains("lang VARCHAR NOT NULL"),
            "LowCardinality should unwrap to plain type: {sql}"
        );
    }

    #[test]
    fn lowcardinality_nullable_is_nullable() {
        let table = CreateTable {
            name: "test".into(),
            columns: vec![ColumnDef::new(
                "visibility",
                ColumnType::LowCardinality(Box::new(ColumnType::Nullable(Box::new(
                    ColumnType::String,
                )))),
            )],
            indexes: vec![],
            projections: vec![],
            engine: Engine {
                name: "ignored".into(),
                args: vec![],
            },
            order_by: vec![],
            primary_key: None,
            settings: vec![],
        };

        let sql = emit_create_table(&table);
        assert!(
            !sql.contains("NOT NULL"),
            "LowCardinality(Nullable(String)) must not emit NOT NULL: {sql}"
        );
        assert!(
            sql.contains("visibility VARCHAR"),
            "should unwrap to VARCHAR: {sql}"
        );
    }

    #[test]
    fn clickhouse_defaults_stripped() {
        let table = CreateTable {
            name: "test".into(),
            columns: vec![
                ColumnDef::new("v", ColumnType::Int64).with_default("now64(6)"),
                ColumnDef::new("s", ColumnType::String).with_default("''"),
                ColumnDef::new("b", ColumnType::Bool).with_default("false"),
            ],
            indexes: vec![],
            projections: vec![],
            engine: Engine {
                name: "ignored".into(),
                args: vec![],
            },
            order_by: vec![],
            primary_key: None,
            settings: vec![],
        };

        let sql = emit_create_table(&table);
        assert!(
            !sql.contains("now64"),
            "ClickHouse-specific default should be stripped: {sql}"
        );
        assert!(
            sql.contains("DEFAULT ''"),
            "string literal default should be preserved: {sql}"
        );
        assert!(
            sql.contains("DEFAULT false"),
            "boolean default should be preserved: {sql}"
        );
    }

    #[test]
    fn codecs_and_indexes_ignored() {
        let table = CreateTable {
            name: "test".into(),
            columns: vec![
                ColumnDef::new("id", ColumnType::Int64)
                    .with_codec(vec![Codec::Delta(8), Codec::ZSTD(1)]),
            ],
            indexes: vec![IndexDef {
                name: "idx_id".into(),
                expression: "id".into(),
                index_type: IndexType::BloomFilter(0.01),
                granularity: 1,
            }],
            projections: vec![ProjectionDef::Reorder {
                name: "by_id".into(),
                order_by: vec!["id".into()],
            }],
            engine: Engine::replacing_merge_tree("_version", "_deleted"),
            order_by: vec!["id".into()],
            primary_key: None,
            settings: vec![TableSetting {
                key: "index_granularity".into(),
                value: "2048".into(),
            }],
        };

        let sql = emit_create_table(&table);
        assert!(!sql.contains("CODEC"), "codecs should be omitted: {sql}");
        assert!(!sql.contains("INDEX"), "indexes should be omitted: {sql}");
        assert!(
            !sql.contains("PROJECTION"),
            "projections should be omitted: {sql}"
        );
        assert!(!sql.contains("ENGINE"), "engine should be omitted: {sql}");
        assert!(
            !sql.contains("SETTINGS"),
            "settings should be omitted: {sql}"
        );
        assert!(
            !sql.contains("ORDER BY"),
            "ORDER BY should be omitted: {sql}"
        );
    }

    #[test]
    fn enum8_maps_to_varchar() {
        let table = CreateTable {
            name: "test".into(),
            columns: vec![ColumnDef::new(
                "status",
                ColumnType::Enum8(vec![("active".into(), 1), ("migrating".into(), 2)]),
            )],
            indexes: vec![],
            projections: vec![],
            engine: Engine {
                name: "ignored".into(),
                args: vec![],
            },
            order_by: vec![],
            primary_key: None,
            settings: vec![],
        };

        let sql = emit_create_table(&table);
        assert!(
            sql.contains("status VARCHAR NOT NULL"),
            "Enum8 should map to VARCHAR: {sql}"
        );
    }
}
