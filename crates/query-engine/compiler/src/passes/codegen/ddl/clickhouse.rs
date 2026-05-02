//! ClickHouse DDL code generation.
//!
//! Emits `CREATE TABLE IF NOT EXISTS` statements from the DDL AST.

use crate::ast::ddl::{Codec, ColumnType, CreateTable, IndexType, ProjectionDef};

/// ClickHouse reserved words that must be backtick-quoted when used as column identifiers.
/// This is a conservative list of words that cause parse ambiguity in CREATE TABLE DDL.
const RESERVED_WORDS: &[&str] = &[
    "when", "order", "table", "database", "select", "from", "where", "group", "having", "limit",
];

/// Backtick-quotes an identifier if it is a ClickHouse reserved word.
/// Escapes any embedded backticks by doubling them (```` → `````````).
fn quote_ident(name: &str) -> String {
    let bare = name.trim_matches('`').replace('`', "``");
    if RESERVED_WORDS.contains(&bare.to_lowercase().as_str()) {
        format!("`{bare}`")
    } else {
        bare
    }
}

fn emit_column_type(ct: &ColumnType) -> String {
    match ct {
        ColumnType::Int64 => "Int64".into(),
        ColumnType::UInt64 => "UInt64".into(),
        ColumnType::UInt32 => "UInt32".into(),
        ColumnType::Bool => "Bool".into(),
        ColumnType::String => "String".into(),
        ColumnType::Date32 => "Date32".into(),
        ColumnType::DateTime => "DateTime".into(),
        ColumnType::Timestamp {
            precision,
            timezone: Some(tz),
        } => {
            let safe_tz = tz.replace('\\', "\\\\").replace('\'', "\\'");
            format!("DateTime64({precision}, '{safe_tz}')")
        }
        ColumnType::Timestamp {
            precision,
            timezone: None,
        } => format!("DateTime64({precision})"),
        ColumnType::Enum8(variants) => {
            let items: Vec<String> = variants
                .iter()
                .map(|(label, value)| {
                    let safe = label.replace('\\', "\\\\").replace('\'', "\\'");
                    format!("'{safe}' = {value}")
                })
                .collect();
            format!("Enum8({})", items.join(", "))
        }
        ColumnType::Nullable(inner) => format!("Nullable({})", emit_column_type(inner)),
        ColumnType::LowCardinality(inner) => {
            format!("LowCardinality({})", emit_column_type(inner))
        }
        ColumnType::Array(inner) => format!("Array({})", emit_column_type(inner)),
    }
}

fn emit_codec(codec: &Codec) -> String {
    match codec {
        Codec::ZSTD(level) => format!("ZSTD({level})"),
        Codec::Delta(width) => format!("Delta({width})"),
        Codec::LZ4 => "LZ4".into(),
    }
}

fn emit_index_type(it: &IndexType) -> String {
    match it {
        IndexType::MinMax => "minmax".into(),
        IndexType::Set(n) => format!("set({n})"),
        IndexType::BloomFilter(rate) => format!("bloom_filter({rate:.2})"),
        IndexType::Text(params) => format!("text({params})"),
        IndexType::NgramBF(params) => format!("ngrambf_v1({params})"),
        IndexType::TokenBF(params) => format!("tokenbf_v1({params})"),
    }
}

/// Emits a complete `CREATE TABLE IF NOT EXISTS` statement for ClickHouse.
pub fn emit_create_table(table: &CreateTable) -> String {
    let mut parts = Vec::new();

    // Column definitions, indexes, and projections live inside the parens.
    let mut body_items: Vec<String> = Vec::new();

    for col in &table.columns {
        let mut col_parts = vec![format!(
            "    {} {}",
            quote_ident(&col.name),
            emit_column_type(&col.data_type)
        )];
        if let Some(default) = &col.default {
            col_parts.push(format!("DEFAULT {default}"));
        }
        if let Some(codecs) = &col.codec {
            let codec_list: Vec<String> = codecs.iter().map(emit_codec).collect();
            col_parts.push(format!("CODEC({})", codec_list.join(", ")));
        }
        body_items.push(col_parts.join(" "));
    }

    for idx in &table.indexes {
        body_items.push(format!(
            "    INDEX {} {} TYPE {} GRANULARITY {}",
            quote_ident(&idx.name),
            quote_ident(&idx.expression),
            emit_index_type(&idx.index_type),
            idx.granularity
        ));
    }

    for proj in &table.projections {
        body_items.push(emit_projection(proj));
    }

    let engine_args = if table.engine.args.is_empty() {
        String::new()
    } else {
        format!("({})", table.engine.args.join(", "))
    };

    parts.push(format!(
        "CREATE TABLE IF NOT EXISTS {} (\n{}\n) ENGINE = {}{}",
        table.name,
        body_items.join(",\n"),
        table.engine.name,
        engine_args,
    ));

    // ORDER BY [PRIMARY KEY]
    // MergeTree-family engines require ORDER BY. Emit `ORDER BY tuple()` as fallback.
    if table.order_by.is_empty() {
        if table.engine.name.contains("MergeTree") {
            parts.push("ORDER BY tuple()".into());
        }
    } else {
        let order_by = format!("ORDER BY ({})", table.order_by.join(", "));
        if let Some(pk) = &table.primary_key {
            let pk_str = format!("PRIMARY KEY ({})", pk.join(", "));
            if pk == &table.order_by {
                // Same value: emit on one line (matches graph.sql convention)
                parts.push(format!("{order_by} {pk_str}"));
            } else {
                // Different values: emit on separate lines
                parts.push(order_by);
                parts.push(pk_str);
            }
        } else {
            parts.push(order_by);
        }
    }

    // SETTINGS
    if !table.settings.is_empty() {
        let settings: Vec<String> = table
            .settings
            .iter()
            .map(|s| format!("{} = {}", s.key, s.value))
            .collect();
        parts.push(format!("SETTINGS {}", settings.join(", ")));
    }

    parts.join("\n")
}

fn emit_projection(proj: &ProjectionDef) -> String {
    match proj {
        ProjectionDef::Reorder { name, order_by } => {
            assert!(
                !order_by.is_empty(),
                "Reorder projection '{name}' has empty order_by"
            );
            let order = if order_by.len() == 1 {
                order_by[0].clone()
            } else {
                format!("({})", order_by.join(", "))
            };
            format!("    PROJECTION {name} (SELECT * ORDER BY {order})")
        }
        ProjectionDef::Aggregate {
            name,
            select,
            group_by,
        } => {
            format!(
                "    PROJECTION {name} (\n      SELECT {}\n      GROUP BY {}\n    )",
                select.join(", "),
                group_by.join(", ")
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::ddl::*;

    #[test]
    fn emit_simple_table() {
        let table = CreateTable {
            name: "checkpoint".into(),
            columns: vec![
                ColumnDef::new("key", ColumnType::String).with_codec(vec![Codec::ZSTD(1)]),
                ColumnDef::new(
                    "_version",
                    ColumnType::Timestamp {
                        precision: 6,
                        timezone: Some("UTC".into()),
                    },
                )
                .with_default("now64(6)")
                .with_codec(vec![Codec::ZSTD(1)]),
                ColumnDef::new("_deleted", ColumnType::Bool).with_default("false"),
            ],
            indexes: vec![],
            projections: vec![],
            engine: Engine::replacing_merge_tree("_version", "_deleted"),
            order_by: vec!["key".into()],
            primary_key: None,
            settings: vec![TableSetting {
                key: "allow_experimental_replacing_merge_with_cleanup".into(),
                value: "1".into(),
            }],
        };

        let sql = emit_create_table(&table);
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS checkpoint"));
        assert!(sql.contains("key String CODEC(ZSTD(1))"));
        assert!(sql.contains("_version DateTime64(6, 'UTC') DEFAULT now64(6) CODEC(ZSTD(1))"));
        assert!(sql.contains("_deleted Bool DEFAULT false"));
        assert!(sql.contains("ENGINE = ReplacingMergeTree(_version, _deleted)"));
        assert!(sql.contains("ORDER BY (key)"));
        assert!(sql.contains("SETTINGS allow_experimental_replacing_merge_with_cleanup = 1"));
        assert!(!sql.contains("PRIMARY KEY"));
    }

    #[test]
    fn emit_table_with_indexes_and_projections() {
        let table = CreateTable {
            name: "gl_project".into(),
            columns: vec![
                ColumnDef::new("id", ColumnType::Int64)
                    .with_codec(vec![Codec::Delta(8), Codec::ZSTD(1)]),
                ColumnDef::new("traversal_path", ColumnType::String)
                    .with_default("'0/'")
                    .with_codec(vec![Codec::ZSTD(1)]),
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
            order_by: vec!["traversal_path".into(), "id".into()],
            primary_key: Some(vec!["traversal_path".into(), "id".into()]),
            settings: vec![
                TableSetting {
                    key: "index_granularity".into(),
                    value: "2048".into(),
                },
                TableSetting {
                    key: "deduplicate_merge_projection_mode".into(),
                    value: "'rebuild'".into(),
                },
            ],
        };

        let sql = emit_create_table(&table);
        assert!(sql.contains("INDEX idx_id id TYPE bloom_filter(0.01) GRANULARITY 1"));
        assert!(sql.contains("PROJECTION by_id (SELECT * ORDER BY id)"));
        assert!(sql.contains("ORDER BY (traversal_path, id)"));
        assert!(sql.contains("PRIMARY KEY (traversal_path, id)"));
        assert!(sql.contains("index_granularity = 2048"));
        assert!(sql.contains("deduplicate_merge_projection_mode = 'rebuild'"));
    }

    #[test]
    fn emit_table_with_aggregate_projection() {
        let table = CreateTable {
            name: "gl_edge".into(),
            columns: vec![
                ColumnDef::new("traversal_path", ColumnType::String),
                ColumnDef::new("source_id", ColumnType::Int64),
            ],
            indexes: vec![],
            projections: vec![ProjectionDef::Aggregate {
                name: "node_edge_counts".into(),
                select: vec![
                    "traversal_path".into(),
                    "source_kind".into(),
                    "target_kind".into(),
                    "relationship_kind".into(),
                    "uniq(source_id)".into(),
                    "uniq(target_id)".into(),
                    "count()".into(),
                ],
                group_by: vec![
                    "traversal_path".into(),
                    "source_kind".into(),
                    "target_kind".into(),
                    "relationship_kind".into(),
                ],
            }],
            engine: Engine::replacing_merge_tree("_version", "_deleted"),
            order_by: vec!["traversal_path".into(), "source_id".into()],
            primary_key: None,
            settings: vec![],
        };

        let sql = emit_create_table(&table);
        assert!(sql.contains("PROJECTION node_edge_counts"));
        assert!(sql.contains("uniq(source_id), uniq(target_id), count()"));
        assert!(
            sql.contains("GROUP BY traversal_path, source_kind, target_kind, relationship_kind")
        );
    }

    #[test]
    fn emit_with_prefix() {
        let table = CreateTable::new(
            "gl_project",
            Engine::replacing_merge_tree("_version", "_deleted"),
        )
        .with_prefix("v1_");
        let sql = emit_create_table(&table);
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS v1_gl_project"));
    }

    #[test]
    fn emit_preserves_lowcardinality_nullable() {
        let table = CreateTable {
            name: "test".into(),
            columns: vec![ColumnDef::new(
                "vis",
                ColumnType::LowCardinality(Box::new(ColumnType::Nullable(Box::new(
                    ColumnType::String,
                )))),
            )],
            indexes: vec![],
            projections: vec![],
            engine: Engine {
                name: "MergeTree".into(),
                args: vec![],
            },
            order_by: vec!["vis".into()],
            primary_key: None,
            settings: vec![],
        };

        let sql = emit_create_table(&table);
        assert!(sql.contains("vis LowCardinality(Nullable(String))"));
        assert!(sql.contains("ENGINE = MergeTree"));
    }

    #[test]
    fn emit_reserved_word_column_gets_quoted() {
        let table = CreateTable {
            name: "test".into(),
            columns: vec![ColumnDef::new("when", ColumnType::String)],
            indexes: vec![],
            projections: vec![],
            engine: Engine {
                name: "MergeTree".into(),
                args: vec![],
            },
            order_by: vec!["id".into()],
            primary_key: None,
            settings: vec![],
        };

        let sql = emit_create_table(&table);
        assert!(
            sql.contains("`when` String"),
            "reserved word should be backtick-quoted: {sql}"
        );
    }

    #[test]
    fn emit_empty_order_by_with_mergetree_emits_tuple() {
        let table = CreateTable {
            name: "test".into(),
            columns: vec![ColumnDef::new("id", ColumnType::Int64)],
            indexes: vec![],
            projections: vec![],
            engine: Engine {
                name: "MergeTree".into(),
                args: vec![],
            },
            order_by: vec![],
            primary_key: None,
            settings: vec![],
        };

        let sql = emit_create_table(&table);
        assert!(
            sql.contains("ORDER BY tuple()"),
            "empty ORDER BY with MergeTree should emit tuple(): {sql}"
        );
    }

    #[test]
    fn emit_uint32_column() {
        let table = CreateTable {
            name: "test".into(),
            columns: vec![ColumnDef::new("version", ColumnType::UInt32)],
            indexes: vec![],
            projections: vec![],
            engine: Engine {
                name: "MergeTree".into(),
                args: vec![],
            },
            order_by: vec!["version".into()],
            primary_key: None,
            settings: vec![],
        };

        let sql = emit_create_table(&table);
        assert!(
            sql.contains("version UInt32"),
            "UInt32 column type should emit: {sql}"
        );
    }

    #[test]
    fn emit_datetime_column() {
        let table = CreateTable {
            name: "test".into(),
            columns: vec![ColumnDef::new("created_at", ColumnType::DateTime)],
            indexes: vec![],
            projections: vec![],
            engine: Engine {
                name: "MergeTree".into(),
                args: vec![],
            },
            order_by: vec!["created_at".into()],
            primary_key: None,
            settings: vec![],
        };

        let sql = emit_create_table(&table);
        assert!(
            sql.contains("created_at DateTime"),
            "DateTime column type should emit: {sql}"
        );
        assert!(
            !sql.contains("DateTime64"),
            "DateTime should not emit DateTime64: {sql}"
        );
    }

    #[test]
    fn emit_enum8_column() {
        let table = CreateTable {
            name: "test".into(),
            columns: vec![ColumnDef::new(
                "status",
                ColumnType::Enum8(vec![
                    ("active".into(), 1),
                    ("migrating".into(), 2),
                    ("deleted".into(), 3),
                ]),
            )],
            indexes: vec![],
            projections: vec![],
            engine: Engine {
                name: "MergeTree".into(),
                args: vec![],
            },
            order_by: vec!["status".into()],
            primary_key: None,
            settings: vec![],
        };

        let sql = emit_create_table(&table);
        assert!(
            sql.contains("Enum8('active' = 1, 'migrating' = 2, 'deleted' = 3)"),
            "Enum8 column type should emit variant list: {sql}"
        );
    }

    #[test]
    fn emit_bloom_filter_stable_precision() {
        let idx = IndexDef {
            name: "idx_id".into(),
            expression: "id".into(),
            index_type: IndexType::BloomFilter(0.01),
            granularity: 1,
        };
        let table = CreateTable {
            name: "test".into(),
            columns: vec![ColumnDef::new("id", ColumnType::Int64)],
            indexes: vec![idx],
            projections: vec![],
            engine: Engine {
                name: "MergeTree".into(),
                args: vec![],
            },
            order_by: vec!["id".into()],
            primary_key: None,
            settings: vec![],
        };

        let sql = emit_create_table(&table);
        assert!(
            sql.contains("bloom_filter(0.01)"),
            "bloom_filter precision should be stable: {sql}"
        );
    }
}
