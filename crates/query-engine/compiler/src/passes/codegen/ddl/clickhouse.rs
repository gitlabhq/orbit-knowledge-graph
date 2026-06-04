//! ClickHouse DDL code generation.
//!
//! Emits `CREATE TABLE IF NOT EXISTS` statements from the DDL AST.

use crate::ast::ddl::{
    Codec, ColumnDef, ColumnType, CreateDictionary, CreateMaterializedView, CreateTable, IndexDef,
    IndexType, ProjectionDef,
};
use ontology::constants::{DELETED_COLUMN, VERSION_COLUMN};

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
        IndexType::BloomFilter(rate) => format!("bloom_filter({rate})"),
        IndexType::Text(params) => format!("text({params})"),
        IndexType::NgramBF(params) => format!("ngrambf_v1({params})"),
        IndexType::TokenBF(params) => format!("tokenbf_v1({params})"),
    }
}

/// Emits a single column fragment without indentation:
/// `name type [DEFAULT expr] [CODEC(...)]`. Shared by `emit_create_table` and
/// `ALTER TABLE ... ADD COLUMN` codegen so the type/codec spelling has one home.
pub fn emit_column(col: &ColumnDef) -> String {
    let mut parts = vec![format!(
        "{} {}",
        quote_ident(&col.name),
        emit_column_type(&col.data_type)
    )];
    if let Some(default) = &col.default {
        parts.push(format!("DEFAULT {default}"));
    }
    if let Some(codecs) = &col.codec {
        let codec_list: Vec<String> = codecs.iter().map(emit_codec).collect();
        parts.push(format!("CODEC({})", codec_list.join(", ")));
    }
    parts.join(" ")
}

/// Emits a secondary-index definition without the leading `INDEX` keyword:
/// `name expr TYPE t GRANULARITY g`. `CREATE TABLE` prepends `INDEX`;
/// `ALTER TABLE ... ADD INDEX` supplies the keyword itself.
pub fn emit_index(idx: &IndexDef) -> String {
    format!(
        "{} {} TYPE {} GRANULARITY {}",
        quote_ident(&idx.name),
        quote_ident(&idx.expression),
        emit_index_type(&idx.index_type),
        idx.granularity
    )
}

/// Emits a complete `CREATE TABLE IF NOT EXISTS` statement for ClickHouse.
pub fn emit_create_table(table: &CreateTable) -> String {
    let mut parts = Vec::new();

    // Column definitions, indexes, and projections live inside the parens.
    let mut body_items: Vec<String> = Vec::new();

    for col in &table.columns {
        body_items.push(format!("    {}", emit_column(col)));
    }

    for idx in &table.indexes {
        body_items.push(format!("    INDEX {}", emit_index(idx)));
    }

    for proj in &table.projections {
        body_items.push(format!("    PROJECTION {}", emit_projection(proj)));
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

/// Emits a complete `CREATE MATERIALIZED VIEW IF NOT EXISTS` statement for ClickHouse.
///
/// The `select_query` must already have `{table_name}` placeholders resolved
/// (see [`CreateMaterializedView::with_prefix`]).
pub fn emit_create_materialized_view(mv: &CreateMaterializedView) -> String {
    let mut parts = Vec::new();

    let mut header = format!("CREATE MATERIALIZED VIEW IF NOT EXISTS {}", mv.name);

    if let Some(ref to_table) = mv.to_table {
        header.push_str(&format!("\nTO {to_table}"));
    } else {
        let engine = mv.engine.as_ref().unwrap_or_else(|| {
            panic!(
                "materialized view '{}' uses implicit storage but has no engine; \
                 either set `to_table` or `engine`",
                mv.name
            )
        });
        let engine_args = if engine.args.is_empty() {
            String::new()
        } else {
            format!("({})", engine.args.join(", "))
        };
        header.push_str(&format!("\nENGINE = {}{engine_args}", engine.name));
        if !mv.order_by.is_empty() {
            header.push_str(&format!("\nORDER BY ({})", mv.order_by.join(", ")));
        }
    }

    if mv.populate {
        header.push_str("\nPOPULATE");
    }

    parts.push(header);
    parts.push(format!("AS {}", mv.select_query));

    parts.join("\n")
}

/// Connection identity for a dictionary's local `CLICKHOUSE` source. ClickHouse
/// rejects a `SOURCE(CLICKHOUSE(...))` that omits the user when the user loading
/// the dictionary isn't `default` (BAD_ARGUMENTS), so the user is always emitted
/// and the password is emitted whenever one is configured.
pub struct DictionarySource<'a> {
    pub database: &'a str,
    pub user: &'a str,
    pub password: Option<&'a str>,
}

/// Escapes a value for a single-quoted ClickHouse string literal.
fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
}

pub fn emit_create_dictionary(dict: &CreateDictionary, source: &DictionarySource) -> String {
    let key_type = dict
        .attributes
        .iter()
        .find(|a| a.name == dict.key)
        .map(|a| emit_column_type(&a.data_type))
        .unwrap_or_else(|| "Int64".into());

    let mut body: Vec<String> = vec![format!("    {} {}", quote_ident(&dict.key), key_type)];
    for attr in &dict.attributes {
        if attr.name == dict.key {
            continue;
        }
        body.push(format!(
            "    {} {}",
            quote_ident(&attr.name),
            emit_column_type(&attr.data_type)
        ));
    }

    let attr_names: Vec<&str> = dict
        .attributes
        .iter()
        .map(|a| a.name.as_str())
        .filter(|n| *n != dict.key)
        .collect();
    let dedup_selects: Vec<String> = attr_names
        .iter()
        .map(|n| format!("argMax({n}, {VERSION_COLUMN}) AS {n}"))
        .collect();
    let outer_selects: Vec<String> = std::iter::once(dict.key.clone())
        .chain(attr_names.iter().map(|n| (*n).to_string()))
        .collect();
    let inner_selects: Vec<String> = std::iter::once(dict.key.clone())
        .chain(dedup_selects)
        .collect();

    let query = format!(
        "SELECT {outer} FROM (SELECT {inner} FROM `{db}`.{table} GROUP BY {key} HAVING argMax({DELETED_COLUMN}, {VERSION_COLUMN}) = false)",
        outer = outer_selects.join(", "),
        inner = inner_selects.join(", "),
        db = source.database,
        table = dict.source_table,
        key = dict.key,
    );

    let credentials = match source.password {
        Some(password) => format!(
            "USER {} PASSWORD {} ",
            quote_literal(source.user),
            quote_literal(password)
        ),
        None => format!("USER {} ", quote_literal(source.user)),
    };

    let layout = match dict.layout.size_in_cells {
        Some(n) => format!("{}(SIZE_IN_CELLS {n})", dict.layout.kind.to_uppercase()),
        None => format!("{}()", dict.layout.kind.to_uppercase()),
    };

    // $q$...$q$ is a ClickHouse heredoc (dollar-quoted string literal), so the backtick-quoted
    // identifiers in `query` need no escaping; the body is schema-derived and never contains $q$.
    format!(
        "CREATE DICTIONARY IF NOT EXISTS {name} (\n{body}\n)\nPRIMARY KEY {key}\nSOURCE(CLICKHOUSE({credentials}QUERY $q${query}$q$))\nLIFETIME(MIN {min} MAX {max})\nLAYOUT({layout})",
        name = dict.name,
        body = body.join(",\n"),
        key = dict.key,
        min = dict.lifetime_min,
        max = dict.lifetime_max,
    )
}

/// Emits a projection definition without the leading `PROJECTION` keyword.
/// `CREATE TABLE` prepends `PROJECTION`; `ALTER TABLE ... ADD PROJECTION`
/// supplies the keyword itself.
pub fn emit_projection(proj: &ProjectionDef) -> String {
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
            format!("{name} (SELECT * ORDER BY {order})")
        }
        ProjectionDef::Lightweight { name, order_by } => {
            assert!(
                !order_by.is_empty(),
                "Lightweight projection '{name}' has empty order_by"
            );
            let order = if order_by.len() == 1 {
                order_by[0].clone()
            } else {
                format!("({})", order_by.join(", "))
            };
            format!("{name} (SELECT _part_offset ORDER BY {order})")
        }
        ProjectionDef::Aggregate {
            name,
            select,
            group_by,
        } => {
            format!(
                "{name} (\n      SELECT {}\n      GROUP BY {}\n    )",
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
    fn emit_materialized_view_with_to_table() {
        let mv = CreateMaterializedView {
            name: "mv_edge_summary".into(),
            to_table: Some("gl_edge_summary".into()),
            select_query:
                "SELECT traversal_path, count() AS cnt FROM gl_edge GROUP BY traversal_path".into(),
            engine: None,
            order_by: vec![],
            populate: false,
        };
        let sql = emit_create_materialized_view(&mv);
        assert!(sql.contains("CREATE MATERIALIZED VIEW IF NOT EXISTS mv_edge_summary"));
        assert!(sql.contains("TO gl_edge_summary"));
        assert!(sql.contains("AS SELECT traversal_path, count()"));
        assert!(!sql.contains("ENGINE"));
        assert!(!sql.contains("POPULATE"));
    }

    #[test]
    fn emit_materialized_view_with_implicit_storage() {
        let mv = CreateMaterializedView {
            name: "mv_counts".into(),
            to_table: None,
            select_query: "SELECT source_kind, count() AS cnt FROM gl_edge GROUP BY source_kind"
                .into(),
            engine: Some(Engine {
                name: "AggregatingMergeTree".into(),
                args: vec![],
            }),
            order_by: vec!["source_kind".into()],
            populate: true,
        };
        let sql = emit_create_materialized_view(&mv);
        assert!(sql.contains("CREATE MATERIALIZED VIEW IF NOT EXISTS mv_counts"));
        assert!(sql.contains("ENGINE = AggregatingMergeTree"));
        assert!(sql.contains("ORDER BY (source_kind)"));
        assert!(sql.contains("POPULATE"));
        assert!(sql.contains("AS SELECT source_kind"));
        assert!(!sql.contains("TO "));
    }

    #[test]
    #[should_panic(expected = "implicit storage but has no engine")]
    fn emit_materialized_view_panics_without_engine_or_to_table() {
        let mv = CreateMaterializedView {
            name: "mv_bad".into(),
            to_table: None,
            select_query: "SELECT 1".into(),
            engine: None,
            order_by: vec![],
            populate: false,
        };
        emit_create_materialized_view(&mv);
    }

    fn dict() -> CreateDictionary {
        CreateDictionary {
            name: "gl_project_traversal_paths_dict".into(),
            source_table: "gl_project".into(),
            key: "id".into(),
            attributes: vec![
                ColumnDef::new("id", ColumnType::Int64),
                ColumnDef::new("traversal_path", ColumnType::String),
            ],
            layout: DictLayout {
                kind: "hashed".into(),
                size_in_cells: None,
            },
            lifetime_min: 60,
            lifetime_max: 300,
        }
    }

    #[test]
    fn dictionary_source_emits_user_without_password() {
        let source = DictionarySource {
            database: "graph",
            user: "gkg",
            password: None,
        };
        let sql = emit_create_dictionary(&dict(), &source);
        assert!(sql.contains("SOURCE(CLICKHOUSE(USER 'gkg' QUERY"), "{sql}");
        assert!(!sql.contains("PASSWORD"), "{sql}");
        assert!(sql.contains("FROM `graph`.gl_project"), "{sql}");
    }

    #[test]
    fn dictionary_source_emits_user_and_password() {
        let source = DictionarySource {
            database: "graph",
            user: "gkg",
            password: Some("s3cr't\\x"),
        };
        let sql = emit_create_dictionary(&dict(), &source);
        assert!(
            sql.contains(r"SOURCE(CLICKHOUSE(USER 'gkg' PASSWORD 's3cr\'t\\x' QUERY"),
            "{sql}"
        );
    }
}
