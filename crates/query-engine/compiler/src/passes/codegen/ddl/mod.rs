//! Ontology-driven DDL generator. The storage metadata in each
//! node/edge/auxiliary YAML is fully explicit: every column, codec, default,
//! index, and projection is specified. The generator is a thin pass-through
//! with no auto-derivation.

pub mod clickhouse;
pub mod duckdb;

use std::collections::BTreeMap;

use ontology::{
    AuxiliaryTable, MaterializedViewDefinition, Ontology, PartitionConfig, StorageColumn,
    StorageIndex, StorageProjection,
};

use crate::ast::ddl::*;

/// Tables are returned unprefixed. Call `.with_prefix()` on each to apply
/// a schema version prefix before codegen.
pub fn generate_graph_tables(ontology: &Ontology) -> Vec<CreateTable> {
    generate_graph_tables_with_prefix(ontology, "")
}

pub fn generate_graph_tables_with_prefix(ontology: &Ontology, prefix: &str) -> Vec<CreateTable> {
    let mut tables: Vec<CreateTable> = Vec::new();

    let partition = ontology.partition();
    for aux in ontology.auxiliary_tables() {
        tables.push(build_auxiliary_table(aux).with_prefix(prefix));
    }
    for node in ontology.nodes() {
        tables.push(build_node_table(node, partition).with_prefix(prefix));
    }
    for name in ontology.edge_tables() {
        if let Some(config) = ontology.edge_table_config(name) {
            tables.push(build_edge_table(name, config, partition).with_prefix(prefix));
        }
    }

    tables
}

/// Views are returned unprefixed. Call
/// [`generate_graph_materialized_views_with_prefix`] to apply a schema
/// version prefix to view names, `TO` targets, and table references inside
/// the `SELECT` query.
pub fn generate_graph_materialized_views(ontology: &Ontology) -> Vec<CreateMaterializedView> {
    generate_graph_materialized_views_with_prefix(ontology, "")
}

/// Generates all materialized view DDL with a schema-version prefix applied
/// to view names, `TO` targets, and `{table_name}` placeholders in the
/// `SELECT` query.
///
/// The prefix is also applied to every known graph table name found inside
/// placeholders, so `{gl_edge}` becomes `v54_gl_edge` when `prefix` is
/// `"v54_"`.
pub fn generate_graph_materialized_views_with_prefix(
    ontology: &Ontology,
    prefix: &str,
) -> Vec<CreateMaterializedView> {
    let known_tables = collect_table_names(ontology);

    ontology
        .materialized_views()
        .iter()
        .map(|mv| build_materialized_view(mv).with_prefix(prefix, &known_tables))
        .collect()
}

/// Collects table names so `{table_name}` placeholders in materialized view
/// queries can be resolved.
fn collect_table_names(ontology: &Ontology) -> Vec<String> {
    let mut names: Vec<String> = Vec::new();
    for aux in ontology.auxiliary_tables() {
        names.push(aux.name.clone());
    }
    for node in ontology.nodes() {
        names.push(node.destination_table.clone());
    }
    for table_name in ontology.edge_tables() {
        names.push(table_name.to_string());
    }
    names
}

pub fn generate_graph_dictionaries(ontology: &Ontology) -> Vec<CreateDictionary> {
    generate_graph_dictionaries_with_prefix(ontology, "")
}

pub fn generate_graph_dictionaries_with_prefix(
    ontology: &Ontology,
    prefix: &str,
) -> Vec<CreateDictionary> {
    ontology
        .auxiliary_dictionaries()
        .iter()
        .map(|d| {
            let key = ColumnDef::new(
                &d.key,
                parse_column_type(&aux_col_ch_type(
                    d.key_type.as_ref().unwrap_or(&ontology::DataType::Int),
                    false,
                )),
            );
            let attributes: Vec<ColumnDef> = std::iter::once(key)
                .chain(d.attributes.iter().map(|c| {
                    let col_type = parse_column_type(&aux_col_ch_type(&c.data_type, c.nullable));
                    ColumnDef::new(&c.name, col_type)
                }))
                .collect();
            CreateDictionary {
                name: d.name.clone(),
                source_table: d.source_table.clone(),
                key: d.key.clone(),
                attributes,
                layout: DictLayout {
                    kind: d.layout.kind.clone(),
                    size_in_cells: d.layout.size_in_cells,
                },
                lifetime_min: d.lifetime.min,
                lifetime_max: d.lifetime.max,
            }
            .with_prefix(prefix)
        })
        .collect()
}
/// Stripped-down versions of the ClickHouse tables: no system columns
/// (`_version`, `_deleted`), and any entity-specific excluded properties
/// are filtered out. The engine/indexes/projections fields are set to empty
/// defaults since DuckDB codegen ignores them.
pub fn generate_local_tables(ontology: &Ontology) -> Vec<CreateTable> {
    let mut tables: Vec<CreateTable> = Vec::new();

    for entity_name in ontology.local_entity_names() {
        if let Some(table) = build_local_node_table(ontology, entity_name) {
            tables.push(table);
        }
    }

    if let Some(table) = build_local_edge_table(ontology) {
        tables.push(table);
    }

    tables
}

/// SAFETY: `default` and `ch_type` are emitted as raw SQL in the DDL output.
/// This is safe because the ontology YAML is developer-controlled configuration
/// embedded at compile time -- not user input. If the trust boundary changes
/// (e.g. dynamic schema from an API), these fields need validation.
fn storage_col_to_def(col: &StorageColumn) -> ColumnDef {
    let col_type = parse_column_type(&col.ch_type);
    let mut def = ColumnDef::new(&col.name, col_type);
    if let Some(ref d) = col.default {
        def = def.with_default(d);
    }
    if let Some(ref codecs) = col.codec {
        def = def.with_codec(codecs.iter().map(|s| parse_codec(s)).collect());
    }
    def
}

fn system_columns(version_type: Option<&str>) -> Vec<ColumnDef> {
    let version = match version_type {
        Some("uint64") => ColumnDef::new("_version", ColumnType::UInt64),
        _ => ColumnDef::new(
            "_version",
            ColumnType::Timestamp {
                precision: 6,
                timezone: Some("UTC".into()),
            },
        )
        .with_default("now64(6)")
        // _version is batch-shared (one now64() per insert) and contiguous within a
        // traversal_path, so it's piecewise-constant: Delta yields runs of zeros that
        // ZSTD compresses well, beating both raw ZSTD and DoubleDelta here.
        .with_codec(vec![Codec::Delta(8), Codec::ZSTD(1)]),
    };
    vec![
        version,
        ColumnDef::new("_deleted", ColumnType::Bool).with_default("false"),
    ]
}

fn parse_column_type(s: &str) -> ColumnType {
    let s = s.trim();
    if let Some(inner) = strip_wrapper(s, "Nullable") {
        return ColumnType::Nullable(Box::new(parse_column_type(inner)));
    }
    if let Some(inner) = strip_wrapper(s, "LowCardinality") {
        return ColumnType::LowCardinality(Box::new(parse_column_type(inner)));
    }
    if let Some(inner) = strip_wrapper(s, "Array") {
        return ColumnType::Array(Box::new(parse_column_type(inner)));
    }
    if s.starts_with("DateTime64") {
        // DateTime64(6, 'UTC') or DateTime64(6)
        let inner = &s[11..s.len() - 1]; // strip "DateTime64(" and ")"
        let parts: Vec<&str> = inner.splitn(2, ',').collect();
        let precision: u8 = parts[0].trim().parse().unwrap_or(6);
        let tz = parts
            .get(1)
            .map(|t| t.trim().trim_matches('\'').to_string());
        return ColumnType::Timestamp {
            precision,
            timezone: tz,
        };
    }
    match s {
        "Int64" => ColumnType::Int64,
        "UInt64" => ColumnType::UInt64,
        "Bool" => ColumnType::Bool,
        "String" => ColumnType::String,
        "Date32" => ColumnType::Date32,
        _ => ColumnType::String,
    }
}

fn parse_codec(s: &str) -> Codec {
    let s = s.to_lowercase();
    match s.as_str() {
        "lz4" => Codec::LZ4,
        _ if s.starts_with("zstd(") => Codec::ZSTD(s[5..s.len() - 1].parse().unwrap_or(1)),
        _ if s.starts_with("delta(") => Codec::Delta(s[6..s.len() - 1].parse().unwrap_or(8)),
        "doubledelta" => Codec::DoubleDelta,
        "t64" => Codec::T64,
        _ => Codec::ZSTD(1),
    }
}

fn parse_index_type(s: &str) -> IndexType {
    let lower = s.to_lowercase();
    match lower.as_str() {
        "minmax" => IndexType::MinMax,
        _ if lower.starts_with("set(") => {
            IndexType::Set(lower[4..lower.len() - 1].parse().unwrap_or(10))
        }
        _ if lower.starts_with("bloom_filter(") => {
            IndexType::BloomFilter(lower[13..lower.len() - 1].parse().unwrap_or(0.01))
        }
        _ if lower.starts_with("text(") => {
            // Preserve original casing for tokenizer/preprocessor params.
            let inner = &s[5..s.len() - 1];
            IndexType::Text(inner.to_string())
        }
        _ if lower.starts_with("ngrambf_v1(") => {
            let inner = &s[11..s.len() - 1];
            IndexType::NgramBF(inner.to_string())
        }
        _ if lower.starts_with("tokenbf_v1(") => {
            let inner = &s[11..s.len() - 1];
            IndexType::TokenBF(inner.to_string())
        }
        _ => IndexType::MinMax,
    }
}

fn strip_wrapper<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    if s.starts_with(prefix) && s.ends_with(')') {
        let start = prefix.len() + 1;
        Some(&s[start..s.len() - 1])
    } else {
        None
    }
}

fn table_settings(
    index_granularity: Option<u32>,
    has_projections: bool,
    explicit_settings: &BTreeMap<String, String>,
) -> Vec<TableSetting> {
    let mut s = Vec::new();
    if let Some(g) = index_granularity {
        upsert_setting(&mut s, "index_granularity", g.to_string());
    }
    if has_projections {
        upsert_setting(&mut s, "deduplicate_merge_projection_mode", "'rebuild'");
    }
    upsert_setting(
        &mut s,
        "allow_experimental_replacing_merge_with_cleanup",
        "1",
    );
    for (key, value) in explicit_settings {
        upsert_setting(&mut s, key, value);
    }
    s
}

fn upsert_setting(
    settings: &mut Vec<TableSetting>,
    key: impl Into<String>,
    value: impl Into<String>,
) {
    let key = key.into();
    let value = value.into();
    if let Some(existing) = settings.iter_mut().find(|s| s.key == key) {
        existing.value = value;
    } else {
        settings.push(TableSetting { key, value });
    }
}

fn convert_index(idx: &StorageIndex) -> IndexDef {
    IndexDef {
        name: idx.name.clone(),
        expression: idx.column.clone(),
        index_type: parse_index_type(&idx.index_type),
        granularity: idx.granularity,
    }
}

/// SAFETY: `select` and `group_by` entries are emitted as raw SQL expressions.
/// Same trust assumption as `storage_col_to_def` -- ontology YAML is developer-controlled.
fn convert_projection(proj: &StorageProjection) -> ProjectionDef {
    match proj {
        StorageProjection::Reorder { name, order_by } => ProjectionDef::Reorder {
            name: name.clone(),
            order_by: order_by.clone(),
        },
        StorageProjection::Lightweight { name, order_by } => ProjectionDef::Lightweight {
            name: name.clone(),
            order_by: order_by.clone(),
        },
        StorageProjection::Aggregate {
            name,
            select,
            group_by,
        } => ProjectionDef::Aggregate {
            name: name.clone(),
            select: select.clone(),
            group_by: group_by.clone(),
        },
    }
}

fn partition_by(partition: Option<&PartitionConfig>) -> Vec<String> {
    partition
        .map(|p| vec![p.partition_by.clone()])
        .unwrap_or_default()
}

fn build_node_table(
    node: &ontology::NodeEntity,
    partition: Option<&PartitionConfig>,
) -> CreateTable {
    let mut columns: Vec<ColumnDef> = node
        .storage
        .columns
        .iter()
        .map(storage_col_to_def)
        .collect();
    columns.extend(system_columns(None));
    // Global hubs (User, Runner) have no traversal_path to bucket on.
    let partition_by = partition_by(partition.filter(|_| node.has_traversal_path));

    let indexes: Vec<IndexDef> = node.storage.indexes.iter().map(convert_index).collect();
    let projections: Vec<ProjectionDef> = node
        .storage
        .projections
        .iter()
        .map(convert_projection)
        .collect();

    let engine = if node.storage.version_only_engine {
        Engine::replacing_merge_tree_version_only("_version")
    } else {
        Engine::replacing_merge_tree("_version", "_deleted")
    };

    CreateTable {
        name: node.destination_table.clone(),
        columns,
        indexes,
        projections: projections.clone(),
        engine,
        partition_by,
        order_by: node.sort_key.clone(),
        primary_key: node.storage.primary_key.clone(),
        settings: table_settings(Some(1024), !projections.is_empty(), &node.storage.settings),
    }
}

fn build_edge_table(
    name: &str,
    config: &ontology::EdgeTableConfig,
    partition: Option<&PartitionConfig>,
) -> CreateTable {
    let mut columns: Vec<ColumnDef> = config
        .storage
        .columns
        .iter()
        .map(storage_col_to_def)
        .collect();
    // Denormalized node properties follow structural columns, before system columns.
    columns.extend(
        config
            .storage
            .denormalized_columns
            .iter()
            .map(storage_col_to_def),
    );
    columns.extend(system_columns(None));
    // Unlike nodes, every edge table carries traversal_path (global edges write
    // '0/'), so all edge tables partition.
    let partition_by = partition_by(partition);

    let mut indexes: Vec<IndexDef> = config.storage.indexes.iter().map(convert_index).collect();
    indexes.extend(
        config
            .storage
            .denormalized_indexes
            .iter()
            .map(convert_index),
    );
    let projections: Vec<ProjectionDef> = config
        .storage
        .projections
        .iter()
        .map(convert_projection)
        .collect();

    CreateTable {
        name: name.into(),
        columns,
        indexes,
        projections: projections.clone(),
        engine: Engine::replacing_merge_tree("_version", "_deleted"),
        partition_by,
        order_by: config.sort_key.clone(),
        primary_key: config.storage.primary_key.clone(),
        settings: table_settings(
            Some(config.storage.index_granularity.unwrap_or(1024)),
            !projections.is_empty(),
            &config.storage.settings,
        ),
    }
}

fn build_auxiliary_table(aux: &AuxiliaryTable) -> CreateTable {
    let mut columns: Vec<ColumnDef> = aux
        .columns
        .iter()
        .map(|c| {
            let col_type = parse_column_type(&aux_col_ch_type(&c.data_type, c.nullable));
            let mut def = ColumnDef::new(&c.name, col_type);
            if let Some(ref codecs) = c.codec {
                def = def.with_codec(codecs.iter().map(|s| parse_codec(s)).collect());
            }
            if let Some(ref d) = c.default {
                def = def.with_default(d);
            }
            def
        })
        .collect();

    columns.extend(system_columns(aux.version_type.as_deref()));

    let engine = if aux.version_only_engine {
        Engine::replacing_merge_tree_version_only("_version")
    } else {
        Engine::replacing_merge_tree("_version", "_deleted")
    };

    let projections: Vec<ProjectionDef> = aux.projections.iter().map(convert_projection).collect();
    let empty_settings = BTreeMap::new();

    CreateTable {
        name: aux.name.clone(),
        columns,
        indexes: vec![],
        projections: projections.clone(),
        engine,
        partition_by: vec![],
        order_by: aux.order_by.clone(),
        primary_key: None,
        settings: table_settings(None, !projections.is_empty(), &empty_settings),
    }
}

/// Auxiliary tables have no explicit `StorageColumn` definitions, so map the
/// ontology `DataType` to a ClickHouse type string directly.
fn aux_col_ch_type(dt: &ontology::DataType, nullable: bool) -> String {
    let base = match dt {
        ontology::DataType::String | ontology::DataType::Uuid => "String",
        ontology::DataType::Int => "Int64",
        ontology::DataType::Bool => "Bool",
        ontology::DataType::DateTime => "DateTime64(6, 'UTC')",
        ontology::DataType::Date => "Date32",
        _ => "String",
    };
    if nullable {
        format!("Nullable({base})")
    } else {
        base.to_string()
    }
}

fn build_materialized_view(mv: &MaterializedViewDefinition) -> CreateMaterializedView {
    CreateMaterializedView {
        name: mv.name.clone(),
        to_table: mv.to_table.clone(),
        select_query: mv.select_query.clone(),
        engine: mv.engine.as_ref().map(|name| Engine {
            name: name.clone(),
            args: mv.engine_args.clone(),
        }),
        order_by: mv.order_by.clone(),
        populate: mv.populate,
    }
}

/// Filters out properties listed in the entity's `exclude_properties`.
fn build_local_node_table(ontology: &Ontology, entity_name: &str) -> Option<CreateTable> {
    let exclude = ontology.local_entity_excludes(entity_name)?;
    let node = ontology.get_node(entity_name)?;

    let columns: Vec<ColumnDef> = node
        .storage
        .columns
        .iter()
        .filter(|col| !exclude.iter().any(|e| e == &col.name))
        .map(storage_col_to_def)
        .collect();

    Some(CreateTable {
        name: node.destination_table.clone(),
        columns,
        indexes: vec![],
        projections: vec![],
        engine: Engine {
            name: String::new(),
            args: vec![],
        },
        partition_by: vec![],
        order_by: node
            .sort_key
            .iter()
            .filter(|k| !exclude.iter().any(|e| e == *k))
            .cloned()
            .collect(),
        primary_key: None,
        settings: vec![],
    })
}

fn build_local_edge_table(ontology: &Ontology) -> Option<CreateTable> {
    let table_name = ontology.local_edge_table_name()?;
    let columns: Vec<ColumnDef> = ontology
        .local_edge_columns()
        .iter()
        .map(|c| {
            let col_type = local_data_type_to_column_type(&c.data_type);
            ColumnDef::new(&c.name, col_type)
        })
        .collect();

    Some(CreateTable {
        name: table_name.to_string(),
        columns,
        indexes: vec![],
        projections: vec![],
        engine: Engine {
            name: String::new(),
            args: vec![],
        },
        partition_by: vec![],
        order_by: ontology
            .local_edge_columns()
            .iter()
            .map(|c| c.name.clone())
            .collect(),
        primary_key: None,
        settings: vec![],
    })
}

fn local_data_type_to_column_type(dt: &ontology::DataType) -> ColumnType {
    match dt {
        ontology::DataType::String | ontology::DataType::Uuid => ColumnType::String,
        ontology::DataType::Int => ColumnType::Int64,
        ontology::DataType::Bool => ColumnType::Bool,
        ontology::DataType::DateTime => ColumnType::Timestamp {
            precision: 6,
            timezone: None,
        },
        ontology::DataType::Date => ColumnType::Date32,
        _ => ColumnType::String,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ontology() -> Ontology {
        Ontology::load_embedded().expect("embedded ontology must load")
    }

    #[test]
    fn generates_tables() {
        let tables = generate_graph_tables(&ontology());
        let names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
        for expected in ["checkpoint", "gl_user", "gl_project", "gl_edge"] {
            assert!(names.contains(&expected), "missing {expected}: {names:?}");
        }
    }

    #[test]
    fn every_table_has_system_columns() {
        for table in &generate_graph_tables(&ontology()) {
            let cols: Vec<&str> = table.columns.iter().map(|c| c.name.as_str()).collect();
            assert!(
                cols.contains(&"_version"),
                "{}: missing _version",
                table.name
            );
            assert!(
                cols.contains(&"_deleted"),
                "{}: missing _deleted",
                table.name
            );
        }
    }

    #[test]
    fn prefix_applies_to_all() {
        for table in generate_graph_tables(&ontology()) {
            let prefixed = table.with_prefix("v1_");
            assert!(prefixed.name.starts_with("v1_"), "{}", prefixed.name);
        }
    }

    #[test]
    fn generated_ddl_snapshot() {
        use super::clickhouse::{DictionarySource, emit_create_dictionary, emit_create_table};

        let tables = generate_graph_tables(&ontology());
        let full_ddl: String = tables
            .iter()
            .map(|t| format!("{};\n", emit_create_table(t)))
            .collect::<Vec<_>>()
            .join("\n");

        eprintln!("\n--- GENERATED DDL ---\n{full_ddl}\n--- END ---\n");

        for table in &tables {
            assert!(!table.columns.is_empty(), "{}: no columns", table.name);
            assert!(!table.order_by.is_empty(), "{}: no ORDER BY", table.name);
        }

        let dicts = generate_graph_dictionaries(&ontology());
        let dict_names: Vec<&str> = dicts.iter().map(|d| d.name.as_str()).collect();
        for expected in [
            "gl_project_traversal_paths_dict",
            "gl_group_traversal_paths_dict",
        ] {
            assert!(
                dict_names.contains(&expected),
                "missing dictionary {expected}: {dict_names:?}"
            );
        }

        let default_source = DictionarySource {
            database: "default",
            user: "default",
            password: None,
        };
        for dict in &dicts {
            let sql = emit_create_dictionary(dict, &default_source);
            eprintln!("\n--- GENERATED DICTIONARY DDL ---\n{sql};\n--- END ---\n");
            assert!(sql.contains("CREATE DICTIONARY IF NOT EXISTS"), "{sql}");
            assert!(sql.contains("id Int64"), "Int64 key: {sql}");
            assert!(sql.contains("PRIMARY KEY id"), "{sql}");
            assert!(
                sql.contains("SOURCE(CLICKHOUSE(USER 'default' QUERY"),
                "explicit source user: {sql}"
            );
            assert!(
                sql.contains("argMax(traversal_path, _version) AS traversal_path"),
                "argMax dedup: {sql}"
            );
            assert!(
                sql.contains("HAVING argMax(_deleted, _version) = false"),
                "tombstone dedup: {sql}"
            );
            assert!(sql.contains("LAYOUT(HASHED())"), "HASHED layout: {sql}");
            assert!(sql.contains("LIFETIME(MIN 60 MAX 300)"), "{sql}");
        }
    }

    #[test]
    fn namespaced_tables_partition_global_tables_do_not() {
        let ontology = ontology();
        let expr = ontology
            .partition_by()
            .expect("embedded ontology declares partitioning");
        let tables = generate_graph_tables(&ontology);

        let edge = tables.iter().find(|t| t.name == "gl_edge").unwrap();
        assert_eq!(edge.partition_by, vec![expr.to_string()]);

        let mr = tables
            .iter()
            .find(|t| t.name == "gl_merge_request")
            .unwrap();
        assert_eq!(mr.partition_by, vec![expr.to_string()]);

        let user = tables.iter().find(|t| t.name == "gl_user").unwrap();
        assert!(user.partition_by.is_empty());
        let runner = tables.iter().find(|t| t.name == "gl_runner").unwrap();
        assert!(runner.partition_by.is_empty());
    }

    #[test]
    fn partition_by_is_emitted_between_engine_and_order_by() {
        use super::clickhouse::emit_create_table;
        let tables = generate_graph_tables(&ontology());
        let edge = tables.iter().find(|t| t.name == "gl_edge").unwrap();
        let sql = emit_create_table(edge);
        let engine_at = sql.find("ENGINE =").unwrap();
        let partition_at = sql.find("PARTITION BY").unwrap();
        let order_at = sql.find("ORDER BY").unwrap();
        assert!(engine_at < partition_at && partition_at < order_at, "{sql}");
    }

    #[test]
    fn forwards_explicit_table_settings_from_ontology() {
        let tables = generate_graph_tables(&ontology());
        let merge_request = tables
            .iter()
            .find(|table| table.name == "gl_merge_request")
            .expect("gl_merge_request table should be generated");

        assert!(merge_request.settings.iter().any(|setting| {
            setting.key == "add_minmax_index_for_temporal_columns" && setting.value == "1"
        }));
    }

    #[test]
    fn generates_no_materialized_views_by_default() {
        let views = generate_graph_materialized_views(&ontology());
        assert!(
            views.is_empty(),
            "default ontology should have no materialized views"
        );
    }

    #[test]
    fn materialized_view_prefix_resolves_table_placeholders() {
        use super::clickhouse::emit_create_materialized_view;

        let mv_def = ontology::MaterializedViewDefinition {
            name: "mv_edge_summary".into(),
            to_table: None,
            select_query:
                "SELECT traversal_path, count() AS cnt FROM {gl_edge} GROUP BY traversal_path"
                    .into(),
            engine: Some("SummingMergeTree".into()),
            engine_args: vec![],
            order_by: vec!["traversal_path".into()],
            populate: false,
        };
        let known_tables = vec!["gl_edge".into(), "gl_project".into()];
        let mv = build_materialized_view(&mv_def).with_prefix("v5_", &known_tables);

        assert_eq!(mv.name, "v5_mv_edge_summary");
        assert!(mv.select_query.contains("v5_gl_edge"));
        assert!(!mv.select_query.contains("{gl_edge}"));

        let sql = emit_create_materialized_view(&mv);
        assert!(sql.contains("CREATE MATERIALIZED VIEW IF NOT EXISTS v5_mv_edge_summary"));
        assert!(sql.contains("ENGINE = SummingMergeTree"));
        assert!(sql.contains("ORDER BY (traversal_path)"));
    }

    #[test]
    fn local_tables_include_expected_entities() {
        let tables = generate_local_tables(&ontology());
        let names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
        for expected in [
            "gl_directory",
            "gl_file",
            "gl_definition",
            "gl_imported_symbol",
            "gl_edge",
        ] {
            assert!(
                names.contains(&expected),
                "missing local table {expected}: {names:?}"
            );
        }
    }

    #[test]
    fn local_node_tables_include_traversal_path() {
        for table in &generate_local_tables(&ontology()) {
            if table.name == "gl_edge" {
                continue;
            }
            let cols: Vec<&str> = table.columns.iter().map(|c| c.name.as_str()).collect();
            assert!(
                cols.contains(&"traversal_path"),
                "{}: should contain traversal_path for hydration TP narrowing",
                table.name
            );
        }
    }

    #[test]
    fn local_tables_have_no_system_columns() {
        for table in &generate_local_tables(&ontology()) {
            let cols: Vec<&str> = table.columns.iter().map(|c| c.name.as_str()).collect();
            assert!(
                !cols.contains(&"_version"),
                "{}: should not contain _version",
                table.name
            );
            assert!(
                !cols.contains(&"_deleted"),
                "{}: should not contain _deleted",
                table.name
            );
        }
    }

    #[test]
    fn local_tables_have_no_clickhouse_features() {
        for table in &generate_local_tables(&ontology()) {
            assert!(
                table.indexes.is_empty(),
                "{}: should have no indexes",
                table.name
            );
            assert!(
                table.projections.is_empty(),
                "{}: should have no projections",
                table.name
            );
            assert!(
                table.settings.is_empty(),
                "{}: should have no settings",
                table.name
            );
        }
    }

    #[test]
    fn local_ddl_snapshot() {
        use super::duckdb::emit_create_table as emit_duckdb;

        let tables = generate_local_tables(&ontology());
        let full_ddl: String = tables
            .iter()
            .map(|t| format!("{};\n", emit_duckdb(t)))
            .collect::<Vec<_>>()
            .join("\n");

        eprintln!("\n--- LOCAL DDL ---\n{full_ddl}\n--- END ---\n");

        for table in &tables {
            assert!(!table.columns.is_empty(), "{}: no columns", table.name);
        }
    }
}
