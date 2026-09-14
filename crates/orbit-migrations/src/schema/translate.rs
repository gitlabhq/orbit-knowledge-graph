use std::collections::BTreeMap;

use ontology::constants::{DELETED_COLUMN, TRAVERSAL_PATH_COLUMN, VERSION_COLUMN};
use ontology::{
    AuxiliaryColumn, AuxiliaryTable, DataType, Ontology, StorageColumn, StorageIndex,
    StorageProjection,
};

use super::{
    Column, Dictionary, Engine, Index, Projection, RefreshableView, Table, UnversionedDefinition,
    View,
};

pub fn build_all_tables(ontology: &Ontology) -> Vec<Table> {
    let mut tables = Vec::new();

    for auxiliary_table in ontology
        .auxiliary_tables()
        .iter()
        .filter(|table| table.versioned)
    {
        tables.push(table_from_auxiliary(auxiliary_table));
    }
    for node in ontology.nodes() {
        tables.push(table_from_node(node));
    }
    for edge_table_name in ontology.edge_tables() {
        if let Some(config) = ontology.edge_table_config(edge_table_name) {
            tables.push(table_from_edge(edge_table_name, config));
        }
    }

    let denormalized: Vec<Table> = ontology
        .denormalized_joins()
        .iter()
        .map(|join| denormalized_table_from_join(join, &tables))
        .collect();
    tables.extend(denormalized);

    tables
}

pub fn build_views(ontology: &Ontology, tables: &[Table]) -> Vec<View> {
    let mut views: Vec<View> = ontology
        .materialized_views()
        .iter()
        .map(view_from_ontology)
        .collect();

    for join in ontology.denormalized_joins() {
        views.extend(denormalized_feeding_views(join, tables));
    }

    views
}

pub fn build_dictionaries(ontology: &Ontology) -> Vec<Dictionary> {
    ontology
        .auxiliary_dictionaries()
        .iter()
        .map(|dictionary_definition| {
            let key_column = Column {
                name: dictionary_definition.key.clone(),
                column_type: clickhouse_type_for_data_type(
                    dictionary_definition
                        .key_type
                        .as_ref()
                        .unwrap_or(&DataType::Int),
                    false,
                ),
                default: None,
                codec: None,
            };

            let mut attributes: Vec<Column> = vec![key_column];
            attributes.extend(
                dictionary_definition
                    .attributes
                    .iter()
                    .map(column_from_auxiliary),
            );

            Dictionary {
                name: dictionary_definition.name.clone(),
                source_table: dictionary_definition.source_table.clone(),
                key: dictionary_definition.key.clone(),
                attributes,
                layout_kind: dictionary_definition.layout.kind.clone(),
                layout_size_in_cells: dictionary_definition.layout.size_in_cells,
                lifetime_min: dictionary_definition.lifetime.min,
                lifetime_max: dictionary_definition.lifetime.max,
            }
        })
        .collect()
}

pub fn build_refreshable_views(ontology: &Ontology) -> Vec<RefreshableView> {
    ontology
        .refreshable_materialized_views()
        .iter()
        .map(|view| RefreshableView {
            name: view.name.clone(),
            select_query: view.select_query.clone(),
            append_to: view.append_to.clone(),
            refresh: view.refresh.clone(),
            versioned: view.versioned,
        })
        .collect()
}

pub fn build_unversioned_definitions(
    ontology: &Ontology,
    all_table_names: &[String],
    replicated: bool,
) -> Vec<UnversionedDefinition> {
    let mut definitions = Vec::new();

    for auxiliary_table in ontology
        .auxiliary_tables()
        .iter()
        .filter(|table| !table.versioned)
    {
        let mut table = table_from_auxiliary(auxiliary_table);
        if replicated {
            table.engine = table.engine.replicated();
        }
        definitions.push(UnversionedDefinition {
            entity_type: "TABLE".into(),
            name: table.name.clone(),
            create_statement: table.to_create_sql(""),
        });
    }

    for definition in ontology
        .materialized_views()
        .iter()
        .filter(|definition| !definition.versioned)
    {
        let mut view =
            view_from_ontology(definition).with_schema_version_prefix("", all_table_names);
        if replicated {
            view.engine = view.engine.map(Engine::replicated);
        }
        definitions.push(UnversionedDefinition {
            entity_type: "MATERIALIZED VIEW".into(),
            name: view.name.clone(),
            create_statement: view.to_create_sql(),
        });
    }

    definitions
}

pub fn collect_all_table_names(ontology: &Ontology) -> Vec<String> {
    let mut names = Vec::new();
    for auxiliary_table in ontology.auxiliary_tables() {
        names.push(auxiliary_table.name.clone());
    }
    for node in ontology.nodes() {
        names.push(node.destination_table.clone());
    }
    for table_name in ontology.edge_tables() {
        names.push(table_name.to_string());
    }
    for join in ontology.denormalized_joins() {
        names.push(join.table.clone());
    }
    names
}

fn table_from_node(node: &ontology::NodeEntity) -> Table {
    let mut columns: Vec<Column> = node
        .storage
        .columns
        .iter()
        .map(column_from_storage)
        .collect();
    columns.extend(system_columns(None));

    let engine = if node.storage.version_only_engine {
        Engine::replacing_merge_tree_version_only()
    } else {
        Engine::replacing_merge_tree()
    };

    let projections: Vec<Projection> = node
        .storage
        .projections
        .iter()
        .map(projection_from_storage)
        .collect();
    let has_projections = !projections.is_empty();

    Table {
        name: node.destination_table.clone(),
        columns,
        indexes: node
            .storage
            .indexes
            .iter()
            .map(index_from_storage)
            .collect(),
        projections,
        engine,
        partition_by: vec![],
        order_by: node.sort_key.clone(),
        primary_key: node.storage.primary_key.clone(),
        settings: table_settings(Some(1024), has_projections, &node.storage.settings),
        ttl: None,
    }
}

fn table_from_edge(name: &str, config: &ontology::EdgeTableConfig) -> Table {
    let mut columns: Vec<Column> = config
        .storage
        .columns
        .iter()
        .map(column_from_storage)
        .collect();
    columns.extend(
        config
            .storage
            .denormalized_columns
            .iter()
            .map(column_from_storage),
    );
    columns.extend(system_columns(None));

    let mut indexes: Vec<Index> = config
        .storage
        .indexes
        .iter()
        .map(index_from_storage)
        .collect();
    indexes.extend(
        config
            .storage
            .denormalized_indexes
            .iter()
            .map(index_from_storage),
    );

    let projections: Vec<Projection> = config
        .storage
        .projections
        .iter()
        .map(projection_from_storage)
        .collect();
    let has_projections = !projections.is_empty();

    Table {
        name: name.into(),
        columns,
        indexes,
        projections,
        engine: Engine::replacing_merge_tree(),
        partition_by: vec![],
        order_by: config.sort_key.clone(),
        primary_key: config.storage.primary_key.clone(),
        settings: table_settings(
            Some(config.storage.index_granularity.unwrap_or(1024)),
            has_projections,
            &config.storage.settings,
        ),
        ttl: None,
    }
}

fn table_from_auxiliary(auxiliary_table: &AuxiliaryTable) -> Table {
    let mut columns: Vec<Column> = auxiliary_table
        .columns
        .iter()
        .map(column_from_auxiliary)
        .collect();
    if auxiliary_table.include_system_columns {
        columns.extend(system_columns(auxiliary_table.version_type.as_deref()));
    }

    let engine = if let Some(engine_name) = &auxiliary_table.engine {
        Engine {
            name: engine_name.clone(),
            args: vec![],
        }
    } else if auxiliary_table.version_only_engine {
        Engine::replacing_merge_tree_version_only()
    } else {
        Engine::replacing_merge_tree()
    };

    let projections: Vec<Projection> = auxiliary_table
        .projections
        .iter()
        .map(projection_from_storage)
        .collect();
    let has_projections = !projections.is_empty();

    Table {
        name: auxiliary_table.name.clone(),
        columns,
        indexes: vec![],
        projections,
        engine,
        partition_by: vec![],
        order_by: auxiliary_table.order_by.clone(),
        primary_key: None,
        settings: table_settings(None, has_projections, &BTreeMap::new()),
        ttl: auxiliary_table.ttl.clone(),
    }
}

fn denormalized_table_from_join(
    join: &ontology::denormalized::DenormalizedJoin,
    source_tables: &[Table],
) -> Table {
    use ontology::denormalized::{copies, prefix};

    let find_source = |table_index: usize| -> &Table {
        let name = join.tables[table_index].table.as_str();
        source_tables
            .iter()
            .find(|table| table.name == name)
            .unwrap_or_else(|| panic!("denormalized join source '{name}' not generated"))
    };

    let anchor = join.anchor_table();
    let mut columns: Vec<Column> = find_source(anchor)
        .columns
        .iter()
        .filter(|column| column.name == TRAVERSAL_PATH_COLUMN)
        .cloned()
        .collect();

    let mut indexes = Vec::new();
    let mut explicit_settings: BTreeMap<String, String> = BTreeMap::new();

    for table_index in 0..join.tables.len() {
        let source = find_source(table_index);
        let is_anchor = table_index == anchor;

        columns.extend(
            source
                .columns
                .iter()
                .filter(|column| {
                    copies(&column.name) && !(is_anchor && column.name == TRAVERSAL_PATH_COLUMN)
                })
                .map(|column| Column {
                    name: join.column_for(table_index, &column.name),
                    ..column.clone()
                }),
        );

        indexes.extend(
            source
                .indexes
                .iter()
                .filter(|index| copies(&index.expression))
                .map(|index| Index {
                    name: match index.name.strip_prefix("idx_") {
                        Some(rest) => format!("idx_{}{rest}", prefix(table_index)),
                        None => format!("{}{}", prefix(table_index), index.name),
                    },
                    expression: join.column_for(table_index, &index.expression),
                    ..index.clone()
                }),
        );

        explicit_settings.extend(
            source
                .settings
                .iter()
                .filter(|(key, _)| {
                    !matches!(
                        key.as_str(),
                        "index_granularity" | "deduplicate_merge_projection_mode"
                    )
                })
                .map(|(key, value)| (key.clone(), value.clone())),
        );
    }

    columns.extend(system_columns(None));

    Table {
        name: join.table.clone(),
        columns,
        indexes,
        projections: vec![],
        engine: Engine::replacing_merge_tree(),
        partition_by: vec![],
        order_by: join.sort_key(),
        primary_key: None,
        settings: table_settings(Some(1024), false, &explicit_settings),
        ttl: None,
    }
}

fn view_from_ontology(definition: &ontology::MaterializedViewDefinition) -> View {
    View {
        name: definition.name.clone(),
        to_table: definition.to_table.clone(),
        select_query: definition.select_query.clone(),
        engine: definition.engine.as_ref().map(|engine_name| Engine {
            name: engine_name.clone(),
            args: definition.engine_args.clone(),
        }),
        order_by: definition.order_by.clone(),
        populate: definition.populate,
        versioned: definition.versioned,
    }
}

fn denormalized_feeding_views(
    join: &ontology::denormalized::DenormalizedJoin,
    source_tables: &[Table],
) -> Vec<View> {
    use ontology::denormalized::alias;

    let projection = denormalized_select_projection(join, source_tables);
    (0..join.tables.len())
        .map(|trigger| View {
            name: format!("{}__on_{}", join.table, alias(trigger)),
            to_table: Some(join.table.clone()),
            select_query: format!(
                "SELECT {projection} {}",
                denormalized_from_clause(join, trigger)
            ),
            engine: None,
            order_by: vec![],
            populate: false,
            versioned: true,
        })
        .collect()
}

fn denormalized_select_projection(
    join: &ontology::denormalized::DenormalizedJoin,
    source_tables: &[Table],
) -> String {
    use ontology::denormalized::{alias, copies};

    let find_source = |table_index: usize| -> &Table {
        let name = join.tables[table_index].table.as_str();
        source_tables
            .iter()
            .find(|table| table.name == name)
            .unwrap_or_else(|| panic!("denormalized join source '{name}' not generated"))
    };

    let all_aliases = || (0..join.tables.len()).map(alias);
    let anchor = join.anchor_table();

    let mut selected_columns = vec![format!(
        "{}.{TRAVERSAL_PATH_COLUMN} AS {TRAVERSAL_PATH_COLUMN}",
        alias(anchor)
    )];

    for table_index in 0..join.tables.len() {
        let source = find_source(table_index);
        let is_anchor = table_index == anchor;
        selected_columns.extend(
            source
                .columns
                .iter()
                .filter(|column| {
                    copies(&column.name) && !(is_anchor && column.name == TRAVERSAL_PATH_COLUMN)
                })
                .map(|column| {
                    format!(
                        "{}.{} AS {}",
                        alias(table_index),
                        column.name,
                        join.column_for(table_index, &column.name)
                    )
                }),
        );
    }

    selected_columns.push(format!(
        "greatest({}) AS {VERSION_COLUMN}",
        all_aliases()
            .map(|table_alias| format!("{table_alias}.{VERSION_COLUMN}"))
            .collect::<Vec<_>>()
            .join(", ")
    ));
    selected_columns.push(format!(
        "({}) AS {DELETED_COLUMN}",
        all_aliases()
            .map(|table_alias| format!("{table_alias}.{DELETED_COLUMN}"))
            .collect::<Vec<_>>()
            .join(" OR ")
    ));

    selected_columns.join(", ")
}

fn denormalized_from_clause(
    join: &ontology::denormalized::DenormalizedJoin,
    trigger: usize,
) -> String {
    use ontology::denormalized::alias;

    let table_reference = |table_index: usize, with_final: bool| {
        format!(
            "{{{}}} AS {}{}",
            join.tables[table_index].table,
            alias(table_index),
            if with_final { " FINAL" } else { "" }
        )
    };
    let row_filters = |table_index: usize| {
        join.tables[table_index]
            .filter
            .iter()
            .map(move |(column, value)| format!("{}.{column} = '{value}'", alias(table_index)))
    };
    let join_condition = |table_index: usize| {
        let hop = join.tables[table_index]
            .join
            .as_ref()
            .expect("table 0 is never joined onto");
        format!(
            "{}.{} = {}.{}",
            alias(table_index - 1),
            hop.prev_column,
            alias(table_index),
            hop.this_column
        )
    };

    let mut sql = format!("FROM {}", table_reference(trigger, false));
    let table_count = join.tables.len();
    let outward_joins = (trigger + 1..table_count)
        .map(|table_index| (table_index, join_condition(table_index)))
        .chain(
            (0..trigger)
                .rev()
                .map(|table_index| (table_index, join_condition(table_index + 1))),
        );

    for (table_index, link) in outward_joins {
        let conditions: Vec<String> = std::iter::once(link)
            .chain(row_filters(table_index))
            .collect();
        sql.push_str(&format!(
            " INNER JOIN {} ON {}",
            table_reference(table_index, true),
            conditions.join(" AND ")
        ));
    }

    let where_conditions: Vec<String> = row_filters(trigger).collect();
    if !where_conditions.is_empty() {
        sql.push_str(&format!(" WHERE {}", where_conditions.join(" AND ")));
    }

    sql
}

fn column_from_storage(storage_column: &StorageColumn) -> Column {
    Column {
        name: storage_column.name.clone(),
        column_type: storage_column.ch_type.clone(),
        default: storage_column.default.clone(),
        codec: storage_column.codec.clone(),
    }
}

fn column_from_auxiliary(auxiliary_column: &AuxiliaryColumn) -> Column {
    Column {
        name: auxiliary_column.name.clone(),
        column_type: clickhouse_type_for_data_type(
            &auxiliary_column.data_type,
            auxiliary_column.nullable,
        ),
        default: auxiliary_column.default.clone(),
        codec: auxiliary_column.codec.clone(),
    }
}

fn system_columns(version_type: Option<&str>) -> Vec<Column> {
    let version = match version_type {
        Some("uint64") => Column {
            name: VERSION_COLUMN.into(),
            column_type: "UInt64".into(),
            default: None,
            codec: None,
        },
        _ => Column {
            name: VERSION_COLUMN.into(),
            column_type: "DateTime64(6, 'UTC')".into(),
            default: Some("now64(6)".into()),
            codec: Some(vec!["Delta(8)".into(), "ZSTD(1)".into()]),
        },
    };

    vec![
        version,
        Column {
            name: DELETED_COLUMN.into(),
            column_type: "Bool".into(),
            default: Some("false".into()),
            codec: None,
        },
    ]
}

fn index_from_storage(storage_index: &StorageIndex) -> Index {
    Index {
        name: storage_index.name.clone(),
        expression: storage_index.column.clone(),
        index_type: storage_index.index_type.clone(),
        granularity: storage_index.granularity,
    }
}

fn projection_from_storage(storage_projection: &StorageProjection) -> Projection {
    match storage_projection {
        StorageProjection::Reorder { name, order_by } => Projection::Reorder {
            name: name.clone(),
            order_by: order_by.clone(),
        },
        StorageProjection::Lightweight { name, order_by } => Projection::Lightweight {
            name: name.clone(),
            order_by: order_by.clone(),
        },
        StorageProjection::Aggregate {
            name,
            select,
            group_by,
        } => Projection::Aggregate {
            name: name.clone(),
            select: select.clone(),
            group_by: group_by.clone(),
        },
    }
}

fn clickhouse_type_for_data_type(data_type: &DataType, nullable: bool) -> String {
    let base = match data_type {
        DataType::String | DataType::Uuid => "String",
        DataType::Int => "Int64",
        DataType::Bool => "Bool",
        DataType::DateTime => "DateTime64(6, 'UTC')",
        DataType::Date => "Date32",
        _ => "String",
    };
    if nullable {
        format!("Nullable({base})")
    } else {
        base.to_string()
    }
}

fn table_settings(
    index_granularity: Option<u32>,
    has_projections: bool,
    explicit: &BTreeMap<String, String>,
) -> Vec<(String, String)> {
    let mut settings: Vec<(String, String)> = Vec::new();

    if let Some(granularity) = index_granularity {
        upsert_setting(&mut settings, "index_granularity", granularity.to_string());
    }
    if has_projections {
        upsert_setting(
            &mut settings,
            "deduplicate_merge_projection_mode",
            "'rebuild'",
        );
    }
    upsert_setting(
        &mut settings,
        "allow_experimental_replacing_merge_with_cleanup",
        "1",
    );
    upsert_setting(&mut settings, "enable_block_number_column", "1");
    upsert_setting(&mut settings, "enable_block_offset_column", "1");
    for (key, value) in explicit {
        upsert_setting(&mut settings, key, value);
    }

    settings
}

fn upsert_setting(
    settings: &mut Vec<(String, String)>,
    key: impl Into<String>,
    value: impl Into<String>,
) {
    let key = key.into();
    let value = value.into();
    if let Some(existing) = settings
        .iter_mut()
        .find(|(existing_key, _)| *existing_key == key)
    {
        existing.1 = value;
    } else {
        settings.push((key, value));
    }
}

pub fn render_refreshable_view_select(
    template: &str,
    ontology: &Ontology,
    version: u32,
    version_prefix: &str,
) -> Result<String, ontology::sql_template::Error> {
    ontology::sql_template::render(
        template,
        ontology::sql_template::context! {
            schema => ontology::sql_template::context! { version => version },
            graph => ontology::sql_template::context! {
                tables => refreshable_view_table_contexts(ontology, version_prefix)
            },
        },
    )
}

#[derive(serde::Serialize)]
struct RefreshableViewTableContext {
    logical_name: String,
    physical_name: String,
    global: bool,
    has_traversal_path: bool,
}

fn refreshable_view_table_contexts(
    ontology: &Ontology,
    version_prefix: &str,
) -> Vec<RefreshableViewTableContext> {
    let mut tables: Vec<RefreshableViewTableContext> = ontology
        .nodes()
        .map(|node| RefreshableViewTableContext {
            logical_name: node.destination_table.clone(),
            physical_name: format!("{version_prefix}{}", node.destination_table),
            global: node.global,
            has_traversal_path: node.has_traversal_path,
        })
        .collect();

    tables.extend(ontology.edge_tables().into_iter().map(|edge_table| {
        RefreshableViewTableContext {
            logical_name: edge_table.to_string(),
            physical_name: format!("{version_prefix}{edge_table}"),
            global: false,
            has_traversal_path: ontology
                .edge_table_config(edge_table)
                .is_some_and(ontology::EdgeTableConfig::has_traversal_path),
        }
    }));

    tables.sort_by(|left, right| left.logical_name.cmp(&right.logical_name));
    tables
}
