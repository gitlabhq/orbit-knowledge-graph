use ontology::constants::{DELETED_COLUMN, VERSION_COLUMN};
use query_data_model::DataModelError;

use super::{
    Column, Dictionary, Engine, Index, Projection, RefreshableView, Table, UnversionedDefinition,
    View,
};
use query_data_model::Relational;
use query_data_model::implementations::clickhouse::layout::{self, ClickHouse, MaterializedJoin};

pub fn build_all_tables(storage: &Relational<ClickHouse>) -> Vec<Table> {
    storage.versioned_tables().map(table_from_catalog).collect()
}

pub fn build_views(storage: &Relational<ClickHouse>) -> Result<Vec<View>, DataModelError> {
    let mut views: Vec<View> = storage.views().iter().map(view_from_catalog).collect();

    for join in storage.joins() {
        views.extend(denormalized_feeding_views(join, storage)?);
    }

    Ok(views)
}

pub fn build_dictionaries(storage: &Relational<ClickHouse>) -> Vec<Dictionary> {
    storage
        .dictionaries()
        .iter()
        .map(|dictionary_definition| Dictionary {
            name: dictionary_definition.name.clone(),
            source_table: dictionary_definition.source_table.clone(),
            key: dictionary_definition.key.clone(),
            attributes: dictionary_definition
                .attributes
                .iter()
                .map(column_from_catalog)
                .collect(),
            layout_kind: dictionary_definition.layout.clone(),
            layout_size_in_cells: dictionary_definition.size_in_cells,
            lifetime_min: dictionary_definition.lifetime_min,
            lifetime_max: dictionary_definition.lifetime_max,
        })
        .collect()
}

pub fn build_refreshable_views(storage: &Relational<ClickHouse>) -> Vec<RefreshableView> {
    storage
        .refreshable_views()
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
    storage: &Relational<ClickHouse>,
    all_table_names: &[String],
    replicated: bool,
) -> Result<Vec<UnversionedDefinition>, DataModelError> {
    let mut definitions = Vec::new();

    for auxiliary_table in storage
        .auxiliary_tables()
        .iter()
        .filter(|table| !table.versioned)
    {
        let mut table = table_from_catalog(&auxiliary_table.table);
        if replicated {
            table.engine = table.engine.replicated();
        }
        definitions.push(UnversionedDefinition {
            entity_type: "TABLE".into(),
            name: table.name.clone(),
            create_statement: table.to_create_sql(""),
        });
    }

    for definition in storage
        .views()
        .iter()
        .filter(|definition| !definition.versioned)
    {
        let mut view =
            view_from_catalog(definition).with_schema_version_prefix("", all_table_names);
        if replicated {
            view.engine = view.engine.map(Engine::replicated);
        }
        definitions.push(UnversionedDefinition {
            entity_type: "MATERIALIZED VIEW".into(),
            name: view.name.clone(),
            create_statement: view.to_create_sql()?,
        });
    }

    Ok(definitions)
}

fn table_from_catalog(table: &layout::Table) -> Table {
    Table {
        name: table.name.clone(),
        columns: table.columns.iter().map(column_from_catalog).collect(),
        indexes: table
            .options
            .indexes
            .iter()
            .map(|index| Index {
                name: index.name.clone(),
                column: index.column.clone(),
                lowercase: index.lowercase,
                index_type: index.index_type.clone(),
                granularity: index.granularity,
            })
            .collect(),
        projections: table
            .options
            .projections
            .iter()
            .map(|projection| {
                use layout::Projection as Stored;
                match projection {
                    Stored::Reorder { name, order_by } => Projection::Reorder {
                        name: name.clone(),
                        order_by: order_by.clone(),
                    },
                    Stored::Lightweight { name, order_by } => Projection::Lightweight {
                        name: name.clone(),
                        order_by: order_by.clone(),
                    },
                    Stored::Aggregate {
                        name,
                        select,
                        group_by,
                    } => Projection::Aggregate {
                        name: name.clone(),
                        select: select.clone(),
                        group_by: group_by.clone(),
                    },
                }
            })
            .collect(),
        engine: engine_from_catalog(&table.options.engine),
        partition_by: vec![],
        order_by: table.sort_key.clone(),
        primary_key: table.primary_key.clone(),
        settings: table.options.settings.clone(),
        ttl: table.options.ttl.clone(),
    }
}

fn column_from_catalog(column: &layout::Column) -> Column {
    Column {
        name: column.name.clone(),
        column_type: column.storage_type.clone(),
        default: column.default.clone(),
        codec: (!column.options.codecs.is_empty()).then(|| column.options.codecs.clone()),
    }
}

fn engine_from_catalog(engine: &layout::Engine) -> Engine {
    Engine {
        name: engine.name.clone(),
        args: engine.arguments.clone(),
    }
}

fn view_from_catalog(definition: &layout::MaterializedView) -> View {
    View {
        name: definition.name.clone(),
        to_table: definition.to_table.clone(),
        select_query: definition.select_query.clone(),
        engine: definition.engine.as_ref().map(engine_from_catalog),
        order_by: definition.order_by.clone(),
        populate: definition.populate,
        versioned: definition.versioned,
    }
}

fn denormalized_feeding_views(
    join: &MaterializedJoin,
    storage: &Relational<ClickHouse>,
) -> Result<Vec<View>, DataModelError> {
    let projection = denormalized_select_projection(join, storage)?;
    (0..join.sources.len())
        .map(|trigger| {
            Ok(View {
                name: format!("{}__on_t{trigger}", join.table),
                to_table: Some(join.table.clone()),
                select_query: format!(
                    "SELECT {projection} {}",
                    denormalized_from_clause(join, trigger)?
                ),
                engine: None,
                order_by: vec![],
                populate: false,
                versioned: true,
            })
        })
        .collect()
}

fn denormalized_select_projection(
    join: &MaterializedJoin,
    storage: &Relational<ClickHouse>,
) -> Result<String, DataModelError> {
    let all_aliases = || (0..join.sources.len()).map(|index| format!("t{index}"));
    let mut selected_columns: Vec<_> = storage
        .table(&join.table)
        .ok_or_else(|| DataModelError::UnknownReference {
            kind: "join table",
            name: join.table.clone(),
        })?
        .columns
        .iter()
        .filter_map(|column| {
            join.sources.iter().enumerate().find_map(|(index, source)| {
                source
                    .columns
                    .iter()
                    .find(|(_, destination)| *destination == &column.name)
                    .map(|(name, _)| format!("t{index}.{name} AS {}", column.name))
            })
        })
        .collect();

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

    Ok(selected_columns.join(", "))
}

fn denormalized_from_clause(
    join: &MaterializedJoin,
    trigger: usize,
) -> Result<String, DataModelError> {
    if trigger >= join.sources.len() {
        return Err(DataModelError::Invalid(format!(
            "join '{}' has no source {trigger}",
            join.table
        )));
    }
    let alias = |index| format!("t{index}");

    let table_reference = |table_index: usize, with_final: bool| {
        format!(
            "{{{}}} AS {}{}",
            join.sources[table_index].table,
            alias(table_index),
            if with_final { " FINAL" } else { "" }
        )
    };
    let row_filters = |table_index: usize| {
        join.sources[table_index]
            .filters
            .iter()
            .map(move |(column, value)| format!("{}.{column} = '{value}'", alias(table_index)))
    };
    let join_condition = |table_index: usize| {
        let hop = join.sources[table_index].join.as_ref().ok_or_else(|| {
            DataModelError::Invalid(format!(
                "join '{}' has no condition for source {table_index}",
                join.table
            ))
        })?;
        Ok::<_, DataModelError>(format!(
            "{}.{} = {}.{}",
            alias(table_index - 1),
            hop.0,
            alias(table_index),
            hop.1
        ))
    };

    let mut sql = format!("FROM {}", table_reference(trigger, false));
    let table_count = join.sources.len();
    let outward_joins = (trigger + 1..table_count)
        .map(|table_index| (table_index, join_condition(table_index)))
        .chain(
            (0..trigger)
                .rev()
                .map(|table_index| (table_index, join_condition(table_index + 1))),
        );

    for (table_index, link) in outward_joins {
        let conditions: Vec<String> = std::iter::once(link?)
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

    Ok(sql)
}

pub fn render_refreshable_view_select(
    template: &str,
    storage: &Relational<ClickHouse>,
    version: u32,
    version_prefix: &str,
) -> Result<String, ontology::sql_template::Error> {
    ontology::sql_template::render(
        template,
        ontology::sql_template::context! {
            schema => ontology::sql_template::context! { version => version },
            graph => ontology::sql_template::context! {
                tables => refreshable_view_table_contexts(storage, version_prefix)
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
    storage: &Relational<ClickHouse>,
    version_prefix: &str,
) -> Vec<RefreshableViewTableContext> {
    let mut tables: Vec<RefreshableViewTableContext> = storage
        .graph_tables()
        .iter()
        .map(|table| RefreshableViewTableContext {
            logical_name: table.name.clone(),
            physical_name: format!("{version_prefix}{}", table.name),
            global: table.global,
            has_traversal_path: table.has_traversal_path,
        })
        .collect();

    tables.sort_by(|left, right| left.logical_name.cmp(&right.logical_name));
    tables
}
