use std::collections::BTreeMap;

use super::resolve::{Layout, merge};
use super::{Column, Index, MaterializedView, Table};
use crate::catalog::Diagnostics;
use crate::implementations::clickhouse::spec::{JoinSourceSpec, JoinSpec, Settings};

const PER_TABLE_SETTINGS: [&str; 2] = ["index_granularity", "deduplicate_merge_projection_mode"];

pub(super) fn resolve(
    name: &str,
    spec: &JoinSpec,
    tables: &[Table],
    layout: &Layout,
    package: &str,
    context: &str,
    diagnostics: &mut Diagnostics,
) -> Option<(Table, Vec<MaterializedView>)> {
    let sources = source_tables(spec, tables, package, context, diagnostics)?;
    check_links(spec, &sources, package, context, diagnostics);
    let (version, deleted) = diagnostics.require(layout.versioning.clone(), package, || {
        format!("{context} needs a versioned store")
    })?;
    let mut columns = Vec::new();
    let mut indexes = Vec::new();
    let mut inherited = BTreeMap::new();
    for (occurrence, (source, table)) in spec.sources.iter().zip(&sources).enumerate() {
        columns.extend(
            copied_columns(table, layout)
                .filter_map(|column| bound(source, column))
                .collect::<Vec<_>>(),
        );
        indexes.extend(table.indexes.iter().filter_map(|index| {
            let column = source.columns.get(&index.column)?;
            Some(Index {
                name: occurrence_index_name(occurrence, &index.name),
                column: column.clone(),
                ..index.clone()
            })
        }));
        inherited.extend(
            table
                .settings
                .iter()
                .filter(|(key, _)| !PER_TABLE_SETTINGS.contains(&key.as_str()))
                .cloned(),
        );
    }
    let path = spec
        .sources
        .iter()
        .find_map(|source| source.path_column.as_ref());
    if let Some(position) =
        path.and_then(|path| columns.iter().position(|column| &column.name == path))
    {
        let column = columns.remove(position);
        columns.insert(0, column);
    }
    columns.extend(layout.columns.iter().cloned());
    let table = Table {
        name: name.to_string(),
        indexes,
        projections: Vec::new(),
        engine: layout.engine.clone(),
        order_by: spec.sort_key.clone(),
        primary_key: None,
        settings: merge(
            &layout.settings,
            &inherited.into_iter().collect::<Settings>(),
        ),
        ttl: None,
        columns,
    };
    let views = feeding_views(name, spec, &table, &version, &deleted);
    Some((table, views))
}

fn source_tables<'a>(
    spec: &JoinSpec,
    tables: &'a [Table],
    package: &str,
    context: &str,
    diagnostics: &mut Diagnostics,
) -> Option<Vec<&'a Table>> {
    let found: Vec<Option<&Table>> = spec
        .sources
        .iter()
        .map(|source| {
            diagnostics.require(
                tables.iter().find(|table| table.name == source.table),
                package,
                || format!("{context} names unknown table {}", source.table),
            )
        })
        .collect();
    found.into_iter().collect()
}

fn copied_columns<'a>(table: &'a Table, layout: &'a Layout) -> impl Iterator<Item = &'a Column> {
    table.columns.iter().filter(|column| {
        !layout
            .columns
            .iter()
            .any(|shared| shared.name == column.name)
    })
}

fn bound(source: &JoinSourceSpec, column: &Column) -> Option<Column> {
    source.columns.get(&column.name).map(|name| Column {
        name: name.clone(),
        ..column.clone()
    })
}

fn check_links(
    spec: &JoinSpec,
    sources: &[&Table],
    package: &str,
    context: &str,
    diagnostics: &mut Diagnostics,
) {
    let has = |table: &Table, column: &str| table.columns.iter().any(|c| c.name == column);
    for (occurrence, (source, table)) in spec.sources.iter().zip(sources).enumerate() {
        for column in source.columns.keys().chain(source.filter.keys()) {
            if !has(table, column) {
                diagnostics.error(
                    package,
                    format!("{context} reads unknown column {}.{column}", table.name),
                );
            }
        }
        match (occurrence, &source.on) {
            (0, Some(_)) => diagnostics.error(
                package,
                format!(
                    "{context} joins its first table {} onto nothing",
                    table.name
                ),
            ),
            (0, None) => {}
            (_, None) => diagnostics.error(
                package,
                format!("{context} has no join condition for {}", table.name),
            ),
            (_, Some(on)) => {
                let previous = sources[occurrence - 1];
                if !has(previous, &on.previous) || !has(table, &on.column) {
                    diagnostics.error(
                        package,
                        format!(
                            "{context} joins {}.{} to unknown {}.{}",
                            previous.name, on.previous, table.name, on.column
                        ),
                    );
                }
            }
        }
    }
}

fn occurrence_index_name(occurrence: usize, name: &str) -> String {
    match name.strip_prefix("idx_") {
        Some(rest) => format!("idx_t{occurrence}_{rest}"),
        None => format!("t{occurrence}_{name}"),
    }
}

fn alias(index: usize) -> String {
    format!("t{index}")
}

fn feeding_views(
    name: &str,
    spec: &JoinSpec,
    table: &Table,
    version: &str,
    deleted: &str,
) -> Vec<MaterializedView> {
    let projection = select_list(spec, table, version, deleted);
    (0..spec.sources.len())
        .map(|trigger| MaterializedView {
            name: format!("{name}__on_t{trigger}"),
            versioned: true,
            to_table: Some(name.to_string()),
            select_query: format!("SELECT {projection} {}", from_clause(spec, trigger)),
            engine: None,
            order_by: Vec::new(),
            populate: false,
        })
        .collect()
}

fn select_list(spec: &JoinSpec, table: &Table, version: &str, deleted: &str) -> String {
    let aliases: Vec<String> = (0..spec.sources.len()).map(alias).collect();
    let mut selected: Vec<String> = table
        .columns
        .iter()
        .filter_map(|column| {
            spec.sources.iter().enumerate().find_map(|(index, source)| {
                source
                    .columns
                    .iter()
                    .find(|(_, destination)| **destination == column.name)
                    .map(|(name, _)| format!("{}.{name} AS {}", alias(index), column.name))
            })
        })
        .collect();
    let versions: Vec<String> = aliases
        .iter()
        .map(|alias| format!("{alias}.{version}"))
        .collect();
    let deletes: Vec<String> = aliases
        .iter()
        .map(|alias| format!("{alias}.{deleted}"))
        .collect();
    selected.push(format!("greatest({}) AS {version}", versions.join(", ")));
    selected.push(format!("({}) AS {deleted}", deletes.join(" OR ")));
    selected.join(", ")
}

fn from_clause(spec: &JoinSpec, trigger: usize) -> String {
    let sources = &spec.sources;
    let reference = |index: usize, with_final: bool| {
        format!(
            "{{{}}} AS {}{}",
            sources[index].table,
            alias(index),
            if with_final { " FINAL" } else { "" }
        )
    };
    let filters = |index: usize| {
        sources[index]
            .filter
            .iter()
            .map(move |(column, value)| format!("{}.{column} = '{value}'", alias(index)))
    };
    let condition = |index: usize| {
        sources[index].on.as_ref().map(|on| {
            format!(
                "{}.{} = {}.{}",
                alias(index - 1),
                on.previous,
                alias(index),
                on.column
            )
        })
    };
    let mut sql = format!("FROM {}", reference(trigger, false));
    let outward = (trigger + 1..sources.len())
        .map(|index| (index, condition(index)))
        .chain(
            (0..trigger)
                .rev()
                .map(|index| (index, condition(index + 1))),
        );
    for (index, link) in outward {
        let conditions: Vec<String> = link.into_iter().chain(filters(index)).collect();
        sql.push_str(&format!(
            " INNER JOIN {} ON {}",
            reference(index, true),
            conditions.join(" AND ")
        ));
    }
    let local: Vec<String> = filters(trigger).collect();
    if !local.is_empty() {
        sql.push_str(&format!(" WHERE {}", local.join(" AND ")));
    }
    sql
}
