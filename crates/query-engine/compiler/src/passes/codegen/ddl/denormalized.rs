//! DDL for denormalized joins, composed from the already-generated definitions
//! of every table in the chain so codecs, types, indexes, and defaults stay
//! identical to the sources. Every table reference in the view bodies is a
//! `{table}` placeholder so [`CreateMaterializedView::with_prefix`] can
//! version them.

use std::collections::BTreeMap;

use ontology::PartitionConfig;
use ontology::constants::{DELETED_COLUMN, TRAVERSAL_PATH_COLUMN, VERSION_COLUMN};
use ontology::denormalized::{DenormalizedJoin, alias, column_for, copies, prefix};

use super::{partition_by, system_columns, table_settings};
use crate::ast::ddl::*;

/// `source_of(i)` is the generated table at chain index `i`. Columns and index
/// expressions are renamed through [`column_for`]; index names get the table
/// prefix so two tables' `idx_state` stay distinct. `index_granularity` and the
/// projection merge mode are per-table choices made here, not inherited.
pub(super) fn build_table<'a>(
    join: &DenormalizedJoin,
    source_of: impl Fn(usize) -> &'a CreateTable,
    partition: Option<&PartitionConfig>,
) -> CreateTable {
    let mut columns: Vec<ColumnDef> = source_of(join.traversal_path_table())
        .columns
        .iter()
        .filter(|c| c.name == TRAVERSAL_PATH_COLUMN)
        .cloned()
        .collect();
    let mut indexes = Vec::new();
    let mut explicit: BTreeMap<String, String> = BTreeMap::new();

    for i in 0..join.tables.len() {
        let table = source_of(i);
        columns.extend(
            table
                .columns
                .iter()
                .filter(|c| copies(&c.name))
                .map(|c| ColumnDef {
                    name: column_for(i, &c.name),
                    ..c.clone()
                }),
        );
        indexes.extend(
            table
                .indexes
                .iter()
                .filter(|idx| copies(&idx.expression))
                .map(|idx| IndexDef {
                    name: match idx.name.strip_prefix("idx_") {
                        Some(rest) => format!("idx_{}{rest}", prefix(i)),
                        None => format!("{}{}", prefix(i), idx.name),
                    },
                    expression: column_for(i, &idx.expression),
                    ..idx.clone()
                }),
        );
        explicit.extend(
            table
                .settings
                .iter()
                .filter(|s| {
                    !matches!(
                        s.key.as_str(),
                        "index_granularity" | "deduplicate_merge_projection_mode"
                    )
                })
                .map(|s| (s.key.clone(), s.value.clone())),
        );
    }
    columns.extend(system_columns(None));

    let partition_by = partition_by(
        partition,
        &join.table,
        columns.iter().map(|c| c.name.as_str()),
    );
    let partitioned = !partition_by.is_empty();

    CreateTable {
        name: join.table.clone(),
        columns,
        indexes,
        projections: vec![],
        engine: Engine::replacing_merge_tree(VERSION_COLUMN, DELETED_COLUMN),
        partition_by,
        order_by: join.sort_key(),
        primary_key: None,
        settings: table_settings(Some(1024), false, partitioned, &explicit),
        ttl: None,
    }
}

/// One view per table in the chain, triggered by inserts to that table and
/// writing complete rows. The trigger is scanned as-is (a view only sees the
/// inserted block); every other table is read with `FINAL`, joined outward
/// from the trigger along the chain so each `ON` references an already-joined
/// alias.
pub(super) fn build_views<'a>(
    join: &DenormalizedJoin,
    source_of: impl Fn(usize) -> &'a CreateTable,
) -> Vec<CreateMaterializedView> {
    let projection = projection(join, &source_of);
    (0..join.tables.len())
        .map(|trigger| CreateMaterializedView {
            name: format!("{}__on_{}", join.table, alias(trigger)),
            to_table: Some(join.table.clone()),
            select_query: format!("SELECT {projection} {}", from_clause(join, trigger)),
            engine: None,
            order_by: vec![],
            populate: false,
        })
        .collect()
}

/// Mirrors [`build_table`]'s column order exactly.
fn projection<'a>(
    join: &DenormalizedJoin,
    source_of: &impl Fn(usize) -> &'a CreateTable,
) -> String {
    let all = || (0..join.tables.len()).map(alias);
    let mut cols = vec![format!(
        "{}.{TRAVERSAL_PATH_COLUMN} AS {TRAVERSAL_PATH_COLUMN}",
        alias(join.traversal_path_table())
    )];
    for i in 0..join.tables.len() {
        cols.extend(
            source_of(i)
                .columns
                .iter()
                .filter(|c| copies(&c.name))
                .map(|c| format!("{}.{} AS {}", alias(i), c.name, column_for(i, &c.name))),
        );
    }
    cols.push(format!(
        "greatest({}) AS {VERSION_COLUMN}",
        all()
            .map(|a| format!("{a}.{VERSION_COLUMN}"))
            .collect::<Vec<_>>()
            .join(", ")
    ));
    cols.push(format!(
        "({}) AS {DELETED_COLUMN}",
        all()
            .map(|a| format!("{a}.{DELETED_COLUMN}"))
            .collect::<Vec<_>>()
            .join(" OR ")
    ));
    cols.join(", ")
}

fn from_clause(join: &DenormalizedJoin, trigger: usize) -> String {
    let scan = |i: usize, final_: bool| {
        format!(
            "{{{}}} AS {}{}",
            join.tables[i].table,
            alias(i),
            if final_ { " FINAL" } else { "" }
        )
    };
    let filter = |i: usize| {
        join.tables[i]
            .filter
            .iter()
            .map(move |(col, value)| format!("{}.{col} = '{value}'", alias(i)))
    };
    // Table `i` joins onto `i - 1` by declaration; walking left of the
    // trigger reuses the same condition from the other side.
    let on = |i: usize| {
        let j = join.tables[i]
            .join
            .as_ref()
            .expect("table 0 is never joined onto");
        let mut parts = Vec::new();
        if j.on_traversal_path {
            parts.push(format!(
                "{}.{TRAVERSAL_PATH_COLUMN} = {}.{TRAVERSAL_PATH_COLUMN}",
                alias(i - 1),
                alias(i)
            ));
        }
        parts.push(format!(
            "{}.{} = {}.{}",
            alias(i - 1),
            j.prev_column,
            alias(i),
            j.this_column
        ));
        parts
    };

    let mut sql = format!("FROM {}", scan(trigger, false));
    let n = join.tables.len();
    let outward = (trigger + 1..n)
        .map(|i| (i, on(i)))
        .chain((0..trigger).rev().map(|i| (i, on(i + 1))));
    for (i, mut conds) in outward {
        conds.extend(filter(i));
        sql.push_str(&format!(
            " INNER JOIN {} ON {}",
            scan(i, true),
            conds.join(" AND ")
        ));
    }
    let where_parts: Vec<String> = filter(trigger).collect();
    if !where_parts.is_empty() {
        sql.push_str(&format!(" WHERE {}", where_parts.join(" AND ")));
    }
    sql
}
