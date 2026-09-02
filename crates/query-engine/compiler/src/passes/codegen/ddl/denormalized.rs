//! DDL for denormalized path tables, composed from the already-generated
//! definitions of every edge and node table on the path so codecs, types,
//! indexes, and defaults stay identical to the sources. Every table reference
//! in the view bodies is a `{table}` placeholder so
//! [`CreateMaterializedView::with_prefix`] can version them.

use std::collections::BTreeMap;

use ontology::PartitionConfig;
use ontology::constants::{
    DELETED_COLUMN, RELATIONSHIP_KIND_COLUMN, SOURCE_ID_COLUMN, SOURCE_KIND_COLUMN,
    TARGET_ID_COLUMN, TARGET_KIND_COLUMN, TRAVERSAL_PATH_COLUMN, VERSION_COLUMN,
};
use ontology::denormalized::{DenormalizedPath, Position};

use super::{partition_by, system_columns, table_settings};
use crate::ast::ddl::*;

/// `source_of(pos)` is the generated table at that path position. Columns and
/// index expressions are renamed through [`Position::column_for`]; index names
/// get the position prefix so two nodes' `idx_state` stay distinct.
/// `index_granularity` and the projection merge mode are per-table choices made
/// here, not inherited.
pub(super) fn build_table<'a>(
    path: &DenormalizedPath,
    source_of: impl Fn(Position) -> &'a CreateTable,
    partition: Option<&PartitionConfig>,
) -> CreateTable {
    let hops = path.hop_count();
    // The row's single traversal_path leads, as it leads every sort key.
    let mut columns: Vec<ColumnDef> = source_of(Position::Edge(0))
        .columns
        .iter()
        .filter(|c| c.name == TRAVERSAL_PATH_COLUMN)
        .cloned()
        .collect();
    let mut indexes = Vec::new();
    let mut explicit: BTreeMap<String, String> = BTreeMap::new();

    for pos in path.positions() {
        let table = source_of(pos);
        columns.extend(
            table
                .columns
                .iter()
                .filter(|c| pos.copies(&c.name) && c.name != TRAVERSAL_PATH_COLUMN)
                .map(|c| ColumnDef {
                    name: pos.column_for(&c.name, hops),
                    ..c.clone()
                }),
        );
        indexes.extend(
            table
                .indexes
                .iter()
                .filter(|i| pos.copies(&i.expression))
                .map(|i| IndexDef {
                    name: prefix_index_name(pos, &i.name),
                    expression: pos.column_for(&i.expression, hops),
                    ..i.clone()
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
        &path.table,
        columns.iter().map(|c| c.name.as_str()),
    );
    let partitioned = !partition_by.is_empty();

    CreateTable {
        name: path.table.clone(),
        columns,
        indexes,
        projections: vec![],
        engine: Engine::replacing_merge_tree(VERSION_COLUMN, DELETED_COLUMN),
        partition_by,
        order_by: path.sort_key.clone(),
        primary_key: None,
        settings: table_settings(Some(1024), false, partitioned, &explicit),
        ttl: None,
    }
}

/// `idx_title_ngram` at node 2 becomes `idx_n2_title_ngram`; a name without
/// the `idx_` convention is prefixed whole.
fn prefix_index_name(pos: Position, name: &str) -> String {
    match name.strip_prefix("idx_") {
        Some(rest) => format!("idx_{}{rest}", pos.prefix()),
        None => format!("{}{name}", pos.prefix()),
    }
}

/// One view per source table, each triggered by inserts to that table and
/// writing complete rows into the path table. The triggering table is scanned
/// as-is (a view only sees the inserted block); every other table is read
/// with `FINAL` for its current state, joined outward from the trigger along
/// the path so each `ON` references an already-joined alias.
pub(super) fn build_views(
    path: &DenormalizedPath,
    table: &CreateTable,
) -> Vec<CreateMaterializedView> {
    let projection = projection(path, table);
    path.positions()
        .map(|trigger| CreateMaterializedView {
            name: format!("{}__on_{}", path.table, trigger.view_alias()),
            to_table: Some(path.table.clone()),
            select_query: format!("SELECT {projection} {}", from_clause(path, trigger)),
            engine: None,
            order_by: vec![],
            populate: false,
        })
        .collect()
}

fn projection(path: &DenormalizedPath, table: &CreateTable) -> String {
    let all = || path.positions().map(|p| p.view_alias());
    table
        .columns
        .iter()
        .map(|c| match c.name.as_str() {
            VERSION_COLUMN => format!(
                "greatest({}) AS {VERSION_COLUMN}",
                all()
                    .map(|a| format!("{a}.{VERSION_COLUMN}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            DELETED_COLUMN => format!(
                "({}) AS {DELETED_COLUMN}",
                all()
                    .map(|a| format!("{a}.{DELETED_COLUMN}"))
                    .collect::<Vec<_>>()
                    .join(" OR ")
            ),
            TRAVERSAL_PATH_COLUMN => format!(
                "{}.{TRAVERSAL_PATH_COLUMN} AS {TRAVERSAL_PATH_COLUMN}",
                Position::Edge(0).view_alias()
            ),
            name => {
                let (pos, col) = Position::of_column(name)
                    .unwrap_or_else(|| panic!("unprefixed column '{name}' in path table"));
                format!("{}.{col} AS {name}", pos.view_alias())
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Join every position onto `trigger`, walking the path outward in both
/// directions. A position's `ON` is its adjacency to the neighbour nearer the
/// trigger; an edge also carries its variant predicate there (or in `WHERE`
/// when the edge itself is the trigger).
fn from_clause(path: &DenormalizedPath, trigger: Position) -> String {
    let positions: Vec<Position> = path.positions().collect();
    let at = positions.iter().position(|p| *p == trigger).unwrap();
    let scan = |pos: Position, final_: bool| {
        format!(
            "{{{}}} AS {}{}",
            path.source_table(pos),
            pos.view_alias(),
            if final_ { " FINAL" } else { "" }
        )
    };

    let mut sql = format!("FROM {}", scan(trigger, false));
    let mut where_parts = Vec::new();
    if let Position::Edge(i) = trigger {
        where_parts.push(edge_predicate(path, i));
    }

    // Right of the trigger, then left; each step's neighbour is the previous
    // position in that direction, which is already in FROM.
    let outward = (at + 1..positions.len())
        .map(|k| (positions[k], positions[k - 1]))
        .chain((0..at).rev().map(|k| (positions[k], positions[k + 1])));
    for (pos, neighbour) in outward {
        let mut on = vec![adjacency(pos, neighbour)];
        if let Position::Edge(i) = pos {
            on.push(edge_predicate(path, i));
        }
        sql.push_str(&format!(
            " INNER JOIN {} ON {}",
            scan(pos, true),
            on.join(" AND ")
        ));
    }
    if !where_parts.is_empty() {
        sql.push_str(&format!(" WHERE {}", where_parts.join(" AND ")));
    }
    sql
}

/// `e{i}.source_id = n{i}.id` or `e{i}.target_id = n{i+1}.id`, whichever
/// pair the two positions form.
fn adjacency(a: Position, b: Position) -> String {
    let (edge, node) = match (a, b) {
        (Position::Edge(i), Position::Node(j)) | (Position::Node(j), Position::Edge(i)) => (i, j),
        _ => unreachable!("path positions alternate between nodes and edges"),
    };
    let id_col = if node == edge {
        SOURCE_ID_COLUMN
    } else {
        TARGET_ID_COLUMN
    };
    format!(
        "{}.{id_col} = {}.id",
        Position::Edge(edge).view_alias(),
        Position::Node(node).view_alias()
    )
}

fn edge_predicate(path: &DenormalizedPath, i: usize) -> String {
    let hop = &path.hops[i];
    let e = Position::Edge(i).view_alias();
    format!(
        "{e}.{RELATIONSHIP_KIND_COLUMN} = '{}' AND {e}.{SOURCE_KIND_COLUMN} = '{}' AND {e}.{TARGET_KIND_COLUMN} = '{}'",
        hop.relationship_kind, hop.source_kind, hop.target_kind
    )
}
