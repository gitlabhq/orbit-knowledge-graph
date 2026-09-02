//! DDL for denormalized join tables, composed from the already-generated
//! definitions of the edge table and the two node tables so codecs, types,
//! and defaults stay identical to the sources. Every table reference in the
//! view bodies is a `{table}` placeholder so
//! [`CreateMaterializedView::with_prefix`] can version them.

use std::collections::BTreeMap;

use ontology::PartitionConfig;
use ontology::constants::{
    DEFAULT_PRIMARY_KEY, DELETED_COLUMN, RELATIONSHIP_KIND_COLUMN, SOURCE_ID_COLUMN,
    SOURCE_KIND_COLUMN, TARGET_ID_COLUMN, TARGET_KIND_COLUMN, VERSION_COLUMN,
};
use ontology::denormalized::{DenormalizedJoinTable, Side};

use super::{partition_by, system_columns, table_settings};
use crate::ast::ddl::*;

/// Column definitions, skip indexes, and explicit settings all come from the
/// three source tables. Node columns and index expressions are renamed through
/// [`Side::column_for`]; index names get the side prefix so the two nodes'
/// `idx_state` stay distinct. Indexes on the node `id` are dropped: it maps to
/// `source_id`/`target_id`, which the edge table's own indexes and the sort
/// key already cover. `index_granularity` and the projection merge mode are
/// per-table choices made here, not inherited.
pub(super) fn build_table(
    denorm: &DenormalizedJoinTable,
    edge: &CreateTable,
    source: &CreateTable,
    target: &CreateTable,
    partition: Option<&PartitionConfig>,
) -> CreateTable {
    let is_system = |c: &ColumnDef| matches!(c.name.as_str(), VERSION_COLUMN | DELETED_COLUMN);
    let node_columns = |side: Side, node: &CreateTable| {
        node.columns
            .iter()
            .filter(|c| DenormalizedJoinTable::copies_node_column(&c.name))
            .map(move |c| ColumnDef {
                name: side.column_for(&c.name),
                ..c.clone()
            })
            .collect::<Vec<_>>()
    };
    let node_indexes = |side: Side, node: &CreateTable| {
        node.indexes
            .iter()
            .filter(|i| i.expression != DEFAULT_PRIMARY_KEY)
            .map(move |i| IndexDef {
                name: prefix_index_name(side, &i.name),
                expression: side.column_for(&i.expression),
                ..i.clone()
            })
            .collect::<Vec<_>>()
    };

    let mut columns: Vec<ColumnDef> = edge
        .columns
        .iter()
        .filter(|c| !is_system(c))
        .cloned()
        .collect();
    columns.extend(node_columns(Side::Source, source));
    columns.extend(node_columns(Side::Target, target));
    columns.extend(system_columns(None));

    let mut indexes = edge.indexes.clone();
    indexes.extend(node_indexes(Side::Source, source));
    indexes.extend(node_indexes(Side::Target, target));

    let partition_by = partition_by(
        partition,
        &denorm.table,
        columns.iter().map(|c| c.name.as_str()),
    );
    let partitioned = !partition_by.is_empty();
    let explicit: BTreeMap<String, String> = [edge, source, target]
        .iter()
        .flat_map(|t| t.settings.iter())
        .filter(|s| {
            !matches!(
                s.key.as_str(),
                "index_granularity" | "deduplicate_merge_projection_mode"
            )
        })
        .map(|s| (s.key.clone(), s.value.clone()))
        .collect();

    CreateTable {
        name: denorm.table.clone(),
        columns,
        indexes,
        projections: vec![],
        engine: Engine::replacing_merge_tree(VERSION_COLUMN, DELETED_COLUMN),
        partition_by,
        order_by: denorm.sort_key(),
        primary_key: None,
        settings: table_settings(Some(1024), false, partitioned, &explicit),
        ttl: None,
    }
}

/// `idx_title_ngram` on the target node becomes `idx_tgt_title_ngram`; a name
/// without the `idx_` convention is prefixed whole.
fn prefix_index_name(side: Side, name: &str) -> String {
    match name.strip_prefix("idx_") {
        Some(rest) => format!("idx_{}{rest}", side.prefix()),
        None => format!("{}{name}", side.prefix()),
    }
}

/// Three views writing into the join table, each triggered by inserts to a
/// different source. The edge view carries new relationships; the node views
/// re-emit rows whose endpoint properties changed. The triggering table is
/// scanned as-is (a view only sees the inserted block); the other two are read
/// with `FINAL` for their current state.
pub(super) fn build_views(
    denorm: &DenormalizedJoinTable,
    table: &CreateTable,
) -> Vec<CreateMaterializedView> {
    let projection: Vec<String> = table
        .columns
        .iter()
        .map(|c| match c.name.as_str() {
            VERSION_COLUMN => {
                format!("greatest(e.{VERSION_COLUMN}, s.{VERSION_COLUMN}, t.{VERSION_COLUMN}) AS {VERSION_COLUMN}")
            }
            DELETED_COLUMN => {
                format!("(e.{DELETED_COLUMN} OR s.{DELETED_COLUMN} OR t.{DELETED_COLUMN}) AS {DELETED_COLUMN}")
            }
            name => match Side::of_column(name) {
                Some((Side::Source, node_col)) => format!("s.{node_col} AS {name}"),
                Some((Side::Target, node_col)) => format!("t.{node_col} AS {name}"),
                None => format!("e.{name} AS {name}"),
            },
        })
        .collect();
    let projection = projection.join(", ");

    let edge_pred = format!(
        "e.{RELATIONSHIP_KIND_COLUMN} = '{}' AND e.{SOURCE_KIND_COLUMN} = '{}' AND e.{TARGET_KIND_COLUMN} = '{}'",
        denorm.relationship_kind, denorm.source_kind, denorm.target_kind
    );
    let on_s = format!("e.{SOURCE_ID_COLUMN} = s.id");
    let on_t = format!("e.{TARGET_ID_COLUMN} = t.id");
    let e = format!("{{{}}} AS e", denorm.edge_table);
    let s = format!("{{{}}} AS s", denorm.source_table);
    let t = format!("{{{}}} AS t", denorm.target_table);

    [
        ("edge", format!("FROM {e} INNER JOIN {s} FINAL ON {on_s} INNER JOIN {t} FINAL ON {on_t} WHERE {edge_pred}")),
        ("source", format!("FROM {s} INNER JOIN {e} FINAL ON {on_s} AND {edge_pred} INNER JOIN {t} FINAL ON {on_t}")),
        ("target", format!("FROM {t} INNER JOIN {e} FINAL ON {on_t} AND {edge_pred} INNER JOIN {s} FINAL ON {on_s}")),
    ]
    .into_iter()
    .map(|(trigger, from)| CreateMaterializedView {
        name: format!("{}__on_{trigger}", denorm.table),
        to_table: Some(denorm.table.clone()),
        select_query: format!("SELECT {projection} {from}"),
        engine: None,
        order_by: vec![],
        populate: false,
    })
    .collect()
}
