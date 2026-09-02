//! DDL for denormalized join tables, composed from the already-generated
//! definitions of the edge table and the two node tables so codecs, types,
//! and defaults stay identical to the sources. Every table reference in the
//! view bodies is a `{table}` placeholder so
//! [`CreateMaterializedView::with_prefix`] can version them.

use ontology::constants::{
    DELETED_COLUMN, RELATIONSHIP_KIND_COLUMN, SOURCE_ID_COLUMN, SOURCE_KIND_COLUMN,
    TARGET_ID_COLUMN, TARGET_KIND_COLUMN, VERSION_COLUMN,
};
use ontology::denormalized::{DenormalizedJoinTable, Side};

use super::{system_columns, table_settings};
use crate::ast::ddl::*;

pub(super) fn build_table(
    denorm: &DenormalizedJoinTable,
    edge: &CreateTable,
    source: &CreateTable,
    target: &CreateTable,
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

    let mut columns: Vec<ColumnDef> = edge
        .columns
        .iter()
        .filter(|c| !is_system(c))
        .cloned()
        .collect();
    columns.extend(node_columns(Side::Source, source));
    columns.extend(node_columns(Side::Target, target));
    columns.extend(system_columns(None));

    CreateTable {
        name: denorm.table.clone(),
        columns,
        indexes: vec![],
        projections: vec![],
        engine: Engine::replacing_merge_tree(VERSION_COLUMN, DELETED_COLUMN),
        partition_by: vec![],
        order_by: denorm.sort_key(),
        primary_key: None,
        settings: table_settings(Some(1024), false, false, &Default::default()),
        ttl: None,
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
