//! DDL for denormalized join tables: one `CreateTable` per opted-in edge
//! variant plus three `TO` materialized views, one per source table. Every
//! table reference in the view bodies is a `{table}` placeholder so
//! [`CreateMaterializedView::with_prefix`] can version them.

use ontology::StorageColumn;
use ontology::constants::{
    DELETED_COLUMN, RELATIONSHIP_KIND_COLUMN, SOURCE_ID_COLUMN, SOURCE_KIND_COLUMN,
    TARGET_ID_COLUMN, TARGET_KIND_COLUMN, VERSION_COLUMN,
};
use ontology::denormalized::{DenormalizedJoinTable, Side};

use super::{storage_col_to_def, system_columns, table_settings};
use crate::ast::ddl::*;

pub(super) fn build_table(denorm: &DenormalizedJoinTable) -> CreateTable {
    let mut columns: Vec<ColumnDef> = denorm.edge_columns.iter().map(storage_col_to_def).collect();
    columns.extend(prefixed_defs(Side::Source, &denorm.source_columns));
    columns.extend(prefixed_defs(Side::Target, &denorm.target_columns));
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

fn prefixed_defs(side: Side, cols: &[StorageColumn]) -> impl Iterator<Item = ColumnDef> + '_ {
    cols.iter().map(move |c| {
        let mut prefixed = c.clone();
        prefixed.name = format!("{}{}", side.prefix(), c.name);
        storage_col_to_def(&prefixed)
    })
}

/// Three views writing into the join table, each triggered by inserts to a
/// different source. The edge view carries new relationships; the node views
/// re-emit rows whose endpoint properties changed. All three share one
/// projection so any of them produces a complete, current row.
pub(super) fn build_views(denorm: &DenormalizedJoinTable) -> Vec<CreateMaterializedView> {
    [Trigger::Edge, Trigger::Source, Trigger::Target]
        .into_iter()
        .map(|trigger| CreateMaterializedView {
            name: format!("{}__on_{}", denorm.table, trigger.suffix()),
            to_table: Some(denorm.table.clone()),
            select_query: select_for(denorm, trigger),
            engine: None,
            order_by: vec![],
            populate: false,
        })
        .collect()
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Trigger {
    Edge,
    Source,
    Target,
}

impl Trigger {
    fn suffix(self) -> &'static str {
        match self {
            Trigger::Edge => "edge",
            Trigger::Source => "source",
            Trigger::Target => "target",
        }
    }
}

/// The triggering table is scanned as-is (a view only sees the inserted
/// block); the other two are read with `FINAL` for their current state.
fn select_for(denorm: &DenormalizedJoinTable, trigger: Trigger) -> String {
    let final_unless = |t: Trigger| if trigger == t { "" } else { " FINAL" };

    let mut cols: Vec<String> = denorm
        .edge_columns
        .iter()
        .map(|c| format!("e.{0} AS {0}", c.name))
        .collect();
    cols.extend(
        denorm
            .source_columns
            .iter()
            .map(|c| format!("s.{} AS {}{}", c.name, Side::Source.prefix(), c.name)),
    );
    cols.extend(
        denorm
            .target_columns
            .iter()
            .map(|c| format!("t.{} AS {}{}", c.name, Side::Target.prefix(), c.name)),
    );
    cols.push(format!(
        "greatest(e.{VERSION_COLUMN}, s.{VERSION_COLUMN}, t.{VERSION_COLUMN}) AS {VERSION_COLUMN}"
    ));
    cols.push(format!(
        "(e.{DELETED_COLUMN} OR s.{DELETED_COLUMN} OR t.{DELETED_COLUMN}) AS {DELETED_COLUMN}"
    ));

    let edge_pred = format!(
        "e.{RELATIONSHIP_KIND_COLUMN} = '{}' AND e.{SOURCE_KIND_COLUMN} = '{}' AND e.{TARGET_KIND_COLUMN} = '{}'",
        denorm.relationship_kind, denorm.source_kind, denorm.target_kind
    );
    let e = format!(
        "{{{}}} AS e{}",
        denorm.edge_table,
        final_unless(Trigger::Edge)
    );
    let s = format!(
        "{{{}}} AS s{}",
        denorm.source_table,
        final_unless(Trigger::Source)
    );
    let t = format!(
        "{{{}}} AS t{}",
        denorm.target_table,
        final_unless(Trigger::Target)
    );
    let on_s = format!("e.{SOURCE_ID_COLUMN} = s.id");
    let on_t = format!("e.{TARGET_ID_COLUMN} = t.id");

    let from = match trigger {
        Trigger::Edge => {
            format!("FROM {e} INNER JOIN {s} ON {on_s} INNER JOIN {t} ON {on_t} WHERE {edge_pred}")
        }
        Trigger::Source => {
            format!("FROM {s} INNER JOIN {e} ON {on_s} AND {edge_pred} INNER JOIN {t} ON {on_t}")
        }
        Trigger::Target => {
            format!("FROM {t} INNER JOIN {e} ON {on_t} AND {edge_pred} INNER JOIN {s} ON {on_s}")
        }
    };

    format!("SELECT {} {from}", cols.join(", "))
}
