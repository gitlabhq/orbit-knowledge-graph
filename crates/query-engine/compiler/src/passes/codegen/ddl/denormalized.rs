//! DDL for denormalized join tables: one `CreateTable` per opted-in edge
//! variant plus three `TO` materialized views, one per source table. Every
//! table reference in the view bodies is a `{table}` placeholder so
//! [`CreateMaterializedView::with_prefix`] can version them.

use ontology::StorageColumn;
use ontology::constants::{
    DELETED_COLUMN, RELATIONSHIP_KIND_COLUMN, SOURCE_ID_COLUMN, SOURCE_KIND_COLUMN,
    TARGET_ID_COLUMN, TARGET_KIND_COLUMN, TRAVERSAL_PATH_COLUMN, VERSION_COLUMN,
};
use ontology::denormalized::{DenormalizedJoinTable, Side};

use super::{parse_column_type, storage_col_to_def, system_columns, table_settings};
use crate::ast::ddl::*;

pub(super) fn build_table(mat: &DenormalizedJoinTable) -> CreateTable {
    let mut columns = vec![
        ColumnDef::new(TRAVERSAL_PATH_COLUMN, ColumnType::String)
            .with_default("'0/'")
            .with_codec(vec![Codec::ZSTD(1)]),
        ColumnDef::new(
            RELATIONSHIP_KIND_COLUMN,
            parse_column_type("LowCardinality(String)"),
        )
        .with_codec(vec![Codec::LZ4]),
    ];
    columns.extend(prefixed_defs(Side::Source, &mat.source_columns));
    columns.extend(prefixed_defs(Side::Target, &mat.target_columns));
    columns.extend(system_columns(None));

    CreateTable {
        name: mat.table.clone(),
        columns,
        indexes: vec![],
        projections: vec![],
        engine: Engine::replacing_merge_tree(VERSION_COLUMN, DELETED_COLUMN),
        partition_by: vec![],
        order_by: mat.sort_key(),
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
pub(super) fn build_views(mat: &DenormalizedJoinTable) -> Vec<CreateMaterializedView> {
    [Trigger::Edge, Trigger::Source, Trigger::Target]
        .into_iter()
        .map(|trigger| CreateMaterializedView {
            name: format!("{}__on_{}", mat.table, trigger.suffix()),
            to_table: Some(mat.table.clone()),
            select_query: select_for(mat, trigger),
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
fn select_for(mat: &DenormalizedJoinTable, trigger: Trigger) -> String {
    let final_unless = |t: Trigger| if trigger == t { "" } else { " FINAL" };

    let mut cols = vec![
        format!("e.{TRAVERSAL_PATH_COLUMN} AS {TRAVERSAL_PATH_COLUMN}"),
        format!("e.{RELATIONSHIP_KIND_COLUMN} AS {RELATIONSHIP_KIND_COLUMN}"),
    ];
    cols.extend(
        mat.source_columns
            .iter()
            .map(|c| format!("s.{} AS {}{}", c.name, Side::Source.prefix(), c.name)),
    );
    cols.extend(
        mat.target_columns
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
        mat.relationship_kind, mat.source_kind, mat.target_kind
    );
    let e = format!("{{{}}} AS e{}", mat.edge_table, final_unless(Trigger::Edge));
    let s = format!(
        "{{{}}} AS s{}",
        mat.source_table,
        final_unless(Trigger::Source)
    );
    let t = format!(
        "{{{}}} AS t{}",
        mat.target_table,
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
