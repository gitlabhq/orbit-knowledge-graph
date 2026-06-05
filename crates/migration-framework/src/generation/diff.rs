//! Diffing a baseline schema against the ontology's desired schema.
//!
//! What counts as in-place vs breaking is validated against the ClickHouse
//! `ALTER` reference (`sql-reference/statements/alter/{column,skipping-index,
//! projection,order-by,setting}`):
//!
//! - In place, non-rewriting: `ADD COLUMN`; `MODIFY COLUMN` for default/codec
//!   only (metadata, leaves existing data); `ADD`/`DROP INDEX`;
//!   `ADD`/`DROP PROJECTION`.
//! - Breaking (refused): a column **type** change (rewriting mutation, with
//!   `Nullable`→non-`Nullable` hazards); a dropped column or table (data loss);
//!   `ORDER BY`/primary-key/engine changes (no safe in-place path); a settings
//!   change (excluded — some settings such as `index_granularity` are immutable,
//!   so auto-generating `MODIFY SETTING` could fail at apply time).

use std::collections::BTreeMap;

use compiler::ddl::{ColumnDef, CreateTable, IndexDef, ProjectionDef};
use ontology::Ontology;
use thiserror::Error;

/// An in-place change a generated migration applies without rewriting row data.
#[derive(Debug, Clone, PartialEq)]
pub enum SchemaChange {
    CreateTable(Box<CreateTable>),
    AddColumn {
        table: String,
        column: ColumnDef,
    },
    /// A default- or codec-only column change (`MODIFY COLUMN`). `from` is kept
    /// so the migration can be reverted.
    ModifyColumn {
        table: String,
        from: Box<ColumnDef>,
        to: Box<ColumnDef>,
    },
    AddIndex {
        table: String,
        index: IndexDef,
    },
    DropIndex {
        table: String,
        index: IndexDef,
    },
    AddProjection {
        table: String,
        projection: ProjectionDef,
    },
    DropProjection {
        table: String,
        projection: ProjectionDef,
    },
}

/// A difference the generator refuses to emit: ClickHouse cannot apply it
/// without rewriting data or losing it, so it needs a deliberate, out-of-band
/// change (a column/table drop follows the finalization path; see ADR 016).
#[derive(Debug, Clone, PartialEq, Error)]
pub enum BreakingChange {
    #[error("table `{0}` was removed from the ontology")]
    DroppedTable(String),
    #[error("column `{table}`.`{column}` was removed")]
    DroppedColumn { table: String, column: String },
    #[error("column `{table}`.`{column}` changed type")]
    ColumnTypeChanged { table: String, column: String },
    #[error("sort key (ORDER BY) of `{table}` changed")]
    SortKeyChanged { table: String },
    #[error("primary key of `{table}` changed")]
    PrimaryKeyChanged { table: String },
    #[error("engine of `{table}` changed")]
    EngineChanged { table: String },
    #[error("settings of `{table}` changed")]
    SettingsChanged { table: String },
}

/// The in-place changes a generated migration will apply, in a deterministic
/// order (new tables first, then per-table column/index/projection changes).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SchemaDiff {
    pub changes: Vec<SchemaChange>,
}

impl SchemaDiff {
    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }
}

/// Diffs `baseline` against `desired`, returning the in-place migration or the
/// full list of breaking changes that block generation.
pub fn diff_schemas(
    baseline: &[CreateTable],
    desired: &[CreateTable],
) -> Result<SchemaDiff, Vec<BreakingChange>> {
    let baseline_by_name: BTreeMap<&str, &CreateTable> =
        baseline.iter().map(|t| (t.name.as_str(), t)).collect();
    let desired_by_name: BTreeMap<&str, &CreateTable> =
        desired.iter().map(|t| (t.name.as_str(), t)).collect();

    let mut changes = Vec::new();
    let mut breaking = Vec::new();

    for (name, desired_table) in &desired_by_name {
        match baseline_by_name.get(name) {
            None => changes.push(SchemaChange::CreateTable(Box::new(
                (*desired_table).clone(),
            ))),
            Some(baseline_table) => {
                diff_table(baseline_table, desired_table, &mut changes, &mut breaking);
            }
        }
    }

    for name in baseline_by_name.keys() {
        if !desired_by_name.contains_key(name) {
            breaking.push(BreakingChange::DroppedTable((*name).to_string()));
        }
    }

    if breaking.is_empty() {
        Ok(SchemaDiff { changes })
    } else {
        Err(breaking)
    }
}

/// Diffs the ontology's desired graph schema against `baseline`.
pub fn generate_from_ontology(
    baseline: &[CreateTable],
    ontology: &Ontology,
) -> Result<SchemaDiff, Vec<BreakingChange>> {
    diff_schemas(baseline, &compiler::generate_graph_tables(ontology))
}

fn diff_table(
    old: &CreateTable,
    new: &CreateTable,
    changes: &mut Vec<SchemaChange>,
    breaking: &mut Vec<BreakingChange>,
) {
    let table = &new.name;

    if old.order_by != new.order_by {
        breaking.push(BreakingChange::SortKeyChanged {
            table: table.clone(),
        });
    }
    if old.primary_key != new.primary_key {
        breaking.push(BreakingChange::PrimaryKeyChanged {
            table: table.clone(),
        });
    }
    if old.engine != new.engine {
        breaking.push(BreakingChange::EngineChanged {
            table: table.clone(),
        });
    }
    if old.settings != new.settings {
        breaking.push(BreakingChange::SettingsChanged {
            table: table.clone(),
        });
    }

    diff_columns(old, new, table, changes, breaking);
    diff_indexes(old, new, table, changes);
    diff_projections(old, new, table, changes);
}

fn diff_columns(
    old: &CreateTable,
    new: &CreateTable,
    table: &str,
    changes: &mut Vec<SchemaChange>,
    breaking: &mut Vec<BreakingChange>,
) {
    let old_by_name: BTreeMap<&str, &ColumnDef> =
        old.columns.iter().map(|c| (c.name.as_str(), c)).collect();
    let new_names: BTreeMap<&str, ()> = new.columns.iter().map(|c| (c.name.as_str(), ())).collect();

    for column in &new.columns {
        match old_by_name.get(column.name.as_str()) {
            None => changes.push(SchemaChange::AddColumn {
                table: table.to_string(),
                column: column.clone(),
            }),
            // A type change is a rewriting mutation; default/codec drift is a
            // metadata-only `MODIFY COLUMN` that leaves existing data alone.
            Some(old_column) if old_column.data_type != column.data_type => {
                breaking.push(BreakingChange::ColumnTypeChanged {
                    table: table.to_string(),
                    column: column.name.clone(),
                });
            }
            Some(old_column) if *old_column != column => {
                changes.push(SchemaChange::ModifyColumn {
                    table: table.to_string(),
                    from: Box::new((*old_column).clone()),
                    to: Box::new(column.clone()),
                });
            }
            Some(_) => {}
        }
    }

    for column in &old.columns {
        if !new_names.contains_key(column.name.as_str()) {
            breaking.push(BreakingChange::DroppedColumn {
                table: table.to_string(),
                column: column.name.clone(),
            });
        }
    }
}

fn diff_indexes(
    old: &CreateTable,
    new: &CreateTable,
    table: &str,
    changes: &mut Vec<SchemaChange>,
) {
    let old_by_name: BTreeMap<&str, &IndexDef> =
        old.indexes.iter().map(|i| (i.name.as_str(), i)).collect();
    let new_names: BTreeMap<&str, ()> = new.indexes.iter().map(|i| (i.name.as_str(), ())).collect();

    for index in &new.indexes {
        match old_by_name.get(index.name.as_str()) {
            None => changes.push(SchemaChange::AddIndex {
                table: table.to_string(),
                index: index.clone(),
            }),
            // A changed index is re-created in place: DROP then ADD.
            Some(old_index) if *old_index != index => {
                changes.push(SchemaChange::DropIndex {
                    table: table.to_string(),
                    index: (*old_index).clone(),
                });
                changes.push(SchemaChange::AddIndex {
                    table: table.to_string(),
                    index: index.clone(),
                });
            }
            Some(_) => {}
        }
    }

    for index in &old.indexes {
        if !new_names.contains_key(index.name.as_str()) {
            changes.push(SchemaChange::DropIndex {
                table: table.to_string(),
                index: index.clone(),
            });
        }
    }
}

fn diff_projections(
    old: &CreateTable,
    new: &CreateTable,
    table: &str,
    changes: &mut Vec<SchemaChange>,
) {
    let old_by_name: BTreeMap<&str, &ProjectionDef> = old
        .projections
        .iter()
        .map(|p| (projection_name(p), p))
        .collect();
    let new_names: BTreeMap<&str, ()> = new
        .projections
        .iter()
        .map(|p| (projection_name(p), ()))
        .collect();

    for projection in &new.projections {
        match old_by_name.get(projection_name(projection)) {
            None => changes.push(SchemaChange::AddProjection {
                table: table.to_string(),
                projection: projection.clone(),
            }),
            Some(old_projection) if *old_projection != projection => {
                changes.push(SchemaChange::DropProjection {
                    table: table.to_string(),
                    projection: (*old_projection).clone(),
                });
                changes.push(SchemaChange::AddProjection {
                    table: table.to_string(),
                    projection: projection.clone(),
                });
            }
            Some(_) => {}
        }
    }

    for projection in &old.projections {
        if !new_names.contains_key(projection_name(projection)) {
            changes.push(SchemaChange::DropProjection {
                table: table.to_string(),
                projection: projection.clone(),
            });
        }
    }
}

pub(crate) fn projection_name(projection: &ProjectionDef) -> &str {
    match projection {
        ProjectionDef::Reorder { name, .. }
        | ProjectionDef::Lightweight { name, .. }
        | ProjectionDef::Aggregate { name, .. } => name,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use compiler::ddl::{Codec, ColumnType, Engine, IndexType};

    fn table(name: &str, columns: Vec<ColumnDef>) -> CreateTable {
        CreateTable {
            name: name.into(),
            columns,
            indexes: vec![],
            projections: vec![],
            engine: Engine::replacing_merge_tree("_version", "_deleted"),
            order_by: vec!["id".into()],
            primary_key: None,
            settings: vec![],
        }
    }

    fn col(name: &str) -> ColumnDef {
        ColumnDef::new(name, ColumnType::String)
    }

    #[test]
    fn new_table_emits_create() {
        let desired = vec![table("gl_user", vec![col("id")])];
        let diff = diff_schemas(&[], &desired).unwrap();
        assert_eq!(
            diff.changes,
            vec![SchemaChange::CreateTable(Box::new(desired[0].clone()))]
        );
    }

    #[test]
    fn added_column_emits_add_column() {
        let baseline = vec![table("gl_user", vec![col("id")])];
        let desired = vec![table("gl_user", vec![col("id"), col("bio")])];
        let diff = diff_schemas(&baseline, &desired).unwrap();
        assert_eq!(
            diff.changes,
            vec![SchemaChange::AddColumn {
                table: "gl_user".into(),
                column: col("bio"),
            }]
        );
    }

    #[test]
    fn codec_only_change_is_in_place_modify_column() {
        let baseline = vec![table(
            "gl_user",
            vec![ColumnDef::new("id", ColumnType::String)],
        )];
        let to = ColumnDef::new("id", ColumnType::String).with_codec(vec![Codec::ZSTD(1)]);
        let desired = vec![table("gl_user", vec![to.clone()])];
        let diff = diff_schemas(&baseline, &desired).unwrap();
        assert_eq!(
            diff.changes,
            vec![SchemaChange::ModifyColumn {
                table: "gl_user".into(),
                from: Box::new(ColumnDef::new("id", ColumnType::String)),
                to: Box::new(to),
            }]
        );
    }

    #[test]
    fn default_only_change_is_in_place_modify_column() {
        let baseline = vec![table("gl_user", vec![col("state")])];
        let to = ColumnDef::new("state", ColumnType::String).with_default("'active'");
        let desired = vec![table("gl_user", vec![to.clone()])];
        let diff = diff_schemas(&baseline, &desired).unwrap();
        assert!(matches!(
            diff.changes.as_slice(),
            [SchemaChange::ModifyColumn { to: t, .. }] if **t == to
        ));
    }

    #[test]
    fn retyped_column_is_breaking() {
        let baseline = vec![table(
            "gl_user",
            vec![ColumnDef::new("id", ColumnType::String)],
        )];
        let desired = vec![table(
            "gl_user",
            vec![ColumnDef::new("id", ColumnType::Int64)],
        )];
        let breaking = diff_schemas(&baseline, &desired).unwrap_err();
        assert_eq!(
            breaking,
            vec![BreakingChange::ColumnTypeChanged {
                table: "gl_user".into(),
                column: "id".into(),
            }]
        );
    }

    #[test]
    fn dropped_index_is_in_place() {
        let index = IndexDef {
            name: "idx_id".into(),
            expression: "id".into(),
            index_type: IndexType::MinMax,
            granularity: 1,
        };
        let mut baseline = table("gl_user", vec![col("id")]);
        baseline.indexes.push(index.clone());
        let diff = diff_schemas(&[baseline], &[table("gl_user", vec![col("id")])]).unwrap();
        assert_eq!(
            diff.changes,
            vec![SchemaChange::DropIndex {
                table: "gl_user".into(),
                index,
            }]
        );
    }

    #[test]
    fn changed_index_is_drop_then_add() {
        let old = IndexDef {
            name: "idx".into(),
            expression: "id".into(),
            index_type: IndexType::MinMax,
            granularity: 1,
        };
        let new = IndexDef {
            granularity: 4,
            ..old.clone()
        };
        let mut baseline = table("gl_user", vec![col("id")]);
        baseline.indexes.push(old.clone());
        let mut desired = table("gl_user", vec![col("id")]);
        desired.indexes.push(new.clone());
        let diff = diff_schemas(&[baseline], &[desired]).unwrap();
        assert_eq!(
            diff.changes,
            vec![
                SchemaChange::DropIndex {
                    table: "gl_user".into(),
                    index: old,
                },
                SchemaChange::AddIndex {
                    table: "gl_user".into(),
                    index: new,
                },
            ]
        );
    }

    #[test]
    fn dropped_table_is_breaking() {
        let baseline = vec![table("gl_user", vec![col("id")])];
        let breaking = diff_schemas(&baseline, &[]).unwrap_err();
        assert_eq!(
            breaking,
            vec![BreakingChange::DroppedTable("gl_user".into())]
        );
    }

    #[test]
    fn dropped_column_is_breaking() {
        let baseline = vec![table("gl_user", vec![col("id"), col("bio")])];
        let desired = vec![table("gl_user", vec![col("id")])];
        let breaking = diff_schemas(&baseline, &desired).unwrap_err();
        assert_eq!(
            breaking,
            vec![BreakingChange::DroppedColumn {
                table: "gl_user".into(),
                column: "bio".into(),
            }]
        );
    }

    #[test]
    fn sort_key_change_is_breaking() {
        let baseline = vec![table("gl_user", vec![col("id")])];
        let mut desired = table("gl_user", vec![col("id")]);
        desired.order_by = vec!["id".into(), "name".into()];
        let breaking = diff_schemas(&baseline, &[desired]).unwrap_err();
        assert_eq!(
            breaking,
            vec![BreakingChange::SortKeyChanged {
                table: "gl_user".into()
            }]
        );
    }

    #[test]
    fn breaking_changes_are_collected_not_short_circuited() {
        let baseline = vec![
            table("gl_user", vec![col("id"), col("bio")]),
            table("gl_old", vec![col("id")]),
        ];
        let desired = vec![table("gl_user", vec![col("id")])];
        let breaking = diff_schemas(&baseline, &desired).unwrap_err();
        assert!(breaking.contains(&BreakingChange::DroppedColumn {
            table: "gl_user".into(),
            column: "bio".into(),
        }));
        assert!(breaking.contains(&BreakingChange::DroppedTable("gl_old".into())));
    }

    #[test]
    fn new_and_drifted_tables_combine() {
        let baseline = vec![table("gl_user", vec![col("id")])];
        let desired = vec![
            table("gl_user", vec![col("id"), col("bio")]),
            table("gl_issue", vec![col("id")]),
        ];
        let diff = diff_schemas(&baseline, &desired).unwrap();
        assert_eq!(diff.changes.len(), 2);
        assert!(diff.changes.iter().any(|c| matches!(
            c,
            SchemaChange::AddColumn { column, .. } if column.name == "bio"
        )));
        assert!(diff.changes.iter().any(|c| matches!(
            c,
            SchemaChange::CreateTable(t) if t.name == "gl_issue"
        )));
    }
}
