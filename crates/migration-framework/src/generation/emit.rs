//! Rendering a [`SchemaDiff`] to ClickHouse migration SQL.

use compiler::{emit_column, emit_create_table, emit_index, emit_projection};

use super::diff::{SchemaChange, SchemaDiff, projection_name};

/// The `up` statements that apply the migration, in diff order.
pub fn render_up(diff: &SchemaDiff) -> Vec<String> {
    diff.changes.iter().map(render_up_one).collect()
}

/// The `down` statements that revert the migration, in reverse order so a
/// drop-then-add pair (a re-created index or projection) unwinds correctly.
pub fn render_down(diff: &SchemaDiff) -> Vec<String> {
    diff.changes.iter().rev().map(render_down_one).collect()
}

fn render_up_one(change: &SchemaChange) -> String {
    match change {
        SchemaChange::CreateTable(table) => format!("{};", emit_create_table(table)),
        SchemaChange::AddColumn { table, column } => {
            format!(
                "ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {};",
                emit_column(column)
            )
        }
        SchemaChange::ModifyColumn { table, to, .. } => {
            format!("ALTER TABLE {table} MODIFY COLUMN {};", emit_column(to))
        }
        SchemaChange::AddIndex { table, index } => {
            format!(
                "ALTER TABLE {table} ADD INDEX IF NOT EXISTS {};",
                emit_index(index)
            )
        }
        SchemaChange::DropIndex { table, index } => {
            format!("ALTER TABLE {table} DROP INDEX IF EXISTS {};", index.name)
        }
        SchemaChange::AddProjection { table, projection } => {
            format!(
                "ALTER TABLE {table} ADD PROJECTION IF NOT EXISTS {};",
                emit_projection(projection)
            )
        }
        SchemaChange::DropProjection { table, projection } => {
            format!(
                "ALTER TABLE {table} DROP PROJECTION IF EXISTS {};",
                projection_name(projection)
            )
        }
    }
}

fn render_down_one(change: &SchemaChange) -> String {
    match change {
        SchemaChange::CreateTable(table) => format!("DROP TABLE IF EXISTS {};", table.name),
        SchemaChange::AddColumn { table, column } => {
            format!("ALTER TABLE {table} DROP COLUMN IF EXISTS {};", column.name)
        }
        SchemaChange::ModifyColumn { table, from, .. } => {
            format!("ALTER TABLE {table} MODIFY COLUMN {};", emit_column(from))
        }
        SchemaChange::AddIndex { table, index } => {
            format!("ALTER TABLE {table} DROP INDEX IF EXISTS {};", index.name)
        }
        SchemaChange::DropIndex { table, index } => {
            format!(
                "ALTER TABLE {table} ADD INDEX IF NOT EXISTS {};",
                emit_index(index)
            )
        }
        SchemaChange::AddProjection { table, projection } => {
            format!(
                "ALTER TABLE {table} DROP PROJECTION IF EXISTS {};",
                projection_name(projection)
            )
        }
        SchemaChange::DropProjection { table, projection } => {
            format!(
                "ALTER TABLE {table} ADD PROJECTION IF NOT EXISTS {};",
                emit_projection(projection)
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use compiler::ddl::{Codec, ColumnDef, ColumnType, IndexDef, IndexType, ProjectionDef};

    #[test]
    fn add_column_up_is_idempotent_alter() {
        let diff = SchemaDiff {
            changes: vec![SchemaChange::AddColumn {
                table: "gl_user".into(),
                column: ColumnDef::new("bio", ColumnType::String).with_default("''"),
            }],
        };
        assert_eq!(
            render_up(&diff),
            vec!["ALTER TABLE gl_user ADD COLUMN IF NOT EXISTS bio String DEFAULT '';"]
        );
        assert_eq!(
            render_down(&diff),
            vec!["ALTER TABLE gl_user DROP COLUMN IF EXISTS bio;"]
        );
    }

    #[test]
    fn modify_column_up_uses_new_down_restores_old() {
        let from = ColumnDef::new("id", ColumnType::Int64);
        let to = ColumnDef::new("id", ColumnType::Int64).with_codec(vec![Codec::ZSTD(1)]);
        let diff = SchemaDiff {
            changes: vec![SchemaChange::ModifyColumn {
                table: "gl_user".into(),
                from: Box::new(from),
                to: Box::new(to),
            }],
        };
        assert_eq!(
            render_up(&diff),
            vec!["ALTER TABLE gl_user MODIFY COLUMN id Int64 CODEC(ZSTD(1));"]
        );
        assert_eq!(
            render_down(&diff),
            vec!["ALTER TABLE gl_user MODIFY COLUMN id Int64;"]
        );
    }

    #[test]
    fn add_index_round_trips() {
        let diff = SchemaDiff {
            changes: vec![SchemaChange::AddIndex {
                table: "gl_user".into(),
                index: IndexDef {
                    name: "idx_id".into(),
                    expression: "id".into(),
                    index_type: IndexType::MinMax,
                    granularity: 1,
                },
            }],
        };
        assert_eq!(
            render_up(&diff),
            vec![
                "ALTER TABLE gl_user ADD INDEX IF NOT EXISTS idx_id id TYPE minmax GRANULARITY 1;"
            ]
        );
        assert_eq!(
            render_down(&diff),
            vec!["ALTER TABLE gl_user DROP INDEX IF EXISTS idx_id;"]
        );
    }

    #[test]
    fn drop_index_up_drops_down_re_adds() {
        let index = IndexDef {
            name: "idx_id".into(),
            expression: "id".into(),
            index_type: IndexType::MinMax,
            granularity: 1,
        };
        let diff = SchemaDiff {
            changes: vec![SchemaChange::DropIndex {
                table: "gl_user".into(),
                index,
            }],
        };
        assert_eq!(
            render_up(&diff),
            vec!["ALTER TABLE gl_user DROP INDEX IF EXISTS idx_id;"]
        );
        assert_eq!(
            render_down(&diff),
            vec![
                "ALTER TABLE gl_user ADD INDEX IF NOT EXISTS idx_id id TYPE minmax GRANULARITY 1;"
            ]
        );
    }

    #[test]
    fn changed_index_down_unwinds_in_reverse() {
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
        let diff = SchemaDiff {
            changes: vec![
                SchemaChange::DropIndex {
                    table: "gl_user".into(),
                    index: old,
                },
                SchemaChange::AddIndex {
                    table: "gl_user".into(),
                    index: new,
                },
            ],
        };
        assert_eq!(
            render_down(&diff),
            vec![
                "ALTER TABLE gl_user DROP INDEX IF EXISTS idx;",
                "ALTER TABLE gl_user ADD INDEX IF NOT EXISTS idx id TYPE minmax GRANULARITY 1;",
            ]
        );
    }

    #[test]
    fn add_projection_has_no_doubled_keyword() {
        let diff = SchemaDiff {
            changes: vec![SchemaChange::AddProjection {
                table: "gl_user".into(),
                projection: ProjectionDef::Reorder {
                    name: "p_by_name".into(),
                    order_by: vec!["name".into()],
                },
            }],
        };
        assert_eq!(
            render_up(&diff),
            vec![
                "ALTER TABLE gl_user ADD PROJECTION IF NOT EXISTS p_by_name (SELECT * ORDER BY name);"
            ]
        );
        assert_eq!(
            render_down(&diff),
            vec!["ALTER TABLE gl_user DROP PROJECTION IF EXISTS p_by_name;"]
        );
    }

    #[test]
    fn create_table_up_is_create_down_is_drop() {
        use compiler::ddl::{CreateTable, Engine};
        let table = CreateTable {
            name: "gl_issue".into(),
            columns: vec![ColumnDef::new("id", ColumnType::Int64)],
            indexes: vec![],
            projections: vec![],
            engine: Engine::replacing_merge_tree("_version", "_deleted"),
            order_by: vec!["id".into()],
            primary_key: None,
            settings: vec![],
        };
        let diff = SchemaDiff {
            changes: vec![SchemaChange::CreateTable(Box::new(table))],
        };
        assert!(render_up(&diff)[0].starts_with("CREATE TABLE IF NOT EXISTS gl_issue ("));
        assert_eq!(render_down(&diff), vec!["DROP TABLE IF EXISTS gl_issue;"]);
    }
}
