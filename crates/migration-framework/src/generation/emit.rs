//! Rendering a [`SchemaDiff`] to ClickHouse migration SQL.

use compiler::{emit_column, emit_create_table, emit_index, emit_projection};

use super::diff::{SchemaChange, SchemaDiff, projection_name};

/// The `up` statements that apply the migration, in diff order.
pub fn render_up(diff: &SchemaDiff) -> Vec<String> {
    diff.changes.iter().map(render_up_one).collect()
}

/// The `down` statements that revert the migration, in diff order.
pub fn render_down(diff: &SchemaDiff) -> Vec<String> {
    diff.changes.iter().map(render_down_one).collect()
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
        SchemaChange::AddIndex { table, index } => {
            format!(
                "ALTER TABLE {table} ADD INDEX IF NOT EXISTS {};",
                emit_index(index)
            )
        }
        SchemaChange::AddProjection { table, projection } => {
            format!(
                "ALTER TABLE {table} ADD PROJECTION IF NOT EXISTS {};",
                emit_projection(projection)
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
        SchemaChange::AddIndex { table, index } => {
            format!("ALTER TABLE {table} DROP INDEX IF EXISTS {};", index.name)
        }
        SchemaChange::AddProjection { table, projection } => {
            format!(
                "ALTER TABLE {table} DROP PROJECTION IF EXISTS {};",
                projection_name(projection)
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use compiler::ddl::{
        ColumnDef, ColumnType, CreateTable, Engine, IndexDef, IndexType, ProjectionDef,
    };

    fn add_column_diff() -> SchemaDiff {
        SchemaDiff {
            changes: vec![SchemaChange::AddColumn {
                table: "gl_user".into(),
                column: ColumnDef::new("bio", ColumnType::String).with_default("''"),
            }],
        }
    }

    #[test]
    fn add_column_up_is_idempotent_alter() {
        assert_eq!(
            render_up(&add_column_diff()),
            vec!["ALTER TABLE gl_user ADD COLUMN IF NOT EXISTS bio String DEFAULT '';"]
        );
    }

    #[test]
    fn add_column_down_drops_it() {
        assert_eq!(
            render_down(&add_column_diff()),
            vec!["ALTER TABLE gl_user DROP COLUMN IF EXISTS bio;"]
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
