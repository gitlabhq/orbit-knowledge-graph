use super::diff::SchemaDiff;
use super::{ColumnSchema, TableSchema};

pub fn render_diff(diff: &SchemaDiff) -> String {
    match diff {
        SchemaDiff::CreateTable(schema) => render_create_table(schema),
        SchemaDiff::AlterTable {
            table_name,
            add_columns,
            warnings,
        } => render_alter_table(table_name, add_columns, warnings),
    }
}

fn render_create_table(schema: &TableSchema) -> String {
    let mut sql = format!("CREATE TABLE IF NOT EXISTS {} (\n", schema.name);

    for (index, column) in schema.columns.iter().enumerate() {
        let trailing = if index < schema.columns.len() - 1 {
            ","
        } else {
            ""
        };
        sql.push_str(&format!("    {}{}\n", render_column(column), trailing));
    }

    sql.push_str(&format!(") ENGINE = {}\n", schema.engine));

    let order_by = schema.order_by.join(", ");
    sql.push_str(&format!("ORDER BY ({order_by})"));

    if schema.primary_key != schema.order_by {
        let primary_key = schema.primary_key.join(", ");
        sql.push_str(&format!("\nPRIMARY KEY ({primary_key})"));
    }

    if !schema.settings.is_empty() {
        let settings = schema.settings.join(", ");
        sql.push_str(&format!("\nSETTINGS {settings}"));
    }

    sql.push_str(";\n");
    sql
}

fn render_alter_table(
    table_name: &str,
    add_columns: &[ColumnSchema],
    warnings: &[String],
) -> String {
    let mut sql = String::new();
    render_warning_comments(&mut sql, warnings);

    for column in add_columns {
        let col_type = column.column_type.to_sql(column.nullable);
        let default_clause = column
            .default_value
            .as_ref()
            .map(|d| format!(" DEFAULT {d}"))
            .unwrap_or_default();

        sql.push_str(&format!(
            "ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {name} {col_type}{default_clause};\n",
            name = column.name,
        ));
    }

    sql
}

pub fn render_rollback(diff: &SchemaDiff) -> String {
    match diff {
        SchemaDiff::CreateTable(schema) => {
            format!("DROP TABLE IF EXISTS {};\n", schema.name)
        }
        SchemaDiff::AlterTable {
            table_name,
            add_columns,
            warnings,
        } => render_rollback_alter_table(table_name, add_columns, warnings),
    }
}

fn render_rollback_alter_table(
    table_name: &str,
    add_columns: &[ColumnSchema],
    warnings: &[String],
) -> String {
    let mut sql = String::new();
    render_warning_comments(&mut sql, warnings);

    for column in add_columns {
        sql.push_str(&format!(
            "ALTER TABLE {table_name} DROP COLUMN IF EXISTS {name};\n",
            name = column.name,
        ));
    }

    sql
}

fn render_warning_comments(sql: &mut String, warnings: &[String]) {
    for warning in warnings {
        sql.push_str(&format!("-- WARNING: {warning}\n"));
    }
}

fn render_column(column: &ColumnSchema) -> String {
    let col_type = column.column_type.to_sql(column.nullable);
    let default_clause = column
        .default_value
        .as_ref()
        .map(|d| format!(" DEFAULT {d}"))
        .unwrap_or_default();

    format!("{} {}{}", column.name, col_type, default_clause)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::{ClickHouseType, Engine};

    #[test]
    fn create_table_renders_correctly() {
        let schema = TableSchema {
            name: "gl_test".to_string(),
            columns: vec![
                ColumnSchema {
                    name: "id".to_string(),
                    column_type: ClickHouseType::Int64,
                    nullable: false,
                    default_value: None,
                },
                ColumnSchema {
                    name: "name".to_string(),
                    column_type: ClickHouseType::String,
                    nullable: true,
                    default_value: None,
                },
                ColumnSchema {
                    name: "_version".to_string(),
                    column_type: ClickHouseType::DateTime64,
                    nullable: false,
                    default_value: Some("now64(6)".to_string()),
                },
                ColumnSchema {
                    name: "_deleted".to_string(),
                    column_type: ClickHouseType::Bool,
                    nullable: false,
                    default_value: Some("false".to_string()),
                },
            ],
            engine: Engine::ReplacingMergeTree {
                version_column: "_version".to_string(),
                deleted_column: Some("_deleted".to_string()),
            },
            order_by: vec!["id".to_string()],
            primary_key: vec!["id".to_string()],
            settings: Vec::new(),
        };

        let sql = render_create_table(&schema);

        assert!(sql.contains("CREATE TABLE IF NOT EXISTS gl_test"));
        assert!(sql.contains("id Int64,"));
        assert!(sql.contains("name Nullable(String),"));
        assert!(sql.contains("_version DateTime64(6, 'UTC') DEFAULT now64(6),"));
        assert!(sql.contains("_deleted Bool DEFAULT false"));
        assert!(sql.contains("ENGINE = ReplacingMergeTree(_version, _deleted)"));
        assert!(sql.contains("ORDER BY (id)"));
        assert!(!sql.contains("PRIMARY KEY"));
    }

    #[test]
    fn create_table_with_different_primary_key() {
        let schema = TableSchema {
            name: "gl_edge".to_string(),
            columns: vec![ColumnSchema {
                name: "source_id".to_string(),
                column_type: ClickHouseType::Int64,
                nullable: false,
                default_value: None,
            }],
            engine: Engine::ReplacingMergeTree {
                version_column: "_version".to_string(),
                deleted_column: Some("_deleted".to_string()),
            },
            order_by: vec![
                "traversal_path".to_string(),
                "source_id".to_string(),
                "source_kind".to_string(),
                "relationship_kind".to_string(),
                "target_id".to_string(),
                "target_kind".to_string(),
            ],
            primary_key: vec![
                "traversal_path".to_string(),
                "source_id".to_string(),
                "source_kind".to_string(),
                "relationship_kind".to_string(),
            ],
            settings: Vec::new(),
        };

        let sql = render_create_table(&schema);
        assert!(
            sql.contains("PRIMARY KEY (traversal_path, source_id, source_kind, relationship_kind)")
        );
    }

    #[test]
    fn alter_table_renders_add_columns() {
        let diff = SchemaDiff::AlterTable {
            table_name: "gl_user".to_string(),
            add_columns: vec![ColumnSchema {
                name: "email".to_string(),
                column_type: ClickHouseType::String,
                nullable: true,
                default_value: None,
            }],
            warnings: Vec::new(),
        };

        let sql = render_diff(&diff);
        assert!(
            sql.contains("ALTER TABLE gl_user ADD COLUMN IF NOT EXISTS email Nullable(String)")
        );
    }

    #[test]
    fn alter_table_includes_warnings() {
        let diff = SchemaDiff::AlterTable {
            table_name: "gl_user".to_string(),
            add_columns: Vec::new(),
            warnings: vec!["type mismatch on column 'id'".to_string()],
        };

        let sql = render_diff(&diff);
        assert!(sql.contains("-- WARNING: type mismatch on column 'id'"));
    }

    #[test]
    fn rollback_create_table_produces_drop() {
        let schema = TableSchema {
            name: "gl_test".to_string(),
            columns: vec![ColumnSchema {
                name: "id".to_string(),
                column_type: ClickHouseType::Int64,
                nullable: false,
                default_value: None,
            }],
            engine: Engine::ReplacingMergeTree {
                version_column: "_version".to_string(),
                deleted_column: Some("_deleted".to_string()),
            },
            order_by: vec!["id".to_string()],
            primary_key: vec!["id".to_string()],
            settings: Vec::new(),
        };

        let sql = render_rollback(&SchemaDiff::CreateTable(schema));
        assert_eq!(sql, "DROP TABLE IF EXISTS gl_test;\n");
    }

    #[test]
    fn rollback_alter_table_drops_columns() {
        let diff = SchemaDiff::AlterTable {
            table_name: "gl_user".to_string(),
            add_columns: vec![
                ColumnSchema {
                    name: "email".to_string(),
                    column_type: ClickHouseType::String,
                    nullable: true,
                    default_value: None,
                },
                ColumnSchema {
                    name: "age".to_string(),
                    column_type: ClickHouseType::Int64,
                    nullable: false,
                    default_value: None,
                },
            ],
            warnings: Vec::new(),
        };

        let sql = render_rollback(&diff);
        assert!(sql.contains("ALTER TABLE gl_user DROP COLUMN IF EXISTS email;"));
        assert!(sql.contains("ALTER TABLE gl_user DROP COLUMN IF EXISTS age;"));
    }

    #[test]
    fn rollback_alter_table_includes_warnings() {
        let diff = SchemaDiff::AlterTable {
            table_name: "gl_user".to_string(),
            add_columns: Vec::new(),
            warnings: vec!["type mismatch on column 'id'".to_string()],
        };

        let sql = render_rollback(&diff);
        assert!(sql.contains("-- WARNING: type mismatch on column 'id'"));
    }
}
