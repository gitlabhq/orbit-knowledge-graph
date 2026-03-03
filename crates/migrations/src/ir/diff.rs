use super::{ColumnSchema, TableSchema};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaDiff {
    CreateTable(TableSchema),
    AlterTable {
        table_name: String,
        add_columns: Vec<ColumnSchema>,
        warnings: Vec<String>,
    },
}

/// Returns `None` when the schemas are already in sync.
pub fn diff_schemas(desired: &TableSchema, current: Option<&TableSchema>) -> Option<SchemaDiff> {
    let Some(current) = current else {
        return Some(SchemaDiff::CreateTable(desired.clone()));
    };

    let current_column_names: std::collections::HashSet<&str> =
        current.columns.iter().map(|c| c.name.as_str()).collect();

    let new_columns: Vec<ColumnSchema> = desired
        .columns
        .iter()
        .filter(|col| !current_column_names.contains(col.name.as_str()))
        .cloned()
        .collect();

    let mut warnings = Vec::new();

    for desired_col in &desired.columns {
        let Some(current_col) = current.columns.iter().find(|c| c.name == desired_col.name) else {
            continue;
        };
        if current_col.column_type != desired_col.column_type {
            warnings.push(format!(
                "column '{}' type mismatch: current={}, desired={} (manual migration required)",
                desired_col.name, current_col.column_type, desired_col.column_type,
            ));
        }
    }

    if desired.order_by != current.order_by {
        warnings.push(format!(
            "ORDER BY changed: current={:?}, desired={:?} (manual migration required)",
            current.order_by, desired.order_by,
        ));
    }

    if new_columns.is_empty() && warnings.is_empty() {
        return None;
    }

    Some(SchemaDiff::AlterTable {
        table_name: desired.name.clone(),
        add_columns: new_columns,
        warnings,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::{ClickHouseType, DELETED_COLUMN, Engine, VERSION_COLUMN};

    fn make_table(name: &str, columns: Vec<ColumnSchema>) -> TableSchema {
        TableSchema {
            name: name.to_string(),
            columns,
            engine: Engine::ReplacingMergeTree {
                version_column: VERSION_COLUMN.to_string(),
                deleted_column: Some(DELETED_COLUMN.to_string()),
            },
            order_by: vec!["id".to_string()],
            primary_key: vec!["id".to_string()],
            settings: Vec::new(),
        }
    }

    fn make_column(name: &str, column_type: ClickHouseType) -> ColumnSchema {
        ColumnSchema {
            name: name.to_string(),
            column_type,
            nullable: false,
            default_value: None,
        }
    }

    #[test]
    fn no_current_produces_create_table() {
        let desired = make_table("test", vec![make_column("id", ClickHouseType::Int64)]);
        let diff = diff_schemas(&desired, None);

        assert!(matches!(diff, Some(SchemaDiff::CreateTable(_))));
    }

    #[test]
    fn identical_schemas_produce_no_diff() {
        let table = make_table("test", vec![make_column("id", ClickHouseType::Int64)]);
        let diff = diff_schemas(&table, Some(&table));

        assert!(diff.is_none());
    }

    #[test]
    fn new_column_produces_alter_table() {
        let current = make_table("test", vec![make_column("id", ClickHouseType::Int64)]);
        let desired = make_table(
            "test",
            vec![
                make_column("id", ClickHouseType::Int64),
                make_column("name", ClickHouseType::String),
            ],
        );

        let diff = diff_schemas(&desired, Some(&current)).unwrap();
        match diff {
            SchemaDiff::AlterTable {
                add_columns,
                warnings,
                ..
            } => {
                assert_eq!(add_columns.len(), 1);
                assert_eq!(add_columns[0].name, "name");
                assert!(warnings.is_empty());
            }
            _ => panic!("expected AlterTable"),
        }
    }

    #[test]
    fn type_mismatch_produces_warning() {
        let current = make_table("test", vec![make_column("id", ClickHouseType::Int64)]);
        let desired = make_table("test", vec![make_column("id", ClickHouseType::String)]);

        let diff = diff_schemas(&desired, Some(&current)).unwrap();
        match diff {
            SchemaDiff::AlterTable { warnings, .. } => {
                assert_eq!(warnings.len(), 1);
                assert!(warnings[0].contains("type mismatch"));
            }
            _ => panic!("expected AlterTable"),
        }
    }

    #[test]
    fn order_by_change_produces_warning() {
        let current = make_table("test", vec![make_column("id", ClickHouseType::Int64)]);
        let mut desired = make_table("test", vec![make_column("id", ClickHouseType::Int64)]);
        desired.order_by = vec!["id".to_string(), "name".to_string()];

        let diff = diff_schemas(&desired, Some(&current)).unwrap();
        match diff {
            SchemaDiff::AlterTable { warnings, .. } => {
                assert_eq!(warnings.len(), 1);
                assert!(warnings[0].contains("ORDER BY changed"));
            }
            _ => panic!("expected AlterTable"),
        }
    }
}
