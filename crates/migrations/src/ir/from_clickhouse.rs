use super::{ClickHouseType, ColumnSchema, Engine, TableSchema, VERSION_COLUMN};

/// A raw row from `DESCRIBE TABLE`.
#[derive(Debug)]
pub struct DescribeRow {
    pub name: String,
    pub column_type: String,
    pub default_type: String,
    pub default_expression: String,
}

/// Parse a `DESCRIBE TABLE` result into a list of `ColumnSchema`.
pub fn parse_describe_rows(rows: &[DescribeRow]) -> Vec<ColumnSchema> {
    rows.iter().map(parse_column_from_describe).collect()
}

fn parse_column_from_describe(row: &DescribeRow) -> ColumnSchema {
    let (column_type, nullable) = parse_type_string(&row.column_type);
    let default_value = if row.default_type.is_empty() {
        None
    } else {
        Some(row.default_expression.clone())
    };

    ColumnSchema {
        name: row.name.clone(),
        column_type,
        nullable,
        default_value,
    }
}

fn parse_type_string(type_str: &str) -> (ClickHouseType, bool) {
    let trimmed = type_str.trim();

    if let Some(inner) = trimmed
        .strip_prefix("Nullable(")
        .and_then(|s| s.strip_suffix(')'))
    {
        let (ch_type, _) = parse_base_type(inner);
        return (ch_type, true);
    }

    let (ch_type, nullable) = parse_base_type(trimmed);
    (ch_type, nullable)
}

fn parse_base_type(type_str: &str) -> (ClickHouseType, bool) {
    let ch_type = match type_str {
        "Int64" => ClickHouseType::Int64,
        "UInt8" => ClickHouseType::UInt8,
        "UInt64" => ClickHouseType::UInt64,
        "Float64" => ClickHouseType::Float64,
        "String" => ClickHouseType::String,
        "Bool" => ClickHouseType::Bool,
        "Date32" => ClickHouseType::Date32,
        "UUID" => ClickHouseType::UUID,
        s if s.starts_with("DateTime64") => ClickHouseType::DateTime64,
        _ => ClickHouseType::String, // fallback
    };
    (ch_type, false)
}

/// Parse the output of `SHOW CREATE TABLE` to extract engine, ORDER BY, and PRIMARY KEY.
///
/// Returns `(engine, order_by, primary_key)`.
pub fn parse_create_table_statement(create_sql: &str) -> (Engine, Vec<String>, Vec<String>) {
    let engine = parse_engine(create_sql);
    let order_by = parse_clause(create_sql, "ORDER BY");
    let primary_key = parse_clause(create_sql, "PRIMARY KEY");

    let primary_key = if primary_key.is_empty() {
        order_by.clone()
    } else {
        primary_key
    };

    (engine, order_by, primary_key)
}

fn parse_engine(sql: &str) -> Engine {
    let version_column;
    let mut deleted_column = None;

    if let Some(start) = sql.find("ReplacingMergeTree(") {
        let after = &sql[start + "ReplacingMergeTree(".len()..];
        if let Some(end) = after.find(')') {
            let params: Vec<&str> = after[..end].split(',').map(|s| s.trim()).collect();
            version_column = params.first().unwrap_or(&VERSION_COLUMN).to_string();
            if params.len() > 1 {
                deleted_column = Some(params[1].to_string());
            }
        } else {
            version_column = VERSION_COLUMN.to_string();
        }
    } else {
        version_column = VERSION_COLUMN.to_string();
    }

    Engine::ReplacingMergeTree {
        version_column,
        deleted_column,
    }
}

fn parse_clause(sql: &str, keyword: &str) -> Vec<String> {
    let search = format!("{keyword} (");
    let Some(start) = sql.find(&search) else {
        return Vec::new();
    };
    let after = &sql[start + search.len()..];
    let Some(end) = after.find(')') else {
        return Vec::new();
    };
    after[..end]
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Build a complete `TableSchema` from DESCRIBE rows and CREATE TABLE statement.
pub fn build_table_schema(
    table_name: &str,
    describe_rows: &[DescribeRow],
    create_statement: &str,
) -> TableSchema {
    let columns = parse_describe_rows(describe_rows);
    let (engine, order_by, primary_key) = parse_create_table_statement(create_statement);

    TableSchema {
        name: table_name.to_string(),
        columns,
        engine,
        order_by,
        primary_key,
        settings: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::DELETED_COLUMN;

    #[test]
    fn parse_nullable_type() {
        let (ch_type, nullable) = parse_type_string("Nullable(String)");
        assert_eq!(ch_type, ClickHouseType::String);
        assert!(nullable);
    }

    #[test]
    fn parse_non_nullable_type() {
        let (ch_type, nullable) = parse_type_string("Int64");
        assert_eq!(ch_type, ClickHouseType::Int64);
        assert!(!nullable);
    }

    #[test]
    fn parse_datetime64_type() {
        let (ch_type, nullable) = parse_type_string("DateTime64(6, 'UTC')");
        assert_eq!(ch_type, ClickHouseType::DateTime64);
        assert!(!nullable);
    }

    #[test]
    fn parse_engine_with_deleted() {
        let sql = "ENGINE = ReplacingMergeTree(_version, _deleted) ORDER BY (id)";
        let engine = parse_engine(sql);
        assert_eq!(
            engine,
            Engine::ReplacingMergeTree {
                version_column: VERSION_COLUMN.to_string(),
                deleted_column: Some(DELETED_COLUMN.to_string()),
            }
        );
    }

    #[test]
    fn parse_engine_without_deleted() {
        let sql = "ENGINE = ReplacingMergeTree(_version) ORDER BY (id)";
        let engine = parse_engine(sql);
        assert_eq!(
            engine,
            Engine::ReplacingMergeTree {
                version_column: VERSION_COLUMN.to_string(),
                deleted_column: None,
            }
        );
    }

    #[test]
    fn parse_order_by_clause() {
        let sql = "ORDER BY (traversal_path, id) PRIMARY KEY (traversal_path, id)";
        let order_by = parse_clause(sql, "ORDER BY");
        assert_eq!(order_by, vec!["traversal_path", "id"]);
    }

    #[test]
    fn parse_primary_key_clause() {
        let sql = "ORDER BY (a, b, c, d) PRIMARY KEY (a, b)";
        let primary_key = parse_clause(sql, "PRIMARY KEY");
        assert_eq!(primary_key, vec!["a", "b"]);
    }

    #[test]
    fn build_schema_from_parts() {
        let rows = vec![
            DescribeRow {
                name: "id".to_string(),
                column_type: "Int64".to_string(),
                default_type: String::new(),
                default_expression: String::new(),
            },
            DescribeRow {
                name: "name".to_string(),
                column_type: "Nullable(String)".to_string(),
                default_type: "DEFAULT".to_string(),
                default_expression: "''".to_string(),
            },
        ];

        let create_sql =
            "ENGINE = ReplacingMergeTree(_version, _deleted) ORDER BY (id) PRIMARY KEY (id)";

        let schema = build_table_schema("gl_test", &rows, create_sql);
        assert_eq!(schema.name, "gl_test");
        assert_eq!(schema.columns.len(), 2);
        assert_eq!(schema.order_by, vec!["id"]);
    }
}
