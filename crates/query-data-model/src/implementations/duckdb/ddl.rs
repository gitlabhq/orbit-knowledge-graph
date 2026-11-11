use super::DuckDb;
use crate::Catalog;
use crate::implementations::relational::{Column, Table};

impl Catalog<DuckDb> {
    /// One `CREATE TABLE` statement per table, entity tables first.
    pub fn create_sql(&self) -> String {
        self.tables()
            .map(|table| format!("{};\n\n", create_table(table)))
            .collect()
    }
}

fn create_table(table: &Table) -> String {
    let columns: Vec<String> = table.columns.iter().map(column).collect();
    format!(
        "CREATE TABLE IF NOT EXISTS {} (\n{}\n)",
        table.name,
        columns.join(",\n")
    )
}

fn column(column: &Column) -> String {
    let mut parts = vec![format!("    {} {}", column.name, column.storage_type)];
    if !column.nullable {
        parts.push("NOT NULL".into());
    }
    if let Some(default) = &column.default {
        parts.push(format!("DEFAULT {default}"));
    }
    parts.join(" ")
}

#[cfg(test)]
mod tests {
    use super::{Column, Table, create_table};
    use crate::DataType;

    fn column(name: &str, storage_type: &str, nullable: bool, default: Option<&str>) -> Column {
        Column {
            storage_type: storage_type.into(),
            nullable,
            default: default.map(str::to_string),
            ..Column::new(name, Some(DataType::String))
        }
    }

    #[test]
    fn renders_not_null_and_defaults() {
        let table = Table::new(
            "test",
            vec![
                column("id", "BIGINT", false, None),
                column("alias", "VARCHAR", true, None),
                column("s", "VARCHAR", false, Some("''")),
                column("b", "BOOLEAN", false, Some("false")),
            ],
        );
        assert_eq!(
            create_table(&table),
            "CREATE TABLE IF NOT EXISTS test (\n    id BIGINT NOT NULL,\n    alias VARCHAR,\n    s VARCHAR NOT NULL DEFAULT '',\n    b BOOLEAN NOT NULL DEFAULT false\n)"
        );
    }
}
