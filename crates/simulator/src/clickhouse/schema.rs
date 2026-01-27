//! ClickHouse schema generation from ontology and config.

use crate::arrow_schema::{ToArrowSchema, edge_schema};
use crate::config::SchemaConfig;
use arrow::datatypes::{DataType as ArrowDataType, Schema};
use ontology::{EDGE_TABLE, Ontology};

/// Generates ClickHouse DDL statements from ontology and config.
pub struct SchemaGenerator<'a> {
    ontology: &'a Ontology,
    config: &'a SchemaConfig,
}

impl<'a> SchemaGenerator<'a> {
    pub fn new(ontology: &'a Ontology, config: &'a SchemaConfig) -> Self {
        Self { ontology, config }
    }

    /// Generate CREATE TABLE statements for all tables.
    pub fn generate_create_tables(&self) -> Vec<(String, String)> {
        let mut statements = Vec::new();

        // Node tables
        for node in self.ontology.nodes() {
            let tbl_name = self.ontology.table_name(&node.name).unwrap();
            let schema = node.to_arrow_schema();
            let primary_key: Vec<&str> = self
                .config
                .node_primary_key
                .iter()
                .map(|s| s.as_str())
                .collect();
            let order_by: Vec<&str> = self
                .config
                .node_order_by
                .iter()
                .map(|s| s.as_str())
                .collect();
            let ddl = self.schema_to_ddl(&tbl_name, &schema, &primary_key, &order_by);
            statements.push((tbl_name, ddl));
        }

        // Edge table
        let edge_schema = edge_schema();
        let primary_key: Vec<&str> = self
            .config
            .edge_primary_key
            .iter()
            .map(|s| s.as_str())
            .collect();
        let order_by: Vec<&str> = self
            .config
            .edge_order_by
            .iter()
            .map(|s| s.as_str())
            .collect();
        let ddl = self.schema_to_ddl(EDGE_TABLE, &edge_schema, &primary_key, &order_by);
        statements.push((EDGE_TABLE.to_string(), ddl));

        statements
    }

    /// Generate DROP TABLE statements.
    pub fn generate_drop_tables(&self) -> Vec<String> {
        let mut drops = Vec::new();

        for node in self.ontology.nodes() {
            let tbl_name = self.ontology.table_name(&node.name).unwrap();
            drops.push(format!("DROP TABLE IF EXISTS {}", tbl_name));
        }

        drops.push(format!("DROP TABLE IF EXISTS {}", EDGE_TABLE));
        drops
    }

    /// Generate ALTER TABLE statements for indexes.
    pub fn generate_add_indexes(&self) -> Vec<String> {
        let mut statements = Vec::new();

        for idx in &self.config.indexes {
            let tables = self.resolve_table_pattern(&idx.table);

            for table in tables {
                statements.push(format!(
                    "ALTER TABLE {} ADD INDEX IF NOT EXISTS {} {} TYPE {} GRANULARITY {}",
                    table, idx.name, idx.expression, idx.index_type, idx.granularity
                ));
            }
        }

        statements
    }

    /// Generate MATERIALIZE INDEX statements.
    pub fn generate_materialize_indexes(&self) -> Vec<String> {
        let mut statements = Vec::new();

        for idx in &self.config.indexes {
            let tables = self.resolve_table_pattern(&idx.table);

            for table in tables {
                statements.push(format!(
                    "ALTER TABLE {} MATERIALIZE INDEX {}",
                    table, idx.name
                ));
            }
        }

        statements
    }

    /// Generate ALTER TABLE statements for projections.
    pub fn generate_add_projections(&self) -> Vec<String> {
        let mut statements = Vec::new();

        for proj in &self.config.projections {
            let tables = self.resolve_table_pattern(&proj.table);

            for table in tables {
                let columns = proj.columns.join(", ");
                let order_by = proj.order_by.join(", ");

                statements.push(format!(
                    "ALTER TABLE {} ADD PROJECTION IF NOT EXISTS {} (SELECT {} ORDER BY ({}))",
                    table, proj.name, columns, order_by
                ));
            }
        }

        statements
    }

    /// Generate MATERIALIZE PROJECTION statements.
    pub fn generate_materialize_projections(&self) -> Vec<String> {
        let mut statements = Vec::new();

        for proj in &self.config.projections {
            let tables = self.resolve_table_pattern(&proj.table);

            for table in tables {
                statements.push(format!(
                    "ALTER TABLE {} MATERIALIZE PROJECTION {}",
                    table, proj.name
                ));
            }
        }

        statements
    }

    /// Resolve table pattern ("*" = all node tables, "edges" = edge table).
    fn resolve_table_pattern(&self, pattern: &str) -> Vec<String> {
        match pattern {
            "*" => self
                .ontology
                .nodes()
                .map(|n| self.ontology.table_name(&n.name).unwrap())
                .collect(),
            "edges" => vec![EDGE_TABLE.to_string()],
            _ => vec![pattern.to_string()],
        }
    }

    /// Convert Arrow schema to ClickHouse CREATE TABLE DDL.
    fn schema_to_ddl(
        &self,
        table_name: &str,
        schema: &Schema,
        primary_key: &[&str],
        order_by: &[&str],
    ) -> String {
        let columns: Vec<String> = schema
            .fields()
            .iter()
            .map(|field| {
                let ch_type = arrow_to_clickhouse_type(field.data_type(), field.is_nullable());
                format!("    {} {}", field.name(), ch_type)
            })
            .collect();

        let order_by_clause = order_by.join(", ");

        // PRIMARY KEY clause (only if different from ORDER BY)
        let primary_key_clause = if primary_key.is_empty() {
            String::new()
        } else {
            format!("\nPRIMARY KEY ({})", primary_key.join(", "))
        };

        let mut settings = vec![format!(
            "index_granularity = {}",
            self.config.index_granularity
        )];
        for (key, value) in &self.config.settings {
            settings.push(format!("{} = {}", key, value));
        }

        format!(
            "CREATE TABLE IF NOT EXISTS {} (\n{}\n) ENGINE = {}\nORDER BY ({}){}\nSETTINGS {};",
            table_name,
            columns.join(",\n"),
            self.config.engine,
            order_by_clause,
            primary_key_clause,
            settings.join(", ")
        )
    }
}

/// Convert Arrow DataType to ClickHouse type string.
fn arrow_to_clickhouse_type(arrow_type: &ArrowDataType, nullable: bool) -> String {
    let base_type = match arrow_type {
        ArrowDataType::Boolean => "Bool",
        ArrowDataType::Int8 => "Int8",
        ArrowDataType::Int16 => "Int16",
        ArrowDataType::Int32 => "Int32",
        ArrowDataType::Int64 => "Int64",
        ArrowDataType::UInt8 => "UInt8",
        ArrowDataType::UInt16 => "UInt16",
        ArrowDataType::UInt32 => "UInt32",
        ArrowDataType::UInt64 => "UInt64",
        ArrowDataType::Float32 => "Float32",
        ArrowDataType::Float64 => "Float64",
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => "String",
        ArrowDataType::Date32 => "Date",
        ArrowDataType::Date64 => "DateTime64(3)",
        ArrowDataType::Timestamp(_, _) => "DateTime64(3)",
        _ => "String",
    };

    if nullable {
        format!("Nullable({})", base_type)
    } else {
        base_type.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::IndexConfig;
    use ontology::DataType;

    fn test_config() -> SchemaConfig {
        SchemaConfig::default()
    }

    #[test]
    fn test_generate_create_tables() {
        let ontology = Ontology::new()
            .with_nodes(["User", "Project"])
            .with_fields(
                "User",
                [("id", DataType::Int), ("username", DataType::String)],
            )
            .with_fields(
                "Project",
                [("id", DataType::Int), ("name", DataType::String)],
            );

        let config = test_config();
        let generator = SchemaGenerator::new(&ontology, &config);
        let statements = generator.generate_create_tables();

        assert_eq!(statements.len(), 3); // User, Project, edges

        let (_, user_ddl) = &statements[1];
        assert!(user_ddl.contains("CREATE TABLE IF NOT EXISTS"));
        assert!(user_ddl.contains("ENGINE = MergeTree"));
        assert!(user_ddl.contains("ORDER BY"));
    }

    #[test]
    fn test_generate_indexes() {
        let ontology = Ontology::new().with_nodes(["User"]);

        let config = SchemaConfig {
            indexes: vec![IndexConfig {
                name: "idx_id".to_string(),
                table: "*".to_string(),
                expression: "id".to_string(),
                index_type: "minmax".to_string(),
                granularity: 4,
            }],
            ..Default::default()
        };

        let generator = SchemaGenerator::new(&ontology, &config);
        let statements = generator.generate_add_indexes();

        assert_eq!(statements.len(), 1);
        assert!(statements[0].contains("ADD INDEX IF NOT EXISTS idx_id"));
        assert!(statements[0].contains("TYPE minmax GRANULARITY 4"));
    }
}
