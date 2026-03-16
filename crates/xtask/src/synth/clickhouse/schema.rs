//! ClickHouse schema generation from ontology and config.

use crate::synth::arrow_schema::{ToArrowSchema, arrow_to_clickhouse_type, edge_schema};
use crate::synth::config::SchemaConfig;
use crate::synth::constants::{TABLE_PATTERN_ALL_NODES, TABLE_PATTERN_EDGES};
use arrow::datatypes::Schema;
use ontology::Ontology;
use ontology::constants::EDGE_TABLE;

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
            let ddl = self.schema_to_ddl(tbl_name, &schema, &primary_key, &order_by);
            statements.push((tbl_name.to_owned(), ddl));
        }

        // Edge table
        let edge_schema = edge_schema(self.ontology);
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
            drops.push(format!("DROP TABLE IF EXISTS {} SYNC", tbl_name));
        }

        drops.push(format!("DROP TABLE IF EXISTS {} SYNC", EDGE_TABLE));
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
            TABLE_PATTERN_ALL_NODES => self
                .ontology
                .nodes()
                .map(|n| self.ontology.table_name(&n.name).unwrap().to_owned())
                .collect(),
            TABLE_PATTERN_EDGES => vec![EDGE_TABLE.to_string()],
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::synth::config::IndexConfig;
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
