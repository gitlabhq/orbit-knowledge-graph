//! ClickHouse schema generation from ontology.

use crate::arrow_schema::{ToArrowSchema, edge_schema, to_clickhouse_ddl};
use ontology::{EDGE_TABLE, Ontology};

/// Generates ClickHouse DDL statements from ontology.
pub struct SchemaGenerator<'a> {
    ontology: &'a Ontology,
}

impl<'a> SchemaGenerator<'a> {
    /// Create a new schema generator.
    pub fn new(ontology: &'a Ontology) -> Self {
        Self { ontology }
    }

    /// Generate all CREATE TABLE statements.
    pub fn generate_all_ddl(&self) -> Vec<(String, String)> {
        let mut ddl_statements = Vec::new();

        // Node tables - ORDER BY (organization_id, traversal_id, id) for efficient auth queries
        for node in self.ontology.nodes() {
            let tbl_name = self.ontology.table_name(&node.name).unwrap();
            let schema = node.to_arrow_schema();
            let order_by = vec!["organization_id", "traversal_id", "id"];
            let ddl = to_clickhouse_ddl(&tbl_name, &schema, &order_by);
            ddl_statements.push((tbl_name, ddl));
        }

        // Edges table
        let edge_schema = edge_schema();
        let edge_ddl = to_clickhouse_ddl(
            EDGE_TABLE,
            &edge_schema,
            &["relationship_kind", "source_kind", "source"],
        );
        ddl_statements.push((EDGE_TABLE.to_string(), edge_ddl));

        ddl_statements
    }

    /// Generate DROP TABLE statements for cleanup.
    pub fn generate_drop_all(&self) -> Vec<String> {
        let mut drops = Vec::new();

        for node in self.ontology.nodes() {
            let tbl_name = self.ontology.table_name(&node.name).unwrap();
            drops.push(format!("DROP TABLE IF EXISTS {}", tbl_name));
        }

        drops.push(format!("DROP TABLE IF EXISTS {}", EDGE_TABLE));

        drops
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ontology::DataType;

    #[test]
    fn test_generate_ddl() {
        let ontology = Ontology::new()
            .with_nodes(["User", "Project"])
            .with_fields(
                "User",
                [
                    ("id", DataType::Int),
                    ("username", DataType::String),
                    ("email", DataType::String),
                ],
            )
            .with_fields(
                "Project",
                [("id", DataType::Int), ("name", DataType::String)],
            );

        let generator = SchemaGenerator::new(&ontology);
        let ddl_statements = generator.generate_all_ddl();

        // Should have User, Project, and edges tables
        assert_eq!(ddl_statements.len(), 3);

        // Check User table
        let (_user_table, user_ddl) = &ddl_statements[1]; // BTreeMap order: Project, User
        assert!(user_ddl.contains("CREATE TABLE IF NOT EXISTS"));
        assert!(user_ddl.contains("organization_id UInt32"));
        assert!(user_ddl.contains("traversal_id String"));

        // Check edges table (no organization_id/traversal_id)
        let (edge_table, edge_ddl) = ddl_statements.last().unwrap();
        assert_eq!(edge_table, EDGE_TABLE);
        assert!(edge_ddl.contains("relationship_kind"));
        assert!(edge_ddl.contains("source_kind"));
        assert!(!edge_ddl.contains("organization_id"));
        assert!(!edge_ddl.contains("traversal_id"));
    }
}
