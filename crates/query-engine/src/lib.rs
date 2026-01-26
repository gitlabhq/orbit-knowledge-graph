//! Query Engine
//!
//! Compiles LLM-generated JSON graph queries into parameterized ClickHouse SQL.
//!
//! # Architecture
//!
//! ```text
//! JSON Input → Parse → Validate → Lower → AST → Codegen → SQL
//! ```
//!
//! - **Parse**: Deserialize JSON into typed `Input` structure
//! - **Validate**: Check against JSON schema and ontology
//! - **Lower**: Convert `Input` to SQL-oriented `AST`
//! - **Codegen**: Emit parameterized SQL from `AST`
//!
//! # Example
//!
//! ```rust
//! use query_engine::compile;
//! use ontology::Ontology;
//!
//! // Create an ontology with valid node labels and relationship types
//! let ontology = Ontology::new()
//!     .with_nodes(["User", "Project"])
//!     .with_edges(["MEMBER_OF"]);
//!
//! let json = r#"{
//!     "query_type": "traversal",
//!     "nodes": [{"id": "u", "label": "User"}],
//!     "limit": 10
//! }"#;
//!
//! let result = compile(json, &ontology).unwrap();
//! println!("SQL: {}", result.sql);
//! println!("Params: {:?}", result.params);
//! ```

pub mod ast;
pub mod codegen;
pub mod error;
pub mod input;
pub mod lower;

pub use ast::{Expr, JoinType, Node, Op, OrderExpr, Query, RecursiveCte, SelectExpr, TableRef};
pub use codegen::ParameterizedQuery;
pub use error::{QueryError, Result};
pub use input::{parse_input, Input, QueryType};
pub use ontology::{Ontology, OntologyError, EDGE_TABLE, RESERVED_COLUMNS};

/// Compile a JSON query into parameterized SQL.
///
/// This is the main entry point for the query engine. It:
/// 1. Parses the JSON input (with identifier validation)
/// 2. Lowers to an AST (validates against ontology)
/// 3. Generates parameterized SQL
#[must_use = "the compiled query should be used"]
pub fn compile(json_input: &str, ontology: &Ontology) -> Result<ParameterizedQuery> {
    // Parse JSON into Input struct (validates identifiers during deserialization)
    let input = parse_input(json_input)?;

    // Lower to AST (validates node labels, relationship types, and fields)
    let ast = lower::lower(&input, ontology)?;

    // Generate SQL (no validation needed, AST is already validated)
    codegen::codegen(&ast)
}

/// Compile JSON input and return the AST without generating SQL.
///
/// Useful for debugging or when you need to manipulate the AST before codegen.
#[must_use = "the compiled AST should be used"]
pub fn compile_to_ast(json_input: &str, ontology: &Ontology) -> Result<Node> {
    let input = parse_input(json_input)?;
    lower::lower(&input, ontology)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_ontology() -> Ontology {
        use ontology::DataType::{Bool, DateTime, String};
        Ontology::new()
            .with_nodes(["User", "Project", "Note", "Group"])
            .with_edges(["AUTHORED", "CONTAINS", "MEMBER_OF"])
            .with_fields("User", [("username", String), ("email", String), ("state", String), ("created_at", DateTime)])
            .with_fields("Project", [("name", String)])
            .with_fields("Note", [("confidential", Bool), ("created_at", DateTime)])
    }

    #[test]
    fn test_compile_traversal() {
        // Node order doesn't matter - we automatically start from the "from" node
        let json = r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "n", "label": "Note", "filters": {"confidential": true}},
                {"id": "u", "label": "User"}
            ],
            "relationships": [
                {"type": "AUTHORED", "from": "u", "to": "n"}
            ],
            "limit": 25,
            "order_by": {"node": "n", "property": "created_at", "direction": "DESC"}
        }"#;

        let result = compile(json, &test_ontology()).unwrap();

        // Basic assertions
        assert!(result.sql.contains("SELECT"));
        // Entity-specific tables: kg_user, kg_note, kg_edges
        assert!(
            result.sql.contains("kg_user AS u"),
            "expected kg_user table: {}",
            result.sql
        );
        assert!(
            result.sql.contains("INNER JOIN kg_edges AS e0 ON"),
            "expected INNER JOIN kg_edges: {}",
            result.sql
        );
        assert!(
            result.sql.contains("u.id = e0.from_id"),
            "expected join condition: {}",
            result.sql
        );
        assert!(
            result.sql.contains("INNER JOIN kg_note AS n ON"),
            "expected INNER JOIN kg_note: {}",
            result.sql
        );
        // Type filters are now applied
        assert!(
            result.sql.contains("e0.label = {type_e0:String}"),
            "expected edge type filter: {}",
            result.sql
        );
        assert!(
            result.sql.contains("n.label = {type_n:String}"),
            "expected node type filter: {}",
            result.sql
        );
        assert!(result.sql.contains("LIMIT 25"));
        // Verify type filter params are set
        assert_eq!(
            result.params.get("type_e0"),
            Some(&serde_json::Value::String("AUTHORED".into()))
        );
        assert_eq!(
            result.params.get("type_n"),
            Some(&serde_json::Value::String("Note".into()))
        );
    }

    #[test]
    fn test_compile_aggregation() {
        let json = r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "n", "label": "Note"},
                {"id": "u", "label": "User"}
            ],
            "relationships": [
                {"type": "AUTHORED", "from": "u", "to": "n"}
            ],
            "aggregations": [
                {"function": "count", "target": "n", "group_by": "u", "alias": "note_count"}
            ],
            "limit": 10
        }"#;

        let result = compile(json, &test_ontology()).unwrap();

        assert!(result.sql.contains("COUNT"));
        assert!(result.sql.contains("GROUP BY"));
    }

    #[test]
    fn test_compile_path_finding() {
        let json = r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "label": "Project", "node_ids": [100]},
                {"id": "end", "label": "Project", "node_ids": [200]}
            ],
            "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3}
        }"#;

        let result = compile(json, &test_ontology()).unwrap();

        assert!(result.sql.contains("WITH RECURSIVE"));
        assert!(result.sql.contains("path_cte"));
        assert!(result.sql.contains("UNION ALL"));
    }

    #[test]
    fn test_compile_with_filters() {
        let json = r#"{
            "query_type": "traversal",
            "nodes": [{
                "id": "u",
                "label": "User",
                "filters": {
                    "created_at": {"op": "gte", "value": "2024-01-01"},
                    "state": {"op": "in", "value": ["active", "blocked"]},
                    "username": {"op": "contains", "value": "admin"}
                }
            }],
            "limit": 30
        }"#;

        let result = compile(json, &test_ontology()).unwrap();

        assert!(result.sql.contains("WHERE"));
        assert!(result.sql.contains(">="));
        assert!(result.sql.contains("IN"));
        assert!(result.sql.contains("LIKE"));
    }

    #[test]
    fn test_compile_to_ast() {
        let json = r#"{
            "query_type": "traversal",
            "nodes": [{"id": "u", "label": "User"}],
            "limit": 10
        }"#;

        let ast = compile_to_ast(json, &test_ontology()).unwrap();

        match ast {
            Node::Query(q) => {
                assert_eq!(q.limit, Some(10));
                assert_eq!(q.select.len(), 1);
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn test_invalid_json() {
        let json = "not valid json";
        let result = compile(json, &test_ontology());
        assert!(result.is_err());
    }

    #[test]
    fn test_missing_required_fields() {
        let json = r#"{"query_type": "traversal"}"#;
        let result = compile(json, &test_ontology());
        assert!(result.is_err());
    }
}

/// Integration tests using the ontology fixtures
#[cfg(test)]
mod ontology_integration_tests {
    use super::*;
    use std::path::Path;

    fn fixtures_ontology() -> Ontology {
        let fixtures_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("crates directory should exist")
            .parent()
            .expect("workspace root should exist")
            .join("fixtures/ontology");
        Ontology::load_from_dir(fixtures_dir).expect("should load ontology fixtures")
    }

    #[test]
    fn test_valid_column_in_order_by() {
        let ontology = fixtures_ontology();
        let json = r#"{
            "query_type": "traversal",
            "nodes": [{"id": "u", "label": "User"}],
            "limit": 10,
            "order_by": {"node": "u", "property": "username", "direction": "ASC"}
        }"#;

        let result = compile(json, &ontology);
        assert!(
            result.is_ok(),
            "expected no error for valid column, got: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_invalid_column_in_order_by() {
        let ontology = fixtures_ontology();
        let json = r#"{
            "query_type": "traversal",
            "nodes": [{"id": "u", "label": "User"}],
            "limit": 10,
            "order_by": {"node": "u", "property": "nonexistent_column", "direction": "ASC"}
        }"#;

        let result = compile(json, &ontology);
        assert!(result.is_err(), "expected error for invalid column");
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("does not exist"),
            "error should mention column doesn't exist: {err}"
        );
    }

    #[test]
    fn test_valid_column_in_filter() {
        let ontology = fixtures_ontology();
        let json = r#"{
            "query_type": "traversal",
            "nodes": [{
                "id": "u",
                "label": "User",
                "filters": {"username": "admin"}
            }],
            "limit": 10
        }"#;

        let result = compile(json, &ontology);
        assert!(
            result.is_ok(),
            "expected no error for valid column, got: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_invalid_column_in_filter() {
        let ontology = fixtures_ontology();
        let json = r#"{
            "query_type": "traversal",
            "nodes": [{
                "id": "u",
                "label": "User",
                "filters": {"nonexistent_column": "value"}
            }],
            "limit": 10
        }"#;

        let result = compile(json, &ontology);
        assert!(result.is_err(), "expected error for invalid column");
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("does not exist"),
            "error should mention column doesn't exist: {err}"
        );
    }

    #[test]
    fn test_valid_column_in_aggregation() {
        let ontology = fixtures_ontology();
        let json = r#"{
            "query_type": "aggregation",
            "nodes": [{"id": "p", "label": "Project"}],
            "aggregations": [
                {"function": "count", "target": "p", "property": "name", "alias": "name_count"}
            ],
            "limit": 10
        }"#;

        let result = compile(json, &ontology);
        assert!(
            result.is_ok(),
            "expected no error for valid column, got: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_invalid_column_in_aggregation() {
        let ontology = fixtures_ontology();
        let json = r#"{
            "query_type": "aggregation",
            "nodes": [{"id": "p", "label": "Project"}],
            "aggregations": [
                {"function": "sum", "target": "p", "property": "invalid_property", "alias": "total"}
            ],
            "limit": 10
        }"#;

        let result = compile(json, &ontology);
        assert!(result.is_err(), "expected error for invalid column");
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("does not exist"),
            "error should mention column doesn't exist: {err}"
        );
    }

    #[test]
    fn test_invalid_type_filter_rejected() {
        let ontology = fixtures_ontology();

        // Test that invalid node labels are rejected during lowering
        let json = r#"{
            "query_type": "traversal",
            "nodes": [{"id": "n", "label": "NonexistentType"}],
            "limit": 10
        }"#;

        let result = compile(json, &ontology);
        assert!(result.is_err(), "expected error for invalid type filter");
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("unknown node label"),
            "error should mention unknown node label: {err}"
        );
    }

    #[test]
    fn test_full_pipeline_with_ontology() {
        let ontology = fixtures_ontology();
        let json = r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "n", "label": "Note", "filters": {"confidential": true}},
                {"id": "u", "label": "User"}
            ],
            "relationships": [
                {"type": "AUTHORED", "from": "u", "to": "n"}
            ],
            "limit": 25,
            "order_by": {"node": "n", "property": "created_at", "direction": "DESC"}
        }"#;

        let result = compile(json, &ontology).unwrap();

        assert!(result.sql.contains("SELECT"));
        assert!(result.sql.contains("INNER JOIN"));
        assert!(result.sql.contains("LIMIT 25"));
        assert!(result.sql.contains("ORDER BY"));
        assert!(result.sql.contains("DESC"));
    }
}
