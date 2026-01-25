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
//! use query_engine::{compile, Schema};
//!
//! // Create a schema with valid node labels and relationship types
//! let schema = Schema::from_ontology(
//!     ["User", "Project"],
//!     ["MEMBER_OF"],
//!     std::collections::HashMap::new(),
//! );
//!
//! let json = r#"{
//!     "query_type": "traversal",
//!     "nodes": [{"id": "u", "label": "User"}],
//!     "limit": 10
//! }"#;
//!
//! let result = compile(json, &schema).unwrap();
//! println!("SQL: {}", result.sql);
//! println!("Params: {:?}", result.params);
//! ```

pub mod ast;
pub mod codegen;
pub mod error;
pub mod input;
pub mod lower;
#[cfg(test)]
mod ontology;
pub mod schema;

pub use ast::{Expr, JoinType, Node, Op, OrderExpr, Query, RecursiveCte, SelectExpr, TableRef};
pub use codegen::ParameterizedQuery;
pub use error::{QueryError, Result};
pub use input::{parse_input, Input, QueryType};
pub use schema::Schema;

/// Compile a JSON query into parameterized SQL.
///
/// This is the main entry point for the query engine. It:
/// 1. Parses the JSON input
/// 2. Validates against the schema
/// 3. Lowers to an AST
/// 4. Generates parameterized SQL
pub fn compile(json_input: &str, schema: &Schema) -> Result<ParameterizedQuery> {
    // Parse JSON into Input struct
    let input = parse_input(json_input)?;

    // Validate the parsed input (basic structure validation)
    let json_value: serde_json::Value = serde_json::from_str(json_input)?;
    schema.validate_json(&json_value)?;

    // Lower to AST
    let ast = lower::lower(&input, schema)?;

    // Generate SQL
    codegen::codegen(&ast, schema)
}

/// Compile JSON input and return the AST without generating SQL.
///
/// Useful for debugging or when you need to manipulate the AST before codegen.
pub fn compile_to_ast(json_input: &str, schema: &Schema) -> Result<Node> {
    let input = parse_input(json_input)?;
    let json_value: serde_json::Value = serde_json::from_str(json_input)?;
    schema.validate_json(&json_value)?;
    lower::lower(&input, schema)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn test_schema() -> Schema {
        Schema::from_ontology(
            ["User", "Project", "Note", "Group"],
            ["AUTHORED", "CONTAINS", "MEMBER_OF"],
            HashMap::new(),
        )
    }

    #[test]
    fn test_compile_traversal() {
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

        let result = compile(json, &test_schema()).unwrap();

        // Basic assertions
        assert!(result.sql.contains("SELECT"));
        assert!(result
            .sql
            .contains("INNER JOIN edges AS e0 ON (u.id = e0.from_id)"));
        assert!(result
            .sql
            .contains("INNER JOIN nodes AS n ON (e0.to_id = n.id)"));
        assert!(result.sql.contains("LIMIT 25"));
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

        let result = compile(json, &test_schema()).unwrap();

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

        let result = compile(json, &test_schema()).unwrap();

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

        let result = compile(json, &test_schema()).unwrap();

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

        let ast = compile_to_ast(json, &test_schema()).unwrap();

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
        let result = compile(json, &test_schema());
        assert!(result.is_err());
    }

    #[test]
    fn test_missing_required_fields() {
        let json = r#"{"query_type": "traversal"}"#;
        let result = compile(json, &test_schema());
        assert!(result.is_err());
    }
}

/// Integration tests using the ontology fixtures
#[cfg(test)]
mod ontology_integration_tests {
    use super::*;
    use crate::ontology::tests::load_ontology_schema;

    #[test]
    fn test_valid_column_in_order_by() {
        let schema = load_ontology_schema();
        let json = r#"{
            "query_type": "traversal",
            "nodes": [{"id": "u", "label": "User"}],
            "limit": 10,
            "order_by": {"node": "u", "property": "username", "direction": "ASC"}
        }"#;

        let result = compile(json, &schema);
        assert!(
            result.is_ok(),
            "expected no error for valid column, got: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_invalid_column_in_order_by() {
        let schema = load_ontology_schema();
        let json = r#"{
            "query_type": "traversal",
            "nodes": [{"id": "u", "label": "User"}],
            "limit": 10,
            "order_by": {"node": "u", "property": "nonexistent_column", "direction": "ASC"}
        }"#;

        let result = compile(json, &schema);
        assert!(result.is_err(), "expected error for invalid column");
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("does not exist"),
            "error should mention column doesn't exist: {err}"
        );
    }

    #[test]
    fn test_valid_column_in_filter() {
        let schema = load_ontology_schema();
        let json = r#"{
            "query_type": "traversal",
            "nodes": [{
                "id": "u",
                "label": "User",
                "filters": {"username": "admin"}
            }],
            "limit": 10
        }"#;

        let result = compile(json, &schema);
        assert!(
            result.is_ok(),
            "expected no error for valid column, got: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_invalid_column_in_filter() {
        let schema = load_ontology_schema();
        let json = r#"{
            "query_type": "traversal",
            "nodes": [{
                "id": "u",
                "label": "User",
                "filters": {"nonexistent_column": "value"}
            }],
            "limit": 10
        }"#;

        let result = compile(json, &schema);
        assert!(result.is_err(), "expected error for invalid column");
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("does not exist"),
            "error should mention column doesn't exist: {err}"
        );
    }

    #[test]
    fn test_valid_column_in_aggregation() {
        let schema = load_ontology_schema();
        let json = r#"{
            "query_type": "aggregation",
            "nodes": [{"id": "p", "label": "Project"}],
            "aggregations": [
                {"function": "count", "target": "p", "property": "name", "alias": "name_count"}
            ],
            "limit": 10
        }"#;

        let result = compile(json, &schema);
        assert!(
            result.is_ok(),
            "expected no error for valid column, got: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_invalid_column_in_aggregation() {
        let schema = load_ontology_schema();
        let json = r#"{
            "query_type": "aggregation",
            "nodes": [{"id": "p", "label": "Project"}],
            "aggregations": [
                {"function": "sum", "target": "p", "property": "invalid_property", "alias": "total"}
            ],
            "limit": 10
        }"#;

        let result = compile(json, &schema);
        assert!(result.is_err(), "expected error for invalid column");
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("does not exist"),
            "error should mention column doesn't exist: {err}"
        );
    }

    #[test]
    fn test_invalid_type_filter_in_codegen() {
        let schema = load_ontology_schema();

        // This test creates an AST directly to test codegen validation
        let query = Query {
            select: vec![SelectExpr {
                expr: Expr::col("n", "id"),
                alias: None,
            }],
            from: TableRef::scan_with_filter("nodes", "n", "NonexistentType"),
            where_clause: None,
            group_by: vec![],
            order_by: vec![],
            limit: Some(10),
        };

        let result = codegen::codegen(&Node::Query(Box::new(query)), &schema);
        assert!(result.is_err(), "expected error for invalid type filter");
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("not a valid"),
            "error should mention invalid type: {err}"
        );
    }

    #[test]
    fn test_full_pipeline_with_ontology() {
        let schema = load_ontology_schema();
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

        let result = compile(json, &schema).unwrap();

        assert!(result.sql.contains("SELECT"));
        assert!(result.sql.contains("INNER JOIN"));
        assert!(result.sql.contains("LIMIT 25"));
        assert!(result.sql.contains("ORDER BY"));
        assert!(result.sql.contains("DESC"));
    }
}
