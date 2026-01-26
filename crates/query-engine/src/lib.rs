//! Query Engine
//!
//! Compiles LLM-generated JSON graph queries into parameterized ClickHouse SQL.
//!
//! # Architecture
//!
//! ```text
//! JSON Input → Schema Validate → Parse → Lower → AST → Codegen → SQL
//! ```
//!
//! - **Schema Validate**: Check structure, identifier patterns, and ontology enums via JSON Schema
//! - **Parse**: Deserialize JSON into typed `Input` structure
//! - **Lower**: Convert `Input` to SQL-oriented `AST` (validates fields against ontology)
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

use std::sync::OnceLock;

pub use ast::{Expr, JoinType, Node, Op, OrderExpr, Query, RecursiveCte, SelectExpr, TableRef};
pub use codegen::ParameterizedQuery;
pub use error::{QueryError, Result};
pub use input::{parse_input, Input, QueryType};

// Re-export from ontology crate for convenience
pub use ontology::{Ontology, OntologyError, EDGE_TABLE, RESERVED_COLUMNS};

/// Base JSON Schema for query validation (embedded at compile time).
/// This validates structure and identifier patterns for SQL injection prevention.
/// Node labels and relationship types are validated separately with ontology-derived schema.
const BASE_SCHEMA_JSON: &str = include_str!("../../ontology/schema.json");

/// Compiled base JSON Schema validator for structural validation (lazily initialized).
/// This validates identifiers, required fields, etc. but NOT ontology-specific enums.
static BASE_SCHEMA_VALIDATOR: OnceLock<jsonschema::Validator> = OnceLock::new();

fn get_base_schema_validator() -> &'static jsonschema::Validator {
    BASE_SCHEMA_VALIDATOR.get_or_init(|| {
        let schema: serde_json::Value = serde_json::from_str(BASE_SCHEMA_JSON)
            .expect("embedded schema.json must be valid JSON");
        jsonschema::validator_for(&schema)
            .expect("embedded schema.json must be a valid JSON Schema")
    })
}

/// Validate JSON input against the base schema (structure + identifiers).
fn validate_base_schema(json_input: &str) -> Result<serde_json::Value> {
    let value: serde_json::Value = serde_json::from_str(json_input)?;

    let validator = get_base_schema_validator();

    let errors: Vec<String> = validator
        .iter_errors(&value)
        .map(|e| format!("{} at {}", e, e.instance_path))
        .collect();

    if !errors.is_empty() {
        return Err(QueryError::Validation(errors.join("; ")));
    }

    Ok(value)
}

/// Validate JSON input against an ontology-derived schema.
/// This validates node labels and relationship types against the ontology.
fn validate_ontology_schema(value: &serde_json::Value, ontology: &Ontology) -> Result<()> {
    // Derive schema with ontology-specific enums
    let derived_schema = ontology
        .derive_json_schema(BASE_SCHEMA_JSON)
        .map_err(|e| QueryError::Validation(format!("failed to derive schema: {e}")))?;

    let validator = jsonschema::validator_for(&derived_schema)
        .map_err(|e| QueryError::Validation(format!("invalid derived schema: {e}")))?;

    let errors: Vec<String> = validator
        .iter_errors(value)
        .map(|e| format!("{} at {}", e, e.instance_path))
        .collect();

    if !errors.is_empty() {
        return Err(QueryError::Validation(errors.join("; ")));
    }

    Ok(())
}

/// Compile a JSON query into parameterized SQL.
///
/// This is the main entry point for the query engine. It:
/// 1. Validates against base JSON Schema (structure + identifier safety)
/// 2. Validates against ontology-derived schema (node labels, relationship types)
/// 3. Parses into typed Input structure
/// 4. Lowers to an AST (validates fields against ontology)
/// 5. Generates parameterized SQL
#[must_use = "the compiled query should be used"]
pub fn compile(json_input: &str, ontology: &Ontology) -> Result<ParameterizedQuery> {
    // Validate against base JSON Schema (structure, identifier patterns)
    let value = validate_base_schema(json_input)?;

    // Validate against ontology-derived schema (node labels, relationship types)
    validate_ontology_schema(&value, ontology)?;

    // Parse validated JSON into Input struct
    let input: Input = serde_json::from_value(value)?;

    // Lower to AST (validates field names against ontology)
    let ast = lower::lower(&input, ontology)?;

    // Generate SQL
    codegen::codegen(&ast)
}

/// Compile JSON input and return the AST without generating SQL.
///
/// Useful for debugging or when you need to manipulate the AST before codegen.
#[must_use = "the compiled AST should be used"]
pub fn compile_to_ast(json_input: &str, ontology: &Ontology) -> Result<Node> {
    let value = validate_base_schema(json_input)?;
    validate_ontology_schema(&value, ontology)?;
    let input: Input = serde_json::from_value(value)?;
    lower::lower(&input, ontology)
}

/// Get the base JSON schema (without ontology values).
/// Useful for documentation or LLM prompts where you want the schema template.
#[must_use]
pub fn base_schema() -> &'static str {
    BASE_SCHEMA_JSON
}

/// Derive a JSON schema with ontology-specific values populated.
/// This includes valid node labels, relationship types, and property definitions.
///
/// Useful for providing to LLMs so they know which values are valid.
pub fn derive_schema(ontology: &Ontology) -> Result<serde_json::Value> {
    ontology
        .derive_json_schema(BASE_SCHEMA_JSON)
        .map_err(|e| QueryError::Ontology(e))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_ontology() -> Ontology {
        use ontology::DataType;

        Ontology::new()
            .with_nodes(["User", "Project", "Note", "Group"])
            .with_edges(["AUTHORED", "CONTAINS", "MEMBER_OF"])
            .with_fields(
                "User",
                [
                    ("username", DataType::String),
                    ("state", DataType::String),
                    ("created_at", DataType::DateTime),
                ],
            )
            .with_fields(
                "Note",
                [
                    ("confidential", DataType::Bool),
                    ("created_at", DataType::DateTime),
                ],
            )
            .with_fields("Project", [("name", DataType::String)])
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

    // SQL injection prevention tests - these should be caught by JSON Schema validation
    #[test]
    fn test_rejects_sql_injection_in_node_id() {
        let json = r#"{
            "query_type": "traversal",
            "nodes": [{"id": "n; DROP TABLE users; --"}]
        }"#;

        let result = compile(json, &test_ontology());
        assert!(result.is_err(), "should reject SQL injection in node id");
        let err = result.unwrap_err();
        assert!(
            matches!(err, QueryError::Validation(_)),
            "error should be a validation error: {err}"
        );
    }

    #[test]
    fn test_rejects_sql_injection_in_relationship() {
        let json = r#"{
            "query_type": "traversal",
            "nodes": [{"id": "a"}, {"id": "b"}],
            "relationships": [
                {"type": "REL", "from": "a' OR '1'='1", "to": "b"}
            ]
        }"#;

        let result = compile(json, &test_ontology());
        assert!(
            result.is_err(),
            "should reject SQL injection in relationship from"
        );
        let err = result.unwrap_err();
        assert!(
            matches!(err, QueryError::Validation(_)),
            "error should be a validation error: {err}"
        );
    }

    #[test]
    fn test_rejects_empty_node_id() {
        let json = r#"{
            "query_type": "traversal",
            "nodes": [{"id": ""}]
        }"#;

        let result = compile(json, &test_ontology());
        assert!(result.is_err(), "should reject empty node id");
    }

    #[test]
    fn test_rejects_id_starting_with_number() {
        let json = r#"{
            "query_type": "traversal",
            "nodes": [{"id": "123abc"}]
        }"#;

        let result = compile(json, &test_ontology());
        assert!(result.is_err(), "should reject id starting with number");
        let err = result.unwrap_err();
        assert!(
            matches!(err, QueryError::Validation(_)),
            "error should be a validation error: {err}"
        );
    }

    #[test]
    fn test_rejects_sql_injection_in_filter_property() {
        let json = r#"{
            "query_type": "traversal",
            "nodes": [{
                "id": "u",
                "label": "User",
                "filters": {"foo; DROP TABLE--": "value"}
            }]
        }"#;

        let result = compile(json, &test_ontology());
        assert!(
            result.is_err(),
            "should reject SQL injection in filter property name"
        );
        let err = result.unwrap_err();
        assert!(
            matches!(err, QueryError::Validation(_)),
            "error should be a validation error: {err}"
        );
    }

    #[test]
    fn test_valid_identifiers_accepted() {
        let json = r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "user_node", "label": "User"},
                {"id": "_private", "label": "Note"},
                {"id": "CamelCase", "label": "Project"},
                {"id": "node123", "label": "Group"}
            ]
        }"#;

        let result = compile(json, &test_ontology());
        assert!(result.is_ok(), "should accept valid identifiers: {:?}", result.err());
    }
}

/// Integration tests using the ontology fixtures
#[cfg(test)]
mod ontology_integration_tests {
    use super::*;
    use std::path::Path;

    fn load_test_ontology() -> Ontology {
        let fixtures_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("fixtures/ontology");

        Ontology::load_from_dir(&fixtures_dir)
            .unwrap_or_else(|e| panic!("Failed to load test ontology: {e}"))
    }

    #[test]
    fn test_valid_column_in_order_by() {
        let ontology = load_test_ontology();
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
        let ontology = load_test_ontology();
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
        let ontology = load_test_ontology();
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
        let ontology = load_test_ontology();
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
        let ontology = load_test_ontology();
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
        let ontology = load_test_ontology();
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
        let ontology = load_test_ontology();

        // Test that invalid node labels are rejected during schema validation
        let json = r#"{
            "query_type": "traversal",
            "nodes": [{"id": "n", "label": "NonexistentType"}],
            "limit": 10
        }"#;

        let result = compile(json, &ontology);
        assert!(result.is_err(), "expected error for invalid type filter");
        let err = result.unwrap_err();
        // Schema validation now catches invalid labels with helpful message showing valid options
        assert!(
            err.to_string().contains("NonexistentType")
                && err.to_string().contains("is not one of"),
            "error should show invalid label and valid options: {err}"
        );
    }

    #[test]
    fn test_full_pipeline_with_ontology() {
        let ontology = load_test_ontology();
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
