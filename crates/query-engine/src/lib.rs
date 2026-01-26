//! Query Engine
//!
//! Compiles JSON graph queries into parameterized ClickHouse SQL.
//!
//! # Pipeline
//!
//! ```text
//! JSON → Schema Validate → Parse → Validate → Lower → Codegen → SQL
//! ```
//!
//! After validation, lowering and codegen are pure transformations that cannot fail.
//!
//! # Example
//!
//! ```rust
//! use query_engine::compile;
//! use ontology::Ontology;
//!
//! let ontology = Ontology::new()
//!     .with_nodes(["User", "Project"])
//!     .with_edges(["MEMBER_OF"]);
//!
//! let json = r#"{
//!     "query_type": "traversal",
//!     "nodes": [{"id": "u", "entity": "User"}],
//!     "limit": 10
//! }"#;
//!
//! let result = compile(json, &ontology).unwrap();
//! println!("SQL: {}", result.sql);
//! ```

pub mod ast;
pub mod codegen;
pub mod error;
pub mod input;
pub mod lower;
pub mod validate;

use std::sync::OnceLock;

pub use ast::{Expr, JoinType, Node, Op, OrderExpr, Query, RecursiveCte, SelectExpr, TableRef};
pub use codegen::ParameterizedQuery;
pub use error::{QueryError, Result};
pub use input::{parse_input, Input, QueryType};
pub use ontology::{Ontology, OntologyError, EDGE_TABLE, RESERVED_COLUMNS};

// ─────────────────────────────────────────────────────────────────────────────
// Schema validation
// ─────────────────────────────────────────────────────────────────────────────

const BASE_SCHEMA_JSON: &str = include_str!("../../ontology/schema.json");

static BASE_SCHEMA_VALIDATOR: OnceLock<jsonschema::Validator> = OnceLock::new();

fn base_validator() -> &'static jsonschema::Validator {
    BASE_SCHEMA_VALIDATOR.get_or_init(|| {
        let schema: serde_json::Value =
            serde_json::from_str(BASE_SCHEMA_JSON).expect("schema.json must be valid JSON");
        jsonschema::validator_for(&schema).expect("schema.json must be a valid JSON Schema")
    })
}

fn validate_json(json: &str) -> Result<serde_json::Value> {
    let value: serde_json::Value = serde_json::from_str(json)?;
    collect_schema_errors(base_validator(), &value)?;
    Ok(value)
}

fn validate_ontology(value: &serde_json::Value, ontology: &Ontology) -> Result<()> {
    let schema = ontology
        .derive_json_schema(BASE_SCHEMA_JSON)
        .map_err(|e| QueryError::Validation(format!("failed to derive schema: {e}")))?;

    let validator = jsonschema::validator_for(&schema)
        .map_err(|e| QueryError::Validation(format!("invalid derived schema: {e}")))?;

    collect_schema_errors(&validator, value)
}

fn collect_schema_errors(
    validator: &jsonschema::Validator,
    value: &serde_json::Value,
) -> Result<()> {
    let errors: Vec<_> = validator
        .iter_errors(value)
        .map(|e| format!("{} at {}", e, e.instance_path))
        .collect();

    if errors.is_empty() {
        Ok(())
    } else {
        Err(QueryError::Validation(errors.join("; ")))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Public API
// ─────────────────────────────────────────────────────────────────────────────

/// Compile a JSON query into parameterized SQL.
///
/// Validates structure, identifiers, and ontology values before generating SQL.
#[must_use = "the compiled query should be used"]
pub fn compile(json_input: &str, ontology: &Ontology) -> Result<ParameterizedQuery> {
    let value = validate_json(json_input)?;
    validate_ontology(&value, ontology)?;
    let input: Input = serde_json::from_value(value)?;
    validate::validate(&input, ontology)?;
    let ast = lower::lower(&input, ontology);
    Ok(codegen::codegen(&ast))
}

/// Compile JSON and return the AST without generating SQL.
#[must_use = "the compiled AST should be used"]
pub fn compile_to_ast(json_input: &str, ontology: &Ontology) -> Result<Node> {
    let value = validate_json(json_input)?;
    validate_ontology(&value, ontology)?;
    let input: Input = serde_json::from_value(value)?;
    validate::validate(&input, ontology)?;
    Ok(lower::lower(&input, ontology))
}

/// Get the base JSON schema template (without ontology values).
#[must_use]
pub fn base_schema() -> &'static str {
    BASE_SCHEMA_JSON
}

/// Derive a JSON schema with ontology values populated (node labels, relationship types).
pub fn derive_schema(ontology: &Ontology) -> Result<serde_json::Value> {
    ontology
        .derive_json_schema(BASE_SCHEMA_JSON)
        .map_err(QueryError::Ontology)
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

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
    fn traversal_query() {
        let json = r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "n", "entity": "Note", "filters": {"confidential": true}},
                {"id": "u", "entity": "User"}
            ],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "n"}],
            "limit": 25,
            "order_by": {"node": "n", "property": "created_at", "direction": "DESC"}
        }"#;

        let result = compile(json, &test_ontology()).unwrap();

        assert!(result.sql.contains("SELECT"));
        assert!(result.sql.contains("kg_user AS u"));
        assert!(result.sql.contains("INNER JOIN kg_edges AS e0 ON"));
        // Edge table uses "source" column
        assert!(
            result.sql.contains("u.id = e0.source"),
            "expected source column: {}",
            result.sql
        );
        assert!(result.sql.contains("INNER JOIN kg_note AS n ON"));
        // Edge type filter uses relationship_kind column
        assert!(
            result
                .sql
                .contains("e0.relationship_kind = {type_e0:String}"),
            "expected relationship_kind: {}",
            result.sql
        );
        // Node tables don't have type filters (entity-specific tables)
        assert!(
            !result.sql.contains("n.label"),
            "node should not have type filter: {}",
            result.sql
        );
        assert!(result.sql.contains("LIMIT 25"));
        assert_eq!(
            result.params.get("type_e0"),
            Some(&serde_json::json!("AUTHORED"))
        );
    }

    #[test]
    fn aggregation_query() {
        let json = r#"{
            "query_type": "aggregation",
            "nodes": [{"id": "n", "entity": "Note"}, {"id": "u", "entity": "User"}],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "n"}],
            "aggregations": [{"function": "count", "target": "n", "group_by": "u", "alias": "note_count"}],
            "limit": 10
        }"#;

        let result = compile(json, &test_ontology()).unwrap();
        assert!(result.sql.contains("COUNT"));
        assert!(result.sql.contains("GROUP BY"));
    }

    #[test]
    fn path_finding_query() {
        let json = r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "entity": "Project", "node_ids": [100]},
                {"id": "end", "entity": "Project", "node_ids": [200]}
            ],
            "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3}
        }"#;

        let result = compile(json, &test_ontology()).unwrap();
        assert!(result.sql.contains("WITH RECURSIVE"));
        assert!(result.sql.contains("path_cte"));
        assert!(result.sql.contains("UNION ALL"));
    }

    #[test]
    fn filter_operators() {
        let json = r#"{
            "query_type": "traversal",
            "nodes": [{
                "id": "u",
                "entity": "User",
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
    fn compile_to_ast_works() {
        let json = r#"{
            "query_type": "traversal",
            "nodes": [{"id": "u", "entity": "User"}],
            "limit": 10
        }"#;

        let Node::Query(q) = compile_to_ast(json, &test_ontology()).unwrap() else {
            panic!("expected Query");
        };
        assert_eq!(q.limit, Some(10));
        assert_eq!(q.select.len(), 1);
    }

    #[test]
    fn invalid_json_rejected() {
        assert!(compile("not valid json", &test_ontology()).is_err());
    }

    #[test]
    fn missing_required_fields_rejected() {
        let result = compile(r#"{"query_type": "traversal"}"#, &test_ontology());
        assert!(result.is_err());
    }

    // SQL injection prevention tests
    #[test]
    fn sql_injection_in_node_id() {
        let json = r#"{"query_type": "traversal", "nodes": [{"id": "n; DROP TABLE users; --"}]}"#;
        let err = compile(json, &test_ontology()).unwrap_err();
        assert!(matches!(err, QueryError::Validation(_)));
    }

    #[test]
    fn sql_injection_in_relationship() {
        let json = r#"{
            "query_type": "traversal",
            "nodes": [{"id": "a"}, {"id": "b"}],
            "relationships": [{"type": "REL", "from": "a' OR '1'='1", "to": "b"}]
        }"#;
        let err = compile(json, &test_ontology()).unwrap_err();
        assert!(matches!(err, QueryError::Validation(_)));
    }

    #[test]
    fn empty_node_id_rejected() {
        let json = r#"{"query_type": "traversal", "nodes": [{"id": ""}]}"#;
        assert!(compile(json, &test_ontology()).is_err());
    }

    #[test]
    fn id_starting_with_number_rejected() {
        let json = r#"{"query_type": "traversal", "nodes": [{"id": "123abc"}]}"#;
        let err = compile(json, &test_ontology()).unwrap_err();
        assert!(matches!(err, QueryError::Validation(_)));
    }

    #[test]
    fn sql_injection_in_filter_property() {
        let json = r#"{
            "query_type": "traversal",
            "nodes": [{"id": "u", "entity": "User", "filters": {"foo; DROP TABLE--": "value"}}]
        }"#;
        let err = compile(json, &test_ontology()).unwrap_err();
        assert!(matches!(err, QueryError::Validation(_)));
    }

    #[test]
    fn valid_identifiers_accepted() {
        let json = r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "user_node", "entity": "User"},
                {"id": "_private", "entity": "Note"},
                {"id": "CamelCase", "entity": "Project"},
                {"id": "node123", "entity": "Group"}
            ]
        }"#;
        assert!(compile(json, &test_ontology()).is_ok());
    }
}

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
        Ontology::load_from_dir(&fixtures_dir).expect("Failed to load test ontology")
    }

    #[test]
    fn valid_column_in_order_by() {
        let json = r#"{
            "query_type": "traversal",
            "nodes": [{"id": "u", "entity": "User"}],
            "limit": 10,
            "order_by": {"node": "u", "property": "username", "direction": "ASC"}
        }"#;
        assert!(compile(json, &load_test_ontology()).is_ok());
    }

    #[test]
    fn invalid_column_in_order_by() {
        let json = r#"{
            "query_type": "traversal",
            "nodes": [{"id": "u", "entity": "User"}],
            "limit": 10,
            "order_by": {"node": "u", "property": "nonexistent_column", "direction": "ASC"}
        }"#;
        let err = compile(json, &load_test_ontology()).unwrap_err();
        assert!(err.to_string().contains("does not exist"));
    }

    #[test]
    fn valid_column_in_filter() {
        let json = r#"{
            "query_type": "traversal",
            "nodes": [{"id": "u", "entity": "User", "filters": {"username": "admin"}}],
            "limit": 10
        }"#;
        assert!(compile(json, &load_test_ontology()).is_ok());
    }

    #[test]
    fn invalid_column_in_filter() {
        let json = r#"{
            "query_type": "traversal",
            "nodes": [{"id": "u", "entity": "User", "filters": {"nonexistent_column": "value"}}],
            "limit": 10
        }"#;
        let err = compile(json, &load_test_ontology()).unwrap_err();
        assert!(err.to_string().contains("does not exist"));
    }

    #[test]
    fn valid_column_in_aggregation() {
        let json = r#"{
            "query_type": "aggregation",
            "nodes": [{"id": "p", "entity": "Project"}],
            "aggregations": [{"function": "count", "target": "p", "property": "name", "alias": "name_count"}],
            "limit": 10
        }"#;
        assert!(compile(json, &load_test_ontology()).is_ok());
    }

    #[test]
    fn invalid_column_in_aggregation() {
        let json = r#"{
            "query_type": "aggregation",
            "nodes": [{"id": "p", "entity": "Project"}],
            "aggregations": [{"function": "sum", "target": "p", "property": "invalid_property", "alias": "total"}],
            "limit": 10
        }"#;
        let err = compile(json, &load_test_ontology()).unwrap_err();
        assert!(err.to_string().contains("does not exist"));
    }

    #[test]
    fn invalid_entity_type_rejected() {
        let json = r#"{
            "query_type": "traversal",
            "nodes": [{"id": "n", "entity": "NonexistentType"}],
            "limit": 10
        }"#;
        let err = compile(json, &load_test_ontology()).unwrap_err();
        // Schema validation catches invalid entity types
        assert!(
            err.to_string().contains("NonexistentType")
                && err.to_string().contains("is not one of"),
            "expected validation error with valid options: {}",
            err
        );
    }

    #[test]
    fn full_pipeline() {
        let json = r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "n", "entity": "Note", "filters": {"confidential": true}},
                {"id": "u", "entity": "User"}
            ],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "n"}],
            "limit": 25,
            "order_by": {"node": "n", "property": "created_at", "direction": "DESC"}
        }"#;

        let result = compile(json, &load_test_ontology()).unwrap();
        assert!(result.sql.contains("SELECT"));
        assert!(result.sql.contains("INNER JOIN"));
        assert!(result.sql.contains("LIMIT 25"));
        assert!(result.sql.contains("ORDER BY"));
        assert!(result.sql.contains("DESC"));
    }
}
