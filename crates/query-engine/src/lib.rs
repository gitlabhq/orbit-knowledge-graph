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
// ! let result = compile(json, &ontology).unwrap();
// ! println!("SQL: {}", result.sql);
//! ```

pub mod ast;
pub mod error;
pub mod input;
pub mod validate;

use std::sync::OnceLock;

pub use ast::{Expr, JoinType, Node, Op, OrderExpr, Query, RecursiveCte, SelectExpr, TableRef};
pub use error::{QueryError, Result};
pub use input::{parse_input, Input, QueryType};
pub use ontology::{Ontology, OntologyError, EDGE_TABLE, NODE_RESERVED_COLUMNS};

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
        .map(|e| format!("{} at {}", e, e.instance_path()))
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
pub fn compile(json_input: &str, ontology: &Ontology) -> Result<()> {
    let value = validate_json(json_input)?;
    validate_ontology(&value, ontology)?;
    let input: Input = serde_json::from_value(value)?;
    validate::validate(&input, ontology)?;
    Ok(())
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
}
