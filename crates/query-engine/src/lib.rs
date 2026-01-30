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
//! # Example
//!
//! ```rust
//! use query_engine::{compile, SecurityContext};
//! use ontology::Ontology;
//!
//! let ontology = Ontology::new()
//!     .with_nodes(["User", "Project"])
//!     .with_edges(["MEMBER_OF"]);
//!
//! let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
//!
//! let json = r#"{
//!     "query_type": "search",
//!     "node": {"id": "u", "entity": "User"},
//!     "limit": 10
//! }"#;
//!
//! let result = compile(json, &ontology, &ctx).unwrap();
//! println!("SQL: {}", result.sql);
//! ```

pub mod ast;
pub mod codegen;
pub mod error;
pub mod input;
pub mod lower;
pub mod result_context;
pub mod r#return;
pub mod security;
pub mod validate;

use std::sync::OnceLock;

pub use ast::{Expr, JoinType, Node, Op, OrderExpr, Query, RecursiveCte, SelectExpr, TableRef};
pub use codegen::{codegen, ParameterizedQuery};
pub use error::{QueryError, Result};
pub use input::{parse_input, Input, QueryType};
pub use ontology::{Ontology, OntologyError, EDGE_TABLE, NODE_RESERVED_COLUMNS};
pub use r#return::enforce_return;
pub use result_context::{id_column, type_column, RedactionNode, ResultContext, PATH_COLUMN};
pub use security::{apply_security_context, SecurityContext};

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
pub fn compile(
    json_input: &str,
    ontology: &Ontology,
    ctx: &SecurityContext,
) -> Result<ParameterizedQuery> {
    let value = validate_json(json_input)?;
    validate_ontology(&value, ontology)?;
    let input: Input = serde_json::from_value(value)?;
    validate::validate(&input, ontology)?;
    let mut node = lower::lower(&input, ontology)?;
    let result_context = enforce_return(&mut node, &input)?;
    apply_security_context(&mut node, ctx)?;
    codegen(&node, result_context)
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

    fn test_ctx() -> SecurityContext {
        SecurityContext::new(1, vec!["1/".into()]).unwrap()
    }

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

    /// Compile JSON and return the AST without generating SQL.
    #[must_use = "the compiled AST should be used"]
    pub fn compile_to_ast(json_input: &str, ontology: &Ontology) -> Result<Node> {
        let value = validate_json(json_input)?;
        validate_ontology(&value, ontology)?;
        let input: Input = serde_json::from_value(value)?;
        validate::validate(&input, ontology)?;
        let node = lower::lower(&input, ontology)?;
        Ok(node)
    }

    #[test]
    fn compile_to_ast_works() {
        let json = r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User"},
            "limit": 10
        }"#;

        let Node::Query(q) = compile_to_ast(json, &test_ontology()).unwrap() else {
            panic!("expected Query");
        };
        assert_eq!(q.limit, Some(10));
        assert_eq!(q.select.len(), 1);
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

        let result = compile(json, &test_ontology(), &test_ctx()).unwrap();

        assert!(result.sql.contains("SELECT"));
        assert!(result.sql.contains("gl_user AS u"));
        assert!(result.sql.contains("INNER JOIN gl_edges AS e0 ON"));
        assert!(
            result.sql.contains("u.id = e0.source_id"),
            "expected source_id column: {}",
            result.sql
        );
        assert!(result.sql.contains("INNER JOIN gl_note AS n ON"));
        assert!(
            result
                .sql
                .contains("e0.relationship_kind = {type_e0:String}"),
            "expected relationship_kind: {}",
            result.sql
        );
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

        let result = compile(json, &test_ontology(), &test_ctx()).unwrap();
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

        let result = compile(json, &test_ontology(), &test_ctx()).unwrap();
        assert!(result.sql.contains("WITH RECURSIVE"));
        assert!(result.sql.contains("path_cte"));
        assert!(result.sql.contains("UNION ALL"));
    }

    #[test]
    fn filter_operators() {
        let json = r#"{
            "query_type": "search",
            "node": {
                "id": "u",
                "entity": "User",
                "filters": {
                    "created_at": {"op": "gte", "value": "2024-01-01"},
                    "state": {"op": "in", "value": ["active", "blocked"]},
                    "username": {"op": "contains", "value": "admin"}
                }
            },
            "limit": 30
        }"#;

        let result = compile(json, &test_ontology(), &test_ctx()).unwrap();
        assert!(result.sql.contains("WHERE"));
        assert!(result.sql.contains(">="));
        assert!(result.sql.contains("IN"));
        assert!(result.sql.contains("LIKE"));
    }

    #[test]
    fn invalid_json_rejected() {
        assert!(compile("not valid json", &test_ontology(), &test_ctx()).is_err());
    }

    #[test]
    fn missing_required_fields_rejected() {
        let result = compile(
            r#"{"query_type": "traversal"}"#,
            &test_ontology(),
            &test_ctx(),
        );
        assert!(result.is_err());
    }

    // SQL injection prevention tests
    #[test]
    fn sql_injection_in_node_id() {
        let json = r#"{"query_type": "traversal", "nodes": [{"id": "n; DROP TABLE users; --"}]}"#;
        let err = compile(json, &test_ontology(), &test_ctx()).unwrap_err();
        assert!(matches!(err, QueryError::Validation(_)));
    }

    #[test]
    fn sql_injection_in_relationship() {
        let json = r#"{
            "query_type": "traversal",
            "nodes": [{"id": "a"}, {"id": "b"}],
            "relationships": [{"type": "REL", "from": "a' OR '1'='1", "to": "b"}]
        }"#;
        let err = compile(json, &test_ontology(), &test_ctx()).unwrap_err();
        assert!(matches!(err, QueryError::Validation(_)));
    }

    #[test]
    fn empty_node_id_rejected() {
        let json = r#"{"query_type": "traversal", "nodes": [{"id": ""}]}"#;
        assert!(compile(json, &test_ontology(), &test_ctx()).is_err());
    }

    #[test]
    fn id_starting_with_number_rejected() {
        let json = r#"{"query_type": "traversal", "nodes": [{"id": "123abc"}]}"#;
        let err = compile(json, &test_ontology(), &test_ctx()).unwrap_err();
        assert!(matches!(err, QueryError::Validation(_)));
    }

    #[test]
    fn sql_injection_in_filter_property() {
        let json = r#"{
            "query_type": "traversal",
            "nodes": [{"id": "u", "entity": "User", "filters": {"foo; DROP TABLE--": "value"}}]
        }"#;
        let err = compile(json, &test_ontology(), &test_ctx()).unwrap_err();
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
        assert!(compile(json, &test_ontology(), &test_ctx()).is_ok());
    }
}

#[cfg(test)]
mod ontology_integration_tests {
    use super::*;
    use ontology::Ontology;

    fn test_ctx() -> SecurityContext {
        SecurityContext::new(1, vec!["1/".into()]).unwrap()
    }

    fn load_test_ontology() -> Ontology {
        Ontology::load_embedded().expect("Failed to load test ontology")
    }

    #[test]
    fn valid_column_in_order_by() {
        let json = r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User"},
            "limit": 10,
            "order_by": {"node": "u", "property": "username", "direction": "ASC"}
        }"#;
        assert!(compile(json, &load_test_ontology(), &test_ctx()).is_ok());
    }

    #[test]
    fn invalid_column_in_order_by() {
        let json = r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User"},
            "limit": 10,
            "order_by": {"node": "u", "property": "nonexistent_column", "direction": "ASC"}
        }"#;
        let err = compile(json, &load_test_ontology(), &test_ctx()).unwrap_err();
        assert!(err.to_string().contains("does not exist"));
    }

    #[test]
    fn valid_column_in_filter() {
        let json = r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "filters": {"username": "admin"}},
            "limit": 10
        }"#;
        assert!(compile(json, &load_test_ontology(), &test_ctx()).is_ok());
    }

    #[test]
    fn invalid_column_in_filter() {
        let json = r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "filters": {"nonexistent_column": "value"}},
            "limit": 10
        }"#;
        let err = compile(json, &load_test_ontology(), &test_ctx()).unwrap_err();
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
        assert!(compile(json, &load_test_ontology(), &test_ctx()).is_ok());
    }

    #[test]
    fn invalid_column_in_aggregation() {
        let json = r#"{
            "query_type": "aggregation",
            "nodes": [{"id": "p", "entity": "Project"}],
            "aggregations": [{"function": "sum", "target": "p", "property": "invalid_property", "alias": "total"}],
            "limit": 10
        }"#;
        let err = compile(json, &load_test_ontology(), &test_ctx()).unwrap_err();
        assert!(err.to_string().contains("does not exist"));
    }

    #[test]
    fn invalid_entity_type_rejected() {
        let json = r#"{
            "query_type": "search",
            "node": {"id": "n", "entity": "NonexistentType"},
            "limit": 10
        }"#;
        let err = compile(json, &load_test_ontology(), &test_ctx()).unwrap_err();
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

        let result = compile(json, &load_test_ontology(), &test_ctx()).unwrap();
        println!("Parameterized: {}", result.sql);
        println!("Params: {:?}", result.params);
        println!("Inlined: {result}");
        assert!(result.sql.contains("SELECT"));
        assert!(result.sql.contains("INNER JOIN"));
        assert!(result.sql.contains("LIMIT 25"));
        assert!(result.sql.contains("ORDER BY"));
        assert!(result.sql.contains("DESC"));
    }

    #[test]
    fn basic_search_query() {
        let json = r#"{
            "query_type": "search",
            "node": {
                "id": "u",
                "entity": "User",
                "filters": {
                    "username": {"op": "eq", "value": "admin"}
                }
            },
            "limit": 10
        }"#;

        let result = compile(json, &load_test_ontology(), &test_ctx()).unwrap();
        println!("Search SQL: {}", result.sql);
        println!("Params: {:?}", result.params);
        println!("Inlined: {result}");

        assert!(result.sql.contains("SELECT"));
        assert!(result.sql.contains("FROM"));
        assert!(result.sql.contains("WHERE"));
        assert!(result.sql.contains("username"));
        assert!(result.sql.contains("LIMIT 10"));
        assert!(
            !result.sql.contains("JOIN"),
            "search queries should not have joins"
        );
    }

    #[test]
    fn complex_search_query() {
        let json = r#"{
            "query_type": "search",
            "node": {
                "id": "u",
                "entity": "User",
                "filters": {
                    "username": {"op": "starts_with", "value": "admin"},
                    "state": {"op": "in", "value": ["active", "blocked"]},
                    "created_at": {"op": "gte", "value": "2024-01-01"}
                }
            },
            "limit": 50,
            "order_by": {"node": "u", "property": "created_at", "direction": "DESC"}
        }"#;

        let result = compile(json, &load_test_ontology(), &test_ctx()).unwrap();
        println!("Complex search SQL: {}", result.sql);
        println!("Params: {:?}", result.params);
        println!("Inlined: {result}");

        assert!(result.sql.contains("SELECT"));
        assert!(result.sql.contains("WHERE"));
        assert!(result.sql.contains("username"));
        assert!(result.sql.contains("state"));
        assert!(result.sql.contains("created_at"));
        assert!(result.sql.contains("ORDER BY"));
        assert!(result.sql.contains("DESC"));
        assert!(result.sql.contains("LIMIT 50"));
        assert!(
            !result.sql.contains("JOIN"),
            "search queries should not have joins"
        );

        // Verify multiple filters are combined with AND
        assert!(result.sql.contains("AND"));
    }

    #[test]
    fn search_with_specific_columns() {
        let json = r#"{
            "query_type": "search",
            "node": {
                "id": "u",
                "entity": "User",
                "columns": ["username", "state"]
            },
            "limit": 10
        }"#;

        let result = compile(json, &load_test_ontology(), &test_ctx()).unwrap();
        println!("Search with columns SQL: {}", result.sql);

        // Should have the selected columns
        assert!(result.sql.contains("u_username"));
        assert!(result.sql.contains("u_state"));
        // Should always have mandatory columns for redaction
        assert!(result.sql.contains("_gkg_u_id"));
        assert!(result.sql.contains("_gkg_u_type"));
    }

    #[test]
    fn search_with_wildcard_columns() {
        let json = r#"{
            "query_type": "search",
            "node": {
                "id": "u",
                "entity": "User",
                "columns": "*"
            },
            "limit": 10
        }"#;

        let result = compile(json, &load_test_ontology(), &test_ctx()).unwrap();
        println!("Search with wildcard SQL: {}", result.sql);

        // Should have all columns from the ontology
        assert!(result.sql.contains("u_id"));
        assert!(result.sql.contains("u_username"));
        // Should always have mandatory columns for redaction
        assert!(result.sql.contains("_gkg_u_id"));
        assert!(result.sql.contains("_gkg_u_type"));
    }

    #[test]
    fn traversal_with_columns() {
        let json = r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "columns": ["username"]},
                {"id": "p", "entity": "Project", "columns": ["name"]}
            ],
            "relationships": [{"type": "CONTAINS", "from": "u", "to": "p"}],
            "limit": 10
        }"#;

        let result = compile(json, &load_test_ontology(), &test_ctx()).unwrap();
        println!("Traversal with columns SQL: {}", result.sql);

        // Should have the selected columns for both nodes
        assert!(result.sql.contains("u_username"));
        assert!(result.sql.contains("p_name"));
        // Should always have mandatory columns for redaction
        assert!(result.sql.contains("_gkg_u_id"));
        assert!(result.sql.contains("_gkg_u_type"));
        assert!(result.sql.contains("_gkg_p_id"));
        assert!(result.sql.contains("_gkg_p_type"));
    }

    #[test]
    fn aggregation_includes_mandatory_columns_for_group_by_node() {
        let json = r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "u", "entity": "User", "columns": ["username"]},
                {"id": "mr", "entity": "MergeRequest"}
            ],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "mr"}],
            "aggregations": [{"function": "count", "target": "mr", "group_by": "u", "alias": "mr_count"}],
            "limit": 10
        }"#;

        let result = compile(json, &load_test_ontology(), &test_ctx()).unwrap();
        println!("Aggregation SQL: {}", result.sql);

        // Aggregation queries only add mandatory columns for group_by nodes (u)
        // The target node (mr) is aggregated so doesn't get individual row columns
        assert!(result.sql.contains("_gkg_u_id"));
        assert!(result.sql.contains("_gkg_u_type"));
        // MR is aggregated, not returned as individual rows
        assert!(!result.sql.contains("_gkg_mr_id"));
        assert!(!result.sql.contains("_gkg_mr_type"));
        // Should have the aggregation
        assert!(result.sql.contains("COUNT"));
        assert!(result.sql.contains("GROUP BY"));
    }

    #[test]
    fn path_finding_uses_gkg_path_not_node_columns() {
        let json = r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "entity": "Project", "node_ids": [100], "columns": ["name"]},
                {"id": "end", "entity": "Project", "node_ids": [200], "columns": ["name"]}
            ],
            "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3}
        }"#;

        let result = compile(json, &load_test_ontology(), &test_ctx()).unwrap();
        println!("Path finding SQL: {}", result.sql);

        // Path finding queries use _gkg_path column (Array of tuples)
        // which contains all node IDs and types along the path
        assert!(result.sql.contains("_gkg_path"));
        // The columns selection on nodes is ignored for path finding
        // because the result is a path, not individual node rows
        assert!(result.result_context.query_type == Some(QueryType::PathFinding));
    }

    #[test]
    fn result_context_populated() {
        let json = r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User"},
                {"id": "p", "entity": "Project"}
            ],
            "relationships": [{"type": "CONTAINS", "from": "u", "to": "p"}],
            "limit": 10
        }"#;

        let result = compile(json, &load_test_ontology(), &test_ctx()).unwrap();

        assert_eq!(result.result_context.len(), 2);

        let user = result.result_context.get("u").unwrap();
        assert_eq!(user.entity_type, "User");
        assert_eq!(user.id_column, "_gkg_u_id");
        assert_eq!(user.type_column, "_gkg_u_type");

        let project = result.result_context.get("p").unwrap();
        assert_eq!(project.entity_type, "Project");
        assert_eq!(project.id_column, "_gkg_p_id");
        assert_eq!(project.type_column, "_gkg_p_type");

        assert!(result.sql.contains("_gkg_u_id"));
        assert!(result.sql.contains("_gkg_u_type"));
        assert!(result.sql.contains("_gkg_p_id"));
        assert!(result.sql.contains("_gkg_p_type"));
    }

    #[test]
    fn multi_hop_traversal_generates_union_subquery() {
        let json = r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User"},
                {"id": "p", "entity": "Project"}
            ],
            "relationships": [{
                "type": "MEMBER_OF",
                "from": "u",
                "to": "p",
                "min_hops": 1,
                "max_hops": 3
            }],
            "limit": 25
        }"#;

        let result = compile(json, &load_test_ontology(), &test_ctx()).unwrap();
        println!("Multi-hop SQL: {}", result.sql);

        // Should generate a union subquery with multiple arms (one per hop count)
        assert!(
            result.sql.contains("UNION ALL"),
            "expected UNION ALL for unrolled multi-hop: {}",
            result.sql
        );
        // Should have the hop_e0 union subquery aliased
        assert!(
            result.sql.contains("AS hop_e0"),
            "expected hop_e0 subquery alias: {}",
            result.sql
        );
        // Should have depth column for filtering
        assert!(
            result.sql.contains("AS depth"),
            "expected depth column: {}",
            result.sql
        );
    }

    #[test]
    fn multi_hop_with_min_hops_filter() {
        let json = r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User"},
                {"id": "p", "entity": "Project"}
            ],
            "relationships": [{
                "type": "MEMBER_OF",
                "from": "u",
                "to": "p",
                "min_hops": 2,
                "max_hops": 3
            }],
            "limit": 10
        }"#;

        let result = compile(json, &load_test_ontology(), &test_ctx()).unwrap();
        println!("Min-hops SQL: {}", result.sql);

        // Should have depth >= 2 filter
        assert!(
            result.sql.contains("hop_e0.depth"),
            "expected depth reference: {}",
            result.sql
        );
    }

    #[test]
    fn single_hop_does_not_generate_cte() {
        let json = r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User"},
                {"id": "n", "entity": "Note"}
            ],
            "relationships": [{
                "type": "AUTHORED",
                "from": "u",
                "to": "n",
                "min_hops": 1,
                "max_hops": 1
            }],
            "limit": 25
        }"#;

        let result = compile(json, &load_test_ontology(), &test_ctx()).unwrap();
        println!("Single-hop SQL: {}", result.sql);

        // Should NOT generate a recursive CTE for single hop
        assert!(
            !result.sql.contains("WITH RECURSIVE"),
            "single hop should not generate CTE: {}",
            result.sql
        );
    }

    #[test]
    fn multi_hop_aggregation() {
        let json = r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "u", "entity": "User"},
                {"id": "p", "entity": "Project"}
            ],
            "relationships": [{
                "type": "MEMBER_OF",
                "from": "u",
                "to": "p",
                "min_hops": 1,
                "max_hops": 2
            }],
            "aggregations": [{"function": "count", "target": "p", "group_by": "u", "alias": "project_count"}],
            "limit": 10
        }"#;

        let result = compile(json, &load_test_ontology(), &test_ctx()).unwrap();
        println!("Multi-hop aggregation SQL: {}", result.sql);

        // Should generate union subquery for multi-hop in aggregation queries
        assert!(
            result.sql.contains("UNION ALL"),
            "aggregation should support multi-hop with union: {}",
            result.sql
        );
        assert!(
            result.sql.contains("AS hop_e0"),
            "expected hop_e0 subquery alias: {}",
            result.sql
        );
        assert!(
            result.sql.contains("COUNT"),
            "expected COUNT in query: {}",
            result.sql
        );
    }
}
