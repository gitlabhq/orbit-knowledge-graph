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
//! use ontology::{Ontology, DataType};
//!
//! let ontology = Ontology::new()
//!     .with_nodes(["User", "Project"])
//!     .with_edges(["MEMBER_OF"])
//!     .with_fields("User", [("username", DataType::String)]);
//!
//! let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
//!
//! let json = r#"{
//!     "query_type": "search",
//!     "node": {"id": "u", "entity": "User", "columns": ["username"]},
//!     "limit": 10
//! }"#;
//!
//! let result = compile(json, &ontology, &ctx).unwrap();
//! println!("SQL: {}", result.structural.sql);
//! ```

pub mod ast;
pub mod codegen;
pub mod constants;
pub mod enforce;
pub mod error;
pub mod input;
pub mod lower;
pub mod normalize;
pub mod security;
pub mod validate;

pub use ast::{Expr, JoinType, Node, Op, OrderExpr, Query, SelectExpr, TableRef};
pub use codegen::{CompiledQuery, HydrationPlan, HydrationTemplate, ParameterizedQuery, codegen};
pub use constants::{
    NEIGHBOR_ID_COLUMN, NEIGHBOR_TYPE_COLUMN, PATH_COLUMN, RELATIONSHIP_TYPE_COLUMN,
};
pub use enforce::{RedactionNode, ResultContext, enforce_return};
pub use error::{QueryError, Result};
pub use input::EntityAuthConfig;
pub use input::{Input, QueryType, parse_input};
pub use lower::{lower, lower_with_columns};
pub use normalize::{build_entity_auth, normalize};
pub use ontology::{EDGE_TABLE, NODE_RESERVED_COLUMNS, Ontology, OntologyError};
pub use security::{SecurityContext, apply_security_context};
pub use validate::Validator;

// ─────────────────────────────────────────────────────────────────────────────
// Public API
// ─────────────────────────────────────────────────────────────────────────────

/// Validate and normalize a JSON query string into a typed `Input`.
fn validated_input(json_input: &str, ontology: &Ontology) -> Result<Input> {
    let v = Validator::new(ontology);
    let value = v.check_json(json_input)?;
    v.check_ontology(&value)?;
    let input: Input = serde_json::from_value(value)?;
    v.check_references(&input)?;
    Ok(normalize(input, ontology))
}

/// Compile a JSON query into a structural query and hydration plan.
///
/// Returns a [`CompiledQuery`] containing the structural SQL (IDs/types only for
/// traversal/search) plus a [`HydrationPlan`] describing how to fetch full entity
/// properties after authorization and redaction.
#[must_use = "the compiled query should be used"]
pub fn compile(
    json_input: &str,
    ontology: &Ontology,
    ctx: &SecurityContext,
) -> Result<CompiledQuery> {
    let input = validated_input(json_input, ontology)?;

    let mut node = lower(&input)?;
    let result_context = enforce_return(&mut node, &input)?;
    apply_security_context(&mut node, ctx)?;
    let structural = codegen(&node, result_context)?;

    let hydration = build_hydration_plan(&input, ontology, ctx)?;

    Ok(CompiledQuery {
        structural,
        hydration,
    })
}

/// Build the hydration plan based on query type.
///
/// - Aggregation: no hydration (results are aggregate values, not entity rows).
/// - Traversal/Search: static hydration — entity types are known at compile time,
///   so we pre-compile one search query template per entity type.
/// - PathFinding/Neighbors: dynamic hydration — entity types are discovered at
///   runtime from edge data, so the server builds search queries on the fly.
fn build_hydration_plan(
    input: &Input,
    ontology: &Ontology,
    ctx: &SecurityContext,
) -> Result<HydrationPlan> {
    match input.query_type {
        QueryType::Aggregation => Ok(HydrationPlan::None),
        QueryType::PathFinding | QueryType::Neighbors => Ok(HydrationPlan::Dynamic),
        QueryType::Traversal | QueryType::Search => {
            let mut templates = Vec::new();

            for node in &input.nodes {
                let Some(entity) = &node.entity else {
                    continue;
                };

                let hydration_json = serde_json::json!({
                    "query_type": "search",
                    "node": {
                        "id": "n",
                        "entity": entity,
                        "columns": "*"
                    },
                    "limit": 1000
                })
                .to_string();

                let hydration_query = compile_with_columns(&hydration_json, ontology, ctx)?;

                templates.push(HydrationTemplate {
                    entity_type: entity.clone(),
                    node_alias: node.id.clone(),
                    query: hydration_query,
                });
            }

            Ok(HydrationPlan::Static(templates))
        }
    }
}

/// Compile a JSON query into a ParameterizedQuery with property columns in SELECT.
///
/// Unlike [`compile`] which produces a slim structural query (IDs/types only),
/// this returns a full query with all requested columns. Used for hydration
/// queries that fetch entity properties after authorization.
pub fn compile_with_columns(
    json_input: &str,
    ontology: &Ontology,
    ctx: &SecurityContext,
) -> Result<ParameterizedQuery> {
    let input = validated_input(json_input, ontology)?;

    let mut node = lower_with_columns(&input)?;
    let result_context = enforce_return(&mut node, &input)?;
    apply_security_context(&mut node, ctx)?;
    codegen(&node, result_context)
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(irrefutable_let_patterns)]
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
            .with_fields("Group", [("name", DataType::String)])
    }

    /// Compile JSON and return the AST without generating SQL.
    #[must_use = "the compiled AST should be used"]
    pub fn compile_to_ast(json_input: &str, ontology: &Ontology) -> Result<Node> {
        let v = validate::Validator::new(ontology);
        let value = v.check_json(json_input)?;
        v.check_ontology(&value)?;
        let input: Input = serde_json::from_value(value)?;
        v.check_references(&input)?;
        let input = normalize::normalize(input, ontology);
        let node = lower::lower(&input)?;
        Ok(node)
    }

    #[test]
    fn compile_to_ast_works() {
        let json = r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username"]},
            "limit": 10
        }"#;

        let node = compile_to_ast(json, &test_ontology()).unwrap();
        let Node::Query(ref q) = node else {
            panic!("expected Query");
        };
        assert_eq!(q.limit, Some(10));
        // Structural query starts with empty SELECT from lower()
        assert!(q.select.is_empty());
    }

    #[test]
    fn traversal_query() {
        let json = r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "n", "entity": "Note", "columns": ["confidential"], "filters": {"confidential": true}},
                {"id": "u", "entity": "User", "columns": ["username"]}
            ],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "n"}],
            "limit": 25,
            "order_by": {"node": "n", "property": "created_at", "direction": "DESC"}
        }"#;

        let result = compile(json, &test_ontology(), &test_ctx()).unwrap();

        assert!(result.structural.sql.contains("SELECT"));
        assert!(result.structural.sql.contains("gl_user AS u"));
        assert!(
            result
                .structural
                .sql
                .contains("INNER JOIN gl_edge AS e0 ON")
        );
        assert!(
            result.structural.sql.contains("u.id = e0.source_id"),
            "expected source_id column: {}",
            result.structural.sql
        );
        assert!(result.structural.sql.contains("INNER JOIN gl_note AS n ON"));
        assert!(
            result
                .structural
                .sql
                .contains("e0.relationship_kind = {type_e0:String}"),
            "expected relationship_kind: {}",
            result.structural.sql
        );
        assert!(
            !result.structural.sql.contains("n.label"),
            "node should not have type filter: {}",
            result.structural.sql
        );
        assert!(result.structural.sql.contains("LIMIT 25"));
        assert_eq!(
            result.structural.params.get("type_e0"),
            Some(&serde_json::json!("AUTHORED"))
        );
    }

    #[test]
    fn bool_filter_value_is_preserved() {
        let json = r#"{
            "query_type": "search",
            "node": {
                "id": "n",
                "entity": "Note",
                "columns": ["confidential"],
                "filters": {
                    "confidential": true
                }
            },
            "limit": 5
        }"#;

        let result = compile(json, &test_ontology(), &test_ctx()).unwrap();
        assert!(
            result
                .structural
                .params
                .values()
                .any(|v| v == &serde_json::Value::Bool(true)),
            "expected boolean filter to remain true in params: {:?}",
            result.structural.params
        );
    }

    #[test]
    fn aggregation_query() {
        let json = r#"{
            "query_type": "aggregation",
            "nodes": [{"id": "n", "entity": "Note", "columns": ["confidential"]}, {"id": "u", "entity": "User", "columns": ["username"]}],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "n"}],
            "aggregations": [{"function": "count", "target": "n", "group_by": "u", "alias": "note_count"}],
            "limit": 10
        }"#;

        let result = compile(json, &test_ontology(), &test_ctx()).unwrap();
        assert!(result.structural.sql.contains("COUNT"));
        assert!(result.structural.sql.contains("GROUP BY"));
    }

    #[test]
    fn path_finding_query() {
        let json = r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "entity": "Project", "columns": ["name"], "node_ids": [100]},
                {"id": "end", "entity": "Project", "columns": ["name"], "node_ids": [200]}
            ],
            "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3}
        }"#;

        let result = compile(json, &test_ontology(), &test_ctx()).unwrap();

        // Recursive CTE named "paths"
        assert!(result.structural.sql.contains("WITH RECURSIVE paths AS"));
        assert!(result.structural.sql.contains("UNION ALL"));

        // Verify recursive structure references "paths"
        assert!(
            result.structural.sql.contains("FROM paths"),
            "recursive branches should reference paths CTE"
        );

        // Verify cycle detection and early termination
        assert!(
            result.structural.sql.matches("NOT has").count() >= 2,
            "should have cycle detection and early termination"
        );

        // Verify path construction with full materialization
        assert!(
            result.structural.sql.contains("arrayConcat"),
            "paths should be extended"
        );
        assert!(
            result.structural.sql.contains("tuple"),
            "path nodes should be typed tuples"
        );
        // Verify path limit to prevent memory explosion
        assert!(
            result.structural.sql.contains("LIMIT 1000"),
            "should limit paths to prevent memory issues"
        );
    }

    #[test]
    fn path_finding_depth_control() {
        // Verify max_depth is used in the recursive CTE
        let shallow = r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "entity": "Project", "columns": ["name"], "node_ids": [1]},
                {"id": "end", "entity": "Project", "columns": ["name"], "node_ids": [2]}
            ],
            "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 1}
        }"#;

        let deep = r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "entity": "Project", "columns": ["name"], "node_ids": [1]},
                {"id": "end", "entity": "Project", "columns": ["name"], "node_ids": [2]}
            ],
            "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3}
        }"#;

        let shallow_result = compile(shallow, &test_ontology(), &test_ctx()).unwrap();
        let deep_result = compile(deep, &test_ontology(), &test_ctx()).unwrap();

        // Both use recursive CTE
        assert!(
            shallow_result
                .structural
                .sql
                .contains("WITH RECURSIVE paths AS")
        );
        assert!(
            deep_result
                .structural
                .sql
                .contains("WITH RECURSIVE paths AS")
        );

        // Depth limit is in WHERE clause (p.depth < N)
        assert!(shallow_result.structural.sql.contains("p.depth < {p"));
        assert!(deep_result.structural.sql.contains("p.depth < {p"));
    }

    #[test]
    fn neighbors_query() {
        let json = r#"{
            "query_type": "neighbors",
            "node": {"id": "u", "entity": "User", "columns": ["username"], "node_ids": [100]},
            "neighbors": {"node": "u", "direction": "both"}
        }"#;

        let result = compile(json, &test_ontology(), &test_ctx()).unwrap();
        assert!(result.structural.sql.contains("SELECT"));
        assert!(result.structural.sql.contains("_gkg_neighbor_id"));
        assert!(result.structural.sql.contains("_gkg_neighbor_type"));
        assert!(result.structural.sql.contains("_gkg_relationship_type"));
        assert!(result.structural.sql.contains("INNER JOIN"));
    }

    #[test]
    fn filter_operators() {
        let json = r#"{
            "query_type": "search",
            "node": {
                "id": "u",
                "entity": "User",
                "columns": ["username", "state", "created_at"],
                "filters": {
                    "created_at": {"op": "gte", "value": "2024-01-01"},
                    "state": {"op": "in", "value": ["active", "blocked"]},
                    "username": {"op": "contains", "value": "admin"}
                }
            },
            "limit": 30
        }"#;

        let result = compile(json, &test_ontology(), &test_ctx()).unwrap();
        assert!(result.structural.sql.contains("WHERE"));
        assert!(result.structural.sql.contains(">="));
        assert!(result.structural.sql.contains("IN"));
        assert!(result.structural.sql.contains("LIKE"));
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
                {"id": "user_node", "entity": "User", "columns": ["username"]},
                {"id": "_private", "entity": "Note", "columns": ["confidential"]},
                {"id": "CamelCase", "entity": "Project", "columns": ["name"]},
                {"id": "node123", "entity": "Group", "columns": ["name"]}
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
            "node": {"id": "u", "entity": "User", "columns": ["username"]},
            "limit": 10,
            "order_by": {"node": "u", "property": "username", "direction": "ASC"}
        }"#;
        assert!(compile(json, &load_test_ontology(), &test_ctx()).is_ok());
    }

    #[test]
    fn invalid_column_in_order_by() {
        let json = r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username"]},
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
            "node": {"id": "u", "entity": "User", "columns": ["username"], "filters": {"username": "admin"}},
            "limit": 10
        }"#;
        assert!(compile(json, &load_test_ontology(), &test_ctx()).is_ok());
    }

    #[test]
    fn invalid_column_in_filter() {
        let json = r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username"], "filters": {"nonexistent_column": "value"}},
            "limit": 10
        }"#;
        let err = compile(json, &load_test_ontology(), &test_ctx()).unwrap_err();
        assert!(
            err.to_string().contains("nonexistent_column"),
            "expected error mentioning invalid column name, got: {err}"
        );
    }

    #[test]
    fn valid_column_in_aggregation() {
        let json = r#"{
            "query_type": "aggregation",
            "nodes": [{"id": "p", "entity": "Project", "columns": ["name"]}],
            "aggregations": [{"function": "count", "target": "p", "property": "name", "alias": "name_count"}],
            "limit": 10
        }"#;
        assert!(compile(json, &load_test_ontology(), &test_ctx()).is_ok());
    }

    #[test]
    fn invalid_column_in_aggregation() {
        let json = r#"{
            "query_type": "aggregation",
            "nodes": [{"id": "p", "entity": "Project", "columns": ["name"]}],
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
            "node": {"id": "n", "entity": "NonexistentType", "columns": ["name"]},
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
                {"id": "n", "entity": "Note", "columns": ["confidential"], "filters": {"confidential": true}},
                {"id": "u", "entity": "User", "columns": ["username"]}
            ],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "n"}],
            "limit": 25,
            "order_by": {"node": "n", "property": "created_at", "direction": "DESC"}
        }"#;

        let result = compile(json, &load_test_ontology(), &test_ctx()).unwrap();
        println!("Parameterized: {}", result.structural.sql);
        println!("Params: {:?}", result.structural.params);
        println!("Inlined: {}", result.structural);
        assert!(result.structural.sql.contains("SELECT"));
        assert!(result.structural.sql.contains("INNER JOIN"));
        assert!(result.structural.sql.contains("LIMIT 25"));
        assert!(result.structural.sql.contains("ORDER BY"));
        assert!(result.structural.sql.contains("DESC"));
    }

    #[test]
    fn basic_search_query() {
        let json = r#"{
            "query_type": "search",
            "node": {
                "id": "u",
                "entity": "User",
                "columns": ["username"],
                "filters": {
                    "username": {"op": "eq", "value": "admin"}
                }
            },
            "limit": 10
        }"#;

        let result = compile(json, &load_test_ontology(), &test_ctx()).unwrap();
        println!("Search SQL: {}", result.structural.sql);
        println!("Params: {:?}", result.structural.params);
        println!("Inlined: {}", result.structural);

        assert!(result.structural.sql.contains("SELECT"));
        assert!(result.structural.sql.contains("FROM"));
        assert!(result.structural.sql.contains("WHERE"));
        assert!(result.structural.sql.contains("username"));
        assert!(result.structural.sql.contains("LIMIT 10"));
        assert!(
            !result.structural.sql.contains("JOIN"),
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
                "columns": ["username", "state", "created_at"],
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
        println!("Complex search SQL: {}", result.structural.sql);
        println!("Params: {:?}", result.structural.params);
        println!("Inlined: {}", result.structural);

        assert!(result.structural.sql.contains("SELECT"));
        assert!(result.structural.sql.contains("WHERE"));
        assert!(result.structural.sql.contains("username"));
        assert!(result.structural.sql.contains("state"));
        assert!(result.structural.sql.contains("created_at"));
        assert!(result.structural.sql.contains("ORDER BY"));
        assert!(result.structural.sql.contains("DESC"));
        assert!(result.structural.sql.contains("LIMIT 50"));
        assert!(
            !result.structural.sql.contains("JOIN"),
            "search queries should not have joins"
        );

        // Verify multiple filters are combined with AND
        assert!(result.structural.sql.contains("AND"));
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
        println!("Search with columns SQL: {}", result.structural.sql);

        // Structural query has only _gkg_* columns; property columns are in hydration
        assert!(result.structural.sql.contains("_gkg_u_id"));
        assert!(result.structural.sql.contains("_gkg_u_type"));
        assert!(
            !result.structural.sql.contains("u_username"),
            "property columns should be in hydration, not structural"
        );

        // Hydration plan should have a template for User with wildcard columns
        let HydrationPlan::Static(templates) = &result.hydration else {
            panic!("expected Static hydration plan");
        };
        assert_eq!(templates.len(), 1);
        assert_eq!(templates[0].entity_type, "User");
        assert!(templates[0].query.sql.contains("username"));
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
        println!("Search with wildcard SQL: {}", result.structural.sql);

        // Structural query has only _gkg_* columns
        assert!(result.structural.sql.contains("_gkg_u_id"));
        assert!(result.structural.sql.contains("_gkg_u_type"));

        // Hydration plan should have a template for User with all columns
        let HydrationPlan::Static(templates) = &result.hydration else {
            panic!("expected Static hydration plan");
        };
        assert_eq!(templates.len(), 1);
        assert_eq!(templates[0].entity_type, "User");
        assert!(templates[0].query.sql.contains("username"));
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
        println!("Traversal with columns SQL: {}", result.structural.sql);

        // Structural query has only _gkg_* columns
        assert!(result.structural.sql.contains("_gkg_u_id"));
        assert!(result.structural.sql.contains("_gkg_u_type"));
        assert!(result.structural.sql.contains("_gkg_p_id"));
        assert!(result.structural.sql.contains("_gkg_p_type"));
        assert!(
            !result.structural.sql.contains("u_username"),
            "property columns should be in hydration, not structural"
        );

        // Hydration plan should have templates for both nodes
        let HydrationPlan::Static(templates) = &result.hydration else {
            panic!("expected Static hydration plan");
        };
        assert_eq!(templates.len(), 2);
        let entity_types: Vec<_> = templates.iter().map(|t| t.entity_type.as_str()).collect();
        assert!(entity_types.contains(&"User"));
        assert!(entity_types.contains(&"Project"));
    }

    #[test]
    fn aggregation_includes_mandatory_columns_for_group_by_node() {
        let json = r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "u", "entity": "User", "columns": ["username"]},
                {"id": "mr", "entity": "MergeRequest", "columns": ["title"]}
            ],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "mr"}],
            "aggregations": [{"function": "count", "target": "mr", "group_by": "u", "alias": "mr_count"}],
            "limit": 10
        }"#;

        let result = compile(json, &load_test_ontology(), &test_ctx()).unwrap();
        println!("Aggregation SQL: {}", result.structural.sql);

        // Aggregation queries only add mandatory columns for group_by nodes (u)
        // The target node (mr) is aggregated so doesn't get individual row columns
        assert!(result.structural.sql.contains("_gkg_u_id"));
        assert!(result.structural.sql.contains("_gkg_u_type"));
        // MR is aggregated, not returned as individual rows
        assert!(!result.structural.sql.contains("_gkg_mr_id"));
        assert!(!result.structural.sql.contains("_gkg_mr_type"));
        // Should have the aggregation
        assert!(result.structural.sql.contains("COUNT"));
        assert!(result.structural.sql.contains("GROUP BY"));
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
        println!("Path finding SQL: {}", result.structural.sql);

        // Path finding queries use _gkg_path column (Array of tuples)
        // which contains all node IDs and types along the path
        assert!(result.structural.sql.contains("_gkg_path"));
        // The columns selection on nodes is ignored for path finding
        // because the result is a path, not individual node rows
        assert!(result.structural.result_context.query_type == Some(QueryType::PathFinding));
    }

    #[test]
    fn result_context_populated() {
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

        assert_eq!(result.structural.result_context.len(), 2);

        let user = result.structural.result_context.get("u").unwrap();
        assert_eq!(user.entity_type, "User");
        assert_eq!(user.id_column, "_gkg_u_id");
        assert_eq!(user.type_column, "_gkg_u_type");

        let project = result.structural.result_context.get("p").unwrap();
        assert_eq!(project.entity_type, "Project");
        assert_eq!(project.id_column, "_gkg_p_id");
        assert_eq!(project.type_column, "_gkg_p_type");

        assert!(result.structural.sql.contains("_gkg_u_id"));
        assert!(result.structural.sql.contains("_gkg_u_type"));
        assert!(result.structural.sql.contains("_gkg_p_id"));
        assert!(result.structural.sql.contains("_gkg_p_type"));
    }

    #[test]
    fn multi_hop_traversal_generates_union_subquery() {
        let json = r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "columns": ["username"]},
                {"id": "p", "entity": "Project", "columns": ["name"]}
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
        println!("Multi-hop SQL: {}", result.structural.sql);

        // Should generate a union subquery with multiple arms (one per hop count)
        assert!(
            result.structural.sql.contains("UNION ALL"),
            "expected UNION ALL for unrolled multi-hop: {}",
            result.structural.sql
        );
        // Should have the hop_e0 union subquery aliased
        assert!(
            result.structural.sql.contains("AS hop_e0"),
            "expected hop_e0 subquery alias: {}",
            result.structural.sql
        );
        // Should have depth column for filtering
        assert!(
            result.structural.sql.contains("AS depth"),
            "expected depth column: {}",
            result.structural.sql
        );
    }

    #[test]
    fn multi_hop_with_min_hops_filter() {
        let json = r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "columns": ["username"]},
                {"id": "p", "entity": "Project", "columns": ["name"]}
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
        println!("Min-hops SQL: {}", result.structural.sql);

        // Should have depth >= 2 filter
        assert!(
            result.structural.sql.contains("hop_e0.depth"),
            "expected depth reference: {}",
            result.structural.sql
        );
    }

    #[test]
    fn single_hop_does_not_generate_cte() {
        let json = r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "columns": ["username"]},
                {"id": "n", "entity": "Note", "columns": ["confidential"]}
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
        println!("Single-hop SQL: {}", result.structural.sql);

        // Should NOT generate a recursive CTE for single hop
        assert!(
            !result.structural.sql.contains("WITH RECURSIVE"),
            "single hop should not generate CTE: {}",
            result.structural.sql
        );
    }

    #[test]
    fn multi_hop_aggregation() {
        let json = r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "u", "entity": "User", "columns": ["username"]},
                {"id": "p", "entity": "Project", "columns": ["name"]}
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
        println!("Multi-hop aggregation SQL: {}", result.structural.sql);

        // Should generate union subquery for multi-hop in aggregation queries
        assert!(
            result.structural.sql.contains("UNION ALL"),
            "aggregation should support multi-hop with union: {}",
            result.structural.sql
        );
        assert!(
            result.structural.sql.contains("AS hop_e0"),
            "expected hop_e0 subquery alias: {}",
            result.structural.sql
        );
        assert!(
            result.structural.sql.contains("COUNT"),
            "expected COUNT in query: {}",
            result.structural.sql
        );
    }

    #[test]
    fn definition_uses_project_id_for_redaction() {
        let json = r#"{
            "query_type": "search",
            "node": {"id": "d", "entity": "Definition", "columns": ["name", "project_id"]},
            "limit": 10
        }"#;

        let result = compile(json, &load_test_ontology(), &test_ctx()).unwrap();

        assert!(
            result.structural.sql.contains("d.project_id AS _gkg_d_id"),
            "Definition should use project_id for redaction ID: {}",
            result.structural.sql
        );
        assert!(result.structural.sql.contains("_gkg_d_type"));
    }

    #[test]
    fn project_still_uses_id_for_redaction() {
        let json = r#"{
            "query_type": "search",
            "node": {"id": "p", "entity": "Project", "columns": ["name"]},
            "limit": 10
        }"#;

        let result = compile(json, &load_test_ontology(), &test_ctx()).unwrap();

        assert!(
            result.structural.sql.contains("p.id AS _gkg_p_id"),
            "Project should use id for redaction ID: {}",
            result.structural.sql
        );
    }
}
