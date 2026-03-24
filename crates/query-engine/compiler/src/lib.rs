//! Graph Query Compiler
//!
//! Compiles JSON graph queries into parameterized ClickHouse SQL.
//!
//! # Pipeline
//!
//! ```text
//! JSON → Schema Validate → Parse → Validate → Lower → Optimize → Enforce → Security → Check → Codegen → SQL
//! ```
//!
//! # Example
//!
//! ```rust
//! use compiler::{compile, SecurityContext};
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
//! println!("SQL: {}", result.base.sql);
//! ```

pub mod ast;
pub mod constants;
pub mod envs;
pub mod error;
pub mod input;
pub mod metrics;
pub mod passes;
pub mod pipeline;
pub mod types;

pub use ast::{Expr, JoinType, Node, Op, OrderExpr, Query, SelectExpr, TableRef};
pub use constants::{
    EDGE_ALIAS_SUFFIXES, EDGE_DST_SUFFIX, EDGE_DST_TYPE_SUFFIX, EDGE_KINDS_COLUMN, EDGE_SRC_SUFFIX,
    EDGE_SRC_TYPE_SUFFIX, EDGE_TYPE_SUFFIX, GKG_COLUMN_PREFIX, HYDRATION_NODE_ALIAS,
    NEIGHBOR_ID_COLUMN, NEIGHBOR_IS_OUTGOING_COLUMN, NEIGHBOR_TYPE_COLUMN, PATH_COLUMN,
    RELATIONSHIP_TYPE_COLUMN,
};
pub use error::{QueryError, Result};
pub use input::{
    ColumnSelection, DynamicColumnMode, EntityAuthConfig, Input, InputNode, QueryType, parse_input,
};
pub use metrics::{METRICS, QueryEngineMetrics};
pub use ontology::constants::EDGE_TABLE;
pub use ontology::{Ontology, OntologyError};
pub use pipeline::{
    CompilerContext, CompilerObserver, CompilerPass, CompilerRunner, MetricsObserver, PipelineEnv,
};

// Re-export pass structs.
pub use envs::{ClickHouseEnv, HydrationEnv};
pub use passes::{
    CheckPass, CodegenPass, EnforcePass, HydrationCodegenPass, LowerPass, NormalizePass,
    OptimizePass, SecurityPass, ValidatePass,
};

// Re-export key types from pass modules.
pub use passes::check::check_ast;
pub use passes::codegen::{
    CompiledQueryContext, HydrationPlan, HydrationTemplate, ParamValue, ParameterizedQuery, codegen,
};
pub use passes::enforce::{EdgeMeta, RedactionNode, ResultContext, enforce_return};
pub use passes::hydrate::generate_hydration_plan;
pub use passes::lower::lower;
pub use passes::normalize::{build_entity_auth, normalize};
pub use passes::optimize::optimize;
pub use passes::security::apply_security_context;
pub use passes::validate::Validator;
pub use types::SecurityContext;

use std::sync::Arc;

use metrics::CountErr;

// ─────────────────────────────────────────────────────────────────────────────
// Public API
// ─────────────────────────────────────────────────────────────────────────────

/// Validate and normalize a JSON query string into a typed `Input`.
pub(crate) fn validated_input(json_input: &str, ontology: &Ontology) -> Result<Input> {
    let v = Validator::new(ontology);
    let value = v.check_json(json_input).count_err()?;
    v.check_ontology(&value).count_err()?;
    let input: Input = serde_json::from_value(value).count_err()?;
    v.check_references(&input).count_err()?;
    normalize(input, ontology).count_err()
}

/// Compile a JSON query into a [`CompiledQueryContext`].
///
/// The context contains the parameterized SQL, bind parameters, result context
/// for redaction, hydration plan, and the validated input.
#[must_use = "the compiled query context should be used"]
pub fn compile(
    json_input: &str,
    ontology: &Ontology,
    ctx: &SecurityContext,
) -> Result<CompiledQueryContext> {
    let input = validated_input(json_input, ontology).count_err()?;
    compile_input(input, ctx)
}

/// Compile from a pre-built `Input`. Used for internal query types (Hydration)
/// that bypass JSON schema validation.
pub fn compile_input(mut input: Input, ctx: &SecurityContext) -> Result<CompiledQueryContext> {
    let mut node = lower(&mut input).count_err()?;
    optimize(&mut node, &mut input, ctx);
    let result_context = enforce_return(&mut node, &input)?;
    if input.query_type != QueryType::Hydration {
        apply_security_context(&mut node, ctx).count_err()?;
        check_ast(&node, ctx).count_err()?;
    }
    let base = codegen(&node, result_context).count_err()?;

    let hydration = generate_hydration_plan(&input);
    let query_type = input.query_type;

    Ok(CompiledQueryContext {
        query_type,
        base,
        hydration,
        input,
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// Pipeline presets
// ─────────────────────────────────────────────────────────────────────────────

/// Standard ClickHouse compilation pipeline.
///
/// ```text
/// JSON → Parse → Validate → Normalize → Lower → Optimize → Enforce → Security → Check → Codegen
/// ```
pub fn compile_clickhouse(
    json: &str,
    ontology: Arc<Ontology>,
    security_ctx: SecurityContext,
) -> Result<CompiledQueryContext> {
    let env = ClickHouseEnv::new(ontology, security_ctx);
    CompilerRunner::new(json, env)
        .with_observer(MetricsObserver)
        .then(&ValidatePass)?
        .then(&NormalizePass)?
        .then(&LowerPass)?
        .then(&OptimizePass)?
        .then(&EnforcePass)?
        .then(&SecurityPass)?
        .then(&CheckPass)?
        .then(&CodegenPass)?
        .into_context()
        .take_output()
        .ok_or_else(|| QueryError::Codegen("CodegenPass did not produce output".into()))
}

/// Hydration pipeline — skips security and check passes.
///
/// ```text
/// Input → Lower → Optimize → Enforce → HydrationCodegen
/// ```
pub fn compile_hydration(
    input: Input,
    ontology: Arc<Ontology>,
    security_ctx: SecurityContext,
) -> Result<CompiledQueryContext> {
    let env = HydrationEnv::new(ontology, security_ctx);
    CompilerRunner::from_input(input, env)
        .with_observer(MetricsObserver)
        .then(&LowerPass)?
        .then(&OptimizePass)?
        .then(&EnforcePass)?
        .then(&HydrationCodegenPass)?
        .into_context()
        .take_output()
        .ok_or_else(|| QueryError::Codegen("HydrationCodegenPass did not produce output".into()))
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(irrefutable_let_patterns)]
mod pipeline_tests {
    use super::*;
    use std::time::Duration;

    fn test_ontology() -> Arc<Ontology> {
        Arc::new(Ontology::load_embedded().expect("ontology must load"))
    }

    fn test_security_ctx() -> SecurityContext {
        SecurityContext::new(1, vec!["1/".into()]).unwrap()
    }

    fn test_ch_env() -> ClickHouseEnv {
        ClickHouseEnv::new(test_ontology(), test_security_ctx())
    }

    #[test]
    fn full_clickhouse_pipeline() {
        let json = r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username"]},
            "limit": 10
        }"#;

        let compiled = compile_clickhouse(json, test_ontology(), test_security_ctx()).unwrap();
        assert!(!compiled.base.sql.is_empty());
        assert_eq!(compiled.query_type, QueryType::Search);
    }

    #[test]
    fn full_traversal_pipeline() {
        let json = r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User"},
                {"id": "mr", "entity": "MergeRequest"}
            ],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "mr"}],
            "limit": 10
        }"#;

        let compiled = compile_clickhouse(json, test_ontology(), test_security_ctx()).unwrap();
        assert!(!compiled.base.sql.is_empty());
        assert_eq!(compiled.query_type, QueryType::Traversal);
    }

    #[test]
    fn hydration_pipeline_skips_security() {
        let ontology = test_ontology();
        let json = r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username"]},
            "limit": 10
        }"#;
        let mut input = validated_input(json, &ontology).unwrap();
        input.query_type = QueryType::Hydration;

        let compiled = compile_hydration(input, test_ontology(), test_security_ctx()).unwrap();
        assert!(!compiled.base.sql.is_empty());
    }

    #[test]
    fn partial_pipeline_inspect_after_lower() {
        let json = r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username"]},
            "limit": 10
        }"#;

        let ctx = CompilerRunner::new(json, test_ch_env())
            .then(&ValidatePass)
            .unwrap()
            .then(&NormalizePass)
            .unwrap()
            .then(&LowerPass)
            .unwrap()
            .into_context();

        let Node::Query(q) = ctx.require_node().unwrap();
        assert!(!q.select.is_empty());
    }

    #[test]
    fn partial_pipeline_inspect_after_normalize() {
        let json = r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username"]},
            "limit": 10
        }"#;

        let ctx = CompilerRunner::new(json, test_ch_env())
            .then(&ValidatePass)
            .unwrap()
            .then(&NormalizePass)
            .unwrap()
            .into_context();

        assert_eq!(ctx.require_input().unwrap().query_type, QueryType::Search);
    }

    #[test]
    fn partial_pipeline_inspect_after_optimize() {
        let json = r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username"]},
            "limit": 10
        }"#;

        let ctx = CompilerRunner::new(json, test_ch_env())
            .then(&ValidatePass)
            .unwrap()
            .then(&NormalizePass)
            .unwrap()
            .then(&LowerPass)
            .unwrap()
            .then(&OptimizePass)
            .unwrap()
            .into_context();

        let Node::Query(q) = ctx.require_node().unwrap();
        assert!(q.limit.is_some());
    }

    #[test]
    fn observer_receives_pass_completions() {
        use std::sync::{Arc, Mutex};

        #[derive(Default)]
        struct RecordingObserver {
            completed: Arc<Mutex<Vec<(&'static str, Duration)>>>,
        }

        impl CompilerObserver for RecordingObserver {
            fn pass_completed(&mut self, name: &'static str, elapsed: Duration) {
                self.completed.lock().unwrap().push((name, elapsed));
            }
            fn pass_failed(&mut self, _name: &'static str, _error: &QueryError) {}
        }

        let completed = Arc::new(Mutex::new(Vec::new()));
        let obs = RecordingObserver {
            completed: Arc::clone(&completed),
        };

        let json = r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username"]},
            "limit": 10
        }"#;

        let _ = CompilerRunner::new(json, test_ch_env())
            .with_observer(obs)
            .then(&ValidatePass)
            .unwrap()
            .then(&NormalizePass)
            .unwrap()
            .then(&LowerPass)
            .unwrap()
            .then(&OptimizePass)
            .unwrap()
            .then(&EnforcePass)
            .unwrap()
            .then(&SecurityPass)
            .unwrap()
            .then(&CheckPass)
            .unwrap()
            .then(&CodegenPass)
            .unwrap()
            .into_context();

        let names: Vec<_> = completed.lock().unwrap().iter().map(|(n, _)| *n).collect();
        assert_eq!(
            names,
            vec![
                "validate",
                "normalize",
                "lower",
                "optimize",
                "enforce",
                "security",
                "check",
                "codegen"
            ]
        );
    }

    #[test]
    fn observer_records_failures() {
        use std::sync::{Arc, Mutex};

        #[derive(Default)]
        struct FailureObserver {
            failed: Arc<Mutex<Vec<&'static str>>>,
        }

        impl CompilerObserver for FailureObserver {
            fn pass_completed(&mut self, _name: &'static str, _elapsed: Duration) {}
            fn pass_failed(&mut self, name: &'static str, _error: &QueryError) {
                self.failed.lock().unwrap().push(name);
            }
        }

        let failed = Arc::new(Mutex::new(Vec::new()));
        let obs = FailureObserver {
            failed: Arc::clone(&failed),
        };

        let bad_input = Input {
            query_type: QueryType::Search,
            ..Input::default()
        };

        let result = CompilerRunner::from_input(bad_input, test_ch_env())
            .with_observer(obs)
            .then(&LowerPass);

        assert!(result.is_err());
        let names = failed.lock().unwrap().clone();
        assert_eq!(names, vec!["lower"]);
    }

    #[test]
    fn compile_clickhouse_matches_legacy_compile() {
        let ontology = test_ontology();
        let ctx = test_security_ctx();

        let json = r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username"]},
            "limit": 10
        }"#;

        let pipeline_result = compile_clickhouse(json, Arc::clone(&ontology), ctx.clone()).unwrap();
        let legacy_result = compile(json, &ontology, &ctx).unwrap();

        assert_eq!(pipeline_result.base.sql, legacy_result.base.sql);
        assert_eq!(pipeline_result.query_type, legacy_result.query_type);
    }
}

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
        let v = Validator::new(ontology);
        let value = v.check_json(json_input)?;
        v.check_ontology(&value)?;
        let input: Input = serde_json::from_value(value)?;
        v.check_references(&input)?;
        let mut input = normalize(input, ontology)?;
        let node = lower(&mut input)?;
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
        // lower() still returns full columns in this stage; slim SELECT comes later
        assert!(!q.select.is_empty());
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

        // Edge-centric: edge table is FROM, no node table joins
        assert!(result.base.sql.contains("SELECT"));
        assert!(result.base.sql.contains("gl_edge"));
        assert!(
            result.base.sql.contains("relationship_kind"),
            "expected relationship_kind filter: {}",
            result.base.sql
        );
        assert!(result.base.sql.contains("LIMIT 25"));
        assert!(
            result
                .base
                .params
                .values()
                .any(|p| p.value == serde_json::json!("AUTHORED")),
            "expected AUTHORED in params: {:?}",
            result.base.params
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
                .base
                .params
                .values()
                .any(|p| p.value == serde_json::Value::Bool(true)),
            "expected boolean filter to remain true in params: {:?}",
            result.base.params
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
        assert!(result.base.sql.contains("COUNT"));
        assert!(result.base.sql.contains("GROUP BY"));
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

        // Non-recursive CTEs: hop frontier + forward + backward
        assert!(
            result.base.sql.contains("forward AS"),
            "should have forward CTE"
        );
        assert!(
            result.base.sql.contains("backward AS"),
            "should have backward CTE"
        );
        assert!(result.base.sql.contains("UNION ALL"));

        // Path construction uses arrayConcat + tuples
        assert!(
            result.base.sql.contains("arrayConcat"),
            "paths should be concatenated"
        );
        assert!(
            result.base.sql.contains("tuple"),
            "path nodes should be typed tuples"
        );

        // Intersection: forward joins backward on meeting point
        assert!(
            result.base.sql.contains("f.end_id") && result.base.sql.contains("b.end_id"),
            "should join forward and backward on end_id"
        );
    }

    #[test]
    fn path_finding_depth_control() {
        // Verify max_depth controls frontier depth
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

        // max_depth=1: only forward CTE (no backward needed)
        assert!(
            shallow_result.base.sql.contains("WITH forward AS"),
            "shallow should have forward CTE"
        );
        assert!(
            !shallow_result.base.sql.contains("backward AS"),
            "shallow (max_depth=1) should not have backward CTE"
        );

        // max_depth=3: both forward + backward CTEs
        assert!(
            deep_result.base.sql.contains("forward AS"),
            "deep should have forward CTE"
        );
        assert!(
            deep_result.base.sql.contains("backward AS"),
            "deep (max_depth=3) should have backward CTE"
        );

        // Deeper query should produce longer SQL (more join arms)
        assert!(
            deep_result.base.sql.len() > shallow_result.base.sql.len(),
            "deeper max_depth should produce more SQL"
        );
    }

    #[test]
    fn neighbors_query() {
        let json = r#"{
            "query_type": "neighbors",
            "node": {"id": "u", "entity": "User", "columns": ["username"], "node_ids": [100]},
            "neighbors": {"node": "u", "direction": "both"}
        }"#;

        let result = compile(json, &test_ontology(), &test_ctx()).unwrap();
        assert!(result.base.sql.contains("SELECT"));
        assert!(result.base.sql.contains("_gkg_neighbor_id"));
        assert!(result.base.sql.contains("_gkg_neighbor_type"));
        assert!(result.base.sql.contains("_gkg_relationship_type"));
        assert!(
            result.base.sql.contains("_gkg_neighbor_is_outgoing"),
            "bidirectional neighbor query should include direction column: {}",
            result.base.sql
        );
        assert!(result.base.sql.contains("INNER JOIN"));
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
        assert!(result.base.sql.contains("WHERE"));
        assert!(result.base.sql.contains(">="));
        assert!(result.base.sql.contains("IN"));
        assert!(result.base.sql.contains("LIKE"));
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
            ],
            "relationships": [
                {"type": "AUTHORED", "from": "user_node", "to": "_private"},
                {"type": "CONTAINS", "from": "CamelCase", "to": "_private"},
                {"type": "MEMBER_OF", "from": "user_node", "to": "node123"}
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
        println!("Parameterized: {}", result.base.sql);
        println!("Params: {:?}", result.base.params);
        println!("Inlined: {}", result.base);
        assert!(result.base.sql.contains("SELECT"));
        assert!(result.base.sql.contains("gl_edge"));
        assert!(result.base.sql.contains("LIMIT 25"));
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
        println!("Search SQL: {}", result.base.sql);
        println!("Params: {:?}", result.base.params);
        println!("Inlined: {}", result.base);

        assert!(result.base.sql.contains("SELECT"));
        assert!(result.base.sql.contains("FROM"));
        assert!(result.base.sql.contains("WHERE"));
        assert!(result.base.sql.contains("username"));
        assert!(result.base.sql.contains("LIMIT 10"));
        assert!(
            !result.base.sql.contains("JOIN"),
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
        println!("Complex search SQL: {}", result.base.sql);
        println!("Params: {:?}", result.base.params);
        println!("Inlined: {}", result.base);

        assert!(result.base.sql.contains("SELECT"));
        assert!(result.base.sql.contains("WHERE"));
        assert!(result.base.sql.contains("username"));
        assert!(result.base.sql.contains("state"));
        assert!(result.base.sql.contains("created_at"));
        assert!(result.base.sql.contains("ORDER BY"));
        assert!(result.base.sql.contains("DESC"));
        assert!(result.base.sql.contains("LIMIT 50"));
        assert!(
            !result.base.sql.contains("JOIN"),
            "search queries should not have joins"
        );

        // Verify multiple filters are combined with AND
        assert!(result.base.sql.contains("AND"));
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
        println!("Search with columns SQL: {}", result.base.sql);

        // Structural query still includes all columns (slim SELECT not yet implemented)
        assert!(result.base.sql.contains("_gkg_u_id"));
        assert!(result.base.sql.contains("_gkg_u_type"));
        assert!(result.base.sql.contains("u_username"));

        // Static hydration disabled — base query already carries all columns
        assert!(matches!(result.hydration, HydrationPlan::None));
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
        println!("Search with wildcard SQL: {}", result.base.sql);

        // Structural query still includes all columns (slim SELECT not yet implemented)
        assert!(result.base.sql.contains("_gkg_u_id"));
        assert!(result.base.sql.contains("_gkg_u_type"));

        // Static hydration disabled — base query already carries all columns
        assert!(matches!(result.hydration, HydrationPlan::None));
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
        println!("Traversal with columns SQL: {}", result.base.sql);

        // Edge-centric: redaction IDs are present, node properties come via hydration
        assert!(result.base.sql.contains("_gkg_u_id"));
        assert!(result.base.sql.contains("_gkg_u_type"));
        assert!(result.base.sql.contains("_gkg_p_id"));
        assert!(result.base.sql.contains("_gkg_p_type"));
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
        println!("Aggregation SQL: {}", result.base.sql);

        // Aggregation queries only add mandatory columns for group_by nodes (u)
        // The target node (mr) is aggregated so doesn't get individual row columns
        assert!(result.base.sql.contains("_gkg_u_id"));
        assert!(result.base.sql.contains("_gkg_u_type"));
        // MR is aggregated, not returned as individual rows
        assert!(!result.base.sql.contains("_gkg_mr_id"));
        assert!(!result.base.sql.contains("_gkg_mr_type"));
        // Should have the aggregation
        assert!(result.base.sql.contains("COUNT"));
        assert!(result.base.sql.contains("GROUP BY"));
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
        println!("Path finding SQL: {}", result.base.sql);

        // Path finding queries use _gkg_path column (Array of tuples)
        // which contains all node IDs and types along the path
        assert!(result.base.sql.contains("_gkg_path"));
        // The columns selection on nodes is ignored for path finding
        // because the result is a path, not individual node rows
        assert!(result.base.result_context.query_type == Some(QueryType::PathFinding));
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

        assert_eq!(result.base.result_context.len(), 2);

        let user = result.base.result_context.get("u").unwrap();
        assert_eq!(user.entity_type, "User");
        assert_eq!(user.id_column, "_gkg_u_id");
        assert_eq!(user.type_column, "_gkg_u_type");

        let project = result.base.result_context.get("p").unwrap();
        assert_eq!(project.entity_type, "Project");
        assert_eq!(project.id_column, "_gkg_p_id");
        assert_eq!(project.type_column, "_gkg_p_type");

        assert!(result.base.sql.contains("_gkg_u_id"));
        assert!(result.base.sql.contains("_gkg_u_type"));
        assert!(result.base.sql.contains("_gkg_p_id"));
        assert!(result.base.sql.contains("_gkg_p_type"));
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
        println!("Multi-hop SQL: {}", result.base.sql);

        // Should generate a union subquery with multiple arms (one per hop count)
        assert!(
            result.base.sql.contains("UNION ALL"),
            "expected UNION ALL for unrolled multi-hop: {}",
            result.base.sql
        );
        // Should have the hop_e0 union subquery aliased
        assert!(
            result.base.sql.contains("AS hop_e0"),
            "expected hop_e0 subquery alias: {}",
            result.base.sql
        );
        // Should have depth column for filtering
        assert!(
            result.base.sql.contains("AS depth"),
            "expected depth column: {}",
            result.base.sql
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
        println!("Min-hops SQL: {}", result.base.sql);

        // Should have depth >= 2 filter
        assert!(
            result.base.sql.contains("hop_e0.depth"),
            "expected depth reference: {}",
            result.base.sql
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
        println!("Single-hop SQL: {}", result.base.sql);

        // Should NOT generate a recursive CTE for single hop
        assert!(
            !result.base.sql.contains("WITH RECURSIVE"),
            "single hop should not generate CTE: {}",
            result.base.sql
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
        println!("Multi-hop aggregation SQL: {}", result.base.sql);

        // Should generate union subquery for multi-hop in aggregation queries
        assert!(
            result.base.sql.contains("UNION ALL"),
            "aggregation should support multi-hop with union: {}",
            result.base.sql
        );
        assert!(
            result.base.sql.contains("AS hop_e0"),
            "expected hop_e0 subquery alias: {}",
            result.base.sql
        );
        assert!(
            result.base.sql.contains("COUNT"),
            "expected COUNT in query: {}",
            result.base.sql
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
            result.base.sql.contains("d.project_id AS _gkg_d_id"),
            "Definition should use project_id for redaction ID: {}",
            result.base.sql
        );
        assert!(result.base.sql.contains("_gkg_d_type"));
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
            result.base.sql.contains("p.id AS _gkg_p_id"),
            "Project should use id for redaction ID: {}",
            result.base.sql
        );
    }

    #[test]
    fn range_pagination() {
        let ontology = load_test_ontology();
        let ctx = test_ctx();

        // Search: range → LIMIT + OFFSET
        let result = compile(
            r#"{
                "query_type": "search",
                "node": {"id": "u", "entity": "User", "columns": ["username"]},
                "range": {"start": 40, "end": 50}
            }"#,
            &ontology,
            &ctx,
        )
        .unwrap();
        assert!(result.base.sql.contains("LIMIT 10"), "{}", result.base.sql);
        assert!(result.base.sql.contains("OFFSET 40"), "{}", result.base.sql);

        // Traversal with ordering
        let result = compile(
            r#"{
                "query_type": "traversal",
                "nodes": [
                    {"id": "u", "entity": "User", "columns": ["username"]},
                    {"id": "p", "entity": "Project", "columns": ["name"]}
                ],
                "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "p"}],
                "range": {"start": 0, "end": 30},
                "order_by": {"node": "u", "property": "created_at", "direction": "DESC"}
            }"#,
            &ontology,
            &ctx,
        )
        .unwrap();
        assert!(result.base.sql.contains("LIMIT 30"), "{}", result.base.sql);
        assert!(result.base.sql.contains("OFFSET 0"), "{}", result.base.sql);
        assert!(result.base.sql.contains("ORDER BY"));
        assert!(result.base.sql.contains("DESC"));

        // Mutual exclusion: limit + range rejected
        let err = compile(
            r#"{
                "query_type": "search",
                "node": {"id": "u", "entity": "User"},
                "limit": 10,
                "range": {"start": 0, "end": 5}
            }"#,
            &ontology,
            &ctx,
        )
        .unwrap_err();
        assert!(matches!(err, QueryError::Validation(_)), "{err}");

        // end == start rejected
        let err = compile(
            r#"{
                "query_type": "search",
                "node": {"id": "u", "entity": "User"},
                "range": {"start": 10, "end": 10}
            }"#,
            &ontology,
            &ctx,
        )
        .unwrap_err();
        assert!(err.to_string().contains("must be greater than"), "{err}");

        // window > 1000 rejected
        let err = compile(
            r#"{
                "query_type": "search",
                "node": {"id": "u", "entity": "User"},
                "range": {"start": 0, "end": 1001}
            }"#,
            &ontology,
            &ctx,
        )
        .unwrap_err();
        assert!(err.to_string().contains("must not exceed 1000"), "{err}");

        // window == 1000 accepted
        assert!(
            compile(
                r#"{
                "query_type": "search",
                "node": {"id": "u", "entity": "User"},
                "range": {"start": 0, "end": 1000}
            }"#,
                &ontology,
                &ctx,
            )
            .is_ok()
        );
    }

    // ─────────────────────────────────────────────────────────────────────
    // ParameterizedQuery::render (compile → render round-trip)
    // ─────────────────────────────────────────────────────────────────────

    #[test]
    fn render_traversal_inlines_all_params() {
        let rendered = compile(
            r#"{
                "query_type": "traversal",
                "nodes": [
                    {"id": "mr", "entity": "MergeRequest", "filters": {"state": "opened"}},
                    {"id": "u", "entity": "User"}
                ],
                "relationships": [{"type": "AUTHORED", "from": "u", "to": "mr"}],
                "limit": 10
            }"#,
            &load_test_ontology(),
            &test_ctx(),
        )
        .unwrap()
        .base
        .render();

        assert!(
            !rendered.contains("{p"),
            "rendered SQL should have no placeholders: {rendered}"
        );
        assert!(rendered.contains("'opened'"));
        assert!(rendered.contains("'AUTHORED'"));
    }

    #[test]
    fn render_in_filter_inlines_array() {
        let rendered = compile(
            r#"{
                "query_type": "search",
                "node": {"id": "u", "entity": "User", "filters": {
                    "user_type": {"op": "in", "value": ["project_bot", "service_account"]}
                }},
                "limit": 10
            }"#,
            &load_test_ontology(),
            &test_ctx(),
        )
        .unwrap()
        .base
        .render();

        assert!(
            !rendered.contains("{p"),
            "rendered SQL should have no placeholders: {rendered}"
        );
        assert!(
            rendered.contains("['project_bot', 'service_account']"),
            "should inline array: {rendered}"
        );
    }

    #[test]
    fn render_node_ids_inlines_array() {
        let rendered = compile(
            r#"{
                "query_type": "search",
                "node": {"id": "u", "entity": "User", "node_ids": [100, 200, 300]},
                "limit": 10
            }"#,
            &load_test_ontology(),
            &test_ctx(),
        )
        .unwrap()
        .base
        .render();

        assert!(
            !rendered.contains("{p"),
            "rendered SQL should have no placeholders: {rendered}"
        );
        assert!(
            rendered.contains("[100, 200, 300]"),
            "should inline node_ids: {rendered}"
        );
    }

    #[test]
    fn debug_json_round_trip() {
        let compiled = compile(
            r#"{
                "query_type": "traversal",
                "nodes": [
                    {"id": "mr", "entity": "MergeRequest", "filters": {"state": "opened"}},
                    {"id": "u", "entity": "User"}
                ],
                "relationships": [{"type": "AUTHORED", "from": "u", "to": "mr"}],
                "limit": 10
            }"#,
            &load_test_ontology(),
            &test_ctx(),
        )
        .unwrap();

        let debug_json = serde_json::json!({
            "base": compiled.base.sql,
            "base_rendered": compiled.base.render(),
            "hydration": serde_json::json!([]),
        });

        let serialized = debug_json.to_string();
        let parsed: serde_json::Value =
            serde_json::from_str(&serialized).expect("should round-trip");

        let base = parsed["base"].as_str().unwrap();
        let rendered = parsed["base_rendered"].as_str().unwrap();

        assert!(base.contains("{p"), "base should have placeholders");
        assert!(
            !rendered.contains("{p"),
            "rendered should have no placeholders"
        );
        assert!(parsed["hydration"].is_array());
    }

    #[test]
    fn hydration_query_type_generates_union_all() {
        let ctx = test_ctx();

        let input = Input {
            query_type: QueryType::Hydration,
            nodes: vec![
                InputNode {
                    id: "hydrate".to_string(),
                    entity: Some("Note".to_string()),
                    table: Some("gl_note".to_string()),
                    columns: Some(ColumnSelection::List(vec![
                        "id".into(),
                        "noteable_type".into(),
                    ])),
                    node_ids: vec![1, 2, 3],
                    ..InputNode::default()
                },
                InputNode {
                    id: "hydrate".to_string(),
                    entity: Some("Project".to_string()),
                    table: Some("gl_project".to_string()),
                    columns: Some(ColumnSelection::List(vec!["id".into(), "name".into()])),
                    node_ids: vec![10, 20],
                    ..InputNode::default()
                },
            ],
            limit: 10,
            ..Input::default()
        };

        let result = compile_input(input, &ctx).unwrap();
        let sql = &result.base.sql;
        let rendered = result.base.render();
        println!("Hydration SQL:\n{sql}");
        println!("\nRendered:\n{rendered}");

        assert!(sql.contains("UNION ALL"), "should contain UNION ALL");
        assert!(sql.contains("toJSONString"), "should contain toJSONString");
        assert!(sql.contains("gl_note"), "should reference gl_note");
        assert!(sql.contains("gl_project"), "should reference gl_project");
        assert!(
            matches!(result.hydration, HydrationPlan::None),
            "hydration query should not trigger further hydration"
        );
    }

    #[test]
    fn hydration_single_entity_no_union_all() {
        let ctx = test_ctx();

        let input = Input {
            query_type: QueryType::Hydration,
            nodes: vec![InputNode {
                id: "hydrate".to_string(),
                entity: Some("User".to_string()),
                table: Some("gl_user".to_string()),
                columns: Some(ColumnSelection::List(vec!["id".into(), "username".into()])),
                node_ids: vec![42],
                ..InputNode::default()
            }],
            limit: 1,
            ..Input::default()
        };

        let result = compile_input(input, &ctx).unwrap();
        let sql = &result.base.sql;

        assert!(
            !sql.contains("UNION ALL"),
            "single entity should not UNION ALL"
        );
        assert!(
            sql.contains("toJSONString"),
            "should still use toJSONString"
        );
        assert!(sql.contains("gl_user"), "should reference gl_user");
    }

    #[test]
    fn hydration_uses_parameterized_ids() {
        let ctx = test_ctx();

        let input = Input {
            query_type: QueryType::Hydration,
            nodes: vec![InputNode {
                id: "hydrate".to_string(),
                entity: Some("Note".to_string()),
                table: Some("gl_note".to_string()),
                columns: Some(ColumnSelection::List(vec![
                    "id".into(),
                    "confidential".into(),
                    "created_at".into(),
                ])),
                node_ids: vec![100, 200, 300],
                ..InputNode::default()
            }],
            limit: 3,
            ..Input::default()
        };

        let result = compile_input(input, &ctx).unwrap();
        let sql = &result.base.sql;

        assert!(
            sql.contains("Array(Int64)"),
            "IDs should be parameterized as Array(Int64), got: {sql}"
        );
        assert!(
            !sql.contains("100"),
            "literal IDs should not appear in parameterized SQL"
        );

        let rendered = result.base.render();
        assert!(
            rendered.contains("100") && rendered.contains("200") && rendered.contains("300"),
            "rendered SQL should inline the IDs"
        );
    }

    #[test]
    fn hydration_skips_security_context() {
        let ctx = test_ctx();

        let input = Input {
            query_type: QueryType::Hydration,
            nodes: vec![InputNode {
                id: "hydrate".to_string(),
                entity: Some("Note".to_string()),
                table: Some("gl_note".to_string()),
                columns: Some(ColumnSelection::List(vec![
                    "id".into(),
                    "confidential".into(),
                ])),
                node_ids: vec![1],
                ..InputNode::default()
            }],
            limit: 1,
            ..Input::default()
        };

        let result = compile_input(input, &ctx).unwrap();
        let sql = &result.base.sql;

        assert!(
            !sql.contains("traversal_path"),
            "hydration should skip security filters, got: {sql}"
        );
        assert!(
            !sql.contains("startsWith"),
            "hydration should not have startsWith filter"
        );
    }

    #[test]
    fn hydration_empty_columns_produces_empty_json() {
        let ctx = test_ctx();

        let input = Input {
            query_type: QueryType::Hydration,
            nodes: vec![InputNode {
                id: "hydrate".to_string(),
                entity: Some("User".to_string()),
                table: Some("gl_user".to_string()),
                columns: Some(ColumnSelection::List(vec!["id".into()])),
                node_ids: vec![1],
                ..InputNode::default()
            }],
            limit: 1,
            ..Input::default()
        };

        let result = compile_input(input, &ctx).unwrap();
        let rendered = result.base.render();

        assert!(
            !rendered.contains("map("),
            "empty props should use literal '{{}}', not map(): {rendered}"
        );
    }

    #[test]
    fn hydration_id_column_excluded_from_map() {
        let ctx = test_ctx();

        let input = Input {
            query_type: QueryType::Hydration,
            nodes: vec![InputNode {
                id: "hydrate".to_string(),
                entity: Some("User".to_string()),
                table: Some("gl_user".to_string()),
                columns: Some(ColumnSelection::List(vec![
                    "id".into(),
                    "username".into(),
                    "state".into(),
                ])),
                node_ids: vec![1],
                ..InputNode::default()
            }],
            limit: 1,
            ..Input::default()
        };

        let result = compile_input(input, &ctx).unwrap();
        let rendered = result.base.render();

        assert!(
            rendered.contains("'username'") && rendered.contains("'state'"),
            "map should contain username and state"
        );

        let map_section = rendered
            .split("map(")
            .nth(1)
            .and_then(|s| s.split(')').next())
            .unwrap_or("");
        assert!(
            !map_section.contains("'id'"),
            "map should not contain 'id' key (it's the PK, selected separately)"
        );
    }
}
