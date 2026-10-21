//! Graph Query Compiler
//!
//! Compiles graph queries into parameterized ClickHouse SQL.
//!
//! # Pipeline
//!
//! ```text
//! GQL text → Input → shared compiler passes → SQL
//! ```
//!
//! # Example
//!
//! ```rust
//! use std::sync::Arc;
//! use compiler::{compile, Frontend, SecurityContext};
//! use ontology::{Ontology, DataType};
//!
//! let ontology = Arc::new(
//!     Ontology::new()
//!         .with_nodes(["User", "Project"])
//!         .with_edges(["MEMBER_OF"])
//!         .with_fields("User", [("username", DataType::String)]),
//! );
//!
//! let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
//!
//! let query = "MATCH (u:User {id: 1}) RETURN u.username LIMIT 10";
//!
//! let result = compile(query, Frontend::Gql, &ontology, &ctx).unwrap();
//! println!("SQL: {}", result.base.sql);
//! ```

pub mod analytics;
pub mod ast;
pub mod constants;
pub mod data_model;
pub mod error;
pub mod input;
pub mod metrics;
pub(crate) mod query_limits;

pub mod scope;
pub mod types;

pub mod config;
pub mod passes;

pub use ast::ddl;
pub use ast::{Expr, Insert, JoinType, Node, Op, OrderExpr, Query, SelectExpr, TableRef};
pub use constants::{
    EDGE_ALIAS_SUFFIXES, EDGE_DST_SUFFIX, EDGE_DST_TYPE_SUFFIX, EDGE_SRC_SUFFIX,
    EDGE_SRC_TYPE_SUFFIX, EDGE_TYPE_SUFFIX, HYDRATION_NODE_ALIAS, edge_kinds_column,
    internal_column_prefix, neighbor_id_column, neighbor_is_outgoing_column, neighbor_type_column,
    path_column, relationship_type_column,
};
pub use error::{QueryError, RejectionReason, Result};
pub use input::{
    ColumnSelection, DynamicColumnMode, FilterOp, Input, InputFilter, InputNode, QueryType,
};
pub use metrics::{METRICS, QueryEngineMetrics};
pub use ontology::{Ontology, OntologyError};
pub use query_data_model::EntityAuthConfig;

pub use analytics::ExecMetrics;
pub use passes::codegen::{
    CompiledQueryContext, ParamValue, ParameterizedQuery, SqlDialect,
    clickhouse::emit_simple_query, codegen,
    ddl::duckdb::emit_create_table as emit_duckdb_create_table, ddl::duckdb::generate_local_ddl,
    ddl::generate_local_tables,
};
pub use passes::enforce::{EdgeMeta, RedactionNode, ResultContext};
pub use passes::frontend::{Frontend, gql};
pub use passes::hydrate::{
    DynamicEntityColumns, HydrationKind, HydrationPlan, HydrationTemplate, VirtualColumnRequest,
    generate_hydration_plan,
};
pub use passes::normalize::build_entity_auth;
pub use passes::plan::HydrationCompileOptions;
pub use scope::ScopeProof;
pub use types::{AccessLevel, AuthorizedPath, DEFAULT_PATH_ACCESS_LEVEL, Realm, SecurityContext};

use metrics::CountErr;
use std::sync::Arc;

use config::CompilerCtx as _;

fn finish<C: config::CompilerCtx>(
    ctx: &mut C,
    run: impl FnOnce(&mut C) -> Result<()>,
) -> Result<CompiledQueryContext> {
    run(ctx)
        .and_then(|()| {
            ctx.take_output().ok_or_else(|| {
                error::QueryError::PipelineInvariant("pipeline did not produce output".into())
            })
        })
        .count_err()
}

#[must_use = "the compiled query context should be used"]
pub fn compile(
    raw: &str,
    fe: Frontend,
    ontology: &Arc<Ontology>,
    ctx: &SecurityContext,
) -> Result<CompiledQueryContext> {
    let data_model = data_model::clickhouse(Arc::clone(ontology))
        .map_err(|error| QueryError::PipelineInvariant(error.to_string()))?;
    compile_model(raw, fe, &data_model, ctx)
}

pub fn compile_model(
    raw: &str,
    fe: Frontend,
    data_model: &Arc<query_data_model::ClickHouseDataModel>,
    ctx: &SecurityContext,
) -> Result<CompiledQueryContext> {
    match fe {
        Frontend::Gql => {
            let mut ctx = config::ClickhouseGqlCtx::new(ctx.clone(), Arc::clone(data_model));
            ctx.set_raw(raw.to_string());
            finish(&mut ctx, config::run_clickhouse_gql)
        }
    }
}

/// Compile a graph query into DuckDB SQL for local execution.
///
/// Collapses edge tables to the local single-table layout before compiling.
/// No security or hydration phases — local mode trusts all data.
#[must_use = "the compiled query context should be used"]
pub fn compile_local(
    raw: &str,
    fe: Frontend,
    ontology: &Arc<Ontology>,
) -> Result<CompiledQueryContext> {
    let mut ontology = ontology.as_ref().clone();
    ontology.remove_data_model_optimizations();
    let ontology = Arc::new(ontology);
    let data_model = data_model::duckdb(Arc::clone(&ontology))
        .map_err(|error| QueryError::PipelineInvariant(error.to_string()))?;
    match fe {
        Frontend::Gql => {
            let mut c = config::DuckdbGqlCtx::new(Arc::clone(&data_model));
            c.set_raw(raw.to_string());
            finish(&mut c, config::run_duckdb_gql)
        }
    }
}

/// Run only parsing, validation, and normalization, returning the normalized [`Input`].
pub fn validate_normalize(raw: &str, fe: Frontend, ontology: &Arc<Ontology>) -> Result<Input> {
    let data_model = data_model::clickhouse(Arc::clone(ontology))
        .map_err(|error| QueryError::PipelineInvariant(error.to_string()))?;
    let (result, input) = match fe {
        Frontend::Gql => {
            let mut ctx = config::ValidateNormalizeGqlCtx::new(data_model);
            ctx.set_raw(raw.to_string());
            (
                config::run_validate_normalize_gql(&mut ctx),
                ctx.take_input(),
            )
        }
    };
    result
        .and_then(|()| {
            input.ok_or_else(|| {
                error::QueryError::PipelineInvariant("validate_normalize produced no input".into())
            })
        })
        .count_err()
}

/// Compile a pre-built hydration `Input` into ClickHouse SQL.
///
/// Runs the hydration pipeline: Restrict → Plan → Lower → Enforce → Settings → Codegen.
/// Skips validation, normalization, security, check, and hydrate plan passes.
/// Codegen defaults to `HydrationPlan::None`.
pub fn compile_input(
    input: Input,
    options: HydrationCompileOptions,
    ontology: &Arc<Ontology>,
    ctx: &SecurityContext,
) -> Result<CompiledQueryContext> {
    let data_model = data_model::clickhouse(Arc::clone(ontology))
        .map_err(|error| QueryError::PipelineInvariant(error.to_string()))?;
    compile_input_model(input, options, &data_model, ctx)
}

pub fn compile_input_model(
    input: Input,
    options: HydrationCompileOptions,
    data_model: &Arc<query_data_model::ClickHouseDataModel>,
    ctx: &SecurityContext,
) -> Result<CompiledQueryContext> {
    let mut ctx = config::ChHydrationCtx::new(ctx.clone(), Arc::clone(data_model));
    ctx.set_input(input);
    ctx.set_hydration_options(options);
    config::run_ch_hydration(&mut ctx)
        .and_then(|()| {
            ctx.take_output().ok_or_else(|| {
                error::QueryError::PipelineInvariant("pipeline did not produce output".into())
            })
        })
        .count_err()
}

#[cfg(test)]
pub(crate) mod testkit {
    use crate::types::{AccessLevel, AuthorizedPath, SecurityContext};

    pub fn non_admin_ctx() -> SecurityContext {
        SecurityContext::new(1, vec!["1/".into()]).unwrap()
    }

    pub fn admin_ctx() -> SecurityContext {
        SecurityContext::new_with_roles(
            1,
            vec![AuthorizedPath::new("1/", AccessLevel::Owner as u32)],
        )
        .unwrap()
        .with_role(true, Some(AccessLevel::Owner as u32))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::LazyLock;

    static ONTOLOGY: LazyLock<Arc<Ontology>> =
        LazyLock::new(|| Arc::new(Ontology::load_embedded().expect("ontology must load")));

    fn security_ctx() -> SecurityContext {
        crate::testkit::non_admin_ctx()
    }

    fn compile_sql(query: &str) -> String {
        compile(query, Frontend::Gql, &ONTOLOGY, &security_ctx())
            .expect("should compile")
            .base
            .render()
    }

    #[test]
    fn relationship_filter_keeps_edge_column_type() {
        let query = r#"MATCH (u:User {id: 1})-[r:MEMBER_OF {target_id: 2}]->(p:Project)
            RETURN u, p"#;

        let compiled = compile(query, Frontend::Gql, &ONTOLOGY, &security_ctx()).unwrap();
        let filter_param = compiled
            .base
            .params
            .values()
            .find(|param| param.value == serde_json::json!(2))
            .expect("target_id filter parameter");
        assert_eq!(
            filter_param.data_type,
            orbit_utils::query_types::SqlType::Int64
        );
    }

    #[test]
    fn malformed_query_increments_compiler_rejected() {
        use std::sync::atomic::Ordering;
        let before = crate::metrics::COUNT_ERR_HITS.load(Ordering::Relaxed);
        let err = compile("not a query", Frontend::Gql, &ONTOLOGY, &security_ctx())
            .expect_err("must reject");
        let after = crate::metrics::COUNT_ERR_HITS.load(Ordering::Relaxed);
        assert!(
            matches!(err, QueryError::Validation(_)),
            "unexpected error: {err:?}"
        );
        assert!(
            after > before,
            "count_err must run on parse errors (before={before}, after={after})"
        );
    }

    #[test]
    fn allowlist_rejected_query_increments_compiler_rejected() {
        use std::sync::atomic::Ordering;
        let query = r#"MATCH (x:NotARealEntity) RETURN x{.id} LIMIT 1"#;
        let before = crate::metrics::COUNT_ERR_HITS.load(Ordering::Relaxed);
        let err =
            compile(query, Frontend::Gql, &ONTOLOGY, &security_ctx()).expect_err("must reject");
        let after = crate::metrics::COUNT_ERR_HITS.load(Ordering::Relaxed);
        assert!(
            !matches!(err, crate::error::QueryError::PipelineInvariant(_)),
            "compile errors from internal passes must surface as their own variant, not PipelineInvariant; got: {err:?}"
        );
        assert!(
            after > before,
            "count_err must run on allowlist rejections (before={before}, after={after})"
        );
    }

    #[test]
    fn traversal_path_scope_rejection_is_observable() {
        use std::sync::atomic::Ordering;
        let query = r#"MATCH (p:Project)
            WHERE p.traversal_path STARTS WITH '1/'
            RETURN p
            LIMIT 1"#;
        let ctx =
            SecurityContext::new(1, vec!["1/100/".to_string()]).expect("valid scoped context");
        let before = crate::metrics::COUNT_ERR_HITS.load(Ordering::Relaxed);
        let err = compile(query, Frontend::Gql, &ONTOLOGY, &ctx).expect_err("must reject");
        let after = crate::metrics::COUNT_ERR_HITS.load(Ordering::Relaxed);

        assert!(
            matches!(err, crate::error::QueryError::Authorization(_)),
            "expected Authorization, got: {err:?}"
        );
        assert!(
            err.is_client_safe(),
            "traversal_path scope rejection should be client safe: {err:?}"
        );
        assert_eq!(
            err.rejection_reason(),
            crate::error::RejectionReason::Authorization
        );
        assert!(
            after > before,
            "count_err must run on traversal_path authorization rejections (before={before}, after={after})"
        );
    }

    #[test]
    fn traversal_path_shape_rejection_is_validate_pass_error() {
        let query = r#"MATCH (p:Project)
            WHERE p.traversal_path STARTS WITH 1
            RETURN p
            LIMIT 1"#;
        let err =
            compile(query, Frontend::Gql, &ONTOLOGY, &security_ctx()).expect_err("must reject");
        let msg = err.to_string();

        assert!(
            matches!(err, crate::error::QueryError::Validation(_)),
            "expected Validation, got: {err:?}"
        );
        assert!(
            err.is_client_safe(),
            "traversal_path shape rejection should be client safe: {err:?}"
        );
        assert!(
            msg.contains("schema violation"),
            "error should come from ValidatePass schema validation, got: {msg}"
        );
        assert!(
            !msg.contains("RestrictPass"),
            "client-facing shape errors should come from validation, got: {msg}"
        );
    }

    #[test]
    fn compile_with_prefixed_ontology_produces_prefixed_sql() {
        let prefixed = Arc::new((**ONTOLOGY).clone().with_schema_version_prefix("v1_"));

        let query = r#"MATCH (g:Group {id: 1}) RETURN g{.name} LIMIT 1"#;
        let compiled =
            compile(query, Frontend::Gql, &prefixed, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

        assert!(
            sql.contains("v1_gl_group"),
            "search SQL should use prefixed node table v1_gl_group, got: {sql}"
        );
    }

    #[test]
    fn compile_uses_supplied_ontology_for_scoped_user_table() {
        let scoped_user = (**ONTOLOGY).clone().with_path_scopable_nodes(["User"]);
        let query = r#"MATCH (u:User {id: 1}) RETURN u{.id} LIMIT 1"#;

        for (ontology, expected_table) in [
            (Arc::new(scoped_user.clone()), "gl_user"),
            (
                Arc::new(scoped_user.with_schema_version_prefix("v1_")),
                "v1_gl_user",
            ),
        ] {
            let sql = compile(query, Frontend::Gql, &ontology, &security_ctx())
                .expect("should compile")
                .base
                .render();
            assert!(
                sql.contains("startsWith(u.traversal_path"),
                "supplied ontology must keep the User alias scoped, got:\n{sql}"
            );
            assert!(
                sql.contains(expected_table),
                "ontology should render {expected_table}, got:\n{sql}"
            );
        }
    }

    #[test]
    fn compile_uses_each_ontologys_global_tables_and_keeps_join_filters() {
        let query = r#"MATCH (user:User {id: 1, username: 'alice'})-[:MEMBER_OF]->(project:Project {name: 'orbit'})
            RETURN user, project
            LIMIT 1"#;

        let mut sources = ontology::migrations::embedded_sources();
        let user_source = sources.get_mut("nodes/core/user.yaml").unwrap();
        *user_source = user_source.replace(
            "destination_table: gl_user",
            "destination_table: gl_renamed_user",
        );
        let archived_ontology = ontology::archive::OntologyArchive::from_sources(1, &sources)
            .expect("renamed archive should build")
            .load_ontology()
            .expect("renamed archive should load");
        let prefixed = archived_ontology.clone().with_schema_version_prefix("v1_");

        for (ontology, expected_user_table) in [
            (Arc::new(archived_ontology.clone()), "gl_renamed_user"),
            (ONTOLOGY.clone(), "gl_user"),
            (Arc::new(prefixed), "v1_gl_renamed_user"),
            (
                Arc::new((**ONTOLOGY).clone().with_schema_version_prefix("v2_")),
                "v2_gl_user",
            ),
            (Arc::new(archived_ontology), "gl_renamed_user"),
        ] {
            let sql = compile(query, Frontend::Gql, &ontology, &security_ctx())
                .expect("should compile")
                .base
                .render();
            assert!(
                sql.contains(expected_user_table),
                "compiled SQL should use {expected_user_table}, got:\n{sql}"
            );
            assert!(
                !sql.contains("startsWith(user.traversal_path"),
                "global User must stay unscoped, got:\n{sql}"
            );
            assert!(
                sql.contains("startsWith(project.traversal_path"),
                "joined Project alias must remain scoped, got:\n{sql}"
            );
            assert!(
                sql.contains("startsWith(_orbit_edge_0.traversal_path"),
                "edge alias e0 must remain scoped, got:\n{sql}"
            );
        }
    }

    #[test]
    fn unfiltered_edge_only_count_emits_bare_count_for_projection_routing() {
        let query = r#"MATCH (parent:Pipeline {id: 1})-[:CHILD_OF]->(child:Pipeline)
            RETURN parent, count(child) AS children
            LIMIT 10"#;

        let sql = compile_sql(query);

        assert!(
            sql.contains("countIf("),
            "single-hop edge count should use countIf on the LIMIT BY path, \
             got:\n{sql}"
        );
        assert!(
            sql.contains("LIMIT 1 BY"),
            "single-hop edge aggregation should use LIMIT BY dedup, got:\n{sql}"
        );
        assert!(
            !sql.contains("COUNT(_orbit_edge_0.source_id)")
                && !sql.contains("count(_orbit_edge_0.source_id)"),
            "must not emit COUNT(source_id) -- forces unnecessary column read, \
             got:\n{sql}"
        );
    }

    /// A single-hop edge scan cannot fan a node out, so it stays a plain scan;
    /// deduplicating the hot single-edge path would cost an unnecessary aggregation.
    #[test]
    fn single_edge_scan_stays_plain() {
        let query = r#"MATCH (mr:MergeRequest {id: 490855697})-[:HAS_LABEL]->(label:Label {title: 'group::source code'})
            RETURN count(mr) AS n
            LIMIT 1"#;

        let sql = compile_sql(query);

        assert!(
            !sql.contains("argMax("),
            "single-hop edge scan must not dedup, got:\n{sql}"
        );
    }

    #[test]
    fn path_finding_code_filtered_endpoints_prune_by_traversal_path() {
        let query = r#"MATCH p = ANY SHORTEST (start:Definition)-[:CALLS*1..3]->(`end`:Definition)
            WHERE start.name = 'compile' AND `end`.name = 'run_query'
            RETURN p
            LIMIT 10"#;

        let sql = compile_sql(query);

        assert!(
            sql.contains("_path_scope_traversal_paths"),
            "code path finding should compute candidate traversal paths, got:\n{sql}"
        );
        assert!(
            sql.contains("e1.traversal_path = e2.traversal_path"),
            "code edge self-joins should stay within one traversal_path, got:\n{sql}"
        );
        assert!(
            sql.contains("f.traversal_path = b.traversal_path"),
            "frontier intersection should stay within one traversal_path, got:\n{sql}"
        );
        assert!(
            sql.contains("forward") && sql.contains("FROM _nf_start"),
            "forward CTE should seed from _nf_start, got:\n{sql}"
        );
        assert!(
            sql.contains(
                "traversal_path IN (SELECT traversal_path FROM _path_scope_traversal_paths)"
            ),
            "edge scans should use traversal_path scope, got:\n{sql}"
        );
        assert!(
            sql.contains("e1.source_kind = 'Definition'"),
            "forward edge scans should constrain source_kind = Definition, got:\n{sql}"
        );
    }

    #[test]
    fn path_finding_cross_level_endpoints_union_traversal_scope() {
        let query = r#"MATCH p = ANY SHORTEST (from:Group {id: 9970})-[:CONTAINS*1..3]->(to:Project {id: 278964})
            RETURN p"#;
        let sql = compile_sql(query);
        assert!(
            sql.contains("_path_scope_traversal_paths"),
            "cross-level path finding should still compute a scope, got:\n{sql}"
        );
        assert!(
            sql.contains("UNION ALL SELECT _path_scope_end.traversal_path"),
            "scope must UNION both endpoints' traversal paths, got:\n{sql}"
        );
        assert!(
            !sql.contains("_path_scope_start.traversal_path = _path_scope_end.traversal_path"),
            "scope must not intersect endpoints on traversal_path equality, got:\n{sql}"
        );
    }

    #[test]
    fn path_finding_without_skip_orders_only_by_depth() {
        let query = r#"MATCH p = ANY SHORTEST (start:User {id: 1})-[:MEMBER_OF|CONTAINS*1..3]->(`end`:Project {id: 100})
            RETURN p
            LIMIT 10"#;

        let sql = compile_sql(query);

        assert!(
            !sql.contains("toString(paths._orbit_path)")
                && !sql.contains("toString(paths._orbit_edge_kinds)"),
            "path array tie-break sorting should only be emitted with SKIP, got:\n{sql}"
        );
    }

    #[test]
    fn wildcard_path_finding_filters_only_endpoint_hops_by_relationship_kind() {
        let query = r#"MATCH p = ANY SHORTEST (start:User {id: 1})-[*1..3]->(`end`:Project {id: 100})
            RETURN p
            LIMIT 10"#;

        let sql = compile_sql(query);

        assert!(
            sql.contains("gl_ci_edge") && sql.contains("gl_code_edge") && sql.contains("gl_edge"),
            "wildcard path finding should UNION ALL across all edge tables, got:\n{sql}"
        );
        assert!(
            sql.contains("e1.source_kind = 'User'"),
            "forward start must constrain source_kind = User, got:\n{sql}"
        );
        assert!(
            sql.contains("e1.target_kind = 'Project'"),
            "backward end must constrain target_kind = Project, got:\n{sql}"
        );
    }

    #[test]
    fn wildcard_traversal_infers_relationship_kinds_from_endpoint_entities() {
        let query = r#"MATCH (u:User {id: 1})-->(mr:MergeRequest)
            RETURN u, mr
            LIMIT 10"#;

        let sql = compile_sql(query);

        assert!(
            sql.contains("relationship_kind") && sql.contains("'AUTHORED'"),
            "wildcard traversal should infer concrete User to MergeRequest relationship kinds, got:\n{sql}"
        );
        assert!(
            !sql.contains("gl_code_edge"),
            "inferred SDLC relationship kinds should avoid scanning code edge table, got:\n{sql}"
        );
    }

    #[test]
    fn wildcard_neighbors_infers_relationship_kinds_from_center_entity() {
        let query = r#"MATCH (u:User {id: 1})-->(neighbor)
            RETURN u, neighbor
            LIMIT 10"#;

        let sql = compile_sql(query);

        assert!(
            sql.contains("relationship_kind") && sql.contains("'AUTHORED'"),
            "wildcard neighbors should infer concrete outgoing User relationship kinds, got:\n{sql}"
        );
        assert!(
            !sql.contains("gl_code_edge"),
            "inferred User relationship kinds should avoid scanning code edge table, got:\n{sql}"
        );
    }

    #[test]
    fn path_finding_clamps_settings_to_safety_floor() {
        let query = r#"MATCH p = ANY SHORTEST (start:User {id: 1})-[:MEMBER_OF|CONTAINS*1..3]->(`end`:Project {id: 100})
            RETURN p
            LIMIT 10"#;

        let sql = compile_sql(query);

        assert!(
            sql.contains("max_execution_time = 15"),
            "pathfinding must clamp max_execution_time to 15, got:\n{sql}"
        );
        assert!(
            sql.contains("max_memory_usage = 16106127360"),
            "pathfinding must clamp max_memory_usage to 15 GiB, got:\n{sql}"
        );
    }

    #[test]
    fn denorm_filter_uncovered_by_query_relationships_filters_node_table() {
        let query = r#"MATCH (mr:MergeRequest)-[:HAS_LABEL]->(cl:Label)
            WHERE mr.state = 'opened' AND cl.title = 'Community contribution'
            RETURN count(mr) AS c"#;
        let sql = compile_sql(query);
        assert!(
            !sql.contains("source_tags, 'state:opened'"),
            "uncovered state filter must not be pushed onto edge tags, got:\n{sql}"
        );
        assert!(
            sql.contains("gl_merge_request") && sql.contains("state = 'opened'"),
            "uncovered state filter must be enforced on the MR node table, got:\n{sql}"
        );
    }

    #[test]
    fn denorm_filter_not_pushed_onto_edges_written_without_tags() {
        for (entity, rel, target) in [
            ("WorkItem", "MENTIONS", "WorkItem"),
            ("MergeRequest", "HAS_NOTE", "Note"),
        ] {
            let query = format!(
                "MATCH (a:{entity})-[:{rel}]->(b:{target}) WHERE a.state = 'opened' RETURN count(a) AS n"
            );
            let sql = compile_sql(&query);
            assert!(
                !sql.contains("source_tags, 'state:opened'"),
                "{rel} edge rows carry no {entity} tags, filter must not be pushed, got:\n{sql}"
            );
            assert!(
                sql.contains("state = 'opened'"),
                "{rel}: state filter must be enforced on the node table, got:\n{sql}"
            );
        }
    }

    #[test]
    fn multi_hop_union_projects_denorm_tag_columns() {
        let query = r#"MATCH (a:WorkItem)-[:RELATED_TO*1..2]->(b:WorkItem)
            WHERE a.state = 'opened'
            RETURN a{.iid}, b{.iid}
            LIMIT 50"#;
        let sql = compile_sql(query);
        assert!(
            sql.contains("AS source_tags"),
            "multi-hop union must project source_tags, got:\n{sql}"
        );
        assert!(
            sql.contains("AS target_tags"),
            "multi-hop union must project target_tags, got:\n{sql}"
        );
        assert!(
            sql.contains("source_tags, 'state:opened'"),
            "anchor state filter must push onto the union's projected source_tags, got:\n{sql}"
        );
    }

    #[test]
    fn denorm_skips_rewrite_when_node_ids_present() {
        let query = r#"MATCH (u:User {id: 1})-[:REVIEWER]->(mr:MergeRequest)
            WHERE mr.id IN [1, 2, 3] AND mr.state = 'merged'
            RETURN u, mr
            LIMIT 10"#;

        let sql = compile_sql(query);

        assert!(
            sql.contains("_orbit_edge_0.target_id IN [1, 2, 3]"),
            "node_ids must be pushed to edge target_id filter, got:\n{sql}"
        );
        assert!(
            sql.contains("has(_orbit_edge_0.target_tags, 'state:merged')"),
            "denorm tag filter is applied alongside node_ids, got:\n{sql}"
        );
    }

    #[test]
    fn denorm_aggregation_count_with_filter_uses_edge_column() {
        let query = r#"MATCH (u:User {id: 1})-[:REVIEWER]->(mr:MergeRequest)
            WHERE mr.state = 'merged'
            RETURN u, count(mr) AS n
            LIMIT 10"#;

        let sql = compile_sql(query);

        assert!(
            sql.contains("has(_orbit_edge_0.target_tags, 'state:merged')"),
            "denorm filter must be pushed to edge target_tags, got:\n{sql}"
        );
        assert!(
            !sql.contains("_nf_mr"),
            "lowerer should not emit _nf_mr when filter is fully denormalized, got:\n{sql}"
        );
    }

    #[test]
    fn count_with_property_on_skip_target_drops_column_arg() {
        let query = r#"MATCH (p:Project)<-[:IN_PROJECT]-(wi:WorkItem {state: 'closed'})<-[:CLOSED]-(u:User)
            RETURN p, count(u.id) AS closers_count
            LIMIT 20"#;

        let sql = compile_sql(query);

        assert!(
            !sql.contains("COUNT(u.id)") && !sql.contains("countIf(u.id"),
            "must not count u.id when User is not hydrated, got:\n{sql}"
        );
        assert!(
            sql.contains("COUNT()") || sql.contains("countIf("),
            "Count over unhydrated target must collapse to bare COUNT()/countIf(cond), got:\n{sql}"
        );
    }

    #[test]
    fn denorm_preserves_role_gated_node_table_for_security() {
        let query = r#"MATCH (v:Vulnerability)-[:IN_PROJECT]->(proj:Project {id: 1})
            WHERE v.state = 'detected'
            RETURN proj, count(v) AS n
            LIMIT 10"#;

        let sql = compile(
            query,
            Frontend::Gql,
            &ONTOLOGY,
            &crate::testkit::admin_ctx(),
        )
        .expect("should compile")
        .base
        .render();

        assert!(
            sql.contains("gl_vulnerability"),
            "role-gated entity gl_vulnerability must NOT be pruned from FROM, got:\n{sql}"
        );
    }

    #[test]
    fn aggregation_group_by_property_emits_scalar_group_key() {
        let query = r#"MATCH (p:Project {id: 1})
            RETURN p.visibility_level, count(p) AS project_count
            LIMIT 10"#;

        let sql = compile_sql(query);

        assert!(
            sql.contains("p.visibility_level AS p_visibility_level"),
            "property group key must be selected as a scalar column, got:\n{sql}"
        );
        assert!(
            sql.contains("GROUP BY p.visibility_level"),
            "property group key must drive GROUP BY, got:\n{sql}"
        );
        assert!(
            !sql.contains("_orbit_p_id"),
            "property grouping should not emit node-group columns, got:\n{sql}"
        );
    }

    #[test]
    fn node_table_reads_use_final_for_latest_rows() {
        let queries = [
            (
                "traversal",
                r#"MATCH (mr:MergeRequest {state: 'merged'})
                    RETURN mr
                    LIMIT 10"#,
            ),
            (
                "aggregation",
                r#"MATCH (mr:MergeRequest {state: 'merged'})-[:IN_PROJECT]->(p:Project)
                    RETURN p, count(mr) AS merged_mrs
                    LIMIT 10"#,
            ),
            (
                "path_finding",
                r#"MATCH p = ANY SHORTEST (start:User {username: 'root'})-[:MEMBER_OF|CONTAINS*1..2]->(`end`:Project {id: 100})
                    RETURN p
                    LIMIT 10"#,
            ),
            (
                "neighbors",
                r#"MATCH (mr:MergeRequest)--(neighbor)
                    WHERE mr.title CONTAINS 'fix'
                    RETURN mr, neighbor
                    LIMIT 10"#,
            ),
        ];

        for (name, query) in queries {
            let compiled = compile(query, Frontend::Gql, &ONTOLOGY, &security_ctx())
                .unwrap_or_else(|err| panic!("{name} should compile: {err}"));
            let sql = compiled.base.render();
            assert!(
                sql.contains(" FINAL"),
                "{name} should use FINAL for node-table dedup, got:\n{sql}"
            );
            if name == "traversal" || name == "path_finding" || name == "neighbors" {
                assert!(
                    sql.contains(" FINAL"),
                    "{name} must still use FINAL for its primary node scan, got:\n{sql}"
                );
            }
        }
    }

    #[test]
    fn scope_anchor_aggregation_rejects_join_predicates() {
        for predicate in ["g.id = p.id", "p.id = g.id"] {
            let query =
                format!("MATCH (g:Group {{id: 100}})-[:CONTAINS]->(p:Project) WHERE {predicate}");
            let error = compile(
                &format!("{query} RETURN count(p) AS c LIMIT 20"),
                Frontend::Gql,
                &ONTOLOGY,
                &security_ctx(),
            )
            .unwrap_err();
            assert!(
                matches!(error, QueryError::Validation(ref message)
                    if message == "cross-node property comparisons are only supported in traversal queries"),
                "{error}"
            );
        }
    }

    #[test]
    fn scope_proofs_preserve_deeper_authorization_filters() {
        let mut context = SecurityContext::new(1, vec!["1/2/3/".into(), "1/2/4/".into()]).unwrap();
        context.scope_proofs =
            std::collections::HashMap::from([("p".into(), ScopeProof::literal("1/2/"))]);
        for (query, lookup, alias, literal_proof) in [
            (
                "MATCH (p:Project {name: 'example'}) RETURN p LIMIT 20",
                false,
                "p",
                true,
            ),
            (
                "MATCH (p:Project {id: 100}) RETURN p LIMIT 20",
                true,
                "p",
                true,
            ),
            (
                "MATCH (p:Project {full_path: 'parent/project'}) RETURN p LIMIT 20",
                true,
                "p",
                true,
            ),
            (
                "MATCH (p:MergeRequest {project_id: 100}) RETURN p LIMIT 20",
                true,
                "p",
                true,
            ),
            (
                "MATCH (g:Group {id: 100})-[:CONTAINS]->(p:Project) RETURN count(p) AS c LIMIT 20",
                true,
                "_orbit_edge_0",
                false,
            ),
        ] {
            let compiled = compile(query, Frontend::Gql, &ONTOLOGY, &context).unwrap();
            let sql = compiled.base.render();
            for path in ["1/2/3/", "1/2/4/"] {
                assert!(
                    sql.contains(&format!("startsWith({alias}.traversal_path, '{path}')")),
                    "{query}: {sql}"
                );
            }
            if literal_proof {
                assert!(
                    sql.contains(&format!("startsWith({alias}.traversal_path, '1/2/')")),
                    "{query}: {sql}"
                );
            }
            if lookup {
                assert!(sql.contains("argMaxOrNull"), "{query}: {sql}");
                for path in ["1/2/3/", "1/2/4/"] {
                    assert!(
                        sql.contains(&format!("startsWith(_scope.traversal_path, '{path}')")),
                        "{query}: {sql}"
                    );
                }
            }
        }
    }

    #[test]
    fn local_string_filters_fold_case_with_lower_on_both_sides() {
        for (operator, function) in [
            ("CONTAINS", "contains"),
            ("STARTS WITH", "starts_with"),
            ("ENDS WITH", "ends_with"),
        ] {
            let query = format!("MATCH (f:File) WHERE f.path {operator} 'ALI' RETURN f LIMIT 10");

            let sql = compile_local(&query, Frontend::Gql, &ONTOLOGY)
                .expect("should compile")
                .base
                .render();

            let folded = format!("{function}(lower(f.path), lower('ALI'))");
            assert!(sql.contains(&folded), "expected {folded}, got:\n{sql}");
        }
    }

    #[test]
    fn single_ref_cte_is_not_materialized() {
        let query = r#"MATCH (u:User {id: 1})
            RETURN u
            LIMIT 10"#;

        let sql = compile_sql(query);

        assert!(
            !sql.contains("MATERIALIZED"),
            "single-ref CTE must not use MATERIALIZED, got:\n{sql}"
        );
        assert!(
            !sql.contains("enable_materialized_cte"),
            "SETTINGS must not include enable_materialized_cte for non-materialized queries, got:\n{sql}"
        );
    }

    #[test]
    fn multi_filter_range_compiles_both_predicates() {
        let ontology = Arc::new(Ontology::load_embedded().expect("ontology must load"));
        let query = r#"MATCH (mr:MergeRequest {id: 1})
            WHERE mr.created_at >= '2026-04-01T00:00:00Z' AND mr.created_at < '2026-05-01T00:00:00Z'
            RETURN mr{.id, .created_at}
            LIMIT 10"#;
        let compiled =
            compile(query, Frontend::Gql, &ontology, &security_ctx()).expect("should compile");
        let sql = compiled.base.render();

        assert!(
            sql.contains(">=") && sql.contains("<"),
            "both range predicates must appear in SQL, got:\n{sql}"
        );
    }

    #[test]
    fn filter_on_virtual_column_compiles_without_sql_predicate() {
        let ontology = Arc::new(Ontology::load_embedded().expect("ontology must load"));
        let compiled = compile(
            r#"MATCH (f:File {id: 1})
                WHERE f.content = 'x'
                RETURN f{.path, .content}
                LIMIT 5"#,
            Frontend::Gql,
            &ontology,
            &security_ctx(),
        )
        .expect("virtual column filter should compile");

        let sql = compiled.base.render();
        assert!(
            !sql.contains("content"),
            "virtual column 'content' must not appear in SQL, got:\n{sql}"
        );

        if let HydrationPlan::Static(templates) = &compiled.hydration {
            assert!(
                templates.iter().any(|t| !t.virtual_filters.is_empty()),
                "hydration plan should carry virtual filters"
            );
        } else {
            panic!("expected static hydration plan");
        }
    }

    #[test]
    fn filter_on_virtual_column_without_selecting_it_injects_resolution() {
        let ontology = Arc::new(Ontology::load_embedded().expect("ontology must load"));
        let compiled = compile(
            r#"MATCH (f:File)
                WHERE f.id IN [1, 2] AND f.content CONTAINS 'needle' AND f.project_id = 1000
                RETURN f{.project_id, .path}
                LIMIT 10"#,
            Frontend::Gql,
            &ontology,
            &security_ctx(),
        )
        .expect("filter-only virtual column should compile");

        let HydrationPlan::Static(templates) = &compiled.hydration else {
            panic!(
                "filter-only virtual column must produce a static hydration plan, got {:?}",
                compiled.hydration
            );
        };
        let template = &templates[0];
        assert!(
            template
                .virtual_columns
                .iter()
                .any(|vc| vc.column_name == "content"),
            "content must be injected for resolution: {template:?}"
        );
        assert_eq!(template.filter_injected_columns, vec!["content"]);
        assert!(
            template
                .virtual_filters
                .iter()
                .any(|(col, _)| col == "content"),
            "content filter must be carried: {template:?}"
        );
    }

    #[test]
    fn filter_on_selected_virtual_column_is_not_marked_injected() {
        let ontology = Arc::new(Ontology::load_embedded().expect("ontology must load"));
        let compiled = compile(
            r#"MATCH (f:File {id: 1})
                WHERE f.content CONTAINS 'needle'
                RETURN f{.path, .content}
                LIMIT 5"#,
            Frontend::Gql,
            &ontology,
            &security_ctx(),
        )
        .expect("should compile");

        let HydrationPlan::Static(templates) = &compiled.hydration else {
            panic!("expected static hydration plan");
        };
        assert!(templates[0].filter_injected_columns.is_empty());
        assert_eq!(
            templates[0]
                .virtual_columns
                .iter()
                .filter(|vc| vc.column_name == "content")
                .count(),
            1
        );
    }

    #[test]
    fn filter_on_virtual_column_rejected_without_node_ids() {
        let ontology = Arc::new(Ontology::load_embedded().expect("ontology must load"));
        let err = compile(
            r#"MATCH (f:File)
                WHERE f.content CONTAINS 'needle' AND f.project_id = 1000
                RETURN f{.path}
                LIMIT 10"#,
            Frontend::Gql,
            &ontology,
            &security_ctx(),
        )
        .expect_err("virtual filter without node_ids should be rejected");

        assert!(err.is_client_safe());
        let msg = err.to_string();
        assert!(
            msg.contains("node_ids") && msg.contains("content"),
            "error should name node_ids and the column: {msg}"
        );
    }

    #[test]
    fn filter_on_virtual_column_rejected_for_aggregation() {
        let ontology = Arc::new(Ontology::load_embedded().expect("ontology must load"));
        let err = compile(
            r#"MATCH (f:File)
                WHERE f.content CONTAINS 'needle'
                RETURN f.language, count(f) AS total
                LIMIT 5"#,
            Frontend::Gql,
            &ontology,
            &security_ctx(),
        )
        .expect_err("virtual filter on aggregation should be rejected");

        assert!(err.is_client_safe());
        let msg = err.to_string();
        assert!(
            msg.contains("aggregation") && msg.contains("content"),
            "error should name the query type and column: {msg}"
        );
    }

    #[test]
    fn filter_on_virtual_column_rejected_for_neighbors() {
        let ontology = Arc::new(Ontology::load_embedded().expect("ontology must load"));
        let err = compile(
            r#"MATCH (f:File {id: 1})-->(neighbor)
                WHERE f.content CONTAINS 'needle'
                RETURN f, neighbor
                LIMIT 5"#,
            Frontend::Gql,
            &ontology,
            &security_ctx(),
        )
        .expect_err("virtual filter on neighbors should be rejected");

        assert!(err.is_client_safe());
        let msg = err.to_string();
        assert!(
            msg.contains("neighbors") && msg.contains("content"),
            "error should name the query type and column: {msg}"
        );
    }

    #[test]
    fn filter_on_virtual_column_rejects_unsupported_op() {
        let ontology = Arc::new(Ontology::load_embedded().expect("ontology must load"));
        let err = compile(
            r#"MATCH (f:File {id: 1})
                WHERE f.content > 'x'
                RETURN f
                LIMIT 5"#,
            Frontend::Gql,
            &ontology,
            &security_ctx(),
        )
        .expect_err("unsupported op on virtual column should be rejected");

        assert!(
            err.is_client_safe(),
            "virtual column op rejection should be client-safe"
        );
        let msg = err.to_string();
        assert!(
            msg.contains("content"),
            "error should name the column: {msg}"
        );
        assert!(msg.contains("gt"), "error should name the operator: {msg}");
    }

    #[test]
    fn final_prewhere_setting_follows_final_scans() {
        let sql = compile_sql(r#"MATCH (u:User {state: 'active'}) RETURN u LIMIT 10"#);
        assert!(sql.contains(" FINAL"), "{sql}");
        assert!(
            sql.contains("optimize_move_to_prewhere_if_final = 1"),
            "{sql}"
        );
        assert!(
            !sql.contains("use_index_for_in_with_subqueries_max_values"),
            "no IN (subquery) in this query, got:\n{sql}"
        );
    }

    #[test]
    fn in_subquery_index_cap_follows_in_select() {
        let sql = compile_sql(
            r#"MATCH (u:User {username: 'alice'})-[:AUTHORED]->(mr:MergeRequest)
                RETURN count(mr) AS c
                LIMIT 20"#,
        );
        assert!(sql.contains(" IN (SELECT"), "{sql}");
        assert!(
            sql.contains("use_index_for_in_with_subqueries_max_values = 1000000"),
            "{sql}"
        );
    }

    fn note_excerpt_chars(limit: u32) -> u32 {
        let ontology = Arc::new(Ontology::load_embedded().expect("ontology must load"));
        let compiled = compile(
            &format!("MATCH (n:Note {{id: 1}}) RETURN n.note LIMIT {limit}"),
            Frontend::Gql,
            &ontology,
            &security_ctx(),
        )
        .expect("note query should compile");
        let sql = compiled.base.render();
        let (_, after) = sql
            .split_once("substringUTF8(n.note, 1, ")
            .unwrap_or_else(|| panic!("expected a text excerpt, got:\n{sql}"));
        after
            .split(')')
            .next()
            .and_then(|chars| chars.parse().ok())
            .unwrap_or_else(|| panic!("expected excerpt length, got:\n{sql}"))
    }

    #[test]
    fn text_excerpt_length_shrinks_with_page_size() {
        let single_row = note_excerpt_chars(1);
        let wide_page = note_excerpt_chars(1000);
        assert!(single_row > 1_000_000, "single row got {single_row} chars");
        assert!(
            (1000..3000).contains(&wide_page),
            "wide page got {wide_page} chars"
        );
    }

    #[test]
    fn gql_where_id_equality_compiles_like_pattern_id() {
        assert_eq!(
            compile_sql("MATCH (w:WorkItem)-[:HAS_NOTE]->(n:Note) WHERE w.id = 7 RETURN n.note"),
            compile_sql("MATCH (w:WorkItem {id: 7})-[:HAS_NOTE]->(n:Note) RETURN n.note"),
        );
    }

    #[test]
    fn neighbors_where_id_compiles_like_pattern_id() {
        assert_eq!(
            compile_sql("MATCH (w:WorkItem)--(m) WHERE w.id = 7 RETURN w, m LIMIT 30"),
            compile_sql("MATCH (w:WorkItem {id: 7})--(m) RETURN w, m LIMIT 30"),
        );
    }

    #[test]
    fn multi_id_neighbors_center_keeps_one_fused_scan() {
        let sql = compile_sql(
            "MATCH (g:Group)-[r:MEMBER_OF|CONTAINS]-(m) WHERE g.id IN [1, 2] RETURN g, r, m LIMIT 30",
        );

        assert!(sql.contains("_orbit_fused"), "{sql}");
    }

    #[test]
    fn pinned_neighbors_center_scans_each_arm_by_its_own_id() {
        let sql = compile_sql(
            "MATCH (w:WorkItem)-[r:RELATED_TO|HAS_LABEL]-(m) WHERE w.id = 7 RETURN w, r, m LIMIT 30",
        );

        assert!(!sql.contains("_orbit_fused"), "{sql}");
        assert!(sql.contains("(e.source_id = 7)"), "{sql}");
        assert!(sql.contains("(e.target_id = 7)"), "{sql}");
    }
    #[test]
    fn text_indexed_equality_also_matches_the_lowercase_index_expression() {
        let sql = compile_sql("MATCH (d:Definition) WHERE d.name = 'compile' RETURN d.name");

        assert!(sql.contains("(d.name = 'compile')"), "{sql}");
        assert!(sql.contains("(lower(d.name) = lower('compile'))"), "{sql}");
    }

    #[test]
    fn equality_without_text_index_or_with_a_sort_key_filter_stays_plain() {
        for query in [
            "MATCH (d:Definition) WHERE d.definition_type = 'Function' RETURN d.name",
            "MATCH (d:Definition) WHERE d.name <> 'compile' RETURN d.name",
            "MATCH (d:Definition) WHERE d.project_id = 9 AND d.name = 'compile' RETURN d.name",
        ] {
            let sql = compile_sql(query);
            assert!(!sql.contains("lower(d."), "{query}:\n{sql}");
        }
    }
}
