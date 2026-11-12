//! Graph Query Compiler
//!
//! Compiles graph queries into parameterized ClickHouse SQL.
//!
//! # Pipeline
//!
//! ```text
//! raw → GQL surface → logical plan → compiler phases → SQL
//! ```
//!
//! # Example
//!
//! ```rust,no_run
//! use std::sync::Arc;
//! use compiler::compile;
//! use compiler::orbit_authorization::{Caller, Grants, ScopeSet};
//! use query_data_model::{Catalog, ClickHouse};
//!
//! let data_model = Arc::new(Catalog::<ClickHouse>::embedded().unwrap());
//!
//! let grants = Grants::new(Caller::new("user:1", false, ()))
//!     .with("global", "read", ScopeSet::All);
//!
//! let query = "MATCH (u:User {id: 1}) RETURN u.username LIMIT 10";
//!
//! let limits = orbit_server_config::AppConfig::embedded_defaults().query.limits;
//! let result = compile(query, &data_model, &grants, &limits).unwrap();
//! println!("SQL: {}", result.base.sql);
//! ```

pub mod analytics;
pub mod constants;
pub mod error;
pub mod hydration;
pub mod metrics;

#[cfg(test)]
mod test_grants;
pub mod tp_bounds;
pub mod types;

pub mod config;
pub mod dag;
pub mod logical;
pub mod passes;
pub mod remote;
pub mod surface;

pub use constants::{
    EDGE_DST_KIND_SUFFIX, EDGE_DST_SUFFIX, EDGE_KIND_SUFFIX, EDGE_SRC_KIND_SUFFIX, EDGE_SRC_SUFFIX,
    edge_kinds_column, hydration_node_alias, internal_column_prefix, neighbor_id_column,
    neighbor_is_outgoing_column, neighbor_type_column, path_column, relationship_type_column,
};
pub use error::{QueryError, RejectionReason, Result};
pub use logical::DynamicColumnMode;
pub use metrics::{METRICS, QueryEngineMetrics};
use orbit_authorization::Grants;
pub use orbit_server_config::QueryLimits;
pub use query_data_model::Direction;
pub use query_data_model::plan::logical::Operator;
use query_data_model::{Catalog, ClickHouse, DuckDb};
pub use types::QueryType;

pub use analytics::ExecMetrics;
pub use analytics::QueryStats;
pub use hydration::{HydrationArm, HydrationCompileOptions, HydrationRequest};
pub use orbit_authorization;
pub use passes::codegen::{
    CompiledQueryContext, ParamValue, ParameterizedQuery, SqlDialect, ddl::generate_local_ddl,
};
pub use passes::hydrate::{
    DynamicEntityColumns, HydrationKind, HydrationPlan, HydrationTemplate, VirtualColumnRequest,
    VirtualFilter, generate_hydration_plan,
};
pub use passes::result_contract::{
    EdgeMeta, EntityAuthConfig, GroupColumn, KeyKind, KeySource, MeasureColumn, RedactionNode,
    RelationshipColumn, RelationshipColumnKind, ResultContext, ResultShape, entity_auth,
};
pub use surface::gql;

use metrics::CountErr;
use std::sync::Arc;

use config::CompilerCtx as _;

fn output<T>(produced: Option<T>) -> Result<T> {
    produced.ok_or_else(|| {
        error::QueryError::PipelineInvariant("pipeline did not produce output".into())
    })
}

/// The server's top-level pipeline over its graph connection alone: parse, normalize,
/// restrict, admit, split into stages.
pub fn split(
    source: dag::Source<'_>,
    data_model: &Arc<Catalog<ClickHouse>>,
    grants: &Grants,
    limits: &QueryLimits,
) -> Result<dag::Dag> {
    let frontend = dag::Frontend::Server(Arc::clone(data_model));
    let connections = graph_only(data_model);
    let env = dag::Env {
        frontend: &frontend,
        connections: &connections,
        grants,
        limits,
        trace: false,
    };
    dag::split_query(source, &env)
}

/// A deployment that serves its graph connection alone.
pub fn graph_only(data_model: &Arc<Catalog<ClickHouse>>) -> dag::Connections {
    dag::Connections::new(vec![dag::Peer::ClickHouse(Arc::clone(data_model))])
}

fn statement(stage: dag::Stage) -> Result<query_data_model::surface::Statement> {
    match stage.query {
        dag::StageQuery::Gql(statement) => Ok(statement),
        _ => Err(error::QueryError::PipelineInvariant(
            "the stage plans from no statement".into(),
        )),
    }
}

fn clickhouse_stage(
    stage: dag::Stage,
    data_model: &Arc<Catalog<ClickHouse>>,
    grants: &Grants,
    limits: &QueryLimits,
) -> Result<config::GqlClickhouseCtx> {
    let mut ctx =
        config::GqlClickhouseCtx::new(grants.clone(), *limits, false, true, Arc::clone(data_model));
    ctx.set_statement(statement(stage)?);
    config::run_gql_clickhouse(&mut ctx).count_err()?;
    Ok(ctx)
}

/// The `(gql, clickhouse)` stage sub-pipeline, from tp_bounds through codegen.
pub fn compile_stage(
    stage: dag::Stage,
    data_model: &Arc<Catalog<ClickHouse>>,
    grants: &Grants,
    limits: &QueryLimits,
) -> Result<CompiledQueryContext> {
    output(clickhouse_stage(stage, data_model, grants, limits)?.take_output())
}

/// Each CTE body of the compiled ClickHouse plan as a statement tests can EXPLAIN.
pub fn subquery_statements(
    raw: &str,
    data_model: &Arc<Catalog<ClickHouse>>,
    grants: &Grants,
    limits: &QueryLimits,
) -> Result<Vec<passes::codegen::SubqueryStatement>> {
    let stage = split(dag::Source::Text(raw), data_model, grants, limits)?.into_single()?;
    let ctx = clickhouse_stage(stage, data_model, grants, limits)?;
    let plan = output(ctx.relational().as_ref())?;
    Ok(passes::codegen::subquery_statements(plan, data_model))
}

/// Compiles a query that reads the ClickHouse connection alone: its one stage.
#[must_use = "the compiled query context should be used"]
pub fn compile(
    raw: &str,
    data_model: &Arc<Catalog<ClickHouse>>,
    grants: &Grants,
    limits: &QueryLimits,
) -> Result<CompiledQueryContext> {
    let stage = split(dag::Source::Text(raw), data_model, grants, limits)?.into_single()?;
    compile_stage(stage, data_model, grants, limits)
}

/// Compile a graph query into DuckDB SQL for local execution, as a caller who holds no scope:
/// local stores are trusted, so their reads need none.
#[must_use = "the compiled query context should be used"]
pub fn compile_local(
    raw: &str,
    data_model: &Arc<Catalog<DuckDb>>,
    limits: &QueryLimits,
) -> Result<CompiledQueryContext> {
    let connections = dag::Connections::new(vec![dag::Peer::DuckDb(Arc::clone(data_model))]);
    let mut top = config::GqlLocalCtx::new(*limits, connections, false, Arc::clone(data_model));
    top.set_raw(raw.to_string());
    let stage = config::run_gql_local(&mut top)
        .and_then(|()| output(top.take_dag()))
        .and_then(dag::Dag::into_single)
        .and_then(statement)
        .count_err()?;
    let grants = Grants::new(orbit_authorization::Caller::new("cli", true, ()));
    let mut ctx = config::GqlDuckdbCtx::new(grants, *limits, false, Arc::clone(data_model));
    ctx.set_statement(stage);
    config::run_gql_duckdb(&mut ctx)
        .and_then(|()| output(ctx.take_output()))
        .count_err()
}

/// Parse and admit a query without a caller, as startup checks of shipped queries do.
pub fn check(raw: &str, data_model: &Arc<Catalog<ClickHouse>>, limits: &QueryLimits) -> Result<()> {
    let mut ctx = config::GqlCheckCtx::new(*limits, graph_only(data_model), Arc::clone(data_model));
    ctx.set_raw(raw.to_string());
    config::run_gql_check(&mut ctx).count_err()
}

/// The grants of a caller that holds every permission of the model on `roots`, for tools and
/// tests that compile without a deployment's providers: hierarchy scopes answer `roots`, global
/// scopes `All`.
pub fn uniform_grants(model: &query_data_model::Model, admin: bool, roots: &[&str]) -> Grants {
    use orbit_authorization::{Caller, ScopeSet};
    use query_data_model::catalog::ScopeKind;
    let access = model.access();
    let roots: Vec<String> = roots.iter().map(|root| root.to_string()).collect();
    let mut grants = Grants::new(Caller::new("tool", admin, ()));
    for (scope, permission) in access.needs() {
        let (scope, permission) = (access.scope(scope), access.permission(permission));
        let set = match scope.kind {
            ScopeKind::Global => ScopeSet::All,
            _ => ScopeSet::Subtrees(roots.clone()),
        };
        grants = grants.with(&scope.name, &permission.name, set);
    }
    grants
}

/// The `(hydration, clickhouse)` stage sub-pipeline: restrict, plan, settings, security, security
/// check, codegen.
pub fn compile_hydration(
    request: HydrationRequest,
    data_model: &Arc<Catalog<ClickHouse>>,
    grants: &Grants,
    limits: &QueryLimits,
) -> Result<ParameterizedQuery> {
    hydration_stage(request, data_model, grants, limits, false).map(|(query, _, _)| query)
}

/// The hydration stage's query, with its planned and emitted plans when traced.
pub(crate) fn hydration_stage(
    mut request: HydrationRequest,
    data_model: &Arc<Catalog<ClickHouse>>,
    grants: &Grants,
    limits: &QueryLimits,
    trace: bool,
) -> Result<(ParameterizedQuery, Vec<String>, Vec<String>)> {
    for arm in &mut request.arms {
        arm.scope_on(data_model);
    }
    let mut ctx =
        config::HydrationClickhouseCtx::new(grants.clone(), *limits, trace, Arc::clone(data_model));
    ctx.set_hydration(request);
    config::run_hydration_clickhouse(&mut ctx).count_err()?;
    if ctx.take_proved() != Some(true) {
        return Err(error::QueryError::Security(
            "the hydration stage carries no proof that each read has its policy".into(),
        ));
    }
    let emitted = match (trace, ctx.relational()) {
        (true, Some(plan)) => dag::describe(&plan.plan),
        _ => Vec::new(),
    };
    let planned = ctx.take_planned().unwrap_or_default();
    Ok((output(ctx.take_hydration_query())?, planned, emitted))
}

#[cfg(test)]
pub(crate) mod testkit {
    use std::sync::LazyLock;

    use orbit_authorization::Grants;
    use query_data_model::Model;

    static MODEL: LazyLock<std::sync::Arc<Model>> =
        LazyLock::new(|| query_data_model::Packages::embedded().load().unwrap());

    pub fn non_admin_ctx() -> Grants {
        crate::test_grants::reporter(&MODEL, &["1/"])
    }

    pub fn admin_ctx() -> Grants {
        crate::test_grants::grants(&MODEL, true, &["1/"], &[])
    }

    pub fn limits() -> &'static orbit_server_config::QueryLimits {
        static LIMITS: LazyLock<orbit_server_config::QueryLimits> = LazyLock::new(|| {
            orbit_server_config::query::init_embedded_defaults();
            orbit_server_config::AppConfig::embedded_defaults()
                .query
                .limits
        });
        &LIMITS
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use query_data_model::implementations::clickhouse::spec::ColumnSpec;
    use query_data_model::spec::{ColumnStorage, Fragment, Fragments};
    use std::sync::LazyLock;

    static MODEL: LazyLock<Arc<Catalog<ClickHouse>>> =
        LazyLock::new(|| Arc::new(Catalog::<ClickHouse>::embedded().expect("model must load")));
    static LOCAL: LazyLock<Arc<Catalog<DuckDb>>> =
        LazyLock::new(|| Arc::new(Catalog::<DuckDb>::embedded().expect("model must load")));

    fn security_ctx() -> orbit_authorization::Grants {
        crate::testkit::non_admin_ctx()
    }

    fn compile_sql(query: &str) -> String {
        compile(query, &MODEL, &security_ctx(), crate::testkit::limits())
            .expect("should compile")
            .base
            .render()
    }

    #[test]
    fn relationship_filter_keeps_edge_column_type() {
        let query = "MATCH (u:User {id: 1})-[:MEMBER_OF {target_id: 2}]->(p:Project) RETURN u, p";

        let compiled = compile(query, &MODEL, &security_ctx(), crate::testkit::limits()).unwrap();
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
        let err = compile(
            "not a query",
            &MODEL,
            &security_ctx(),
            crate::testkit::limits(),
        )
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
        let query = "MATCH (x:NotARealEntity) RETURN x{.id} LIMIT 1";
        let before = crate::metrics::COUNT_ERR_HITS.load(Ordering::Relaxed);
        let err = compile(query, &MODEL, &security_ctx(), crate::testkit::limits())
            .expect_err("must reject");
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

    fn packaged_model(fragments: &Fragments) -> Arc<Catalog<ClickHouse>> {
        let model = Arc::new(query_data_model::Model::load(fragments).expect("model must load"));
        Arc::new(Catalog::<ClickHouse>::from_model(&model).expect("model must bind"))
    }

    fn versioned(fragments: Fragments, prefix: &str) -> Fragments {
        query_data_model::implementations::clickhouse::versioned(fragments, prefix)
            .expect("prefix must apply")
    }

    fn embedded_fragments() -> Fragments {
        query_data_model::Packages::embedded()
            .fragments()
            .expect("packages must parse")
    }

    #[test]
    fn compile_with_versioned_packages_produces_prefixed_sql() {
        let prefixed = packaged_model(&versioned(embedded_fragments(), "v1_"));

        let query = "MATCH (g:Group {id: 1}) RETURN g{.name} LIMIT 1";
        let compiled = compile(query, &prefixed, &security_ctx(), crate::testkit::limits())
            .expect("should compile");
        let sql = compiled.base.render();

        assert!(
            sql.contains("v1_gl_group"),
            "search SQL should use prefixed node table v1_gl_group, got: {sql}"
        );
    }

    #[test]
    fn compile_uses_supplied_model_for_scoped_user_table() {
        let scoped = || {
            let mut fragments = embedded_fragments();
            for fragment in fragments.iter_mut() {
                match fragment {
                    Fragment::Graph(graph) => {
                        if let Some(user) = graph.entities.get_mut("User") {
                            let property = orbit_utils::yaml::from_str("string").unwrap();
                            user.properties.insert("traversal_path".into(), property);
                        }
                    }
                    Fragment::Storage(storage) if storage.store == "graph" => {
                        if let Some(user) = storage.entities.get_mut("User") {
                            user.path_column = None;
                            let column = ColumnSpec {
                                storage_type: "String".into(),
                                default: None,
                                codec: Vec::new(),
                            };
                            user.columns.insert(
                                "traversal_path".into(),
                                ColumnStorage {
                                    immutable: false,
                                    column: None,
                                    selectivity: None,
                                    stored_as_text: false,
                                    backend: Box::new(Some(column)),
                                },
                            );
                        }
                    }
                    Fragment::Authorization(access) => {
                        if let Some(user) = access.entities.get_mut("User") {
                            user.scope = "namespace".into();
                            let policy = orbit_utils::yaml::from_str("{scope_key: traversal_path}");
                            user.enforce.insert("clickhouse".into(), policy.unwrap());
                            user.properties.hidden.push("traversal_path".into());
                        }
                    }
                    _ => {}
                }
            }
            fragments
        };
        let query = "MATCH (u:User {id: 1}) RETURN u{.id} LIMIT 1";

        for (model, expected_table) in [
            (packaged_model(&scoped()), "gl_user"),
            (packaged_model(&versioned(scoped(), "v1_")), "v1_gl_user"),
        ] {
            let sql = compile(query, &model, &security_ctx(), crate::testkit::limits())
                .expect("should compile")
                .base
                .render();
            assert!(
                sql.contains("startsWith(u.traversal_path"),
                "supplied model must keep the User alias scoped, got:\n{sql}"
            );
            assert!(
                sql.contains(expected_table),
                "model should render {expected_table}, got:\n{sql}"
            );
        }
    }

    #[test]
    fn compile_uses_each_models_global_tables_and_keeps_join_filters() {
        let query = "MATCH (user:User {id: 1, username: 'alice'})-[:MEMBER_OF]->(project:Project {name: 'orbit'}) RETURN user, project LIMIT 1";

        let renamed = || {
            let mut fragments = embedded_fragments();
            for fragment in fragments.iter_mut() {
                if let Fragment::Storage(storage) = fragment
                    && let Some(user) = storage.entities.get_mut("User")
                    && storage.store == "graph"
                {
                    user.table = Some("gl_renamed_user".into());
                }
            }
            fragments
        };

        for (model, expected_user_table) in [
            (packaged_model(&renamed()), "gl_renamed_user"),
            (Arc::clone(&MODEL), "gl_user"),
            (
                packaged_model(&versioned(renamed(), "v1_")),
                "v1_gl_renamed_user",
            ),
            (
                packaged_model(&versioned(embedded_fragments(), "v2_")),
                "v2_gl_user",
            ),
            (packaged_model(&renamed()), "gl_renamed_user"),
        ] {
            let sql = compile(query, &model, &security_ctx(), crate::testkit::limits())
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
        let query = "MATCH (parent:Pipeline {id: 1})-[:CHILD_OF]->(child:Pipeline) RETURN parent, count(child) AS children LIMIT 10";

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
        let query = "MATCH (mr:MergeRequest {id: 490855697})-[:HAS_LABEL]->(label:Label {title: 'group::source code'}) RETURN count(mr) AS n LIMIT 1";

        let sql = compile_sql(query);

        assert!(
            !sql.contains("argMax("),
            "single-hop edge scan must not dedup, got:\n{sql}"
        );
    }

    #[test]
    fn path_finding_code_filtered_endpoints_prune_by_traversal_path() {
        let query = "MATCH path = ANY SHORTEST (start:Definition {name: 'compile'})-[:CALLS*1..3]->(`end`:Definition {name: 'run_query'}) RETURN path LIMIT 10";

        let sql = compile_sql(query);

        assert!(
            sql.contains("_orbit_path_scope"),
            "code path finding should compute candidate traversal paths, got:\n{sql}"
        );
        assert!(
            sql.contains("_orbit_e1.traversal_path = _orbit_e2.traversal_path"),
            "code edge self-joins should stay within one traversal_path, got:\n{sql}"
        );
        assert!(
            sql.contains("_orbit_forward.traversal_path = _orbit_backward.traversal_path"),
            "frontier intersection should stay within one traversal_path, got:\n{sql}"
        );
        assert!(
            sql.contains("_orbit_forward") && sql.contains("FROM _orbit_nf_start"),
            "forward CTE should seed from _orbit_nf_start, got:\n{sql}"
        );
        assert!(
            sql.contains("traversal_path IN (SELECT traversal_path FROM _orbit_path_scope)"),
            "edge scans should use traversal_path scope, got:\n{sql}"
        );
        assert!(
            sql.contains("_orbit_e1.source_kind = 'Definition'"),
            "forward edge scans should constrain source_kind = Definition, got:\n{sql}"
        );
    }

    #[test]
    fn path_finding_cross_level_endpoints_union_traversal_scope() {
        let query = "MATCH path = ANY SHORTEST (from:Group {id: 9970})-[:CONTAINS*1..3]->(to:Project {id: 278964}) RETURN path";
        let sql = compile_sql(query);
        assert!(
            sql.contains("_orbit_path_scope"),
            "cross-level path finding should still compute a scope, got:\n{sql}"
        );
        assert!(
            sql.contains("UNION ALL SELECT _orbit_path_scope_end.traversal_path"),
            "scope must UNION both endpoints' traversal paths, got:\n{sql}"
        );
        assert!(
            !sql.contains("_path_scope_start.traversal_path = _path_scope_end.traversal_path"),
            "scope must not intersect endpoints on traversal_path equality, got:\n{sql}"
        );
    }

    #[test]
    fn path_finding_without_skip_orders_only_by_depth() {
        let query = "MATCH path = ANY SHORTEST (start:User {id: 1})-[:MEMBER_OF|CONTAINS*1..3]->(`end`:Project {id: 100}) RETURN path LIMIT 10";

        let sql = compile_sql(query);

        assert!(
            !sql.contains("toString(paths._orbit_path)")
                && !sql.contains("toString(paths._orbit_edge_kinds)"),
            "path array tie-break sorting should only be emitted with SKIP, got:\n{sql}"
        );
    }

    #[test]
    fn wildcard_traversal_infers_relationship_kinds_from_endpoint_entities() {
        let query = "MATCH (u:User {id: 1})-->(mr:MergeRequest) RETURN u, mr LIMIT 10";

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
        let query = "MATCH (u:User {id: 1})-->(neighbor) RETURN u, neighbor LIMIT 10";

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
    fn neighbors_records_a_center_only_for_one_id() {
        let center = |ids: &str| {
            let query = format!(
                "MATCH (u:User)-->(neighbor) WHERE u.id IN {ids} RETURN u, neighbor LIMIT 10"
            );
            let compiled =
                compile(&query, &MODEL, &security_ctx(), crate::testkit::limits()).unwrap();
            let context = &compiled.base.result_context;
            (
                context
                    .neighbors_center()
                    .map(|(entity, id)| (entity.to_string(), id.clone())),
                context.label("User").map(str::to_string),
            )
        };
        assert_eq!(
            center("[1]"),
            (
                Some(("User".into(), orbit_authorization::Key::Int(1))),
                Some("username".into())
            )
        );
        assert_eq!(center("[1, 2]"), (None, None));
    }

    #[test]
    fn path_finding_clamps_settings_to_safety_ceiling() {
        let query = "MATCH path = ANY SHORTEST (start:User {id: 1})-[:MEMBER_OF|CONTAINS*1..3]->(`end`:Project {id: 100}) RETURN path LIMIT 10";

        let sql = compile_sql(query);

        assert!(
            sql.contains("max_execution_time = 15"),
            "pathfinding max_execution_time must not exceed 15, got:\n{sql}"
        );
        assert!(
            sql.contains("max_memory_usage = 16106127360"),
            "pathfinding max_memory_usage must not exceed 15 GiB, got:\n{sql}"
        );
    }

    #[test]
    fn denorm_filter_uncovered_by_query_relationships_filters_node_table() {
        let query = "MATCH (mr:MergeRequest {state: 'opened'})-[:HAS_LABEL]->(cl:Label {title: 'Community contribution'}) RETURN count(mr) AS c";
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
                "MATCH (a:{entity} {{state: 'opened'}})-[:{rel}]->(b:{target}) RETURN count(a) AS n"
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
        let query = "MATCH (a:WorkItem {state: 'opened'})-[:RELATED_TO*1..2]->(b:WorkItem) RETURN a{.iid}, b{.iid} LIMIT 50";
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
        let query = "MATCH (u:User {id: 1})-[:REVIEWER]->(mr:MergeRequest {state: 'merged'}) WHERE mr.id IN [1, 2, 3] RETURN u, mr LIMIT 10";

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
        let query = "MATCH (u:User {id: 1})-[:REVIEWER]->(mr:MergeRequest {state: 'merged'}) RETURN u, count(mr) AS n LIMIT 10";

        let sql = compile_sql(query);

        assert!(
            sql.contains("has(_orbit_edge_0.target_tags, 'state:merged')"),
            "denorm filter must be pushed to edge target_tags, got:\n{sql}"
        );
        assert!(
            !sql.contains("_orbit_nf_mr"),
            "lowerer should not emit _orbit_nf_mr when filter is fully denormalized, got:\n{sql}"
        );
    }

    #[test]
    fn count_with_property_on_skip_target_drops_column_arg() {
        let query = "MATCH (wi:WorkItem {state: 'closed'})-[:IN_PROJECT]->(p:Project), (u:User)-[:CLOSED]->(wi) RETURN p, count(u.id) AS closers_count LIMIT 20";

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
        let query = "MATCH (v:Vulnerability {state: 'detected'})-[:IN_PROJECT]->(proj:Project {id: 1}) RETURN proj, count(v) AS n LIMIT 10";

        let sql = compile(
            query,
            &MODEL,
            &crate::testkit::admin_ctx(),
            crate::testkit::limits(),
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
        let query = "MATCH (p:Project {id: 1}) RETURN p.visibility_level, count(p) AS project_count LIMIT 10";

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
                "MATCH (mr:MergeRequest {state: 'merged'}) RETURN mr LIMIT 10",
            ),
            (
                "aggregation",
                "MATCH (mr:MergeRequest {state: 'merged'})-[:IN_PROJECT]->(p:Project) RETURN p, count(mr) AS merged_mrs LIMIT 10",
            ),
            (
                "path_finding",
                "MATCH path = ANY SHORTEST (start:User {username: 'root'})-[:MEMBER_OF|CONTAINS*1..2]->(`end`:Project {id: 100}) RETURN path LIMIT 10",
            ),
            (
                "neighbors",
                "MATCH (mr:MergeRequest)--(neighbor) WHERE mr.title CONTAINS 'fix' RETURN mr, neighbor LIMIT 10",
            ),
        ];

        for (name, query) in queries {
            let compiled = compile(query, &MODEL, &security_ctx(), crate::testkit::limits())
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
                &MODEL,
                &security_ctx(),
                crate::testkit::limits(),
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
    fn local_string_filters_fold_case_with_lower_on_both_sides() {
        for (operator, function) in [
            ("CONTAINS", "contains"),
            ("STARTS WITH", "starts_with"),
            ("ENDS WITH", "ends_with"),
        ] {
            let query = format!("MATCH (f:File) WHERE f.path {operator} 'ALI' RETURN f LIMIT 10");

            let sql = compile_local(&query, &LOCAL, crate::testkit::limits())
                .expect("should compile")
                .base
                .render();

            let folded = format!("{function}(lower(f.path), lower('ALI'))");
            assert!(sql.contains(&folded), "expected {folded}, got:\n{sql}");
        }
    }

    #[test]
    fn single_ref_cte_is_not_materialized() {
        let query = "MATCH (u:User {id: 1}) RETURN u LIMIT 10";

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
        let model = Arc::clone(&MODEL);
        let query = "MATCH (mr:MergeRequest {id: 1}) WHERE mr.created_at >= '2026-04-01T00:00:00Z' AND mr.created_at < '2026-05-01T00:00:00Z' RETURN mr{.id, .created_at} LIMIT 10";
        let compiled = compile(query, &model, &security_ctx(), crate::testkit::limits())
            .expect("should compile");
        let sql = compiled.base.render();

        assert!(
            sql.contains(">=") && sql.contains("<"),
            "both range predicates must appear in SQL, got:\n{sql}"
        );
    }

    #[test]
    fn filter_on_virtual_column_compiles_without_sql_predicate() {
        let model = Arc::clone(&MODEL);
        let compiled = compile(
            "MATCH (f:File {id: 1, content: 'x'}) RETURN f{.path, .content} LIMIT 5",
            &model,
            &security_ctx(),
            crate::testkit::limits(),
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
        let model = Arc::clone(&MODEL);
        let compiled = compile(
            "MATCH (f:File {project_id: 1000}) WHERE f.id IN [1, 2] AND f.content CONTAINS 'needle' RETURN f{.project_id, .path} LIMIT 10",
            &model,
            &security_ctx(), crate::testkit::limits(),
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
        let model = Arc::clone(&MODEL);
        let compiled = compile(
            "MATCH (f:File {id: 1}) WHERE f.content CONTAINS 'needle' RETURN f{.path, .content} LIMIT 5",
            &model,
            &security_ctx(), crate::testkit::limits(),
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
        let model = Arc::clone(&MODEL);
        let err = compile(
            "MATCH (f:File {project_id: 1000}) WHERE f.content CONTAINS 'needle' RETURN f{.path} LIMIT 10",
            &model,
            &security_ctx(), crate::testkit::limits(),
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
        let model = Arc::clone(&MODEL);
        let err = compile(
            "MATCH (f:File) WHERE f.content CONTAINS 'needle' RETURN f.language, count(f) AS total LIMIT 5",
            &model,
            &security_ctx(), crate::testkit::limits(),
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
        let model = Arc::clone(&MODEL);
        let err = compile(
            "MATCH (f:File {id: 1})-->(neighbor) WHERE f.content CONTAINS 'needle' RETURN f, neighbor LIMIT 5",
            &model,
            &security_ctx(), crate::testkit::limits(),
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
        let model = Arc::clone(&MODEL);
        let err = compile(
            "MATCH (f:File {id: 1}) WHERE f.content > 'x' RETURN f LIMIT 5",
            &model,
            &security_ctx(),
            crate::testkit::limits(),
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
    fn final_settings_follow_final_scans() {
        let sql = compile_sql("MATCH (u:User {state: 'active'}) RETURN u LIMIT 10");
        assert!(sql.contains(" FINAL"), "{sql}");
        for setting in [
            "optimize_move_to_prewhere_if_final = 1",
            "use_skip_indexes_if_final = 1",
            "use_skip_indexes_if_final_exact_mode = 1",
        ] {
            assert!(sql.contains(setting), "{sql}");
        }
        assert!(
            !sql.contains("use_index_for_in_with_subqueries_max_values"),
            "no IN (subquery) in this query, got:\n{sql}"
        );
    }

    #[test]
    fn in_subquery_index_cap_follows_in_select() {
        let sql = compile_sql(
            "MATCH (u:User {username: 'alice'})-[:AUTHORED]->(mr:MergeRequest) RETURN count(mr) AS c LIMIT 20",
        );
        assert!(sql.contains(" IN (SELECT"), "{sql}");
        assert!(
            sql.contains("use_index_for_in_with_subqueries_max_values = 1000000"),
            "{sql}"
        );
    }

    fn note_excerpt_chars(limit: u32) -> u32 {
        let model = Arc::clone(&MODEL);
        let compiled = compile(
            &format!("MATCH (n:Note {{id: 1}}) RETURN n{{.note}} LIMIT {limit}"),
            &model,
            &security_ctx(),
            crate::testkit::limits(),
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
        assert!(
            sql.contains("(_orbit_neighbor_edge.source_id = 7)"),
            "{sql}"
        );
        assert!(
            sql.contains("(_orbit_neighbor_edge.target_id = 7)"),
            "{sql}"
        );
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
