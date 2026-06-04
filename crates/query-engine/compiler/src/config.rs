//! Compiler pipeline configuration via `define_compiler_ctx!`.
//!
//! This is the SSOT declaration for the compiler's env fields, state fields,
//! phase grants, and pipeline presets. The macro generates the `CompilerCtx`
//! trait, per-pipeline context structs, and runner functions.
//!
//! Phase functions live below the macro invocation. Each takes
//! `&mut impl CompilerCtx` and delegates to the existing pass interiors.

use std::sync::Arc;

use gkg_server_config::QueryConfig;
use ontology::Ontology;

use crate::ast::Node;
use crate::error::{QueryError, Result};
use crate::input::Input;
use crate::passes::codegen::CompiledQueryContext;
use crate::passes::enforce::ResultContext;
use crate::passes::hydrate::HydrationPlan;
use crate::passes::plan::QueryPlan;
use crate::passes::{
    check, codegen, enforce, hydrate, lower, normalize, plan, restrict, security, settings,
    validate,
};
use crate::types::SecurityContext;

/// Unwrap an `Option<T>` from ctx, returning `PipelineInvariant` if `None`.
fn require<T>(opt: Option<T>, field: &str) -> Result<T> {
    opt.ok_or_else(|| QueryError::PipelineInvariant(format!("{field} not yet populated")))
}

compiler_pipeline_macros::define_compiler_ctx! {
    env {
        pub ontology: Arc<Ontology>,
        pub security_ctx: SecurityContext,
    }

    state {
        pub json: String,
        pub input: Input,
        pub query_plan: QueryPlan,
        pub node: Node,
        pub result_ctx: ResultContext,
        pub query_config: QueryConfig,
        pub hydration_plan: HydrationPlan,
        pub output: CompiledQueryContext,
    }

    phases {
        validate {
            reads_env: [ontology]
            mutates: [json, input]
        }
        normalize {
            reads_env: [ontology]
            mutates: [input]
        }
        restrict {
            reads_env: [ontology, security_ctx]
            mutates: [input]
        }
        plan {
            mutates: [input, query_plan]
        }
        lower {
            reads_state: [input]
            mutates: [query_plan, node]
        }
        enforce {
            reads_state: [input]
            mutates: [query_plan, node, result_ctx]
        }
        security {
            reads_env: [security_ctx, ontology]
            mutates: [node]
        }
        check {
            reads_env: [security_ctx]
            reads_state: [node]
        }
        hydrate_plan {
            reads_env: [ontology, security_ctx]
            reads_state: [input]
            mutates: [hydration_plan]
        }
        settings {
            reads_state: [input, node]
            mutates: [query_plan, query_config]
        }
        codegen {
            reads_state: [node, input]
            mutates: [result_ctx, query_config, hydration_plan, output]
        }
    }

    pipelines {
        clickhouse {
            env: [ontology, security_ctx]
            state: [json, input, query_plan, node, result_ctx, query_config, hydration_plan, output]
            phases: [validate, normalize, restrict, plan, lower, enforce, security, check, hydrate_plan, settings, codegen]
        }
        ch_hydration {
            env: [ontology, security_ctx]
            state: [input, query_plan, node, result_ctx, query_config, hydration_plan, output]
            phases: [restrict, plan, lower, enforce, settings, codegen]
        }
        validate_normalize {
            env: [ontology]
            state: [json, input]
            phases: [validate, normalize]
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Phase functions
// ─────────────────────────────────────────────────────────────────────────────

fn validate(ctx: &mut impl CompilerCtx) -> Result<()> {
    let json = require(ctx.take_json(), "json")?;
    let ontology = ctx.ontology();
    let v = validate::Validator::new(ontology);
    let value = v.check_json(&json)?;
    v.check_ontology(&value)?;
    let mut input: Input = serde_json::from_value(value)?;
    v.check_references(&input)?;
    v.annotate_filter_types(&mut input);
    ctx.set_input(input);
    Ok(())
}

fn normalize(ctx: &mut impl CompilerCtx) -> Result<()> {
    let input = require(ctx.take_input(), "input")?;
    ctx.set_input(normalize::normalize(input, ctx.ontology())?);
    Ok(())
}

fn restrict(ctx: &mut impl CompilerCtx) -> Result<()> {
    let ontology = ctx.ontology().clone();
    let security_ctx = ctx.security_ctx().clone();
    let mut input = require(ctx.take_input(), "input")?;
    restrict::restrict(&mut input, &ontology, &security_ctx)?;
    ctx.set_input(input);
    Ok(())
}

fn plan(ctx: &mut impl CompilerCtx) -> Result<()> {
    let mut input = require(ctx.take_input(), "input")?;
    // The hydration pipeline skips `normalize`, so source node sort keys (used
    // for LIMIT BY dedup) straight from the ontology when absent.
    if input.compiler.table_sort_keys.is_empty() {
        for node in ctx.ontology().nodes() {
            input
                .compiler
                .table_sort_keys
                .insert(node.destination_table.clone(), node.sort_key.clone());
        }
    }
    let query_plan = plan::plan(&mut input)?;
    ctx.set_input(input);
    ctx.set_query_plan(query_plan);
    Ok(())
}

fn lower(ctx: &mut impl CompilerCtx) -> Result<()> {
    let query_plan = require(ctx.take_query_plan(), "query_plan")?;
    let input = require(ctx.input().clone(), "input")?;
    let node = lower::emit(&query_plan, &input)?;
    ctx.set_query_plan(query_plan);
    ctx.set_node(node);
    Ok(())
}

fn enforce(ctx: &mut impl CompilerCtx) -> Result<()> {
    let query_plan = require(ctx.take_query_plan(), "query_plan")?;
    let node_edge_col = query_plan.node_edge_mappings();
    ctx.set_query_plan(query_plan);
    let mut node = require(ctx.take_node(), "node")?;
    let input = require(ctx.input().clone(), "input")?;
    let result_context = enforce::enforce_return(&mut node, &input, &node_edge_col)?;
    ctx.set_node(node);
    ctx.set_result_ctx(result_context);
    Ok(())
}

fn security(ctx: &mut impl CompilerCtx) -> Result<()> {
    let security_ctx = ctx.security_ctx().clone();
    let ontology = ctx.ontology().clone();
    let mut node = require(ctx.take_node(), "node")?;
    security::apply_security_context(&mut node, &security_ctx, &ontology)?;
    ctx.set_node(node);
    Ok(())
}

fn check(ctx: &mut impl CompilerCtx) -> Result<()> {
    let node = require(ctx.node().clone(), "node")?;
    check::check_ast(&node, ctx.security_ctx())
}

fn hydrate_plan(ctx: &mut impl CompilerCtx) -> Result<()> {
    let input = require(ctx.input().clone(), "input")?;
    let plan = hydrate::generate_hydration_plan(&input, ctx.ontology(), ctx.security_ctx());
    ctx.set_hydration_plan(plan);
    Ok(())
}

fn settings(ctx: &mut impl CompilerCtx) -> Result<()> {
    let input = require(ctx.input().clone(), "input")?;
    let query_type: &str = input.query_type.into();
    let has_cursor = input.cursor.is_some();
    let mut config = settings::resolve(query_type, has_cursor);

    let node = require(ctx.node().clone(), "node")?;
    if let Node::Query(q) = &node
        && q.ctes.iter().any(|c| c.materialized)
    {
        config.compiler_derived.enable_materialized_cte = true;
    }

    let query_plan = require(ctx.take_query_plan(), "query_plan")?;
    if query_plan.hops.len() >= 3 {
        config.compiler_derived.join_order_algorithm = Some("dpsize".into());
    }
    ctx.set_query_plan(query_plan);
    ctx.set_query_config(config);
    Ok(())
}

fn codegen(ctx: &mut impl CompilerCtx) -> Result<()> {
    let result_context = require(ctx.take_result_ctx(), "result_ctx")?;
    let query_config = ctx.take_query_config().unwrap_or_default();
    let hydration = ctx.take_hydration_plan().unwrap_or(HydrationPlan::None);
    let node = require(ctx.node().clone(), "node")?;
    let input = require(ctx.input().clone(), "input")?;
    let base = codegen::codegen(&node, result_context, query_config)?;
    let query_type = input.query_type;
    ctx.set_output(CompiledQueryContext {
        query_type,
        base,
        hydration,
        input,
    });
    Ok(())
}
