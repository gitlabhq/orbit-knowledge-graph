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
use crate::error::Result;
use crate::input::{Input, QueryType};
use crate::passes::codegen::CompiledQueryContext;
use crate::passes::enforce::ResultContext;
use crate::passes::hydrate::HydrationPlan;
use crate::passes::plan::QueryPlan;
use crate::passes::{
    check, codegen, enforce, hydrate, lower, normalize, plan, restrict, security, settings,
    validate,
};
use crate::types::SecurityContext;

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
        validate_phase {
            reads_env: [ontology]
            mutates: [json, input]
        }
        normalize_phase {
            reads_env: [ontology]
            mutates: [input]
        }
        restrict_phase {
            reads_env: [ontology, security_ctx]
            mutates: [input]
        }
        plan_phase {
            mutates: [input, query_plan]
        }
        lower_phase {
            reads_state: [input]
            mutates: [query_plan, node]
        }
        enforce_phase {
            reads_state: [input]
            mutates: [query_plan, node, result_ctx]
        }
        security_phase {
            reads_env: [security_ctx, ontology]
            mutates: [node]
        }
        check_phase {
            reads_env: [security_ctx]
            reads_state: [node]
        }
        hydrate_plan_phase {
            reads_env: [ontology, security_ctx]
            reads_state: [input]
            mutates: [hydration_plan]
        }
        settings_phase {
            reads_state: [input, node]
            mutates: [query_plan, query_config]
        }
        codegen_phase {
            reads_state: [node, input]
            mutates: [result_ctx, query_config, hydration_plan, output]
        }
        duckdb_codegen_phase {
            reads_state: [node, input]
            mutates: [result_ctx, hydration_plan, output]
        }
    }

    pipelines {
        clickhouse {
            env: [ontology, security_ctx]
            state: [json, input, query_plan, node, result_ctx, query_config, hydration_plan, output]
            run: [validate_phase, normalize_phase, restrict_phase, plan_phase, lower_phase, enforce_phase, security_phase, check_phase, hydrate_plan_phase, settings_phase, codegen_phase]
        }
        from_input {
            env: [ontology, security_ctx]
            state: [input, query_plan, node, result_ctx, query_config, hydration_plan, output]
            run: [restrict_phase, plan_phase, lower_phase, enforce_phase, security_phase, check_phase, hydrate_plan_phase, settings_phase, codegen_phase]
        }
        ch_hydration {
            env: [ontology, security_ctx]
            state: [input, query_plan, node, result_ctx, query_config, hydration_plan, output]
            run: [restrict_phase, plan_phase, lower_phase, enforce_phase, settings_phase, codegen_phase]
        }
        duckdb {
            env: [ontology, security_ctx]
            state: [json, input, query_plan, node, result_ctx, hydration_plan, output]
            run: [validate_phase, normalize_phase, plan_phase, lower_phase, enforce_phase, hydrate_plan_phase, duckdb_codegen_phase]
        }
        duckdb_hydration {
            env: [ontology, security_ctx]
            state: [input, query_plan, node, result_ctx, output]
            run: [plan_phase, lower_phase, enforce_phase, duckdb_codegen_phase]
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Phase functions
// ─────────────────────────────────────────────────────────────────────────────

fn validate_phase(ctx: &mut impl CompilerCtx) -> Result<()> {
    let json = ctx.take_json().expect("json required");
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

fn normalize_phase(ctx: &mut impl CompilerCtx) -> Result<()> {
    let input = ctx.take_input().expect("input required");
    ctx.set_input(normalize::normalize(input, ctx.ontology())?);
    Ok(())
}

fn restrict_phase(ctx: &mut impl CompilerCtx) -> Result<()> {
    let ontology = ctx.ontology().clone();
    let security_ctx = ctx.security_ctx().clone();
    let input = ctx.input_mut().as_mut().expect("input required");
    restrict::restrict(input, &ontology, &security_ctx)
}

fn plan_phase(ctx: &mut impl CompilerCtx) -> Result<()> {
    let input = ctx.input_mut().as_mut().expect("input required");
    let query_plan = plan::plan(input)?;
    ctx.set_query_plan(query_plan);
    Ok(())
}

fn lower_phase(ctx: &mut impl CompilerCtx) -> Result<()> {
    let query_plan = ctx.take_query_plan().expect("query_plan required");
    let input = ctx.input().as_ref().expect("input required");
    let node = lower::emit(&query_plan, input)?;
    ctx.set_query_plan(query_plan);
    ctx.set_node(node);
    Ok(())
}

fn enforce_phase(ctx: &mut impl CompilerCtx) -> Result<()> {
    let query_plan = ctx.take_query_plan().expect("query_plan required");
    let node_edge_col = query_plan.node_edge_mappings();
    ctx.set_query_plan(query_plan);
    let mut node = ctx.take_node().expect("node required");
    let input = ctx.input().as_ref().expect("input required");
    let result_context = enforce::enforce_return(&mut node, input, &node_edge_col)?;
    ctx.set_node(node);
    ctx.set_result_ctx(result_context);
    Ok(())
}

fn security_phase(ctx: &mut impl CompilerCtx) -> Result<()> {
    let security_ctx = ctx.security_ctx().clone();
    let ontology = ctx.ontology().clone();
    let node = ctx.node_mut().as_mut().expect("node required");
    security::apply_security_context(node, &security_ctx, &ontology)
}

fn check_phase(ctx: &mut impl CompilerCtx) -> Result<()> {
    let node = ctx.node().as_ref().expect("node required");
    check::check_ast(node, ctx.security_ctx())
}

fn hydrate_plan_phase(ctx: &mut impl CompilerCtx) -> Result<()> {
    let input = ctx.input().as_ref().expect("input required");
    let plan = hydrate::generate_hydration_plan(input, ctx.ontology(), ctx.security_ctx());
    ctx.set_hydration_plan(plan);
    Ok(())
}

fn settings_phase(ctx: &mut impl CompilerCtx) -> Result<()> {
    let input = ctx.input().as_ref().expect("input required");
    let query_type: &str = input.query_type.into();
    let has_cursor = input.cursor.is_some();
    let mut config = settings::resolve(query_type, has_cursor);

    if let Node::Query(q) = ctx.node().as_ref().expect("node required")
        && q.ctes.iter().any(|c| c.materialized)
    {
        config.compiler_derived.enable_materialized_cte = true;
    }

    let query_plan = ctx.take_query_plan().expect("query_plan required");
    if query_plan.hops.len() >= 3 {
        config.compiler_derived.join_order_algorithm = Some("dpsize".into());
    }
    ctx.set_query_plan(query_plan);
    ctx.set_query_config(config);
    Ok(())
}

fn codegen_phase(ctx: &mut impl CompilerCtx) -> Result<()> {
    let result_context = ctx.take_result_ctx().expect("result_ctx required");
    let query_config = ctx.take_query_config().unwrap_or_default();
    let hydration = ctx.take_hydration_plan().unwrap_or(HydrationPlan::None);
    let node = ctx.node().as_ref().expect("node required");
    let input = ctx.input().as_ref().expect("input required");
    let base = codegen::codegen(node, result_context, query_config)?;
    let query_type = input.query_type;
    let input = input.clone();
    ctx.set_output(CompiledQueryContext {
        query_type,
        base,
        hydration,
        input,
    });
    Ok(())
}

fn duckdb_codegen_phase(ctx: &mut impl CompilerCtx) -> Result<()> {
    let result_context = ctx.take_result_ctx().expect("result_ctx required");
    let hydration = ctx.take_hydration_plan().unwrap_or(HydrationPlan::None);
    let node = ctx.node().as_ref().expect("node required");
    let input = ctx.input().as_ref().expect("input required");
    let base = codegen::duckdb::codegen(node, result_context)?;
    let query_type = input.query_type;
    let input = input.clone();
    ctx.set_output(CompiledQueryContext {
        query_type,
        base,
        hydration,
        input,
    });
    Ok(())
}
