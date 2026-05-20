//! Compiler pipeline configuration via `define_compiler_ctx!`.
//!
//! This is the SSOT declaration for the compiler's env fields, state fields,
//! phase grants, and pipeline presets. The macro generates:
//!
//! - `CompilerCtx` trait with guarded accessors
//! - Per-pipeline context structs (`ClickhouseCtx`, `DuckdbCtx`, etc.)
//! - Per-pipeline runner functions (`run_clickhouse`, `run_duckdb`, etc.)

use std::sync::Arc;

use gkg_server_config::QueryConfig;
use ontology::Ontology;

use crate::ast::Node;
use crate::input::Input;
use crate::passes::codegen::CompiledQueryContext;
use crate::passes::enforce::ResultContext;
use crate::passes::hydrate::HydrationPlan;
use crate::passes::plan::QueryPlan;
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
        duckdb_codegen {
            reads_state: [node, input]
            mutates: [result_ctx, hydration_plan, output]
        }
    }

    pipelines {
        clickhouse {
            env: [ontology, security_ctx]
            state: [json, input, query_plan, node, result_ctx, query_config, hydration_plan, output]
            run: [validate, normalize, restrict, plan, lower, enforce, security, check, hydrate_plan, settings, codegen]
        }
        from_input {
            env: [ontology, security_ctx]
            state: [input, query_plan, node, result_ctx, query_config, hydration_plan, output]
            run: [restrict, plan, lower, enforce, security, check, hydrate_plan, settings, codegen]
        }
        hydration {
            env: [ontology, security_ctx]
            state: [input, query_plan, node, result_ctx, query_config, output]
            run: [restrict, plan, lower, enforce, settings, codegen]
        }
        duckdb {
            env: [ontology, security_ctx]
            state: [json, input, query_plan, node, result_ctx, hydration_plan, output]
            run: [validate, normalize, plan, lower, enforce, hydrate_plan, duckdb_codegen]
        }
        duckdb_hydration {
            env: [ontology, security_ctx]
            state: [input, query_plan, node, result_ctx, output]
            run: [plan, lower, enforce, duckdb_codegen]
        }
    }
}
