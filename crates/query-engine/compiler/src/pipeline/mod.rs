//! Pipeline infrastructure.
//!
//! The `define_compiler_ctx!` proc macro (in `compiler-pipeline-macros`)
//! generates per-pipeline context structs and runners. See `config.rs`
//! for the pipeline declarations.

#[cfg(test)]
mod ctx_tests {
    use crate::error::Result;

    #[derive(Clone)]
    struct Ontology(String);
    #[derive(Clone)]
    struct SecurityCtx(u32);
    #[derive(Clone, Debug, PartialEq)]
    struct Input(String);
    #[derive(Clone, Debug, PartialEq)]
    struct Node(String);
    #[derive(Clone, Debug, PartialEq)]
    struct Output(String);

    compiler_pipeline_macros::define_compiler_ctx! {
        env {
            pub ontology: Ontology,
            pub security_ctx: SecurityCtx,
        }
        state {
            pub input: Input,
            pub node: Node,
            pub output: Output,
        }
        phases {
            normalize {
                reads_env: [ontology]
                mutates: [input]
            }
            lower {
                reads_state: [input]
                mutates: [node]
            }
            secure {
                reads_env: [security_ctx]
                mutates: [node]
            }
            codegen {
                reads_state: [node]
                mutates: [output]
            }
        }
        pipelines {
            full {
                env: [ontology, security_ctx]
                state: [input, node, output]
                run: [normalize, lower, secure, codegen]
            }
            local {
                env: [ontology]
                state: [input, node, output]
                run: [normalize, lower, codegen]
            }
        }
    }

    fn normalize(ctx: &mut impl CompilerCtx) -> Result<()> {
        let input = ctx.take_input().expect("input required");
        ctx.set_input(Input(format!("normalized({})", input.0)));
        Ok(())
    }

    fn lower(ctx: &mut impl CompilerCtx) -> Result<()> {
        let input = ctx.input().as_ref().expect("input required").clone();
        ctx.set_node(Node(format!("ast({})", input.0)));
        Ok(())
    }

    fn secure(ctx: &mut impl CompilerCtx) -> Result<()> {
        let node = ctx.node_mut().as_mut().expect("node required");
        node.0 = format!("secured({})", node.0);
        Ok(())
    }

    fn codegen(ctx: &mut impl CompilerCtx) -> Result<()> {
        let node = ctx.node().as_ref().expect("node required").clone();
        ctx.set_output(Output(format!("sql({})", node.0)));
        Ok(())
    }

    #[test]
    fn full_pipeline_runs_all_phases() {
        let mut ctx = FullCtx::new(Ontology("ont".into()), SecurityCtx(1));
        ctx.set_current_phase("normalize");
        ctx.set_input(Input("raw".into()));

        run_full(&mut ctx).expect("pipeline should succeed");

        ctx.set_current_phase("codegen");
        assert_eq!(
            ctx.output().as_ref(),
            Some(&Output("sql(secured(ast(normalized(raw))))".into()))
        );
    }

    #[test]
    fn local_pipeline_skips_security() {
        let mut ctx = LocalCtx::new(Ontology("ont".into()));
        ctx.set_current_phase("normalize");
        ctx.set_input(Input("raw".into()));

        run_local(&mut ctx).expect("pipeline should succeed");

        ctx.set_current_phase("codegen");
        assert_eq!(
            ctx.output().as_ref(),
            Some(&Output("sql(ast(normalized(raw)))".into()))
        );
    }

    #[test]
    fn missing_input_panics() {
        let mut ctx = FullCtx::new(Ontology("ont".into()), SecurityCtx(1));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_full(&mut ctx).ok();
        }));
        assert!(result.is_err(), "should panic on missing input");
    }

    #[test]
    #[should_panic(expected = "cannot read `node`")]
    fn unauthorized_read_panics() {
        let mut ctx = FullCtx::new(Ontology("ont".into()), SecurityCtx(1));
        ctx.set_current_phase("normalize");
        let _ = ctx.node();
    }

    #[test]
    #[should_panic(expected = "cannot mutate `output`")]
    fn unauthorized_mutate_panics() {
        let mut ctx = FullCtx::new(Ontology("ont".into()), SecurityCtx(1));
        ctx.set_current_phase("normalize");
        ctx.set_output(Output("bad".into()));
    }
}
