//! Composable, two-phase compiler pipeline.
//!
//! Generic over:
//! - `E: PipelineEnv` — immutable per-pipeline config
//! - `S: PipelineState` — mutable per-execution state
//!
//! Lifecycle: build → configure → seal → execute

pub mod macros;

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::error::{QueryError, Result};

// ═════════════════════════════════════════════════════════════════════════════
// Core traits
// ═════════════════════════════════════════════════════════════════════════════

pub trait PipelineEnv: 'static {}
pub trait PipelineState: 'static {}

pub trait CompilerPass<E: PipelineEnv, S: PipelineState>: Send + Sync {
    const NAME: &'static str;
    fn run(&self, env: &E, state: &mut S) -> Result<()>;
}

pub trait Seal<S: PipelineState>: Send + Sync {
    fn seal(&self, state: &mut S);
}

pub trait PipelineObserver: Send + Sync {
    fn pass_completed(&self, _pass_name: &'static str, _elapsed: Duration) {}
    fn pass_failed(&self, pass_name: &'static str, error: &QueryError);
    fn pass_skipped(&self, _pass_name: &'static str) {}
}

// ═════════════════════════════════════════════════════════════════════════════
// Internals
// ═════════════════════════════════════════════════════════════════════════════

trait ErasedPass<E: PipelineEnv, S: PipelineState>: Send + Sync {
    fn name(&self) -> &'static str;
    fn run(&self, env: &E, state: &mut S) -> Result<()>;
}

impl<E: PipelineEnv, S: PipelineState, P: CompilerPass<E, S>> ErasedPass<E, S> for P {
    fn name(&self) -> &'static str {
        P::NAME
    }
    fn run(&self, env: &E, state: &mut S) -> Result<()> {
        CompilerPass::run(self, env, state)
    }
}

struct Step<E: PipelineEnv, S: PipelineState> {
    pass: Box<dyn ErasedPass<E, S>>,
    seals: Vec<Box<dyn Seal<S>>>,
    enabled: bool,
}

// ═════════════════════════════════════════════════════════════════════════════
// Builder
// ═════════════════════════════════════════════════════════════════════════════

pub(crate) struct PipelineBuilder<E: PipelineEnv, S: PipelineState> {
    steps: Vec<Step<E, S>>,
    observer: Option<Arc<dyn PipelineObserver>>,
}

impl<E: PipelineEnv, S: PipelineState> PipelineBuilder<E, S> {
    pub fn pass<P: CompilerPass<E, S> + 'static>(mut self, pass: P) -> StepBuilder<E, S> {
        self.steps.push(Step {
            pass: Box::new(pass),
            seals: Vec::new(),
            enabled: true,
        });
        StepBuilder { inner: self }
    }

    pub fn build(self) -> Pipeline<E, S> {
        Pipeline {
            steps: self.steps,
            observer: self.observer,
        }
    }
}

/// Configures the most recently added pass. Chain `.seal()` calls,
/// then continue with `.add()` or `.build()`.
pub(crate) struct StepBuilder<E: PipelineEnv, S: PipelineState> {
    inner: PipelineBuilder<E, S>,
}

impl<E: PipelineEnv, S: PipelineState> StepBuilder<E, S> {
    pub fn seal(mut self, seal: impl Seal<S> + 'static) -> Self {
        self.inner
            .steps
            .last_mut()
            .unwrap()
            .seals
            .push(Box::new(seal));
        self
    }

    pub fn pass<P: CompilerPass<E, S> + 'static>(self, pass: P) -> StepBuilder<E, S> {
        self.inner.pass(pass)
    }

    pub fn build(self) -> Pipeline<E, S> {
        self.inner.build()
    }
}

// ═════════════════════════════════════════════════════════════════════════════
// Pipeline — configurable, not yet executable
// ═════════════════════════════════════════════════════════════════════════════

pub struct Pipeline<E: PipelineEnv, S: PipelineState> {
    steps: Vec<Step<E, S>>,
    observer: Option<Arc<dyn PipelineObserver>>,
}

impl<E: PipelineEnv, S: PipelineState> Pipeline<E, S> {
    pub(crate) fn builder() -> PipelineBuilder<E, S> {
        PipelineBuilder {
            steps: Vec::new(),
            observer: None,
        }
    }

    pub fn disable(mut self, pass_name: &str) -> Self {
        for step in &mut self.steps {
            if step.pass.name() == pass_name {
                step.enabled = false;
            }
        }
        self
    }

    pub fn enable(mut self, pass_name: &str) -> Self {
        for step in &mut self.steps {
            if step.pass.name() == pass_name {
                step.enabled = true;
            }
        }
        self
    }

    pub fn observe(mut self, obs: impl PipelineObserver + 'static) -> Self {
        self.observer = Some(Arc::new(obs));
        self
    }

    pub fn passes(&self) -> Vec<(&'static str, bool)> {
        self.steps
            .iter()
            .map(|s| (s.pass.name(), s.enabled))
            .collect()
    }

    pub fn seal(self) -> SealedPipeline<E, S> {
        SealedPipeline {
            steps: self.steps.into(),
            observer: self.observer,
        }
    }
}

// ═════════════════════════════════════════════════════════════════════════════
// SealedPipeline — frozen, executable, shareable
// ═════════════════════════════════════════════════════════════════════════════

pub struct SealedPipeline<E: PipelineEnv, S: PipelineState> {
    steps: Arc<[Step<E, S>]>,
    observer: Option<Arc<dyn PipelineObserver>>,
}

impl<E: PipelineEnv, S: PipelineState> SealedPipeline<E, S> {
    pub fn execute(&self, mut state: S, env: &E) -> Result<S> {
        for step in self.steps.iter() {
            if !step.enabled {
                if let Some(ref obs) = self.observer {
                    obs.pass_skipped(step.pass.name());
                }
                continue;
            }

            let start = Instant::now();
            match step.pass.run(env, &mut state) {
                Ok(()) => {
                    if let Some(ref obs) = self.observer {
                        obs.pass_completed(step.pass.name(), start.elapsed());
                    }
                    for seal in &step.seals {
                        seal.seal(&mut state);
                    }
                }
                Err(e) => {
                    if let Some(ref obs) = self.observer {
                        obs.pass_failed(step.pass.name(), &e);
                    }
                    return Err(e);
                }
            }
        }
        Ok(state)
    }
}

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

    // Every phase takes &mut impl CompilerCtx — works with any pipeline
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
        let _ = ctx.node(); // normalize can't read node
    }

    #[test]
    #[should_panic(expected = "cannot mutate `output`")]
    fn unauthorized_mutate_panics() {
        let mut ctx = FullCtx::new(Ontology("ont".into()), SecurityCtx(1));
        ctx.set_current_phase("normalize");
        ctx.set_output(Output("bad".into())); // normalize can't write output
    }
}
