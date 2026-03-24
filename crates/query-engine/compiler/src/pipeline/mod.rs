//! Composable, two-phase compiler pipeline.
//!
//! Generic over:
//! - `E: PipelineEnv` — immutable per-pipeline config
//! - `S: PipelineState` — mutable per-execution state
//!
//! Lifecycle: build → configure → seal → execute

pub mod macros;
use std::collections::HashSet;
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

pub struct PipelineBuilder<E: PipelineEnv, S: PipelineState> {
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

    pub fn pass_if<P: CompilerPass<E, S> + 'static>(self, cond: bool, pass: P) -> Self {
        if cond { self.pass(pass).done() } else { self }
    }

    pub fn observe(mut self, obs: impl PipelineObserver + 'static) -> Self {
        self.observer = Some(Arc::new(obs));
        self
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
pub struct StepBuilder<E: PipelineEnv, S: PipelineState> {
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

    pub fn done(self) -> PipelineBuilder<E, S> {
        self.inner
    }

    pub fn pass<P: CompilerPass<E, S> + 'static>(self, pass: P) -> StepBuilder<E, S> {
        self.inner.pass(pass)
    }

    pub fn pass_if<P: CompilerPass<E, S> + 'static>(self, cond: bool, pass: P) -> Self {
        if cond {
            StepBuilder {
                inner: self.inner.pass(pass).inner,
            }
        } else {
            self
        }
    }

    pub fn observe(self, obs: impl PipelineObserver + 'static) -> PipelineBuilder<E, S> {
        self.inner.observe(obs)
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
    pub fn builder() -> PipelineBuilder<E, S> {
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

    pub fn disable_all(mut self, pass_names: &[&str]) -> Self {
        let names: HashSet<&str> = pass_names.iter().copied().collect();
        for step in &mut self.steps {
            if names.contains(step.pass.name()) {
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
