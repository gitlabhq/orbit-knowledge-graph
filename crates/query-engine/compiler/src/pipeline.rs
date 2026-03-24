//! Composable, type-safe compiler pipeline.
//!
//! The pipeline is generic over an environment type `E` that carries
//! pipeline-specific configuration (ontology, security context, backend
//! config, etc.). Passes read from the environment via the context.
//!
//! # Architecture
//!
//! - **`E: PipelineEnv`** — user-defined environment (e.g. `ClickHouseEnv`).
//! - **[`CompilerContext<P, E>`]** — phase-tagged compilation state + environment.
//! - **[`CompilerRunner<P, E>`]** — chains passes with compile-time phase checking.
//! - **[`CompilerPass<E>`]** — unit struct implementing a single transformation.
//!
//! # Pipelines
//!
//! ```text
//! ClickHouse:  Raw → Parse → Validate → Normalize → Lower → Optimize → Enforce → Security → Check → Codegen
//! Hydration:   Normalized → Lower → Optimize → Enforce → HydrationCodegen
//! DuckDB:      Raw → Parse → Validate → Normalize → Lower → Optimize → Enforce → DuckDbCodegen (future)
//! ```
//!
//! # Example
//!
//! ```ignore
//! let env = ClickHouseEnv::new(ontology, security_ctx);
//! let result = CompilerRunner::new(json, env)
//!     .then(&ParsePass)?
//!     .then(&ValidatePass)?
//!     .then(&NormalizePass)?
//!     .then(&LowerPass)?
//!     .then(&OptimizePass)?
//!     .then(&EnforcePass)?
//!     .then(&SecurityPass)?
//!     .then(&CheckPass)?
//!     .then(&CodegenPass)?
//!     .into_context()
//!     .take_output()
//!     .unwrap();
//! ```

use std::marker::PhantomData;
use std::time::{Duration, Instant};

use crate::ast::Node;
use crate::error::{QueryError, Result};
use crate::input::Input;
use crate::passes::codegen::CompiledQueryContext;
use crate::passes::enforce::ResultContext;

// ─────────────────────────────────────────────────────────────────────────────
// Phases
// ─────────────────────────────────────────────────────────────────────────────

mod sealed {
    pub trait Sealed {}
}

/// Marker trait for pipeline phases. Sealed — only the phases defined
/// in this module can implement it.
pub trait Phase: sealed::Sealed + 'static {}

macro_rules! define_phases {
    ($($name:ident),+ $(,)?) => {
        $(
            #[derive(Debug)]
            pub struct $name;
            impl sealed::Sealed for $name {}
            impl Phase for $name {}
        )+
    };
}

define_phases!(
    Raw, Parsed, Validated, Normalized, Lowered, Optimized, Enforced, Secured, Checked, Emitted
);

// ─────────────────────────────────────────────────────────────────────────────
// PipelineEnv
// ─────────────────────────────────────────────────────────────────────────────

/// Marker trait for pipeline environment types.
///
/// Each pipeline variant (ClickHouse, DuckDB, etc.) defines its own env
/// struct carrying backend-specific config. Passes access the env through
/// [`CompilerContext::env()`].
pub trait PipelineEnv: 'static {}

// ─────────────────────────────────────────────────────────────────────────────
// CompilerContext
// ─────────────────────────────────────────────────────────────────────────────

/// Compilation state, parameterized by the current phase `P` and
/// the pipeline environment `E`.
///
/// Pipeline state fields (`input`, `node`, etc.) are progressively
/// populated as passes run. The environment is immutable and available
/// at all phases.
pub struct CompilerContext<P: Phase, E: PipelineEnv> {
    env: E,
    pub(crate) json: Option<String>,
    pub(crate) input: Option<Input>,
    pub(crate) node: Option<Node>,
    pub(crate) result_context: Option<ResultContext>,
    pub(crate) output: Option<CompiledQueryContext>,
    _phase: PhantomData<P>,
}

impl<P: Phase, E: PipelineEnv> CompilerContext<P, E> {
    /// Zero-cost phase transition — same memory layout, different phantom type.
    fn advance<Q: Phase>(self) -> CompilerContext<Q, E> {
        CompilerContext {
            env: self.env,
            json: self.json,
            input: self.input,
            node: self.node,
            result_context: self.result_context,
            output: self.output,
            _phase: PhantomData,
        }
    }

    /// The pipeline environment (ontology, security context, backend config, etc.).
    pub fn env(&self) -> &E {
        &self.env
    }

    /// The parsed input. Populated by ParsePass.
    pub fn input(&self) -> &Input {
        self.input.as_ref().expect("input not yet populated")
    }

    /// The lowered AST node. Populated by LowerPass.
    pub fn node(&self) -> &Node {
        self.node.as_ref().expect("node not yet populated")
    }

    /// The result context for redaction. Populated by EnforcePass.
    pub fn result_context(&self) -> &ResultContext {
        self.result_context
            .as_ref()
            .expect("result_context not yet populated")
    }

    /// Consume the context and extract the compiled output.
    /// Populated by CodegenPass or HydrationCodegenPass.
    pub fn take_output(self) -> Option<CompiledQueryContext> {
        self.output
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// CompilerPass trait
// ─────────────────────────────────────────────────────────────────────────────

/// A single compiler pass that transforms the context in place.
///
/// Generic over the environment `E` so passes can be shared across
/// pipeline variants (when `E` is unconstrained) or specialized for
/// a specific backend (when `E` is concrete).
pub trait CompilerPass<E: PipelineEnv> {
    /// Human-readable name for observability.
    const NAME: &'static str;

    /// Phase the context must be in before this pass can run.
    type In: Phase;

    /// Phase the context transitions to after this pass runs.
    type Out: Phase;

    /// Execute the pass, mutating the context in place.
    ///
    /// The runner advances the phase automatically on success.
    fn run(&self, ctx: &mut CompilerContext<Self::In, E>) -> Result<()>;
}

// ─────────────────────────────────────────────────────────────────────────────
// CompilerObserver
// ─────────────────────────────────────────────────────────────────────────────

/// Optional hook for timing and error reporting across compiler passes.
pub trait CompilerObserver {
    /// Called after a pass completes successfully.
    fn pass_completed(&mut self, pass_name: &'static str, elapsed: Duration);

    /// Called when a pass returns an error.
    fn pass_failed(&mut self, pass_name: &'static str, error: &QueryError);
}

/// Observer that records per-pass OTel metrics via the existing counter
/// infrastructure.
pub struct MetricsObserver;

impl CompilerObserver for MetricsObserver {
    fn pass_completed(&mut self, _pass_name: &'static str, _elapsed: Duration) {}

    fn pass_failed(&mut self, _pass_name: &'static str, error: &QueryError) {
        use crate::metrics::counter_info;
        use opentelemetry::KeyValue;
        let (counter, reason) = counter_info(error);
        counter.add(1, &[KeyValue::new("reason", reason)]);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// CompilerRunner
// ─────────────────────────────────────────────────────────────────────────────

/// Typed pipeline runner that chains [`CompilerPass`] invocations.
///
/// Generic over the phase `P` and the environment `E`. The phase advances
/// through `.then(pass)` calls; the environment is fixed at construction.
pub struct CompilerRunner<P: Phase, E: PipelineEnv> {
    ctx: CompilerContext<P, E>,
    observer: Option<Box<dyn CompilerObserver>>,
}

impl<E: PipelineEnv> CompilerRunner<Raw, E> {
    /// Start a pipeline from a raw JSON query string.
    pub fn new(json: impl Into<String>, env: E) -> Self {
        CompilerRunner {
            ctx: CompilerContext {
                env,
                json: Some(json.into()),
                input: None,
                node: None,
                result_context: None,
                output: None,
                _phase: PhantomData,
            },
            observer: None,
        }
    }
}

impl<E: PipelineEnv> CompilerRunner<Normalized, E> {
    /// Start from a pre-built, normalized `Input` (for hydration queries or tests).
    pub fn from_input(input: Input, env: E) -> Self {
        CompilerRunner {
            ctx: CompilerContext {
                env,
                json: None,
                input: Some(input),
                node: None,
                result_context: None,
                output: None,
                _phase: PhantomData,
            },
            observer: None,
        }
    }
}

impl<P: Phase, E: PipelineEnv> CompilerRunner<P, E> {
    /// Attach an observer for pass-level timing and error recording.
    pub fn with_observer(mut self, obs: impl CompilerObserver + 'static) -> Self {
        self.observer = Some(Box::new(obs));
        self
    }

    /// Run a pass, advancing the pipeline to the next phase.
    ///
    /// The compiler enforces `S::In == P` — you cannot call a pass that
    /// expects a different input phase. The runner handles the phase
    /// transition; the pass only mutates the context.
    pub fn then<S: CompilerPass<E, In = P>>(
        mut self,
        pass: &S,
    ) -> Result<CompilerRunner<S::Out, E>> {
        let start = Instant::now();
        match pass.run(&mut self.ctx) {
            Ok(()) => {
                if let Some(ref mut obs) = self.observer {
                    obs.pass_completed(S::NAME, start.elapsed());
                }
                Ok(CompilerRunner {
                    ctx: self.ctx.advance(),
                    observer: self.observer,
                })
            }
            Err(e) => {
                if let Some(ref mut obs) = self.observer {
                    obs.pass_failed(S::NAME, &e);
                }
                Err(e)
            }
        }
    }

    /// Extract the context at the current phase (for tests/inspection).
    pub fn into_context(self) -> CompilerContext<P, E> {
        self.ctx
    }
}
