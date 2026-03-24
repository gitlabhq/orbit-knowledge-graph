//! Composable, type-safe compiler pipeline.
//!
//! Each compiler pass is a [`CompilerPass`] that transforms a
//! [`CompilerContext`] from one phase to the next. The [`CompilerRunner`]
//! chains passes with compile-time phase checking so invalid orderings
//! are rejected by `rustc`, not at runtime.
//!
//! # Pipelines
//!
//! ```text
//! ClickHouse:  Raw → Parse → Validate → Normalize → Lower → Optimize → Enforce → Security → Check → Codegen
//! Hydration:   Normalized → Lower → Optimize → Enforce → HydrationCodegen (no security)
//! DuckDB:      Raw → Parse → Validate → Normalize → Lower → Optimize → Enforce → DuckDbCodegen (future)
//! ```
//!
//! # Example
//!
//! ```ignore
//! let compiled = CompilerRunner::new(json)
//!     .then(&ParsePass)?
//!     .then(&ValidatePass::new(&ontology))?
//!     .then(&NormalizePass::new(&ontology))?
//!     .then(&LowerPass)?
//!     .then(&OptimizePass::new(&security_ctx))?
//!     .then(&EnforcePass)?
//!     .then(&SecurityPass::new(&security_ctx))?
//!     .then(&CheckPass::new(&security_ctx))?
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
// CompilerContext
// ─────────────────────────────────────────────────────────────────────────────

/// Compilation state, parameterized by the current pipeline phase.
///
/// Fields are progressively populated as passes run. The phantom type
/// parameter prevents accessing fields before the pass that populates
/// them has executed.
pub struct CompilerContext<P: Phase> {
    pub(crate) json: Option<String>,
    pub(crate) input: Option<Input>,
    pub(crate) node: Option<Node>,
    pub(crate) result_context: Option<ResultContext>,
    pub(crate) output: Option<CompiledQueryContext>,
    _phase: PhantomData<P>,
}

impl<P: Phase> CompilerContext<P> {
    /// Zero-cost phase transition — same memory layout, different phantom type.
    fn advance<Q: Phase>(self) -> CompilerContext<Q> {
        CompilerContext {
            json: self.json,
            input: self.input,
            node: self.node,
            result_context: self.result_context,
            output: self.output,
            _phase: PhantomData,
        }
    }
}

// Phase-gated accessors — each field is only accessible after the pass
// that populates it.

// input is available from Parsed onward.
macro_rules! impl_input_accessors {
    ($($phase:ty),+ $(,)?) => {
        $(
            impl CompilerContext<$phase> {
                pub fn input(&self) -> &Input {
                    self.input.as_ref().expect("input must exist at this phase")
                }
            }
        )+
    };
}

impl_input_accessors!(
    Parsed, Validated, Normalized, Lowered, Optimized, Enforced, Secured, Checked, Emitted
);

// node is available from Lowered onward.
macro_rules! impl_node_accessors {
    ($($phase:ty),+ $(,)?) => {
        $(
            impl CompilerContext<$phase> {
                pub fn node(&self) -> &Node {
                    self.node.as_ref().expect("node must exist at this phase")
                }
            }
        )+
    };
}

impl_node_accessors!(Lowered, Optimized, Enforced, Secured, Checked);

// result_context is available from Enforced onward.
macro_rules! impl_result_context_accessors {
    ($($phase:ty),+ $(,)?) => {
        $(
            impl CompilerContext<$phase> {
                pub fn result_context(&self) -> &ResultContext {
                    self.result_context.as_ref().expect("result_context must exist at this phase")
                }
            }
        )+
    };
}

impl_result_context_accessors!(Enforced, Secured, Checked);

impl CompilerContext<Emitted> {
    /// Consume the context and extract the compiled output.
    pub fn take_output(self) -> Option<CompiledQueryContext> {
        self.output
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// CompilerPass trait
// ─────────────────────────────────────────────────────────────────────────────

/// A single compiler pass that transforms the context in place.
///
/// Passes carry their own dependencies (ontology, security context, etc.)
/// so each pass declares exactly what it needs. The phase transition is
/// handled by the [`CompilerRunner`] — passes never call `advance()`.
pub trait CompilerPass {
    /// Human-readable name for observability.
    const NAME: &'static str;

    /// Phase the context must be in before this pass can run.
    type In: Phase;

    /// Phase the context transitions to after this pass runs.
    type Out: Phase;

    /// Execute the pass, mutating the context in place.
    ///
    /// The runner advances the phase automatically on success.
    fn run(&self, ctx: &mut CompilerContext<Self::In>) -> Result<()>;
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
/// The phantom type `P` tracks the current phase. `.then(pass)` advances
/// the phase; the compiler rejects chains where `pass.In != P`.
pub struct CompilerRunner<P: Phase> {
    ctx: CompilerContext<P>,
    observer: Option<Box<dyn CompilerObserver>>,
}

impl CompilerRunner<Raw> {
    /// Start a pipeline from a raw JSON query string.
    pub fn new(json: impl Into<String>) -> Self {
        CompilerRunner {
            ctx: CompilerContext {
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

impl CompilerRunner<Normalized> {
    /// Start from a pre-built, normalized `Input` (for hydration queries or tests).
    pub fn from_input(input: Input) -> Self {
        CompilerRunner {
            ctx: CompilerContext {
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

impl<P: Phase> CompilerRunner<P> {
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
    pub fn then<S: CompilerPass<In = P>>(mut self, pass: &S) -> Result<CompilerRunner<S::Out>> {
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
    pub fn into_context(self) -> CompilerContext<P> {
        self.ctx
    }
}
