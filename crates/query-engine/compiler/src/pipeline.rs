//! Composable compiler pipeline.
//!
//! The pipeline is generic over an environment type `E` that carries
//! pipeline-specific configuration. Passes declare what they need from
//! the environment via trait bounds — the compiler enforces that the
//! env provides it.
//!
//! # Architecture
//!
//! - **`E: PipelineEnv`** — user-defined environment (e.g. `ClickHouseEnv`).
//! - **[`CompilerContext<E>`]** — compilation state + environment.
//! - **[`CompilerRunner<E>`]** — chains passes.
//! - **[`CompilerPass<E>`]** — unit struct implementing a single transformation.
//!   Trait bounds on `E` control which passes can run in which pipeline.
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

use std::time::{Duration, Instant};

use crate::ast::Node;
use crate::error::{QueryError, Result};
use crate::input::Input;
use crate::passes::codegen::CompiledQueryContext;
use crate::passes::enforce::ResultContext;

// ─────────────────────────────────────────────────────────────────────────────
// PipelineEnv
// ─────────────────────────────────────────────────────────────────────────────

/// Marker trait for pipeline environment types.
///
/// Each pipeline variant (ClickHouse, DuckDB, etc.) defines its own env
/// struct carrying backend-specific config. Passes access the env through
/// [`CompilerContext::env()`] and declare requirements via trait bounds
/// (e.g. `E: HasOntology + HasSecurityCtx`).
pub trait PipelineEnv: 'static {}

// ─────────────────────────────────────────────────────────────────────────────
// CompilerContext
// ─────────────────────────────────────────────────────────────────────────────

/// Compilation state, generic over the pipeline environment `E`.
///
/// Fields are progressively populated as passes run. The environment
/// is immutable and available to all passes.
pub struct CompilerContext<E: PipelineEnv> {
    env: E,
    pub(crate) json: Option<String>,
    pub(crate) input: Option<Input>,
    pub(crate) node: Option<Node>,
    pub(crate) result_context: Option<ResultContext>,
    pub(crate) output: Option<CompiledQueryContext>,
}

impl<E: PipelineEnv> CompilerContext<E> {
    /// The pipeline environment.
    pub fn env(&self) -> &E {
        &self.env
    }

    /// The raw JSON query string.
    pub fn require_json(&self) -> Result<&str> {
        self.json
            .as_deref()
            .ok_or_else(|| QueryError::PipelineInvariant("json not yet populated".into()))
    }

    /// The parsed and validated input.
    pub fn require_input(&self) -> Result<&Input> {
        self.input
            .as_ref()
            .ok_or_else(|| QueryError::PipelineInvariant("input not yet populated".into()))
    }

    /// Mutable access to the input.
    pub fn require_input_mut(&mut self) -> Result<&mut Input> {
        self.input
            .as_mut()
            .ok_or_else(|| QueryError::PipelineInvariant("input not yet populated".into()))
    }

    /// The lowered AST node.
    pub fn require_node(&self) -> Result<&Node> {
        self.node
            .as_ref()
            .ok_or_else(|| QueryError::PipelineInvariant("node not yet populated".into()))
    }

    /// Mutable access to the AST node.
    pub fn require_node_mut(&mut self) -> Result<&mut Node> {
        self.node
            .as_mut()
            .ok_or_else(|| QueryError::PipelineInvariant("node not yet populated".into()))
    }

    /// The result context for redaction.
    pub fn require_result_context(&self) -> Result<&ResultContext> {
        self.result_context
            .as_ref()
            .ok_or_else(|| QueryError::PipelineInvariant("result_context not yet populated".into()))
    }

    /// Take ownership of the result context (consumed by codegen).
    pub fn take_result_context(&mut self) -> Result<ResultContext> {
        self.result_context
            .take()
            .ok_or_else(|| QueryError::PipelineInvariant("result_context not yet populated".into()))
    }

    /// Mutable node + immutable input. Used by passes that transform the AST
    /// based on input metadata (enforce).
    pub fn require_node_mut_and_input(&mut self) -> Result<(&mut Node, &Input)> {
        match (&mut self.node, &self.input) {
            (Some(node), Some(input)) => Ok((node, input)),
            (None, _) => Err(QueryError::PipelineInvariant(
                "node not yet populated".into(),
            )),
            (_, None) => Err(QueryError::PipelineInvariant(
                "input not yet populated".into(),
            )),
        }
    }

    /// Mutable node + mutable input. Used by passes that mutate both
    /// (optimize).
    pub fn require_node_mut_and_input_mut(&mut self) -> Result<(&mut Node, &mut Input)> {
        match (&mut self.node, &mut self.input) {
            (Some(node), Some(input)) => Ok((node, input)),
            (None, _) => Err(QueryError::PipelineInvariant(
                "node not yet populated".into(),
            )),
            (_, None) => Err(QueryError::PipelineInvariant(
                "input not yet populated".into(),
            )),
        }
    }

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
/// Trait bounds on `E` declare what the pass needs from the environment.
/// Passes that need nothing are generic over any `E: PipelineEnv`.
/// Passes that need the ontology require `E: HasOntology`. Etc.
///
/// This means the env type controls which passes can appear in a pipeline —
/// if `DuckDbEnv` doesn't impl `HasSecurityCtx`, you can't chain
/// `SecurityPass` into a DuckDB pipeline.
pub trait CompilerPass<E: PipelineEnv> {
    /// Human-readable name for observability.
    const NAME: &'static str;

    /// Execute the pass, mutating the context in place.
    fn run(&self, ctx: &mut CompilerContext<E>) -> Result<()>;
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

/// Observer that records per-pass OTel metrics.
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

/// Pipeline runner that chains [`CompilerPass`] invocations.
///
/// Generic over the environment `E`. Which passes can be chained is
/// determined by `E`'s trait impls — the Rust compiler enforces that
/// each pass's bounds are satisfied.
pub struct CompilerRunner<E: PipelineEnv> {
    ctx: CompilerContext<E>,
    observer: Option<Box<dyn CompilerObserver>>,
}

impl<E: PipelineEnv> CompilerRunner<E> {
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
            },
            observer: None,
        }
    }

    /// Start from a pre-built `Input` (for hydration queries or tests).
    pub fn from_input(input: Input, env: E) -> Self {
        CompilerRunner {
            ctx: CompilerContext {
                env,
                json: None,
                input: Some(input),
                node: None,
                result_context: None,
                output: None,
            },
            observer: None,
        }
    }

    /// Attach an observer for pass-level timing and error recording.
    pub fn with_observer(mut self, obs: impl CompilerObserver + 'static) -> Self {
        self.observer = Some(Box::new(obs));
        self
    }

    /// Run a pass.
    ///
    /// The Rust compiler enforces that `S: CompilerPass<E>` — meaning
    /// the pass's trait bounds on `E` must be satisfied by the runner's env.
    pub fn then<S: CompilerPass<E>>(mut self, pass: &S) -> Result<Self> {
        let start = Instant::now();
        match pass.run(&mut self.ctx) {
            Ok(()) => {
                if let Some(ref mut obs) = self.observer {
                    obs.pass_completed(S::NAME, start.elapsed());
                }
                Ok(self)
            }
            Err(e) => {
                if let Some(ref mut obs) = self.observer {
                    obs.pass_failed(S::NAME, &e);
                }
                Err(e)
            }
        }
    }

    /// Extract the context (for tests/inspection).
    pub fn into_context(self) -> CompilerContext<E> {
        self.ctx
    }
}
