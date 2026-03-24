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
//! ClickHouse:  Raw → Parse → Normalize → Lower → Optimize → Enforce → Security → Check → Codegen
//! Hydration:   Normalized → Lower → Optimize → Enforce → HydrationCodegen (no security)
//! DuckDB:      Raw → Parse → Normalize → Lower → Optimize → Enforce → DuckDbCodegen (future)
//! ```
//!
//! # Example
//!
//! ```ignore
//! let compiled = CompilerRunner::new(json)
//!     .then(&ParsePass::new(&ontology))?
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
use crate::codegen::CompiledQueryContext;
use crate::enforce::ResultContext;
use crate::error::{QueryError, Result};
use crate::input::Input;

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
    Raw, Parsed, Normalized, Lowered, Optimized, Enforced, Secured, Checked, Emitted
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
    Parsed, Normalized, Lowered, Optimized, Enforced, Secured, Checked, Emitted
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

// ─────────────────────────────────────────────────────────────────────────────
// Pipeline presets
// ─────────────────────────────────────────────────────────────────────────────

use crate::check::CheckPass;
use crate::codegen::CodegenPass;
use crate::enforce::EnforcePass;
use crate::hydrate::HydrationCodegenPass;
use crate::lower::LowerPass;
use crate::normalize::NormalizePass;
use crate::optimize::OptimizePass;
use crate::security::SecurityPass;
use crate::validate::ParsePass;

/// Standard ClickHouse compilation pipeline.
///
/// ```text
/// JSON → Parse → Normalize → Lower → Optimize → Enforce → Security → Check → Codegen
/// ```
pub fn compile_clickhouse(
    json: &str,
    ontology: &ontology::Ontology,
    security_ctx: &crate::SecurityContext,
) -> Result<CompiledQueryContext> {
    CompilerRunner::new(json)
        .with_observer(MetricsObserver)
        .then(&ParsePass::new(ontology))?
        .then(&NormalizePass::new(ontology))?
        .then(&LowerPass)?
        .then(&OptimizePass::new(security_ctx))?
        .then(&EnforcePass)?
        .then(&SecurityPass::new(security_ctx))?
        .then(&CheckPass::new(security_ctx))?
        .then(&CodegenPass)?
        .into_context()
        .take_output()
        .ok_or_else(|| QueryError::Codegen("CodegenPass did not produce output".into()))
}

/// Hydration pipeline — skips security and check passes.
///
/// Hydration queries are internal-only (not user-facing), operate on
/// pre-authorized IDs, and don't have `traversal_path` columns.
///
/// ```text
/// Input → Lower → Optimize → Enforce → HydrationCodegen
/// ```
pub fn compile_hydration(
    input: Input,
    security_ctx: &crate::SecurityContext,
) -> Result<CompiledQueryContext> {
    CompilerRunner::from_input(input)
        .with_observer(MetricsObserver)
        .then(&LowerPass)?
        .then(&OptimizePass::new(security_ctx))?
        .then(&EnforcePass)?
        .then(&HydrationCodegenPass)?
        .into_context()
        .take_output()
        .ok_or_else(|| QueryError::Codegen("HydrationCodegenPass did not produce output".into()))
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::QueryType;
    use ontology::Ontology;

    fn test_ontology() -> Ontology {
        Ontology::load_embedded().expect("ontology must load")
    }

    fn test_security_ctx() -> crate::SecurityContext {
        crate::SecurityContext::new(1, vec!["1/".into()]).unwrap()
    }

    #[test]
    fn full_clickhouse_pipeline() {
        let ontology = test_ontology();
        let ctx = test_security_ctx();

        let json = r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username"]},
            "limit": 10
        }"#;

        let compiled = compile_clickhouse(json, &ontology, &ctx).unwrap();
        assert!(!compiled.base.sql.is_empty());
        assert_eq!(compiled.query_type, QueryType::Search);
    }

    #[test]
    fn full_traversal_pipeline() {
        let ontology = test_ontology();
        let ctx = test_security_ctx();

        let json = r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User"},
                {"id": "mr", "entity": "MergeRequest"}
            ],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "mr"}],
            "limit": 10
        }"#;

        let compiled = compile_clickhouse(json, &ontology, &ctx).unwrap();
        assert!(!compiled.base.sql.is_empty());
        assert_eq!(compiled.query_type, QueryType::Traversal);
    }

    #[test]
    fn hydration_pipeline_skips_security() {
        let ontology = test_ontology();
        let ctx = test_security_ctx();

        // Build a hydration-type Input directly (like HydrationStage does).
        let json = r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username"]},
            "limit": 10
        }"#;
        let mut input = crate::validated_input(json, &ontology).unwrap();
        input.query_type = QueryType::Hydration;

        let compiled = compile_hydration(input, &ctx).unwrap();
        assert!(!compiled.base.sql.is_empty());
    }

    #[test]
    fn partial_pipeline_inspect_after_lower() {
        let ontology = test_ontology();

        let json = r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username"]},
            "limit": 10
        }"#;

        let ctx = CompilerRunner::new(json)
            .then(&ParsePass::new(&ontology))
            .unwrap()
            .then(&NormalizePass::new(&ontology))
            .unwrap()
            .then(&LowerPass)
            .unwrap()
            .into_context();

        // Can inspect the AST after lowering.
        let Node::Query(q) = ctx.node();
        assert!(!q.select.is_empty());
    }

    #[test]
    fn partial_pipeline_inspect_after_normalize() {
        let ontology = test_ontology();

        let json = r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username"]},
            "limit": 10
        }"#;

        let ctx = CompilerRunner::new(json)
            .then(&ParsePass::new(&ontology))
            .unwrap()
            .then(&NormalizePass::new(&ontology))
            .unwrap()
            .into_context();

        // Input is available after normalize.
        assert_eq!(ctx.input().query_type, QueryType::Search);
    }

    #[test]
    fn partial_pipeline_inspect_after_optimize() {
        let ontology = test_ontology();
        let sec_ctx = test_security_ctx();

        let json = r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username"]},
            "limit": 10
        }"#;

        let ctx = CompilerRunner::new(json)
            .then(&ParsePass::new(&ontology))
            .unwrap()
            .then(&NormalizePass::new(&ontology))
            .unwrap()
            .then(&LowerPass)
            .unwrap()
            .then(&OptimizePass::new(&sec_ctx))
            .unwrap()
            .into_context();

        let Node::Query(q) = ctx.node();
        assert!(q.limit.is_some());
    }

    #[test]
    fn parse_error_propagates() {
        let ontology = test_ontology();

        let result = CompilerRunner::new("not valid json").then(&ParsePass::new(&ontology));
        assert!(result.is_err());
    }

    #[test]
    fn observer_receives_pass_completions() {
        use std::sync::{Arc, Mutex};

        #[derive(Default)]
        struct RecordingObserver {
            completed: Arc<Mutex<Vec<(&'static str, Duration)>>>,
        }

        impl CompilerObserver for RecordingObserver {
            fn pass_completed(&mut self, name: &'static str, elapsed: Duration) {
                self.completed.lock().unwrap().push((name, elapsed));
            }
            fn pass_failed(&mut self, _name: &'static str, _error: &QueryError) {}
        }

        let ontology = test_ontology();
        let ctx = test_security_ctx();
        let completed = Arc::new(Mutex::new(Vec::new()));
        let obs = RecordingObserver {
            completed: Arc::clone(&completed),
        };

        let json = r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username"]},
            "limit": 10
        }"#;

        let _ = CompilerRunner::new(json)
            .with_observer(obs)
            .then(&ParsePass::new(&ontology))
            .unwrap()
            .then(&NormalizePass::new(&ontology))
            .unwrap()
            .then(&LowerPass)
            .unwrap()
            .then(&OptimizePass::new(&ctx))
            .unwrap()
            .then(&EnforcePass)
            .unwrap()
            .then(&SecurityPass::new(&ctx))
            .unwrap()
            .then(&CheckPass::new(&ctx))
            .unwrap()
            .then(&CodegenPass)
            .unwrap()
            .into_context();

        let names: Vec<_> = completed.lock().unwrap().iter().map(|(n, _)| *n).collect();
        assert_eq!(
            names,
            vec![
                "parse",
                "normalize",
                "lower",
                "optimize",
                "enforce",
                "security",
                "check",
                "codegen"
            ]
        );
    }

    #[test]
    fn observer_records_failures() {
        use std::sync::{Arc, Mutex};

        #[derive(Default)]
        struct FailureObserver {
            failed: Arc<Mutex<Vec<&'static str>>>,
        }

        impl CompilerObserver for FailureObserver {
            fn pass_completed(&mut self, _name: &'static str, _elapsed: Duration) {}
            fn pass_failed(&mut self, name: &'static str, _error: &QueryError) {
                self.failed.lock().unwrap().push(name);
            }
        }

        let failed = Arc::new(Mutex::new(Vec::new()));
        let obs = FailureObserver {
            failed: Arc::clone(&failed),
        };

        let bad_input = Input {
            query_type: QueryType::Search,
            ..Input::default()
        };

        let result = CompilerRunner::from_input(bad_input)
            .with_observer(obs)
            .then(&LowerPass);

        assert!(result.is_err());
        let names = failed.lock().unwrap().clone();
        assert_eq!(names, vec!["lower"]);
    }

    #[test]
    fn compile_clickhouse_matches_legacy_compile() {
        let ontology = test_ontology();
        let ctx = test_security_ctx();

        let json = r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username"]},
            "limit": 10
        }"#;

        let pipeline_result = compile_clickhouse(json, &ontology, &ctx).unwrap();
        let legacy_result = crate::compile(json, &ontology, &ctx).unwrap();

        assert_eq!(pipeline_result.base.sql, legacy_result.base.sql);
        assert_eq!(pipeline_result.query_type, legacy_result.query_type);
    }
}
