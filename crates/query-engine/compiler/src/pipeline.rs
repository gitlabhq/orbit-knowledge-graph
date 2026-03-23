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
//! ClickHouse:  Parsed → Lower → Optimize → Enforce → Security → Check → Codegen
//! Hydration:   Parsed → Lower → Optimize → Enforce → Codegen (no security)
//! DuckDB:      Parsed → Lower → Optimize → Enforce → DuckDbCodegen (future)
//! ```
//!
//! # Example
//!
//! ```ignore
//! let compiled = CompilerRunner::parse(json, &ontology)?
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
use crate::codegen::{CompiledQueryContext, HydrationPlan};
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
    Parsed, Lowered, Optimized, Enforced, Secured, Checked, Emitted
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
    input: Input,
    node: Option<Node>,
    result_context: Option<ResultContext>,
    output: Option<CompiledQueryContext>,
    _phase: PhantomData<P>,
}

impl<P: Phase> CompilerContext<P> {
    /// Zero-cost phase transition — same memory layout, different phantom type.
    fn advance<Q: Phase>(self) -> CompilerContext<Q> {
        CompilerContext {
            input: self.input,
            node: self.node,
            result_context: self.result_context,
            output: self.output,
            _phase: PhantomData,
        }
    }

    /// Read access to the input (available at all phases).
    pub fn input(&self) -> &Input {
        &self.input
    }
}

// Phase-gated accessors: node is available after Lowered.
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

// result_context is available after Enforced.
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

/// A single compiler pass that transforms the context from one phase to
/// another.
///
/// Passes carry their own dependencies (ontology, security context, etc.)
/// so each pass declares exactly what it needs.
pub trait CompilerPass {
    /// Human-readable name for observability.
    const NAME: &'static str;

    /// Phase the context must be in before this pass can run.
    type In: Phase;

    /// Phase the context transitions to after this pass runs.
    type Out: Phase;

    /// Execute the pass, consuming the context at phase `In` and producing
    /// a context at phase `Out`.
    fn run(&self, ctx: CompilerContext<Self::In>) -> Result<CompilerContext<Self::Out>>;
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

impl CompilerRunner<Parsed> {
    /// Parse and validate a JSON query string into a typed `Input`, producing
    /// a runner at the `Parsed` phase.
    pub fn parse(json: &str, ontology: &ontology::Ontology) -> Result<Self> {
        let input = crate::validated_input(json, ontology)?;
        Ok(Self::from_input(input))
    }

    /// Start from a pre-built `Input` (for hydration queries or tests).
    pub fn from_input(input: Input) -> Self {
        CompilerRunner {
            ctx: CompilerContext {
                input,
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
    /// expects a different input phase.
    pub fn then<S: CompilerPass<In = P>>(mut self, pass: &S) -> Result<CompilerRunner<S::Out>> {
        let start = Instant::now();
        match pass.run(self.ctx) {
            Ok(ctx) => {
                if let Some(ref mut obs) = self.observer {
                    obs.pass_completed(S::NAME, start.elapsed());
                }
                Ok(CompilerRunner {
                    ctx,
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
// Pass implementations
// ─────────────────────────────────────────────────────────────────────────────

/// Lowers a validated `Input` into an AST `Node`.
pub struct LowerPass;

impl CompilerPass for LowerPass {
    const NAME: &'static str = "lower";
    type In = Parsed;
    type Out = Lowered;

    fn run(&self, mut ctx: CompilerContext<Parsed>) -> Result<CompilerContext<Lowered>> {
        let node = crate::lower::lower(&mut ctx.input)?;
        ctx.node = Some(node);
        Ok(ctx.advance())
    }
}

/// Applies query optimizations (SIP, join reordering, keyset pagination, etc.).
pub struct OptimizePass<'a> {
    pub security_context: &'a crate::SecurityContext,
}

impl<'a> OptimizePass<'a> {
    pub fn new(security_context: &'a crate::SecurityContext) -> Self {
        Self { security_context }
    }
}

impl CompilerPass for OptimizePass<'_> {
    const NAME: &'static str = "optimize";
    type In = Lowered;
    type Out = Optimized;

    fn run(&self, mut ctx: CompilerContext<Lowered>) -> Result<CompilerContext<Optimized>> {
        let node = ctx.node.as_mut().expect("node must exist after lowering");
        crate::optimize::optimize(node, &mut ctx.input, self.security_context);
        Ok(ctx.advance())
    }
}

/// Enforces redaction return columns (`_gkg_*`) and produces a `ResultContext`.
pub struct EnforcePass;

impl CompilerPass for EnforcePass {
    const NAME: &'static str = "enforce";
    type In = Optimized;
    type Out = Enforced;

    fn run(&self, mut ctx: CompilerContext<Optimized>) -> Result<CompilerContext<Enforced>> {
        let node = ctx.node.as_mut().expect("node must exist after optimize");
        let result_context = crate::enforce::enforce_return(node, &ctx.input)?;
        ctx.result_context = Some(result_context);
        Ok(ctx.advance())
    }
}

/// Injects `startsWith(traversal_path, ...)` security filters into the AST.
pub struct SecurityPass<'a> {
    pub security_context: &'a crate::SecurityContext,
}

impl<'a> SecurityPass<'a> {
    pub fn new(security_context: &'a crate::SecurityContext) -> Self {
        Self { security_context }
    }
}

impl CompilerPass for SecurityPass<'_> {
    const NAME: &'static str = "security";
    type In = Enforced;
    type Out = Secured;

    fn run(&self, mut ctx: CompilerContext<Enforced>) -> Result<CompilerContext<Secured>> {
        let node = ctx.node.as_mut().expect("node must exist after enforce");
        crate::security::apply_security_context(node, self.security_context)?;
        Ok(ctx.advance())
    }
}

/// Validates that all required security filters are present in the AST.
pub struct CheckPass<'a> {
    pub security_context: &'a crate::SecurityContext,
}

impl<'a> CheckPass<'a> {
    pub fn new(security_context: &'a crate::SecurityContext) -> Self {
        Self { security_context }
    }
}

impl CompilerPass for CheckPass<'_> {
    const NAME: &'static str = "check";
    type In = Secured;
    type Out = Checked;

    fn run(&self, ctx: CompilerContext<Secured>) -> Result<CompilerContext<Checked>> {
        let node = ctx.node.as_ref().expect("node must exist after security");
        crate::check::check_ast(node, self.security_context)?;
        Ok(ctx.advance())
    }
}

/// Generates parameterized SQL and a hydration plan.
///
/// This is the standard codegen pass for secured queries (ClickHouse).
pub struct CodegenPass;

impl CompilerPass for CodegenPass {
    const NAME: &'static str = "codegen";
    type In = Checked;
    type Out = Emitted;

    fn run(&self, mut ctx: CompilerContext<Checked>) -> Result<CompilerContext<Emitted>> {
        let node = ctx.node.as_ref().expect("node must exist");
        let result_context = ctx
            .result_context
            .take()
            .expect("result_context must exist");
        let base = crate::codegen::codegen(node, result_context)?;
        let hydration = crate::hydrate::generate_hydration_plan(&ctx.input);
        let query_type = ctx.input.query_type;
        let input = ctx.input.clone();

        let mut ctx: CompilerContext<Emitted> = ctx.advance();
        ctx.output = Some(CompiledQueryContext {
            query_type,
            base,
            hydration,
            input,
        });
        Ok(ctx)
    }
}

/// Codegen for hydration queries — accepts `Enforced` directly, skipping
/// security and check passes. Hydration queries are internal-only and
/// operate on pre-authorized IDs.
pub struct HydrationCodegenPass;

impl CompilerPass for HydrationCodegenPass {
    const NAME: &'static str = "hydration_codegen";
    type In = Enforced;
    type Out = Emitted;

    fn run(&self, mut ctx: CompilerContext<Enforced>) -> Result<CompilerContext<Emitted>> {
        let node = ctx.node.as_ref().expect("node must exist");
        let result_context = ctx
            .result_context
            .take()
            .expect("result_context must exist");
        let base = crate::codegen::codegen(node, result_context)?;
        let query_type = ctx.input.query_type;
        let input = ctx.input.clone();

        let mut ctx: CompilerContext<Emitted> = ctx.advance();
        ctx.output = Some(CompiledQueryContext {
            query_type,
            base,
            hydration: HydrationPlan::None,
            input,
        });
        Ok(ctx)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Pipeline presets
// ─────────────────────────────────────────────────────────────────────────────

/// Standard ClickHouse compilation pipeline.
///
/// ```text
/// JSON → Parse → Lower → Optimize → Enforce → Security → Check → Codegen
/// ```
pub fn compile_clickhouse(
    json: &str,
    ontology: &ontology::Ontology,
    security_ctx: &crate::SecurityContext,
) -> Result<CompiledQueryContext> {
    CompilerRunner::parse(json, ontology)?
        .with_observer(MetricsObserver)
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

        let ctx = CompilerRunner::parse(json, &ontology)
            .unwrap()
            .then(&LowerPass)
            .unwrap()
            .into_context();

        // Can inspect the AST after lowering.
        let Node::Query(q) = ctx.node();
        assert!(!q.select.is_empty());
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

        let ctx = CompilerRunner::parse(json, &ontology)
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

        let result = CompilerRunner::parse("not valid json", &ontology);
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

        let _ = CompilerRunner::parse(json, &ontology)
            .unwrap()
            .with_observer(obs)
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
                "lower", "optimize", "enforce", "security", "check", "codegen"
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
