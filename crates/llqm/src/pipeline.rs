use crate::ir::plan::Plan;

// ---------------------------------------------------------------------------
// Traits
// ---------------------------------------------------------------------------

/// Parses an external input format into the IR plan.
///
/// Implemented by each modality (query-engine for JSON DSL, indexer for ETL)
/// in their own crates. llqm provides the trait; consumers provide the impl.
pub trait Frontend {
    type Input;
    type Error;

    fn lower(&self, input: Self::Input) -> Result<Plan, Self::Error>;
}

/// Lowers an IR plan to a target-specific representation.
///
/// Each backend implements this trait to produce output for its execution
/// engine (e.g. parameterized SQL for ClickHouse, raw Substrait for DataFusion).
pub trait Backend {
    type Output;
    type Error;

    fn emit(&self, plan: &Plan) -> Result<Self::Output, Self::Error>;
}

// ---------------------------------------------------------------------------
// Phase markers (type-state)
// ---------------------------------------------------------------------------

pub struct Empty;
pub struct FrontendPhase<F: Frontend>(F, F::Input);
pub struct IrPhase(Plan);
pub struct EmittedPhase<O>(O);

// ---------------------------------------------------------------------------
// Pass traits — one per phase
// ---------------------------------------------------------------------------

/// Transforms the frontend's input type before lowering to IR.
pub trait FrontendPass<T> {
    type Error;
    fn transform(&self, input: T) -> Result<T, Self::Error>;
}

/// Transforms the IR plan between lowering and code generation.
pub trait IrPass {
    type Error;
    fn transform(&self, plan: Plan) -> Result<Plan, Self::Error>;
}

/// Transforms the emitted output after code generation.
pub trait EmitPass<O> {
    type Error;
    fn transform(&self, output: O) -> Result<O, Self::Error>;
}

// ---------------------------------------------------------------------------
// Pipeline
// ---------------------------------------------------------------------------

pub struct Pipeline<Phase>(Phase);

// --- Empty → .input(frontend, raw_input) → FrontendPhase ---

impl Pipeline<Empty> {
    pub fn new() -> Self {
        Pipeline(Empty)
    }

    pub fn input<F: Frontend>(self, frontend: F, raw: F::Input) -> Pipeline<FrontendPhase<F>> {
        Pipeline(FrontendPhase(frontend, raw))
    }
}

impl Default for Pipeline<Empty> {
    fn default() -> Self {
        Self::new()
    }
}

// --- FrontendPhase: .pass() and .lower() ---

impl<F: Frontend> Pipeline<FrontendPhase<F>> {
    pub fn pass<P>(self, p: &P) -> Result<Self, P::Error>
    where
        P: FrontendPass<F::Input>,
    {
        let FrontendPhase(frontend, input) = self.0;
        let input = p.transform(input)?;
        Ok(Pipeline(FrontendPhase(frontend, input)))
    }

    pub fn lower(self) -> Result<Pipeline<IrPhase>, F::Error> {
        let FrontendPhase(frontend, input) = self.0;
        let plan = frontend.lower(input)?;
        Ok(Pipeline(IrPhase(plan)))
    }
}

// --- IrPhase: .pass() and .emit() ---

impl Pipeline<IrPhase> {
    pub fn pass<P: IrPass>(self, p: &P) -> Result<Self, P::Error> {
        let plan = p.transform(self.0.0)?;
        Ok(Pipeline(IrPhase(plan)))
    }

    pub fn emit<B: Backend>(
        self,
        backend: &B,
    ) -> Result<Pipeline<EmittedPhase<B::Output>>, B::Error> {
        let output = backend.emit(&self.0.0)?;
        Ok(Pipeline(EmittedPhase(output)))
    }

    pub fn plan(&self) -> &Plan {
        &self.0.0
    }
}

// --- EmittedPhase: .pass() and .finish() ---

impl<O> Pipeline<EmittedPhase<O>> {
    pub fn pass<P: EmitPass<O>>(self, p: &P) -> Result<Self, P::Error> {
        let output = p.transform(self.0.0)?;
        Ok(Pipeline(EmittedPhase(output)))
    }

    pub fn output(&self) -> &O {
        &self.0.0
    }

    pub fn finish(self) -> O {
        self.0.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::clickhouse::ClickHouseBackend;
    use crate::ir::expr::*;
    use crate::ir::plan::Rel;

    // -- Minimal test frontend --

    struct TestFrontend;

    #[derive(Debug)]
    struct TestInput {
        table: String,
        filter_value: String,
    }

    #[derive(Debug, thiserror::Error)]
    #[error("test error")]
    struct TestError;

    impl Frontend for TestFrontend {
        type Input = TestInput;
        type Error = TestError;

        fn lower(&self, input: Self::Input) -> Result<Plan, Self::Error> {
            Ok(Rel::read(
                &input.table,
                "t",
                &[("id", DataType::Int64), ("name", DataType::String)],
            )
            .filter(col("t", "name").eq(string(&input.filter_value)))
            .project(&[(col("t", "id"), "id")])
            .into_plan())
        }
    }

    // -- Minimal test passes --

    struct UpperCasePass;

    impl FrontendPass<TestInput> for UpperCasePass {
        type Error = TestError;
        fn transform(&self, mut input: TestInput) -> Result<TestInput, Self::Error> {
            input.filter_value = input.filter_value.to_uppercase();
            Ok(input)
        }
    }

    struct NoopIrPass;

    impl IrPass for NoopIrPass {
        type Error = TestError;
        fn transform(&self, plan: Plan) -> Result<Plan, Self::Error> {
            Ok(plan)
        }
    }

    #[test]
    fn full_pipeline() {
        let input = TestInput {
            table: "users".into(),
            filter_value: "alice".into(),
        };

        let pq = Pipeline::new()
            .input(TestFrontend, input)
            .pass(&UpperCasePass)
            .unwrap()
            .lower()
            .unwrap()
            .pass(&NoopIrPass)
            .unwrap()
            .emit(&ClickHouseBackend)
            .unwrap()
            .finish();

        // UpperCasePass transforms "alice" → "ALICE" before lowering.
        // The ClickHouse backend parameterizes string literals, so the
        // value lives in pq.params, not inline in the SQL.
        assert!(pq.sql.contains("users"), "sql: {}", pq.sql);
        let val = &pq.params["p0"].value;
        assert_eq!(val, &serde_json::Value::String("ALICE".into()));
    }

    #[test]
    fn pipeline_without_passes() {
        let input = TestInput {
            table: "projects".into(),
            filter_value: "test".into(),
        };

        let pq = Pipeline::new()
            .input(TestFrontend, input)
            .lower()
            .unwrap()
            .emit(&ClickHouseBackend)
            .unwrap()
            .finish();

        assert!(pq.sql.contains("projects"), "sql: {}", pq.sql);
    }

    #[test]
    fn multiple_frontend_passes() {
        struct PrefixPass(&'static str);
        impl FrontendPass<TestInput> for PrefixPass {
            type Error = TestError;
            fn transform(&self, mut input: TestInput) -> Result<TestInput, Self::Error> {
                input.filter_value = format!("{}{}", self.0, input.filter_value);
                Ok(input)
            }
        }

        let input = TestInput {
            table: "t".into(),
            filter_value: "val".into(),
        };

        let pq = Pipeline::new()
            .input(TestFrontend, input)
            .pass(&PrefixPass("A_"))
            .unwrap()
            .pass(&PrefixPass("B_"))
            .unwrap()
            .lower()
            .unwrap()
            .emit(&ClickHouseBackend)
            .unwrap()
            .finish();

        let val = &pq.params["p0"].value;
        assert_eq!(val, &serde_json::Value::String("B_A_val".into()));
    }
}
