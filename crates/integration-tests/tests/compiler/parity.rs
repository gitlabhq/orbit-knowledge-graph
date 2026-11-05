use std::sync::Arc;

use compiler::parity::Outcome;
use compiler::passes::codegen::CompiledQueryContext;
use compiler::{Frontend, HydrationCompileOptions, Input, Ontology, SecurityContext};

fn assert_parity(
    compiled: &compiler::Result<CompiledQueryContext>,
    check: impl FnOnce(&CompiledQueryContext) -> Result<Outcome, String>,
) {
    if let Ok(compiled) = compiled
        && let Err(mismatch) = check(compiled)
    {
        panic!("renderer parity: {mismatch}");
    }
}

fn check_clickhouse(
    compiled: &CompiledQueryContext,
    ontology: &Arc<Ontology>,
) -> Result<Outcome, String> {
    let model = compiler::data_model::clickhouse(Arc::clone(ontology))
        .map_err(|error| error.to_string())?;
    compiler::parity::check_clickhouse(compiled, &model)
}

pub fn compile(
    raw: &str,
    frontend: Frontend,
    ontology: &Arc<Ontology>,
    context: &SecurityContext,
) -> compiler::Result<CompiledQueryContext> {
    let compiled = compiler::compile(raw, frontend, ontology, context);
    assert_parity(&compiled, |compiled| check_clickhouse(compiled, ontology));
    compiled
}

pub fn compile_input(
    input: Input,
    options: HydrationCompileOptions,
    ontology: &Arc<Ontology>,
    context: &SecurityContext,
) -> compiler::Result<CompiledQueryContext> {
    let compiled = compiler::compile_input(input, options, ontology, context);
    assert_parity(&compiled, |compiled| check_clickhouse(compiled, ontology));
    compiled
}

pub fn compile_local(
    raw: &str,
    frontend: Frontend,
    ontology: &Arc<Ontology>,
) -> compiler::Result<CompiledQueryContext> {
    let compiled = compiler::compile_local(raw, frontend, ontology);
    assert_parity(&compiled, |compiled| {
        let mut local = ontology.as_ref().clone();
        local.remove_data_model_optimizations();
        let model =
            compiler::data_model::duckdb(Arc::new(local)).map_err(|error| error.to_string())?;
        compiler::parity::check_duckdb(compiled, &model)
    });
    compiled
}
