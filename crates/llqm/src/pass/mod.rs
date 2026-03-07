pub mod security;

use crate::ir::plan::Plan;

/// An IR-to-IR transformation pass.
///
/// Passes inspect and/or rewrite the Substrait plan tree. They run between
/// the frontend (which produces the plan) and the backend (which lowers it
/// to a target representation).
///
/// Examples: security context injection, query verification, optimization.
pub trait Pass {
    type Error;

    fn transform(&self, plan: Plan) -> Result<Plan, Self::Error>;
}

/// Run a sequence of passes over a plan.
///
/// Passes execute in order; each receives the output of the previous one.
/// Short-circuits on the first error.
pub fn run_passes<E>(plan: Plan, passes: &[&dyn Pass<Error = E>]) -> Result<Plan, E> {
    passes.iter().try_fold(plan, |p, pass| pass.transform(p))
}
