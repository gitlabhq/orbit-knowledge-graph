pub mod etl;
pub mod json_dsl;

use crate::ir::plan::Plan;

/// Parses an external input format into the IR plan.
///
/// Each frontend implements this trait for its input format
/// (e.g. JSON query DSL, ontology ETL config).
pub trait Frontend {
    type Input;
    type Error;

    fn lower(&self, input: Self::Input) -> Result<Plan, Self::Error>;
}
