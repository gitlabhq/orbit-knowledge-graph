pub mod clickhouse;
pub mod datafusion;

use crate::ir::plan::Plan;

/// Lowers an IR plan to a target-specific representation.
///
/// Each backend implements this trait to produce output for its execution
/// engine (e.g. parameterized SQL for ClickHouse, raw Substrait for DataFusion).
pub trait Backend {
    type Output;
    type Error;

    fn emit(&self, plan: &Plan) -> Result<Self::Output, Self::Error>;
}
