//! DataFusion backend — Substrait passthrough.
//!
//! DataFusion natively consumes Substrait plans via `datafusion-substrait`,
//! so this backend simply unwraps the IR plan into its raw Substrait form.

use substrait::proto::Plan as SubstraitPlan;

use crate::ir::plan::Plan;

/// DataFusion backend — passes through the raw Substrait plan.
pub struct DataFusionBackend;

impl super::Backend for DataFusionBackend {
    type Output = SubstraitPlan;
    type Error = DataFusionError;

    fn emit(&self, plan: &Plan) -> Result<Self::Output, Self::Error> {
        if !plan.ctes.is_empty() {
            return Err(DataFusionError::UnsupportedCtes);
        }
        Ok(plan.substrait_plan().clone())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DataFusionError {
    #[error("CTEs are not supported in the DataFusion backend (use the ClickHouse backend)")]
    UnsupportedCtes,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::Backend;
    use crate::ir::expr::*;
    use crate::ir::plan::PlanBuilder;

    #[test]
    fn passthrough_returns_substrait_plan() {
        let mut b = PlanBuilder::new();
        let root = b.read("t", "t", &[("id", DataType::Int64)]);
        let plan = b.build(root);

        let result = DataFusionBackend.emit(&plan).unwrap();
        assert_eq!(result.relations.len(), 1);
    }

    #[test]
    fn rejects_ctes() {
        let mut b = PlanBuilder::new();
        let root = b.read("t", "t", &[("id", DataType::Int64)]);
        let plan = b.build_with_ctes(
            root,
            vec![crate::ir::plan::CteDef {
                name: "cte".into(),
                plan: {
                    let mut b2 = PlanBuilder::new();
                    let r = b2.read("t", "t", &[("id", DataType::Int64)]);
                    b2.build(r)
                },
                recursive: false,
            }],
        );

        assert!(DataFusionBackend.emit(&plan).is_err());
    }
}
