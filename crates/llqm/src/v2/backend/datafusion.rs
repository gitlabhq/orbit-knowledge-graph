//! v2 DataFusion backend — encodes a v2 `Plan` to Substrait protobuf.
//!
//! DataFusion natively consumes Substrait plans via `datafusion-substrait`,
//! so this backend encodes the v2 plan into its Substrait form.
//! All Substrait construction is delegated to `v2::substrait::encode`.

use crate::v2::plan::Plan;
use crate::v2::substrait as v2_substrait;

pub use v2_substrait::EncodeError;

pub struct DataFusionBackend;

impl DataFusionBackend {
    pub fn emit(&self, plan: &Plan) -> Result<::substrait::proto::Plan, EncodeError> {
        v2_substrait::encode(plan)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::expr::*;
    use crate::v2::plan::{CteDef, Rel};

    #[test]
    fn passthrough_returns_substrait_plan() {
        let plan = Rel::read("t", "t", &[("id", DataType::Int64)])
            .project(&[(col("t", "id"), "id")])
            .into_plan();

        let result = DataFusionBackend.emit(&plan).unwrap();
        assert_eq!(result.relations.len(), 1);
    }

    #[test]
    fn rejects_ctes() {
        let cte_plan = Rel::read("t", "t", &[("id", DataType::Int64)])
            .project(&[(col("t", "id"), "id")])
            .into_plan();

        let plan = Rel::read("base", "b", &[("id", DataType::Int64)])
            .project(&[(col("b", "id"), "id")])
            .into_plan_with_ctes(vec![CteDef {
                name: "base".into(),
                plan: cte_plan,
                recursive: false,
            }]);

        assert!(DataFusionBackend.emit(&plan).is_err());
    }

    #[test]
    fn join_encodes_correctly() {
        let plan = Rel::read("gl_project", "p", &[("id", DataType::Int64)])
            .join(
                JoinType::Inner,
                Rel::read("gl_merge_request", "mr", &[("project_id", DataType::Int64)]),
                col("p", "id").eq(col("mr", "project_id")),
            )
            .project(&[(col("p", "id"), "id")])
            .into_plan();

        let result = DataFusionBackend.emit(&plan).unwrap();
        assert_eq!(result.relations.len(), 1);
        assert!(
            !result.extensions.is_empty(),
            "should have function extensions"
        );
    }
}
