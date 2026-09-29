use crate::ast::{ChType, Expr as SqlExpr, Op};
use crate::error::{QueryError, Result};
use crate::planning::generic::Expr;
use crate::planning::physical::Scalar;

use super::{Bindings, resolve};

pub fn emit(expression: &Expr<Scalar>, bindings: &Bindings) -> Result<SqlExpr> {
    Ok(match expression {
        Expr::Value(value) => resolve(bindings, *value)?,
        Expr::Bool(value) => SqlExpr::param(ChType::Bool, *value),
        Expr::Int64(value) => SqlExpr::int(*value),
        Expr::String(value) => SqlExpr::string(value),
        Expr::Float64(value) if value.is_finite() => SqlExpr::param(ChType::Float64, *value),
        Expr::Null(_) => SqlExpr::lit(serde_json::Value::Null),
        Expr::Call {
            function,
            arguments,
        } => {
            let arguments = arguments
                .iter()
                .map(|arg| emit(arg, bindings))
                .collect::<Result<Vec<_>>>()?;
            match (function, arguments.as_slice()) {
                (Scalar::Equal, [left, right]) => SqlExpr::eq(left.clone(), right.clone()),
                (Scalar::And, [left, right]) => SqlExpr::and(left.clone(), right.clone()),
                (Scalar::IsNull, [value]) => SqlExpr::unary(Op::IsNull, value.clone()),
                _ => return Err(QueryError::Lowering("invalid scalar arguments".into())),
            }
        }
        _ => {
            return Err(QueryError::Lowering(
                "scalar has no SQL implementation yet".into(),
            ));
        }
    })
}
