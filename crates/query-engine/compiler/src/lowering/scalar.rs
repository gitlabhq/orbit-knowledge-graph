use crate::ast::{Expr as SqlExpr, Op};
use crate::error::{QueryError, Result};
use crate::planning::generic::Expr;
use crate::planning::physical::Scalar;

use super::{Bindings, resolve};

pub fn emit(expression: &Expr<Scalar>, bindings: &Bindings) -> Result<SqlExpr> {
    Ok(match expression {
        Expr::Value(value) => resolve(bindings, *value)?,
        Expr::Bool(value) => SqlExpr::lit(*value),
        Expr::Int64(value) => SqlExpr::lit(*value),
        Expr::UInt64(value) => SqlExpr::lit(*value),
        Expr::String(value) => SqlExpr::lit(value.clone()),
        Expr::Float64(value) if value.is_finite() => SqlExpr::lit(*value),
        Expr::Null(_) => SqlExpr::lit(serde_json::Value::Null),
        Expr::Cast { value, data_type } => SqlExpr::Cast {
            value: Box::new(emit(value, bindings)?),
            data_type: data_type.clone(),
        },
        Expr::Call {
            function,
            arguments,
        } => {
            let arguments = arguments
                .iter()
                .map(|arg| emit(arg, bindings))
                .collect::<Result<Vec<_>>>()?;

            match (function, arguments.as_slice()) {
                (Scalar::Record, _) => SqlExpr::func("tuple", arguments),
                (Scalar::List, _) => SqlExpr::func("array", arguments),
                (Scalar::Equal, [left, right]) => SqlExpr::eq(left.clone(), right.clone()),
                (Scalar::NotEqual, [left, right]) => {
                    SqlExpr::binary(Op::Ne, left.clone(), right.clone())
                }
                (Scalar::Greater, [left, right]) => {
                    SqlExpr::binary(Op::Gt, left.clone(), right.clone())
                }
                (Scalar::GreaterEqual, [left, right]) => {
                    SqlExpr::binary(Op::Ge, left.clone(), right.clone())
                }
                (Scalar::Less, [left, right]) => {
                    SqlExpr::binary(Op::Lt, left.clone(), right.clone())
                }
                (Scalar::LessEqual, [left, right]) => {
                    SqlExpr::binary(Op::Le, left.clone(), right.clone())
                }
                (Scalar::And, [left, right]) => SqlExpr::and(left.clone(), right.clone()),
                (Scalar::Or, [left, right]) => SqlExpr::binary(Op::Or, left.clone(), right.clone()),
                (Scalar::IsNull, [value]) => SqlExpr::unary(Op::IsNull, value.clone()),
                (Scalar::IsNotNull, [value]) => SqlExpr::unary(Op::IsNotNull, value.clone()),
                (Scalar::Contains, [value, search]) => SqlExpr::binary(
                    Op::Gt,
                    SqlExpr::func(
                        "positionCaseInsensitive",
                        vec![value.clone(), search.clone()],
                    ),
                    SqlExpr::int(0),
                ),
                (Scalar::StartsWith, [value, prefix]) => {
                    SqlExpr::func("startsWith", vec![value.clone(), prefix.clone()])
                }
                (Scalar::EndsWith, [value, suffix]) => {
                    SqlExpr::func("endsWith", vec![value.clone(), suffix.clone()])
                }
                (Scalar::Truncate(unit), [value]) => SqlExpr::func(
                    "dateTrunc",
                    vec![SqlExpr::string(unit.name()), value.clone()],
                ),
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
