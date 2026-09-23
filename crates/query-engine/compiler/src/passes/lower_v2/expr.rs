//! Plan expressions to SQL AST expressions.

use crate::ast::*;
use crate::passes::plan_v2::*;
use crate::passes::shared::filter_to_expr;

pub fn expr(e: &PExpr) -> Expr {
    match e {
        PExpr::Col(a, c) => Expr::col(a, c),
        PExpr::Ident(name) => name.parse::<i64>().map_or_else(|_| Expr::ident(name), Expr::int),
        PExpr::Lit(Lit::Int(i)) => Expr::int(*i),
        PExpr::Lit(Lit::Str(s)) => Expr::string(s),
        PExpr::Lit(Lit::Bool(b)) => Expr::param(ChType::Bool, *b),
        PExpr::Func(name, args) => Expr::func(name, args.iter().map(expr).collect()),
        PExpr::Cmp(op, l, r) => {
            let op = match op {
                CmpOp::Eq => Op::Eq,
                CmpOp::Ne => Op::Ne,
                CmpOp::Lt => Op::Lt,
                CmpOp::Le => Op::Le,
                CmpOp::Gt => Op::Gt,
                CmpOp::Ge => Op::Ge,
            };
            Expr::binary(op, expr(l), expr(r))
        }
        PExpr::And(xs) => xs
            .iter()
            .map(expr)
            .reduce(Expr::and)
            .unwrap_or_else(|| Expr::lit(1)),
        // Balanced so hundreds of alternatives stay within parser depth.
        PExpr::Or(xs) => or_balanced(xs.iter().map(expr).collect()),
        PExpr::In(x, vs) => {
            let PExpr::Col(a, c) = x.as_ref() else {
                panic!("IN over a non-column expression");
            };
            let ch_type = match vs.first() {
                Some(Lit::Int(_)) => ChType::Int64,
                Some(Lit::Bool(_)) => ChType::Bool,
                _ => ChType::String,
            };
            let values = vs
                .iter()
                .map(|v| match v {
                    Lit::Int(i) => serde_json::Value::from(*i),
                    Lit::Str(s) => serde_json::Value::from(s.as_str()),
                    Lit::Bool(b) => serde_json::Value::from(*b),
                })
                .collect();
            Expr::col_in(a, c, ch_type, values).unwrap_or_else(|| Expr::param(ChType::Bool, false))
        }
        PExpr::Lambda(param, body) => Expr::lambda(param, expr(body)),
        PExpr::DateTrunc(unit, value) => {
            let truncated = Expr::func(unit.ch_function(), vec![expr(value)]);
            match unit {
                TruncateUnit::Minute | TruncateUnit::Hour => {
                    Expr::func("toDateTime64", vec![truncated, Expr::ident("0")])
                }
                _ => Expr::func("toDate32", vec![truncated]),
            }
        }
        PExpr::NodeFilter {
            alias,
            property,
            filter,
        } => filter_to_expr(alias, property, filter),
        PExpr::Scope(alias, prefix) => prefix.predicate(alias),
        PExpr::ScopeResolved(prefix) => prefix.resolved(),
    }
}

fn or_balanced(mut xs: Vec<Expr>) -> Expr {
    match xs.len() {
        0 => Expr::lit(0),
        1 => xs.pop().unwrap(),
        n => {
            let right = xs.split_off(n / 2);
            Expr::binary(Op::Or, or_balanced(xs), or_balanced(right))
        }
    }
}
