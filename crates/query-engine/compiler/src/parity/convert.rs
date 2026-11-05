use query_data_model::implementations::relational::plan::{
    BinaryOp, Expr, Function, RelationalError, TextMatch, TimeUnit, UnaryOp,
};
use serde_json::Value;

use super::query::{Imported, Importer};
use super::tail::versioning;
use crate::ast::{self, Op, SqlType};
use crate::input::TruncateUnit;
use crate::passes::codegen::SqlDialect;

impl Importer {
    pub(super) fn predicate(&self, predicate: Option<&ast::Expr>) -> Option<ast::Expr> {
        match self.dialect {
            SqlDialect::ClickHouse => predicate.cloned(),
            SqlDialect::DuckDb => predicate.and_then(strip_deleted),
        }
    }
}

pub(super) fn deleted(expr: &ast::Expr) -> bool {
    matches!(expr, ast::Expr::BinaryOp { op: Op::Eq, left, .. } if versioning(left))
}

fn strip_deleted(expr: &ast::Expr) -> Option<ast::Expr> {
    match expr {
        expr if deleted(expr) => None,
        ast::Expr::BinaryOp {
            op: Op::And,
            left,
            right,
        } => match (strip_deleted(left), strip_deleted(right)) {
            (Some(left), Some(right)) => Some(ast::Expr::and(left, right)),
            (left, right) => left.or(right),
        },
        other => Some(other.clone()),
    }
}

pub(super) fn literal(value: &Value) -> Expr {
    match value {
        Value::Null => Expr::Null,
        Value::Array(items) => Expr::List(items.iter().map(literal).collect()),
        value => Expr::Parameter {
            data_type: SqlType::from_value(value),
            value: value.clone(),
        },
    }
}

pub(super) fn time_unit(unit: TruncateUnit) -> TimeUnit {
    match unit {
        TruncateUnit::Minute => TimeUnit::Minute,
        TruncateUnit::Hour => TimeUnit::Hour,
        TruncateUnit::Day => TimeUnit::Day,
        TruncateUnit::Week => TimeUnit::Week,
        TruncateUnit::Month => TimeUnit::Month,
        TruncateUnit::Quarter => TimeUnit::Quarter,
        TruncateUnit::Year => TimeUnit::Year,
    }
}

pub(super) fn text_match(mode: ast::TextMatch) -> TextMatch {
    match mode {
        ast::TextMatch::Contains => TextMatch::Contains,
        ast::TextMatch::TokenMatch => TextMatch::Token,
        ast::TextMatch::AllTokens => TextMatch::AllTokens,
        ast::TextMatch::AnyTokens => TextMatch::AnyTokens,
    }
}

pub(super) fn binary(op: Op) -> Imported<BinaryOp> {
    Ok(match op {
        Op::Eq => BinaryOp::Eq,
        Op::Ne => BinaryOp::Ne,
        Op::Lt => BinaryOp::Lt,
        Op::Le => BinaryOp::Le,
        Op::Gt => BinaryOp::Gt,
        Op::Ge => BinaryOp::Ge,
        Op::In => BinaryOp::In,
        Op::And => BinaryOp::And,
        Op::Or => BinaryOp::Or,
        Op::Add => BinaryOp::Add,
        Op::Not | Op::IsNull | Op::IsNotNull => {
            return Err(RelationalError::Unsupported("unary operators as binary"));
        }
    })
}

pub(super) fn unary(op: Op) -> Imported<UnaryOp> {
    Ok(match op {
        Op::Not => UnaryOp::Not,
        Op::IsNull => UnaryOp::IsNull,
        Op::IsNotNull => UnaryOp::IsNotNull,
        _ => return Err(RelationalError::Unsupported("binary operators as unary")),
    })
}

pub(super) fn function(function: ast::Function) -> Imported<Function> {
    use ast::Function as F;
    Ok(match function {
        F::StartsWith => Function::StartsWith,
        F::EndsWith => Function::EndsWith,
        F::Lower => Function::Lower,
        F::ToString => Function::ToString,
        F::ToJson => Function::ToJson,
        F::Object => Function::Object,
        F::If => Function::If,
        F::Coalesce => Function::Coalesce,
        F::NullIf => Function::NullIf,
        F::ByteLength => Function::ByteLength,
        F::Substring => Function::Substring,
        F::Concat => Function::Concat,
        F::CountSubstrings => Function::CountSubstrings,
        F::Array => Function::Array,
        F::Tuple => Function::Tuple,
        F::ArrayConcat => Function::ArrayConcat,
        F::ArrayReverse => Function::ArrayReverse,
        F::ArrayResize => Function::ArrayResize,
        F::ArrayContains => Function::ArrayContains,
        F::ArrayContainsAny => Function::ArrayContainsAny,
        F::ArrayContainsAll => Function::ArrayContainsAll,
        F::ArrayFilter => Function::ArrayFilter,
        F::ArrayMap => Function::ArrayMap,
        F::ArrayExists => Function::ArrayExists,
        F::Unnest => Function::Unnest,
        F::TupleElement => Function::TupleElement,
        F::ArgMax | F::ArgMaxOrNull => {
            return Err(RelationalError::Unsupported(
                "aggregates outside an aggregation",
            ));
        }
    })
}
