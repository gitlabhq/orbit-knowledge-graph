use orbit_utils::query_types::SqlType;
use serde_json::Value;

use crate::plan::tracking::{BodyId, Ref};

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Column(Ref),
    Variable(String),
    Parameter {
        data_type: SqlType,
        value: Value,
    },
    List(Vec<Expr>),
    Null,
    Bool(bool),
    Call {
        function: Function,
        args: Vec<Expr>,
    },
    EmptyArray(Vec<SqlType>),
    TimeBucket {
        unit: TimeUnit,
        value: Box<Expr>,
    },
    TextSearch {
        mode: TextMatch,
        value: Box<Expr>,
        query: Box<Expr>,
    },
    Lambda {
        param: String,
        body: Box<Expr>,
    },
    Binary {
        op: BinaryOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Unary {
        op: UnaryOp,
        value: Box<Expr>,
    },
    In {
        value: Box<Expr>,
        query: BodyId,
    },
    Scalar(BodyId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Function {
    StartsWith,
    EndsWith,
    Lower,
    ToString,
    ToJson,
    Object,
    If,
    Coalesce,
    NullIf,
    ByteLength,
    Substring,
    Concat,
    CountSubstrings,
    Array,
    Tuple,
    ArrayConcat,
    ArrayReverse,
    ArrayResize,
    ArrayContains,
    ArrayContainsAny,
    ArrayContainsAll,
    ArrayFilter,
    ArrayMap,
    ArrayExists,
    Unnest,
    TupleElement,
}

impl Function {
    pub fn accepts(self, count: usize) -> bool {
        match self {
            Self::Array | Self::Tuple => true,
            Self::Object => count.is_multiple_of(2),
            Self::Coalesce | Self::Concat | Self::ArrayConcat => count >= 1,
            Self::Lower
            | Self::ToString
            | Self::ToJson
            | Self::ByteLength
            | Self::ArrayReverse
            | Self::Unnest => count == 1,
            Self::If | Self::Substring => count == 3,
            _ => count == 2,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeUnit {
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Quarter,
    Year,
}

impl TimeUnit {
    pub fn name(self) -> &'static str {
        match self {
            Self::Minute => "minute",
            Self::Hour => "hour",
            Self::Day => "day",
            Self::Week => "week",
            Self::Month => "month",
            Self::Quarter => "quarter",
            Self::Year => "year",
        }
    }

    pub fn keeps_time(self) -> bool {
        matches!(self, Self::Minute | Self::Hour)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TextMatch {
    Contains,
    Token,
    AllTokens,
    AnyTokens,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    In,
    And,
    Or,
    Add,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Not,
    IsNull,
    IsNotNull,
}

impl Expr {
    pub(super) fn visit(&mut self, visit: &mut dyn FnMut(&mut Ref)) {
        match self {
            Self::Column(output) => visit(output),
            Self::List(items) | Self::Call { args: items, .. } => {
                items.iter_mut().for_each(|item| item.visit(visit));
            }
            Self::TimeBucket { value, .. }
            | Self::Lambda { body: value, .. }
            | Self::Unary { value, .. }
            | Self::In { value, .. } => value.visit(visit),
            Self::TextSearch { value, query, .. } => {
                value.visit(visit);
                query.visit(visit);
            }
            Self::Binary { left, right, .. } => {
                left.visit(visit);
                right.visit(visit);
            }
            Self::Variable(_)
            | Self::Parameter { .. }
            | Self::Null
            | Self::Bool(_)
            | Self::EmptyArray(_)
            | Self::Scalar(_) => {}
        }
    }

    pub(super) fn children(&self) -> Vec<&Expr> {
        match self {
            Self::List(items) | Self::Call { args: items, .. } => items.iter().collect(),
            Self::TimeBucket { value, .. }
            | Self::Lambda { body: value, .. }
            | Self::Unary { value, .. }
            | Self::In { value, .. } => vec![value],
            Self::TextSearch { value, query, .. } => vec![value, query],
            Self::Binary { left, right, .. } => vec![left, right],
            Self::Column(_)
            | Self::Variable(_)
            | Self::Parameter { .. }
            | Self::Null
            | Self::Bool(_)
            | Self::EmptyArray(_)
            | Self::Scalar(_) => Vec::new(),
        }
    }

    pub(super) fn width(&self) -> usize {
        match self {
            Self::List(items)
            | Self::Call {
                function: Function::Tuple,
                args: items,
            } => items.len(),
            _ => 1,
        }
    }
}
