use crate::input::{AggFunction, FilterOp, InputFilter, TruncateUnit};
use ontology::DataType;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RelationId(pub u32);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnRef {
    pub relation: RelationId,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    Int(i64),
    Float(String),
    String(String),
    Bool(bool),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expr {
    Column(ColumnRef),
    Identifier(String),
    Literal(Value),
    Compare {
        op: CompareOp,
        left: Box<Self>,
        right: Box<Self>,
    },
    Filter {
        op: FilterOp,
        left: Box<Self>,
        right: Option<Box<Self>>,
        data_type: Option<DataType>,
    },
    And(Vec<Self>),
    Or(Vec<Self>),
    In {
        value: Box<Self>,
        values: Vec<Value>,
        data_type: Option<DataType>,
    },
    DateTrunc {
        unit: TruncateUnit,
        value: Box<Self>,
    },
    Aggregate {
        function: AggFunction,
        value: Option<Box<Self>>,
    },
    Array(Vec<Self>),
    Tuple(Vec<Self>),
    JsonObject(Vec<(String, Self)>),
    Stringify(Box<Self>),
    PathPrefixAny {
        value: Box<Self>,
        prefixes: Vec<String>,
    },
    ListContains {
        list: Box<Self>,
        value: Value,
    },
    TokenMatch {
        value: Box<Self>,
        token: Value,
        tokenizer: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamedExpr {
    pub expression: Expr,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortKey {
    pub expression: Expr,
    pub descending: bool,
}

impl Expr {
    pub fn eq(self, right: Self) -> Self {
        self.compare(CompareOp::Eq, right)
    }

    pub fn compare(self, op: CompareOp, right: Self) -> Self {
        Self::Compare {
            op,
            left: Box::new(self),
            right: Box::new(right),
        }
    }

    pub fn and(expressions: impl IntoIterator<Item = Self>) -> Option<Self> {
        let mut expressions = expressions.into_iter();
        let first = expressions.next()?;
        let rest: Vec<_> = expressions.collect();
        if rest.is_empty() {
            Some(first)
        } else {
            Some(Self::And(std::iter::once(first).chain(rest).collect()))
        }
    }

    pub fn relations(&self) -> HashSet<RelationId> {
        let mut relations = HashSet::new();
        self.visit(&mut |expression| {
            if let Self::Column(column) = expression {
                relations.insert(column.relation);
            }
        });
        relations
    }

    pub fn rewrite(self, rewrite: &mut impl FnMut(Self) -> Self) -> Self {
        let expression = match self {
            Self::Compare { op, left, right } => Self::Compare {
                op,
                left: Box::new(left.rewrite(rewrite)),
                right: Box::new(right.rewrite(rewrite)),
            },
            Self::Filter {
                op,
                left,
                right,
                data_type,
            } => Self::Filter {
                op,
                left: Box::new(left.rewrite(rewrite)),
                right: right.map(|right| Box::new(right.rewrite(rewrite))),
                data_type,
            },
            Self::And(expressions) => Self::And(
                expressions
                    .into_iter()
                    .map(|expression| expression.rewrite(rewrite))
                    .collect(),
            ),
            Self::Or(expressions) => Self::Or(
                expressions
                    .into_iter()
                    .map(|expression| expression.rewrite(rewrite))
                    .collect(),
            ),
            Self::In {
                value,
                values,
                data_type,
            } => Self::In {
                value: Box::new(value.rewrite(rewrite)),
                values,
                data_type,
            },
            Self::DateTrunc { unit, value } => Self::DateTrunc {
                unit,
                value: Box::new(value.rewrite(rewrite)),
            },
            Self::Aggregate { function, value } => Self::Aggregate {
                function,
                value: value.map(|value| Box::new(value.rewrite(rewrite))),
            },
            Self::Array(expressions) => Self::Array(
                expressions
                    .into_iter()
                    .map(|expression| expression.rewrite(rewrite))
                    .collect(),
            ),
            Self::Tuple(expressions) => Self::Tuple(
                expressions
                    .into_iter()
                    .map(|expression| expression.rewrite(rewrite))
                    .collect(),
            ),
            Self::JsonObject(entries) => Self::JsonObject(
                entries
                    .into_iter()
                    .map(|(key, value)| (key, value.rewrite(rewrite)))
                    .collect(),
            ),
            Self::Stringify(value) => Self::Stringify(Box::new(value.rewrite(rewrite))),
            Self::PathPrefixAny { value, prefixes } => Self::PathPrefixAny {
                value: Box::new(value.rewrite(rewrite)),
                prefixes,
            },
            Self::ListContains { list, value } => Self::ListContains {
                list: Box::new(list.rewrite(rewrite)),
                value,
            },
            Self::TokenMatch {
                value,
                token,
                tokenizer,
            } => Self::TokenMatch {
                value: Box::new(value.rewrite(rewrite)),
                token,
                tokenizer,
            },
            leaf => leaf,
        };
        rewrite(expression)
    }

    pub fn substitute(self, columns: &HashMap<ColumnRef, Expr>) -> Self {
        self.rewrite(&mut |expression| match expression {
            Self::Column(column) => columns
                .get(&column)
                .cloned()
                .unwrap_or(Self::Column(column)),
            other => other,
        })
    }

    pub fn rebind(self, from: RelationId, to: RelationId) -> Self {
        self.rewrite(&mut |expression| match expression {
            Self::Column(mut column) => {
                if column.relation == from {
                    column.relation = to;
                }
                Self::Column(column)
            }
            other => other,
        })
    }

    fn visit(&self, visitor: &mut impl FnMut(&Self)) {
        visitor(self);
        match self {
            Self::Compare { left, right, .. } => {
                left.visit(visitor);
                right.visit(visitor);
            }
            Self::Filter { left, right, .. } => {
                left.visit(visitor);
                if let Some(right) = right {
                    right.visit(visitor);
                }
            }
            Self::And(expressions) | Self::Or(expressions) | Self::Array(expressions) | Self::Tuple(expressions) => {
                expressions.iter().for_each(|expression| expression.visit(visitor));
            }
            Self::In { value, .. }
            | Self::DateTrunc { value, .. }
            | Self::Stringify(value)
            | Self::PathPrefixAny { value, .. }
            | Self::ListContains { list: value, .. }
            | Self::TokenMatch { value, .. } => value.visit(visitor),
            Self::Aggregate { value, .. } => {
                if let Some(value) = value {
                    value.visit(visitor);
                }
            }
            Self::JsonObject(entries) => entries
                .iter()
                .for_each(|(_, value)| value.visit(visitor)),
            Self::Column(_) | Self::Identifier(_) | Self::Literal(_) => {}
        }
    }
}

pub fn column(relation: RelationId, name: impl Into<String>) -> Expr {
    Expr::Column(ColumnRef {
        relation: relation.into(),
        name: name.into(),
    })
}

pub fn literal(value: impl Into<Value>) -> Expr {
    Expr::Literal(value.into())
}

pub fn named(expression: Expr, name: impl Into<String>) -> NamedExpr {
    NamedExpr {
        expression,
        name: name.into(),
    }
}

pub fn filters(
    relation: RelationId,
    filters: &std::collections::HashMap<String, Vec<InputFilter>>,
) -> Vec<Expr> {
    let mut properties: Vec<_> = filters.iter().collect();
    properties.sort_unstable_by_key(|(property, _)| *property);
    properties
        .into_iter()
        .flat_map(|(property, filters)| {
            filters
                .iter()
                .map(move |filter| filter_expr(relation, property, filter))
        })
        .collect()
}

fn filter_expr(relation: RelationId, property: &str, filter: &InputFilter) -> Expr {
    let left = column(relation, property);
    if filter.op == Some(FilterOp::In) {
        return Expr::In {
            value: Box::new(left),
            values: filter
                .value
                .as_ref()
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
                .map(Value::from)
                .collect(),
            data_type: filter.data_type,
        };
    }
    Expr::Filter {
        op: filter.op.unwrap_or(FilterOp::Eq),
        left: Box::new(left),
        right: filter.rhs_column.as_ref().map_or_else(
            || filter.value.as_ref().map(|value| Box::new(literal(Value::from(value)))),
            |_| None,
        ),
        data_type: filter.data_type,
    }
}

impl From<&serde_json::Value> for Value {
    fn from(value: &serde_json::Value) -> Self {
        match value {
            serde_json::Value::Bool(value) => Self::Bool(*value),
            serde_json::Value::Number(value) => value
                .as_i64()
                .map_or_else(|| Self::Float(value.to_string()), Self::Int),
            serde_json::Value::String(value) => Self::String(value.clone()),
            other => Self::String(other.to_string()),
        }
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Self::Int(value)
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Self::String(value.to_string())
    }
}
