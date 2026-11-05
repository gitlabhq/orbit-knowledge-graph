use super::{Expr, Literal, Operand};
use crate::plan::tracking::Ref;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Comparison {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Matching {
    Contains,
    StartsWith,
    EndsWith,
    Token,
    AllTokens,
    AnyTokens,
}

impl Matching {
    pub fn tokens(self) -> bool {
        matches!(self, Self::Token | Self::AllTokens | Self::AnyTokens)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    Compare {
        left: Expr,
        comparison: Comparison,
        right: Operand,
    },
    In {
        left: Expr,
        values: Vec<Literal>,
    },
    Text {
        left: Expr,
        matching: Matching,
        needle: String,
    },
    Null {
        left: Expr,
        null: bool,
    },
    And(Vec<Predicate>),
    Not(Box<Predicate>),
}

impl Predicate {
    pub(super) fn references(&mut self, visit: &mut dyn FnMut(&mut Ref)) {
        match self {
            Self::Compare { left, right, .. } => {
                left.references(visit);
                if let Operand::Expr(right) = right {
                    right.references(visit);
                }
            }
            Self::In { left, .. } | Self::Text { left, .. } | Self::Null { left, .. } => {
                left.references(visit)
            }
            Self::And(all) => all
                .iter_mut()
                .for_each(|predicate| predicate.references(visit)),
            Self::Not(inner) => inner.references(visit),
        }
    }

    pub fn exprs(&self) -> Vec<Expr> {
        match self {
            Self::Compare {
                left,
                right: Operand::Expr(right),
                ..
            } => vec![*left, *right],
            Self::Compare { left, .. }
            | Self::In { left, .. }
            | Self::Text { left, .. }
            | Self::Null { left, .. } => vec![*left],
            Self::And(all) => all.iter().flat_map(Self::exprs).collect(),
            Self::Not(inner) => inner.exprs(),
        }
    }

    pub fn operator(&self) -> Option<&'static str> {
        Some(match self {
            Self::Compare { comparison, .. } => match comparison {
                Comparison::Eq => "eq",
                Comparison::Ne => "ne",
                Comparison::Lt => "lt",
                Comparison::Le => "lte",
                Comparison::Gt => "gt",
                Comparison::Ge => "gte",
            },
            Self::In { .. } => "in",
            Self::Text { matching, .. } => match matching {
                Matching::Contains => "contains",
                Matching::StartsWith => "starts_with",
                Matching::EndsWith => "ends_with",
                Matching::Token => "token_match",
                Matching::AllTokens => "all_tokens",
                Matching::AnyTokens => "any_tokens",
            },
            Self::Null { null: true, .. } => "is_null",
            Self::Null { null: false, .. } => "is_not_null",
            Self::And(_) | Self::Not(_) => return None,
        })
    }

    pub fn leaves(&self) -> usize {
        match self {
            Self::And(all) => all.iter().map(Self::leaves).sum(),
            Self::Not(inner) => inner.leaves(),
            _ => 1,
        }
    }

    #[must_use]
    pub fn and(self, other: Predicate) -> Predicate {
        match self {
            Self::And(mut all) => {
                all.push(other);
                Self::And(all)
            }
            first => Self::And(vec![first, other]),
        }
    }
}

impl std::ops::Not for Predicate {
    type Output = Predicate;

    fn not(self) -> Predicate {
        Self::Not(Box::new(self))
    }
}
