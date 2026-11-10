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

/// Every comparison a condition can make, with the names looked-up properties list.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::AsRefStr, strum::Display)]
#[strum(serialize_all = "snake_case")]
pub enum Operator {
    Eq,
    Ne,
    Gt,
    Lt,
    Gte,
    Lte,
    In,
    Contains,
    StartsWith,
    EndsWith,
    IsNull,
    IsNotNull,
    TokenMatch,
    AllTokens,
    AnyTokens,
}

impl Operator {
    pub fn comparison(self) -> Option<Comparison> {
        Some(match self {
            Self::Eq => Comparison::Eq,
            Self::Ne => Comparison::Ne,
            Self::Gt => Comparison::Gt,
            Self::Lt => Comparison::Lt,
            Self::Gte => Comparison::Ge,
            Self::Lte => Comparison::Le,
            _ => return None,
        })
    }

    pub fn matching(self) -> Option<Matching> {
        Some(match self {
            Self::Contains => Matching::Contains,
            Self::StartsWith => Matching::StartsWith,
            Self::EndsWith => Matching::EndsWith,
            Self::TokenMatch => Matching::Token,
            Self::AllTokens => Matching::AllTokens,
            Self::AnyTokens => Matching::AnyTokens,
            _ => return None,
        })
    }
}

impl From<Comparison> for Operator {
    fn from(comparison: Comparison) -> Self {
        match comparison {
            Comparison::Eq => Self::Eq,
            Comparison::Ne => Self::Ne,
            Comparison::Lt => Self::Lt,
            Comparison::Le => Self::Lte,
            Comparison::Gt => Self::Gt,
            Comparison::Ge => Self::Gte,
        }
    }
}

impl From<Matching> for Operator {
    fn from(matching: Matching) -> Self {
        match matching {
            Matching::Contains => Self::Contains,
            Matching::StartsWith => Self::StartsWith,
            Matching::EndsWith => Self::EndsWith,
            Matching::Token => Self::TokenMatch,
            Matching::AllTokens => Self::AllTokens,
            Matching::AnyTokens => Self::AnyTokens,
        }
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

    pub fn operator(&self) -> Option<Operator> {
        Some(match self {
            Self::Compare { comparison, .. } => (*comparison).into(),
            Self::In { .. } => Operator::In,
            Self::Text { matching, .. } => (*matching).into(),
            Self::Null { null: true, .. } => Operator::IsNull,
            Self::Null { null: false, .. } => Operator::IsNotNull,
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
#[derive(Debug, Clone, Copy)]
pub enum Conjunct<'p> {
    Leaf {
        predicate: &'p Predicate,
        negated: bool,
    },
    Negated(&'p Predicate),
}

impl<'p> Conjunct<'p> {
    pub fn attached(self) -> Option<&'p Predicate> {
        match self {
            Self::Leaf { predicate, negated } if !negated || predicate.complementable() => {
                Some(predicate)
            }
            _ => None,
        }
    }

    pub fn negated_leaves(self) -> Vec<&'p Predicate> {
        match self {
            Self::Leaf { predicate, negated } if negated && !predicate.complementable() => {
                vec![predicate]
            }
            Self::Leaf { .. } => Vec::new(),
            Self::Negated(group) => group.leaf_predicates(),
        }
    }
}

impl Predicate {
    pub fn conjuncts(&self) -> Vec<Conjunct<'_>> {
        fn walk<'p>(predicate: &'p Predicate, negated: bool, into: &mut Vec<Conjunct<'p>>) {
            match predicate {
                Predicate::Not(inner) => walk(inner, !negated, into),
                Predicate::And(all) if all.len() == 1 => walk(&all[0], negated, into),
                Predicate::And(_) if negated => into.push(Conjunct::Negated(predicate)),
                Predicate::And(all) => all.iter().for_each(|inner| walk(inner, false, into)),
                leaf => into.push(Conjunct::Leaf {
                    predicate: leaf,
                    negated,
                }),
            }
        }
        let mut conjuncts = Vec::new();
        walk(self, false, &mut conjuncts);
        conjuncts
    }

    pub fn complementable(&self) -> bool {
        matches!(
            self,
            Self::Compare {
                comparison: Comparison::Eq | Comparison::Ne,
                ..
            } | Self::Null { .. }
        )
    }

    pub fn leaf_predicates(&self) -> Vec<&Predicate> {
        match self {
            Self::And(all) => all.iter().flat_map(Self::leaf_predicates).collect(),
            Self::Not(inner) => inner.leaf_predicates(),
            leaf => vec![leaf],
        }
    }
}
