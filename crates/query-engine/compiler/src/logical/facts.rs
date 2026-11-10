use query_data_model::plan::logical::{
    Comparison, Conjunct, Expr, Literal, LogicalOperation, Matching, Operand, Predicate,
};
use query_data_model::plan::tracking::Ref;

use serde_json::Value;

use query_data_model::constants::DEFAULT_PRIMARY_KEY;

use super::{Binding, Query, binding};

#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::AsRefStr)]
#[strum(serialize_all = "snake_case")]
pub enum FilterOp {
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

#[derive(Clone, Copy)]
pub struct Fact<'p> {
    pub leaf: &'p Predicate,
    pub left: Expr,
    pub attached: bool,
    pub negated: bool,
    pub complemented: bool,
    pub comparison: Option<Comparison>,
}

impl<'p> Fact<'p> {
    pub fn binding(self) -> Ref {
        binding(self.left)
    }

    pub fn right(self) -> Option<Expr> {
        match self.leaf {
            Predicate::Compare {
                right: Operand::Expr(right),
                ..
            } => Some(*right),
            _ => None,
        }
    }

    pub fn compares_bindings(self) -> bool {
        self.right().is_some_and(|right| match self.left {
            Expr::Property { .. } => binding(right) != self.binding(),
            _ => true,
        })
    }

    /// The operator the leaf applies once its negation is folded in.
    pub fn op(self) -> Option<FilterOp> {
        Some(match self.leaf {
            Predicate::Null { null, .. } => match *null != self.complemented {
                true => FilterOp::IsNull,
                false => FilterOp::IsNotNull,
            },
            Predicate::In { .. } => FilterOp::In,
            Predicate::Text { matching, .. } => match matching {
                Matching::Contains => FilterOp::Contains,
                Matching::StartsWith => FilterOp::StartsWith,
                Matching::EndsWith => FilterOp::EndsWith,
                Matching::Token => FilterOp::TokenMatch,
                Matching::AllTokens => FilterOp::AllTokens,
                Matching::AnyTokens => FilterOp::AnyTokens,
            },
            Predicate::Compare { .. } => match self.comparison? {
                Comparison::Eq => FilterOp::Eq,
                Comparison::Ne => FilterOp::Ne,
                Comparison::Lt => FilterOp::Lt,
                Comparison::Le => FilterOp::Lte,
                Comparison::Gt => FilterOp::Gt,
                Comparison::Ge => FilterOp::Gte,
            },
            Predicate::And(_) | Predicate::Not(_) => return None,
        })
    }

    /// The literal operand: the compared value, the IN list, or the text needle.
    pub fn value(self) -> Option<Value> {
        let json = |literal: &Literal| match literal {
            Literal::String(text) | Literal::Temporal(text) => Value::String(text.clone()),
            Literal::Int(number) => Value::from(*number),
            Literal::Float(number) => Value::from(*number),
            Literal::Bool(flag) => Value::Bool(*flag),
        };
        match self.leaf {
            Predicate::Compare {
                right: Operand::Literal(literal),
                ..
            } => Some(json(literal)),
            Predicate::In { values, .. } => Some(Value::Array(values.iter().map(json).collect())),
            Predicate::Text { needle, .. } => Some(Value::String(needle.clone())),
            _ => None,
        }
    }

    pub fn literals(self) -> &'p [Literal] {
        match self.leaf {
            Predicate::Compare {
                right: Operand::Literal(literal),
                ..
            } => std::slice::from_ref(literal),
            Predicate::In { values, .. } => values,
            _ => &[],
        }
    }
}

pub fn facts<'p>(query: Query<'p>) -> Vec<Fact<'p>> {
    let mut facts = Vec::new();
    for (_, operation) in query.operations() {
        let LogicalOperation::Filter(predicate) = operation else {
            continue;
        };
        for conjunct in predicate.conjuncts() {
            match conjunct {
                Conjunct::Leaf { predicate, negated } => {
                    facts.extend(fact(predicate, negated, true));
                }
                Conjunct::Negated(group) => facts.extend(
                    group
                        .leaf_predicates()
                        .into_iter()
                        .filter_map(|leaf| fact(leaf, true, false)),
                ),
            }
        }
    }
    facts
}

fn fact(leaf: &Predicate, negated: bool, top: bool) -> Option<Fact<'_>> {
    let complemented = negated && leaf.complementable() && top;
    let left = *leaf.exprs().first()?;
    let comparison = match leaf {
        Predicate::Compare { comparison, .. } if complemented => Some(match comparison {
            Comparison::Eq => Comparison::Ne,
            Comparison::Ne => Comparison::Eq,
            other => *other,
        }),
        Predicate::Compare { comparison, .. } => Some(*comparison),
        _ => None,
    };
    let mut fact = Fact {
        leaf,
        left,
        attached: top && (!negated || leaf.complementable()),
        negated: negated && !complemented,
        complemented,
        comparison,
    };
    fact.attached &= !fact.compares_bindings();
    Some(fact)
}

pub struct Anchor<'p> {
    pub binding: Binding<'p>,
    /// The ids every top-level id equality and IN list admits; empty when they contradict.
    pub pinned: Option<Vec<i64>>,
    pub range: Option<(i64, i64)>,
    pub filters: Vec<Fact<'p>>,
}

pub fn anchors<'p>(query: Query<'p>, facts: &[Fact<'p>]) -> Vec<Anchor<'p>> {
    query
        .bindings()
        .into_iter()
        .map(|binding| anchor(query, binding, facts))
        .collect()
}

fn anchor<'p>(query: Query<'p>, binding: Binding<'p>, facts: &[Fact<'p>]) -> Anchor<'p> {
    let is_id = |fact: &Fact<'_>| matches!(fact.left, Expr::Property { property, .. } if query.property_name(property) == DEFAULT_PRIMARY_KEY);
    let (mut pinned, mut lower, mut upper) = (None::<Vec<i64>>, None::<i64>, None::<i64>);
    let mut filters = Vec::new();
    for fact in facts
        .iter()
        .copied()
        .filter(|fact| fact.attached && fact.binding() == binding.output)
    {
        match id_fact(fact).filter(|_| is_id(&fact)) {
            Some(IdFact::Pin(ids)) => {
                pinned = Some(match pinned {
                    Some(mut kept) => {
                        kept.retain(|id| ids.contains(id));
                        kept
                    }
                    None => ids,
                });
            }
            Some(IdFact::Lower(bound)) => {
                lower = Some(lower.map_or(bound, |kept| kept.max(bound)));
                filters.push((fact, true));
            }
            Some(IdFact::Upper(bound)) => {
                upper = Some(upper.map_or(bound, |kept| kept.min(bound)));
                filters.push((fact, true));
            }
            None => filters.push((fact, false)),
        }
    }
    let range = lower.zip(upper);
    Anchor {
        binding,
        pinned,
        range,
        filters: filters
            .into_iter()
            .filter(|(_, bound)| range.is_none() || !bound)
            .map(|(fact, _)| fact)
            .collect(),
    }
}

enum IdFact {
    Pin(Vec<i64>),
    Lower(i64),
    Upper(i64),
}

fn id_fact(fact: Fact<'_>) -> Option<IdFact> {
    match fact.leaf {
        Predicate::In { values, .. } => values
            .iter()
            .map(|value| match value {
                Literal::Int(id) => Some(*id),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()
            .map(IdFact::Pin),
        Predicate::Compare {
            right: Operand::Literal(Literal::Int(value)),
            ..
        } => match fact.comparison? {
            Comparison::Eq => Some(IdFact::Pin(vec![*value])),
            Comparison::Ge => Some(IdFact::Lower(*value)),
            Comparison::Gt => value.checked_add(1).map(IdFact::Lower),
            Comparison::Le => Some(IdFact::Upper(*value)),
            Comparison::Lt => value.checked_sub(1).map(IdFact::Upper),
            Comparison::Ne => None,
        },
        _ => None,
    }
}
