use std::borrow::Cow;

use query_data_model::PropertyId;
use query_data_model::plan::logical::{
    Comparison, Conjunct, EdgeField, Expr, Literal, LogicalOperation, Operand, Predicate,
};
use query_data_model::plan::tracking::Ref;
use serde_json::Value;

use super::{Boolean, Query};

/// A filter leaf after the planner's NOT rule: a NOT folds into the leaf when it has an exact
/// complement and stays above it otherwise.
#[derive(Debug, Clone)]
pub struct Leaf<'p> {
    pub predicate: Cow<'p, Predicate>,
    /// Some NOT stays above the leaf.
    pub negated: bool,
}

/// The top-level conjuncts of a filter, with NOT pushed down to the leaves that complement.
pub fn polarized(predicate: &Predicate) -> Vec<Boolean<Leaf<'_>>> {
    fn walk(predicate: &Predicate, negated: bool) -> Vec<Boolean<Leaf<'_>>> {
        let leaf = |predicate, negated| Boolean::Leaf(Leaf { predicate, negated });
        let conjunct = |conjunct| match conjunct {
            Conjunct::Leaf {
                predicate,
                negated: false,
            } => leaf(Cow::Borrowed(predicate), negated),
            Conjunct::Leaf { predicate, .. } => match predicate.complement() {
                Some(complement) => leaf(Cow::Owned(complement), negated),
                None => Boolean::Not(Box::new(leaf(Cow::Borrowed(predicate), true))),
            },
            Conjunct::Negated(group) => {
                let children = match group {
                    Predicate::And(all) => all.iter().flat_map(|child| walk(child, true)).collect(),
                    other => walk(other, true),
                };
                Boolean::Not(Box::new(Boolean::And(children)))
            }
        };
        predicate.conjuncts().into_iter().map(conjunct).collect()
    }
    walk(predicate, false)
}

pub fn json(literal: &Literal) -> Value {
    match literal {
        Literal::String(text) | Literal::Temporal(text) => Value::String(text.clone()),
        Literal::Int(number) => Value::from(*number),
        Literal::Float(number) => Value::from(*number),
        Literal::Bool(flag) => Value::Bool(*flag),
    }
}

impl Leaf<'_> {
    pub fn left(&self) -> Expr {
        match &*self.predicate {
            Predicate::Compare { left, .. }
            | Predicate::In { left, .. }
            | Predicate::Text { left, .. }
            | Predicate::Null { left, .. } => *left,
            Predicate::And(_) | Predicate::Not(_) => unreachable!("polarized leaves are leaves"),
        }
    }

    pub fn binding(&self) -> Ref {
        binding(self.left())
    }

    pub fn right(&self) -> Option<Expr> {
        match &*self.predicate {
            Predicate::Compare {
                right: Operand::Expr(right),
                ..
            } => Some(*right),
            _ => None,
        }
    }

    pub fn edge_kind(&self) -> bool {
        matches!(
            self.left(),
            Expr::Edge {
                field: EdgeField::Kind,
                ..
            }
        )
    }

    pub fn compares_bindings(&self) -> bool {
        self.right().is_some_and(|right| match self.left() {
            Expr::Property { .. } => binding(right) != self.binding(),
            _ => true,
        })
    }

    /// Whether the leaf filters its own binding's rows: no NOT above it and no other binding compared.
    pub fn attached(&self) -> bool {
        !self.negated && !self.compares_bindings()
    }

    pub fn comparison(&self) -> Option<Comparison> {
        match &*self.predicate {
            Predicate::Compare { comparison, .. } => Some(*comparison),
            _ => None,
        }
    }

    /// The compared value or the IN list.
    pub fn literals(&self) -> &[Literal] {
        match &*self.predicate {
            Predicate::Compare {
                right: Operand::Literal(literal),
                ..
            } => std::slice::from_ref(literal),
            Predicate::In { values, .. } => values,
            _ => &[],
        }
    }

    /// The literal operand: the compared value, the IN list, or the text needle.
    pub fn value(&self) -> Option<Value> {
        match &*self.predicate {
            Predicate::Compare {
                right: Operand::Literal(literal),
                ..
            } => Some(json(literal)),
            Predicate::In { values, .. } => Some(Value::Array(values.iter().map(json).collect())),
            Predicate::Text { needle, .. } => Some(Value::String(needle.clone())),
            _ => None,
        }
    }
}

pub fn binding(expr: Expr) -> Ref {
    match expr {
        Expr::Property { binding, .. }
        | Expr::Truncate { binding, .. }
        | Expr::Edge { binding, .. }
        | Expr::Attribute { binding, .. }
        | Expr::Node(binding)
        | Expr::Output(binding) => binding,
    }
}

impl<'p> Query<'p> {
    pub fn property_name(self, property: PropertyId) -> &'p str {
        &self.model.graph().property(property).name
    }

    /// The top-level conjuncts of every filter, polarized.
    pub fn filters(self) -> Vec<Boolean<Leaf<'p>>> {
        self.operations()
            .filter_map(|(_, operation)| match operation {
                LogicalOperation::Filter(predicate) => Some(polarized(predicate)),
                _ => None,
            })
            .flatten()
            .collect()
    }

    pub fn leaves(self) -> Vec<Leaf<'p>> {
        self.filters()
            .iter()
            .flat_map(|tree| tree.leaves().cloned())
            .collect()
    }
}
