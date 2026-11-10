use query_data_model::PropertyId;
use query_data_model::plan::logical::{
    Comparison, EdgeField, Expr, Literal, LogicalOperation, Matching, Operand, Predicate,
};
use query_data_model::plan::tracking::Ref;

use super::{Boolean, Query};

/// A filter leaf after the planner's NOT rule. NOT folds into `=`, `<>` and null checks, whose
/// complements are exact under SQL's three-valued logic, and stays above every other operand.
#[derive(Debug, Clone, Copy)]
pub struct Leaf<'p> {
    pub predicate: &'p Predicate,
    pub left: Expr,
    pub complemented: bool,
}

#[derive(Debug, Clone, Copy)]
pub enum Literals<'p> {
    One(&'p Literal),
    Many(&'p [Literal]),
    None,
}

/// The filter tree with NOT pushed down by the planner's rule: negation flips at every NOT, a
/// negated conjunction keeps its NOT, and a negated leaf folds in when it has an exact complement.
pub fn polarized(predicate: &Predicate) -> Boolean<Leaf<'_>> {
    fn walk(predicate: &Predicate, negated: bool) -> Boolean<Leaf<'_>> {
        let tree = match predicate {
            Predicate::Not(inner) => return walk(inner, !negated),
            Predicate::And(all) if all.len() == 1 => return walk(&all[0], negated),
            Predicate::And(all) => Boolean::And(
                all.iter()
                    .flat_map(|child| walk(child, false).conjuncts())
                    .collect(),
            ),
            Predicate::Compare { left, .. }
            | Predicate::In { left, .. }
            | Predicate::Text { left, .. }
            | Predicate::Null { left, .. } => {
                let complemented = negated && predicate.complementable();
                let leaf = Boolean::Leaf(Leaf {
                    predicate,
                    left: *left,
                    complemented,
                });
                if complemented {
                    return leaf;
                }
                leaf
            }
        };
        match negated {
            true => Boolean::Not(Box::new(tree)),
            false => tree,
        }
    }
    walk(predicate, false)
}

impl<'p> Leaf<'p> {
    pub fn binding(self) -> Ref {
        binding(self.left)
    }

    pub fn property(self) -> Option<PropertyId> {
        match self.left {
            Expr::Property { property, .. } | Expr::Truncate { property, .. } => Some(property),
            _ => None,
        }
    }

    pub fn edge_kind(self) -> bool {
        matches!(
            self.left,
            Expr::Edge {
                field: EdgeField::Kind,
                ..
            }
        )
    }

    pub fn compared(self) -> Option<Expr> {
        match self.predicate {
            Predicate::Compare {
                right: Operand::Expr(right),
                ..
            } => Some(*right),
            _ => None,
        }
    }

    /// The comparison with a folded NOT applied.
    pub fn comparison(self) -> Option<Comparison> {
        match self.predicate {
            Predicate::Compare { comparison, .. } => Some(match (comparison, self.complemented) {
                (Comparison::Eq, true) => Comparison::Ne,
                (Comparison::Ne, true) => Comparison::Eq,
                (comparison, _) => *comparison,
            }),
            _ => None,
        }
    }

    pub fn matching(self) -> Option<Matching> {
        match self.predicate {
            Predicate::Text { matching, .. } => Some(*matching),
            _ => None,
        }
    }

    pub fn literals(self) -> Literals<'p> {
        match self.predicate {
            Predicate::Compare {
                right: Operand::Literal(literal),
                ..
            } => Literals::One(literal),
            Predicate::In { values, .. } => Literals::Many(values),
            _ => Literals::None,
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
            .into_iter()
            .filter_map(|(_, operation)| match operation {
                LogicalOperation::Filter(predicate) => Some(polarized(predicate).conjuncts()),
                _ => None,
            })
            .flatten()
            .collect()
    }

    pub fn leaves(self) -> Vec<Leaf<'p>> {
        self.filters()
            .iter()
            .flat_map(|tree| tree.leaves().copied())
            .collect()
    }

    /// The top-level leaves no NOT stays above, other than comparisons across bindings and edge kinds.
    pub fn attached(self) -> Vec<Leaf<'p>> {
        self.filters()
            .into_iter()
            .filter_map(|tree| match tree {
                Boolean::Leaf(leaf) => Some(leaf),
                _ => None,
            })
            .filter(|leaf| leaf.compared().is_none() && !leaf.edge_kind())
            .collect()
    }
}
