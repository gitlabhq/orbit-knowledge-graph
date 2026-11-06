use query_data_model::PropertyId;
use query_data_model::plan::logical::{
    Comparison, EdgeField, Expr, Literal, Matching, Operand, Predicate,
};
use query_data_model::plan::tracking::Ref;

use super::Query;

#[derive(Debug, Clone, Copy)]
pub struct Leaf<'p> {
    pub predicate: &'p Predicate,
    pub left: Expr,
    pub negated: bool,
}

#[derive(Debug, Clone, Copy)]
pub enum Literals<'p> {
    One(&'p Literal),
    Many(&'p [Literal]),
    None,
}

impl<'p> Leaf<'p> {
    pub fn of(predicate: &'p Predicate) -> Option<Self> {
        let (inner, negated) = match predicate {
            Predicate::Not(inner) => (inner.as_ref(), true),
            other => (other, false),
        };
        let left = match inner {
            Predicate::Compare { left, .. }
            | Predicate::In { left, .. }
            | Predicate::Text { left, .. }
            | Predicate::Null { left, .. } => *left,
            Predicate::And(_) | Predicate::Not(_) => return None,
        };
        Some(Self {
            predicate: inner,
            left,
            negated,
        })
    }

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

    pub fn comparison(self) -> Option<Comparison> {
        match self.predicate {
            Predicate::Compare { comparison, .. } => Some(*comparison),
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

    pub fn leaves(self) -> Vec<Leaf<'p>> {
        fn walk<'a>(predicate: &'a Predicate, into: &mut Vec<Leaf<'a>>) {
            match predicate {
                Predicate::And(all) => all.iter().for_each(|inner| walk(inner, into)),
                Predicate::Not(inner) if matches!(inner.as_ref(), Predicate::And(_)) => {
                    walk(inner, into)
                }
                Predicate::Not(inner) => match Leaf::of(predicate) {
                    Some(leaf) => into.push(leaf),
                    None => walk(inner, into),
                },
                leaf => into.extend(Leaf::of(leaf)),
            }
        }
        let mut leaves = Vec::new();
        for conjunct in self.conjuncts() {
            walk(conjunct, &mut leaves);
        }
        leaves
    }

    pub fn attached(self) -> Vec<Leaf<'p>> {
        self.conjuncts()
            .into_iter()
            .filter_map(Leaf::of)
            .filter(|leaf| !leaf.negated && leaf.compared().is_none() && !leaf.edge_kind())
            .collect()
    }

    pub fn has_negation(self) -> bool {
        self.conjuncts()
            .iter()
            .any(|conjunct| matches!(conjunct, Predicate::Not(_)))
    }
}
