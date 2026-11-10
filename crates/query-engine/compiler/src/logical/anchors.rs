use query_data_model::constants::DEFAULT_PRIMARY_KEY;
use query_data_model::plan::logical::{Comparison, Expr, Literal, Operand, Predicate};

use super::{Binding, Leaf, Query};

pub struct Anchor<'p> {
    pub binding: Binding<'p>,
    /// The ids every top-level id equality and IN list admits; empty when they contradict.
    pub pinned: Option<Vec<i64>>,
    /// Each top-level id equality and IN list in filter order.
    pub pins: Vec<Vec<i64>>,
    pub range: Option<(i64, i64)>,
    pub filters: Vec<Leaf<'p>>,
}

impl<'p> Query<'p> {
    /// One anchor per binding, built from the query's polarized leaves.
    pub fn anchors(self, leaves: &[Leaf<'p>]) -> Vec<Anchor<'p>> {
        self.bindings()
            .into_iter()
            .map(|binding| anchor(self, binding, leaves))
            .collect()
    }
}

fn anchor<'p>(query: Query<'p>, binding: Binding<'p>, leaves: &[Leaf<'p>]) -> Anchor<'p> {
    let is_id = |leaf: &Leaf<'_>| matches!(leaf.left(), Expr::Property { property, .. } if query.property_name(property) == DEFAULT_PRIMARY_KEY);
    let (mut lower, mut upper) = (None::<i64>, None::<i64>);
    let (mut pins, mut filters) = (Vec::new(), Vec::new());
    for leaf in leaves
        .iter()
        .filter(|leaf| leaf.attached() && leaf.binding() == binding.output)
    {
        match id_fact(leaf).filter(|_| is_id(leaf)) {
            Some(IdFact::Pin(ids)) => pins.push(ids),
            Some(IdFact::Lower(bound)) => {
                lower = Some(lower.map_or(bound, |kept| kept.max(bound)));
                filters.push((leaf, true));
            }
            Some(IdFact::Upper(bound)) => {
                upper = Some(upper.map_or(bound, |kept| kept.min(bound)));
                filters.push((leaf, true));
            }
            None => filters.push((leaf, false)),
        }
    }
    let range = lower.zip(upper);
    let pinned = pins.split_first().map(|(first, rest)| {
        first
            .iter()
            .copied()
            .filter(|id| rest.iter().all(|pin| pin.contains(id)))
            .collect()
    });
    Anchor {
        binding,
        pinned,
        pins,
        range,
        filters: filters
            .into_iter()
            .filter(|(_, bound)| range.is_none() || !bound)
            .map(|(leaf, _)| leaf.clone())
            .collect(),
    }
}

enum IdFact {
    Pin(Vec<i64>),
    Lower(i64),
    Upper(i64),
}

fn id_fact(leaf: &Leaf<'_>) -> Option<IdFact> {
    match &*leaf.predicate {
        Predicate::In { values, .. } => values
            .iter()
            .map(|value| match value {
                Literal::Int(id) => Some(*id),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()
            .map(IdFact::Pin),
        Predicate::Compare {
            comparison,
            right: Operand::Literal(Literal::Int(value)),
            ..
        } => match comparison {
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
