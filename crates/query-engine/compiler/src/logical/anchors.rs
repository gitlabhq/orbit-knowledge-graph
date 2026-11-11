use orbit_authorization::Key;
use query_data_model::DataType;
use query_data_model::plan::logical::{Comparison, Expr, Literal, Operand, Predicate};

use super::{Binding, Leaf, Query};

pub struct Anchor<'p> {
    pub binding: Binding<'p>,
    /// The identity keys every top-level equality and IN list admits; empty when they contradict.
    pub pinned: Option<Vec<Key>>,
    /// Each top-level identity equality and IN list in filter order.
    pub pins: Vec<Vec<Key>>,
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
    let graph = query.model.graph();
    let identity = match graph.entity(binding.entity).identity.as_slice() {
        [identity] => Some(*identity),
        _ => None,
    };
    let kind = identity.map(|identity| graph.property(identity).data_type);
    let is_id = |leaf: &Leaf<'_>| matches!(leaf.left(), Expr::Property { property, .. } if Some(property) == identity);
    let (mut lower, mut upper) = (None::<i64>, None::<i64>);
    let (mut pins, mut filters) = (Vec::new(), Vec::new());
    for leaf in leaves
        .iter()
        .filter(|leaf| leaf.attached() && leaf.binding() == binding.output)
    {
        match kind
            .and_then(|kind| id_fact(leaf, kind))
            .filter(|_| is_id(leaf))
        {
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
            .filter(|id| rest.iter().all(|pin| pin.contains(id)))
            .cloned()
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
    Pin(Vec<Key>),
    Lower(i64),
    Upper(i64),
}

/// The identity key a literal spells, in the identity's own type.
fn key(literal: &Literal, kind: DataType) -> Option<Key> {
    match (literal, kind) {
        (Literal::Int(id), DataType::Int) => Some(Key::Int(*id)),
        (Literal::String(id), DataType::String) => Some(Key::Text(id.clone())),
        (Literal::String(id), DataType::Uuid) => Some(Key::Uuid(id.clone())),
        _ => None,
    }
}

/// The literal that spells an identity key; a composite key spells none.
pub fn literal(key: &Key) -> Option<Literal> {
    match key {
        Key::Int(id) => Some(Literal::Int(*id)),
        Key::Text(id) | Key::Uuid(id) => Some(Literal::String(id.clone())),
        Key::Composite(_) => None,
    }
}

fn id_fact(leaf: &Leaf<'_>, kind: DataType) -> Option<IdFact> {
    match &*leaf.predicate {
        Predicate::In { values, .. } => values
            .iter()
            .map(|value| key(value, kind))
            .collect::<Option<Vec<_>>>()
            .map(IdFact::Pin),
        Predicate::Compare {
            comparison: Comparison::Eq,
            right: Operand::Literal(literal),
            ..
        } => key(literal, kind).map(|id| IdFact::Pin(vec![id])),
        Predicate::Compare {
            comparison,
            right: Operand::Literal(Literal::Int(value)),
            ..
        } if kind == DataType::Int => match comparison {
            Comparison::Ge => Some(IdFact::Lower(*value)),
            Comparison::Gt => value.checked_add(1).map(IdFact::Lower),
            Comparison::Le => Some(IdFact::Upper(*value)),
            Comparison::Lt => value.checked_sub(1).map(IdFact::Upper),
            Comparison::Eq | Comparison::Ne => None,
        },
        _ => None,
    }
}
