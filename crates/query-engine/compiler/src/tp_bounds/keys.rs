use orbit_authorization::Key;
use query_data_model::TraversalPathKind;
use query_data_model::plan::logical::{Comparison, Expr, Literal};
use query_data_model::{EntityId, PropertyId};

use crate::logical::Query;
use crate::logical::anchors::Anchor;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) enum TpLookupValue {
    Numeric(i64),
    Text(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct TpLookupKey {
    pub entity: EntityId,
    pub kind: TraversalPathKind,
    pub value: TpLookupValue,
}

pub(super) fn keys(
    query: Query<'_>,
    anchor: &Anchor<'_>,
    anchors: &[(PropertyId, EntityId)],
) -> Vec<TpLookupKey> {
    let entity = anchor.binding.entity;
    let key = |entity: EntityId, kind, value| TpLookupKey {
        entity,
        kind,
        value,
    };
    let single_eq = |tested: &dyn Fn(PropertyId) -> bool| -> Option<&Literal> {
        let mut filters = anchor.filters.iter().filter(|leaf| {
            leaf.right().is_none()
                && matches!(leaf.left(), Expr::Property { property, .. } if tested(property))
        });
        match (filters.next(), filters.next()) {
            (Some(leaf), None) if leaf.comparison() == Some(Comparison::Eq) => {
                leaf.literals().first()
            }
            _ => None,
        }
    };
    let mut keys = Vec::new();
    for id in anchor.pins.first().into_iter().flatten() {
        let value = match id {
            Key::Int(id) => TpLookupValue::Numeric(*id),
            Key::Text(id) | Key::Uuid(id) => TpLookupValue::Text(id.clone()),
            Key::Composite(_) => continue,
        };
        keys.push(key(entity, TraversalPathKind::Id, value));
    }
    if let Some(Literal::String(path)) =
        single_eq(&|property| query.property_name(property) == "full_path")
    {
        keys.push(key(
            entity,
            TraversalPathKind::FullPath,
            TpLookupValue::Text(path.clone()),
        ));
    }
    for (foreign_key, anchor) in anchors {
        if let Some(Literal::Int(id)) = single_eq(&|property| property == *foreign_key) {
            keys.push(key(
                *anchor,
                TraversalPathKind::Id,
                TpLookupValue::Numeric(*id),
            ));
        }
    }
    keys
}
