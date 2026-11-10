use query_data_model::EntityId;
use query_data_model::TraversalPathKind;
use query_data_model::plan::logical::{Comparison, Expr, Literal};
use query_data_model::plan::tracking::Ref;

use crate::logical::{Leaf, Literals, Query};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) enum TpLookupValue {
    Numeric(i64),
    Text(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct TpLookupKey {
    pub entity: String,
    pub kind: TraversalPathKind,
    pub value: TpLookupValue,
}

pub(super) fn keys(
    query: Query<'_>,
    output: Ref,
    anchors: &[(String, EntityId)],
) -> Vec<TpLookupKey> {
    let Some(entity) = query.entity_name(output) else {
        return Vec::new();
    };
    let key = |entity: &str, kind, value| TpLookupKey {
        entity: entity.to_string(),
        kind,
        value,
    };
    let filters = |property: &str| -> Vec<Leaf<'_>> {
        query
            .attached()
            .into_iter()
            .filter(|leaf| {
                leaf.binding() == output
                    && matches!(leaf.left, Expr::Property { property: id, .. } if query.property_name(id) == property)
            })
            .collect()
    };
    let mut keys = Vec::new();
    let ids = pinned(&filters("id"));
    for id in &ids {
        keys.push(key(
            entity,
            TraversalPathKind::Id,
            TpLookupValue::Numeric(*id),
        ));
    }
    if let Some(Literal::String(path)) = single_eq(&filters("full_path")) {
        keys.push(key(
            entity,
            TraversalPathKind::FullPath,
            TpLookupValue::Text(path.clone()),
        ));
    }
    for (column, anchor) in anchors {
        if let Some(Literal::Int(id)) = single_eq(&filters(column)) {
            let anchor = &query.model.graph().entity(*anchor).name;
            keys.push(key(
                anchor,
                TraversalPathKind::Id,
                TpLookupValue::Numeric(*id),
            ));
        }
    }
    keys
}

fn pinned(filters: &[Leaf<'_>]) -> Vec<i64> {
    let ints = |values: &[Literal]| -> Option<Vec<i64>> {
        values
            .iter()
            .map(|value| match value {
                Literal::Int(id) => Some(*id),
                Literal::String(text) => text.parse().ok(),
                _ => None,
            })
            .collect()
    };
    filters
        .iter()
        .find_map(|leaf| match (leaf.comparison(), leaf.literals()) {
            (None, Literals::Many(values)) if !values.is_empty() => ints(values),
            (Some(Comparison::Eq), Literals::One(value)) => ints(std::slice::from_ref(value)),
            _ => None,
        })
        .unwrap_or_default()
}

fn single_eq<'p>(filters: &[Leaf<'p>]) -> Option<&'p Literal> {
    match filters {
        [leaf] => match (leaf.comparison(), leaf.literals()) {
            (Some(Comparison::Eq), Literals::One(value)) => Some(value),
            _ => None,
        },
        _ => None,
    }
}
