use std::collections::{HashMap, HashSet};

use crate::passes::codegen::RelationalModel;
use query_data_model::plan::tracking::Ref;

use super::TpBound;
use super::derive::Edge;
use crate::logical::Query;

struct Scoped {
    from: Ref,
    to: Ref,
    preserving: bool,
    to_source: bool,
}

pub(super) fn propagate(
    query: Query<'_>,
    edges: &[Edge],
    seed: &HashMap<Ref, TpBound>,
    model: &impl RelationalModel,
) -> HashMap<Ref, TpBound> {
    if seed.is_empty() {
        return HashMap::new();
    }
    let edges: Vec<Scoped> = edges
        .iter()
        .map(|edge| scoped(query, edge, model))
        .collect();
    let mut tainted = HashSet::new();
    loop {
        let mut changed = false;
        for edge in edges.iter().filter(|edge| !edge.preserving) {
            for (from, to) in [(edge.from, edge.to), (edge.to, edge.from)] {
                if (seed.contains_key(&from) || tainted.contains(&from)) && tainted.insert(to) {
                    changed = true;
                }
            }
        }
        if !changed {
            break;
        }
    }
    let mut result = seed.clone();
    loop {
        let mut changed = false;
        for edge in edges.iter().filter(|edge| edge.preserving) {
            let next = match (
                result.get(&edge.from).cloned(),
                result.get(&edge.to).cloned(),
            ) {
                (Some(bound), None) if !tainted.contains(&edge.to) => Some((edge.to, bound)),
                (None, Some(bound)) if edge.to_source && !tainted.contains(&edge.from) => {
                    Some((edge.from, bound))
                }
                _ => None,
            };
            if let Some((output, bound)) = next {
                result.insert(output, bound);
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }
    result
}

fn scoped(query: Query<'_>, edge: &Edge, model: &impl RelationalModel) -> Scoped {
    let source = query.entity_name(edge.from).unwrap_or_default();
    let (from, to) = (query.entity(edge.from), query.entity(edge.to));
    let scope = |kind: &String| super::variant_scope(model, kind, from, to);
    Scoped {
        from: edge.from,
        to: edge.to,
        preserving: edge
            .types
            .iter()
            .all(|kind| scope(kind).is_some_and(ontology::EdgeVariantScope::is_scope_preserving)),
        to_source: edge
            .types
            .iter()
            .all(|kind| scope(kind).is_some_and(|scope| scope.propagates_to_source(kind, source))),
    }
}
