use query_data_model::Catalog;
use std::collections::HashMap;

use crate::passes::codegen::SqlBackend;
use query_data_model::plan::tracking::Ref;
use query_data_model::{Direction, RelationshipId};

use super::keys::{TpLookupKey, keys};
use super::{TpBound, TpBounds, TpLookup};
use crate::logical::Kind;
use crate::logical::Query;

const MAX_LOOKUPS_PER_ALIAS: usize = 8;

pub(super) struct Edge {
    pub from: Ref,
    pub to: Ref,
    pub relationships: Vec<RelationshipId>,
}

impl TpBounds {
    pub fn derive(query: Query<'_>, model: &Catalog<impl SqlBackend>) -> Self {
        let anchors = super::anchor_keys(model);
        let edges = edges(query);
        let keys: HashMap<Ref, Vec<TpLookupKey>> = query
            .anchors(&query.leaves())
            .iter()
            .map(|anchor| (anchor.binding.output, keys(query, anchor, &anchors)))
            .collect();
        let nodes = match query.kind() {
            Kind::Traversal | Kind::Aggregation => nodes(query, &keys, &edges, model),
            _ => HashMap::new(),
        };
        let relationships: Vec<Option<TpBound>> = edges
            .iter()
            .map(|edge| relationship(query, edge, &nodes, model))
            .collect();
        let shared = |output: Ref, bound: &TpBound| {
            nodes
                .iter()
                .any(|(other, b)| *other != output && b == bound)
                || relationships.iter().flatten().any(|b| b == bound)
        };
        let self_seeded = keys
            .iter()
            .filter(|(output, keys)| {
                let entity = query.entity(**output);
                !keys.is_empty()
                    && keys.iter().all(|key| Some(key.entity) == entity)
                    && nodes
                        .get(output)
                        .is_some_and(|bound| !shared(**output, bound))
            })
            .map(|(output, _)| query.name(*output).to_string())
            .collect();
        Self {
            nodes: nodes
                .into_iter()
                .map(|(output, bound)| (query.name(output).to_string(), bound))
                .collect(),
            relationships,
            self_seeded,
        }
    }
}

fn edges(query: Query<'_>) -> Vec<Edge> {
    query
        .steps()
        .into_iter()
        .map(|step| {
            let (from, to) = match step.direction {
                Direction::Incoming => (step.to, step.from),
                _ => (step.from, step.to),
            };
            Edge {
                from,
                to,
                relationships: step.relationships.to_vec(),
            }
        })
        .collect()
}

fn relationship(
    query: Query<'_>,
    edge: &Edge,
    bounds: &HashMap<Ref, TpBound>,
    model: &Catalog<impl SqlBackend>,
) -> Option<TpBound> {
    let (from_bound, to_bound) = (bounds.get(&edge.from), bounds.get(&edge.to));
    if from_bound == to_bound {
        return from_bound.cloned();
    }
    let (from, to) = (query.entity(edge.from)?, query.entity(edge.to)?);
    edge.relationships.iter().find_map(|relationship| {
        match super::variant_scope(model, *relationship, Some(from), Some(to)) {
            Some(query_data_model::catalog::EdgeScope::Source) => from_bound,
            Some(query_data_model::catalog::EdgeScope::Target) => to_bound,
            _ => None,
        }
        .cloned()
    })
}

fn nodes(
    query: Query<'_>,
    keys: &HashMap<Ref, Vec<TpLookupKey>>,
    edges: &[Edge],
    model: &Catalog<impl SqlBackend>,
) -> HashMap<Ref, TpBound> {
    let seed: HashMap<Ref, TpBound> = keys
        .iter()
        .filter_map(|(output, keys)| {
            let lookups: Vec<TpLookup> = keys
                .iter()
                .filter_map(|key| {
                    let lookup = model.path_lookup(key.entity, key.kind)?;
                    Some(TpLookup {
                        source_table: lookup.table.clone(),
                        key_column: model.column(lookup.property)?.to_string(),
                        value: key.value.clone(),
                    })
                })
                .collect();
            (1..=MAX_LOOKUPS_PER_ALIAS)
                .contains(&lookups.len())
                .then(|| (*output, TpBound::from_lookups(lookups)))
        })
        .collect();
    let mut bounds = super::propagate::propagate(query, edges, &seed, model);
    bounds.retain(|output, _| {
        query
            .entity(*output)
            .is_some_and(|entity| model.has_path_column(entity))
    });
    bounds
}
