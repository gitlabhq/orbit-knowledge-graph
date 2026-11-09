use std::collections::HashMap;

use crate::passes::codegen::RelationalModel;
use query_data_model::Direction;
use query_data_model::plan::tracking::Ref;

use super::keys::{TpLookupKey, keys};
use super::{TpBound, TpBounds, TpLookup};
use crate::logical::Kind;
use crate::logical::Query;

const MAX_LOOKUPS_PER_ALIAS: usize = 8;

pub(super) struct Edge {
    pub from: Ref,
    pub to: Ref,
    pub types: Vec<String>,
}

impl TpBounds {
    pub fn derive(query: Query<'_>, model: &impl RelationalModel) -> Self {
        let anchors = super::anchor_keys(model);
        let edges = edges(query);
        let keys: HashMap<Ref, Vec<TpLookupKey>> = query
            .bindings()
            .into_iter()
            .map(|binding| (binding.output, keys(query, binding.output, &anchors)))
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
                let entity = query.entity_name(**output);
                !keys.is_empty()
                    && keys.iter().all(|key| Some(key.entity.as_str()) == entity)
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
    let graph = query.model.graph();
    query
        .steps()
        .into_iter()
        .map(|step| {
            let (from, to) = match step.direction {
                Direction::Incoming => (step.to, step.from),
                _ => (step.from, step.to),
            };
            let types = match step.relationships {
                [] => vec!["*".to_string()],
                ids => ids
                    .iter()
                    .map(|id| graph.relationship(*id).name.clone())
                    .collect(),
            };
            Edge { from, to, types }
        })
        .collect()
}

fn relationship(
    query: Query<'_>,
    edge: &Edge,
    bounds: &HashMap<Ref, TpBound>,
    model: &impl RelationalModel,
) -> Option<TpBound> {
    let (from_bound, to_bound) = (bounds.get(&edge.from), bounds.get(&edge.to));
    if from_bound == to_bound {
        return from_bound.cloned();
    }
    let (from, to) = (query.entity(edge.from)?, query.entity(edge.to)?);
    edge.types.iter().find_map(|kind| {
        match super::variant_scope(model, kind, Some(from), Some(to)) {
            Some(ontology::EdgeVariantScope::PruneToSource) => from_bound,
            Some(ontology::EdgeVariantScope::PruneToTarget) => to_bound,
            _ => None,
        }
        .cloned()
    })
}

fn nodes(
    query: Query<'_>,
    keys: &HashMap<Ref, Vec<TpLookupKey>>,
    edges: &[Edge],
    model: &impl RelationalModel,
) -> HashMap<Ref, TpBound> {
    let seed: HashMap<Ref, TpBound> = keys
        .iter()
        .filter_map(|(output, keys)| {
            let lookups: Vec<TpLookup> = keys
                .iter()
                .filter_map(|key| {
                    let (table, column) = model.traversal_path_lookup(&key.entity, key.kind)?;
                    Some(TpLookup {
                        source_table: table.to_string(),
                        key_column: column.to_string(),
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
            .is_some_and(|entity| model.catalog().has_path_column(entity))
    });
    bounds
}
