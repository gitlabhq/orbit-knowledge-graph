use std::collections::HashMap;

use query_data_model::QueryDataModel;

use super::{QueryScope, ScopeProof, is_scope_only};
use crate::input::{Input, InputNode, InputRelationship, QueryType};

pub fn prepare(
    input: &mut Input,
    proofs: HashMap<String, ScopeProof>,
    model: &(impl QueryDataModel + ?Sized),
) -> QueryScope {
    let mut scope = QueryScope {
        relationships: input
            .relationships
            .iter()
            .map(|relationship| relationship_proof(input, relationship, &proofs, model))
            .collect(),
        nodes: proofs,
        requirements: Vec::new(),
    };
    if input.query_type != QueryType::Aggregation {
        return scope;
    }

    let non_fk: Vec<_> = input
        .relationships
        .iter()
        .enumerate()
        .filter(|(_, relationship)| {
            endpoints(input, relationship)
                .is_none_or(|(from, to)| model.foreign_key(&relationship.types, from, to).is_none())
        })
        .collect();
    let [(index, relationship)] = non_fk.as_slice() else {
        return scope;
    };
    let Some(proof) = scope.relationships[*index].clone() else {
        return scope;
    };
    let Some((from, to)) = endpoints(input, relationship) else {
        return scope;
    };
    let scope_preserving = !relationship.types.is_empty()
        && relationship.types.iter().all(|kind| {
            model
                .variant_scope(kind, from, to)
                .is_some_and(ontology::EdgeVariantScope::is_scope_preserving)
                || model
                    .variant_scope(kind, to, from)
                    .is_some_and(ontology::EdgeVariantScope::is_scope_preserving)
        });
    if !scope_preserving || !relationship.filters.is_empty() {
        return scope;
    }
    let Some(anchor) = [&relationship.from, &relationship.to]
        .into_iter()
        .filter_map(|alias| input.nodes.iter().find(|node| node.id == *alias))
        .find(|node| scope_only_container(input, node, model))
        .map(|node| node.id.clone())
    else {
        return scope;
    };

    let index = *index;
    scope.requirements.push(proof);
    scope.relationships.remove(index);
    input.relationships.remove(index);
    input.nodes.retain(|node| node.id != anchor);
    scope
}

fn scope_only_container(
    input: &Input,
    node: &InputNode,
    model: &(impl QueryDataModel + ?Sized),
) -> bool {
    node.entity
        .as_deref()
        .is_some_and(|entity| model.entity_has_traversal_path(entity))
        && is_scope_only(node)
        && input
            .relationships
            .iter()
            .filter(|relationship| relationship.from == node.id || relationship.to == node.id)
            .count()
            == 1
        && !input
            .aggregation
            .group_by
            .iter()
            .any(|group| group.node() == node.id)
        && !input
            .aggregation
            .metrics
            .iter()
            .any(|metric| metric.expr.node() == node.id)
        && !input
            .order_by
            .as_ref()
            .is_some_and(|order| order.node == node.id)
}

fn relationship_proof(
    input: &Input,
    relationship: &InputRelationship,
    proofs: &HashMap<String, ScopeProof>,
    model: &(impl QueryDataModel + ?Sized),
) -> Option<ScopeProof> {
    let from_proof = proofs.get(&relationship.from);
    let to_proof = proofs.get(&relationship.to);
    if from_proof == to_proof {
        return from_proof.cloned();
    }
    let (from, to) = endpoints(input, relationship)?;
    relationship.types.iter().find_map(|kind| {
        match model.variant_scope(kind, from, to) {
            Some(ontology::EdgeVariantScope::PruneToSource) => from_proof,
            Some(ontology::EdgeVariantScope::PruneToTarget) => to_proof,
            _ => None,
        }
        .cloned()
    })
}

fn endpoints<'a>(input: &'a Input, relationship: &InputRelationship) -> Option<(&'a str, &'a str)> {
    let from = input
        .nodes
        .iter()
        .find(|node| node.id == relationship.from)?
        .entity
        .as_deref()?;
    let to = input
        .nodes
        .iter()
        .find(|node| node.id == relationship.to)?
        .entity
        .as_deref()?;
    Some((from, to))
}
