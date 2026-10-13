use std::collections::HashMap;

use query_data_model::OrbitQueryModel;

use super::{QueryScope, ScopeProof};
use crate::query::{Direction, Query, QueryType};

pub fn prepare(
    query: &mut Query,
    proofs: HashMap<String, ScopeProof>,
    model: &(impl OrbitQueryModel + ?Sized),
) -> QueryScope {
    let mut scope = QueryScope {
        relationships: query
            .relationships()
            .iter()
            .map(|relationship| relationship_proof(query, relationship, &proofs, model))
            .collect(),
        nodes: proofs,
        requirements: Vec::new(),
        table_scans: Default::default(),
    };
    if query.query_type() != QueryType::Aggregation {
        return scope;
    }
    let non_fk: Vec<_> = query
        .relationships()
        .iter()
        .enumerate()
        .filter(|(_, relationship)| {
            let from = query.node(relationship.from);
            let to = query.node(relationship.to);
            model
                .foreign_key(
                    &relationship.names_vec(),
                    &from.entity_name,
                    &to.entity_name,
                )
                .is_none()
        })
        .collect();
    let [(index, relationship)] = non_fk.as_slice() else {
        return scope;
    };
    let Some(proof) = scope.relationships[*index].clone() else {
        return scope;
    };
    let from = query.node(relationship.from);
    let to = query.node(relationship.to);
    let names = relationship.names_vec();
    let scope_preserving = !names.is_empty()
        && names.iter().all(|kind| {
            model
                .variant_scope(kind, &from.entity_name, &to.entity_name)
                .is_some_and(ontology::EdgeVariantScope::is_scope_preserving)
                || model
                    .variant_scope(kind, &to.entity_name, &from.entity_name)
                    .is_some_and(ontology::EdgeVariantScope::is_scope_preserving)
        });
    if !scope_preserving || !relationship.filters.is_empty() {
        return scope;
    }
    let candidate = [relationship.from, relationship.to]
        .into_iter()
        .filter(|node| scope_only_container(query, *node))
        .find_map(|anchor| {
            container_target_scope(query, relationship, anchor, &scope.nodes, model)
                .map(|(target, proof)| (anchor, target, proof))
        });
    let Some((anchor, target, target_proof)) = candidate else {
        return scope;
    };
    let index = *index;
    let target_alias = query.node(target).alias.clone();
    scope.table_scans.insert(target_alias.clone());
    scope.nodes.insert(target_alias, target_proof);
    scope.requirements.push(proof);
    scope.relationships.remove(index);
    query.remove_relationship(index);
    query.remove_node(anchor);
    scope
}

fn relationship_proof(
    query: &Query,
    relationship: &crate::query::Relationship,
    proofs: &HashMap<String, ScopeProof>,
    model: &(impl OrbitQueryModel + ?Sized),
) -> Option<ScopeProof> {
    let from = query.node(relationship.from);
    let to = query.node(relationship.to);
    let from_proof = proofs.get(&from.alias);
    let to_proof = proofs.get(&to.alias);
    if from_proof == to_proof {
        return from_proof.cloned();
    }
    relationship.names().find_map(|kind| {
        match model.variant_scope(kind, &from.entity_name, &to.entity_name) {
            Some(ontology::EdgeVariantScope::PruneToSource) => from_proof,
            Some(ontology::EdgeVariantScope::PruneToTarget) => to_proof,
            _ => None,
        }
        .cloned()
    })
}

fn scope_only_container(query: &Query, node: crate::query::NodeId) -> bool {
    let node_ref = query.node(node);
    node_ref.has_traversal_path
        && super::scope_keys(node_ref, &[]).len() == 1
        && node_ref.id_range.is_none()
        && node_ref.node_ids.len() <= 1
        && query
            .relationships()
            .iter()
            .filter(|relationship| relationship.from == node || relationship.to == node)
            .count()
            == 1
        && !query
            .aggregation()
            .expect("aggregation query")
            .groups
            .iter()
            .any(|group| group.node() == node)
        && !query
            .aggregation()
            .expect("aggregation query")
            .metrics
            .iter()
            .any(|metric| metric.target == node)
        && !query.order().is_some_and(|order| order.node == node)
        && !query
            .comparisons()
            .iter()
            .any(|comparison| comparison.left.node == node || comparison.right.node == node)
}

fn container_target_scope(
    query: &Query,
    relationship: &crate::query::Relationship,
    anchor: crate::query::NodeId,
    proofs: &HashMap<String, ScopeProof>,
    model: &(impl OrbitQueryModel + ?Sized),
) -> Option<(crate::query::NodeId, ScopeProof)> {
    let target = if relationship.from == anchor {
        relationship.to
    } else {
        relationship.from
    };
    let anchor_node = query.node(anchor);
    let target_node = query.node(target);
    let proof = proofs.get(&anchor_node.alias)?;
    if !proof.is_single_source()
        || proofs.get(&target_node.alias) != Some(proof)
        || !target_node.has_traversal_path
    {
        return None;
    }
    let owns_path_segment = |node: &crate::query::Node| {
        model
            .traversal_path_lookup(&node.entity_name, ontology::TraversalPathKind::FullPath)
            .is_some()
    };
    let source = match relationship.direction {
        Direction::Outgoing => relationship.from,
        Direction::Incoming => relationship.to,
        Direction::Both => return None,
    };
    if !owns_path_segment(anchor_node) || source != anchor {
        return None;
    }
    let (min, max) = if owns_path_segment(target_node) {
        (relationship.hops.min, relationship.hops.max)
    } else {
        (relationship.hops.max == 1).then_some((0, 0))?
    };
    Some((target, proof.clone().with_depth(min, max)))
}
