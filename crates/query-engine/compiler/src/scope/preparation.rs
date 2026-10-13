use std::collections::HashMap;

use query_data_model::OrbitQueryModel;

use super::{QueryScope, ScopeProof, is_scope_only};
use crate::input::{Direction, Input, InputNode, InputRelationship, QueryType};
use crate::query::Query;

pub fn prepare(
    input: &mut Input,
    proofs: HashMap<String, ScopeProof>,
    model: &(impl OrbitQueryModel + ?Sized),
) -> QueryScope {
    let mut scope = QueryScope {
        relationships: input
            .relationships
            .iter()
            .map(|relationship| relationship_proof(input, relationship, &proofs, model))
            .collect(),
        nodes: proofs,
        requirements: Vec::new(),
        table_scans: Default::default(),
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
    let Some((anchor, target, target_proof)) = [&relationship.from, &relationship.to]
        .into_iter()
        .filter_map(|alias| input.nodes.iter().find(|node| node.id == *alias))
        .filter(|node| scope_only_container(input, node, model))
        .find_map(|node| {
            container_target_scope(input, relationship, node, &scope.nodes, model)
                .map(|(target, proof)| (node.id.clone(), target, proof))
        })
    else {
        return scope;
    };

    let index = *index;
    let target = input.nodes[target].id.clone();
    scope.table_scans.insert(target.clone());
    scope.nodes.insert(target, target_proof);
    scope.requirements.push(proof);
    scope.relationships.remove(index);
    input.relationships.remove(index);
    input.nodes.retain(|node| node.id != anchor);
    scope
}

pub fn prepare_bound(
    query: &mut Query,
    proofs: HashMap<String, ScopeProof>,
    model: &(impl OrbitQueryModel + ?Sized),
) -> QueryScope {
    let mut scope = QueryScope {
        relationships: query
            .relationships
            .iter()
            .map(|relationship| bound_relationship_proof(query, relationship, &proofs, model))
            .collect(),
        nodes: proofs,
        requirements: Vec::new(),
        table_scans: Default::default(),
    };
    if query.query_type != QueryType::Aggregation {
        return scope;
    }
    let non_fk: Vec<_> = query
        .relationships
        .iter()
        .enumerate()
        .filter(|(_, relationship)| {
            let from = query.node(relationship.from);
            let to = query.node(relationship.to);
            model
                .foreign_key(&relationship.names, &from.entity_name, &to.entity_name)
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
    let scope_preserving = !relationship.names.is_empty()
        && relationship.names.iter().all(|kind| {
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
        .filter(|node| bound_scope_only_container(query, *node))
        .find_map(|anchor| {
            bound_container_target_scope(query, relationship, anchor, &scope.nodes, model)
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
    query.relationships.remove(index);
    query.remove_node(anchor);
    scope
}

fn bound_relationship_proof(
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
    relationship.names.iter().find_map(|kind| {
        match model.variant_scope(kind, &from.entity_name, &to.entity_name) {
            Some(ontology::EdgeVariantScope::PruneToSource) => from_proof,
            Some(ontology::EdgeVariantScope::PruneToTarget) => to_proof,
            _ => None,
        }
        .cloned()
    })
}

fn bound_scope_only_container(query: &Query, node: crate::query::NodeId) -> bool {
    let node_ref = query.node(node);
    node_ref.has_traversal_path
        && super::bound_scope_keys(node_ref, &[]).len() == 1
        && node_ref.id_range.is_none()
        && node_ref.node_ids.len() <= 1
        && query
            .relationships
            .iter()
            .filter(|relationship| relationship.from == node || relationship.to == node)
            .count()
            == 1
        && !query
            .aggregation
            .groups
            .iter()
            .any(|group| group.node() == node)
        && !query
            .aggregation
            .metrics
            .iter()
            .any(|metric| metric.target == node)
        && !query.order.as_ref().is_some_and(|order| order.node == node)
        && !query
            .comparisons
            .iter()
            .any(|comparison| comparison.left.node == node || comparison.right.node == node)
}

fn bound_container_target_scope(
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

fn container_target_scope(
    input: &Input,
    relationship: &InputRelationship,
    anchor: &InputNode,
    proofs: &HashMap<String, ScopeProof>,
    model: &(impl OrbitQueryModel + ?Sized),
) -> Option<(usize, ScopeProof)> {
    let target = if relationship.from == anchor.id {
        &relationship.to
    } else {
        &relationship.from
    };
    let (index, target) = input
        .nodes
        .iter()
        .enumerate()
        .find(|(_, node)| node.id == *target)?;
    let proof = proofs.get(&anchor.id)?;
    if !proof.is_single_source()
        || proofs.get(&target.id) != Some(proof)
        || !model.entity_has_traversal_path(target.entity.as_deref()?)
    {
        return None;
    }
    let owns_path_segment = |node: &InputNode| {
        node.entity.as_deref().is_some_and(|entity| {
            model
                .traversal_path_lookup(entity, ontology::TraversalPathKind::FullPath)
                .is_some()
        })
    };
    let source = match relationship.direction {
        Direction::Outgoing => &relationship.from,
        Direction::Incoming => &relationship.to,
        Direction::Both => return None,
    };
    if !owns_path_segment(anchor) || *source != anchor.id {
        return None;
    }
    let (min, max) = if owns_path_segment(target) {
        (relationship.hops.min, relationship.hops.max)
    } else {
        (relationship.hops.max == 1).then_some((0, 0))?
    };
    Some((index, proof.clone().with_depth(min, max)))
}

fn scope_only_container(
    input: &Input,
    node: &InputNode,
    model: &(impl OrbitQueryModel + ?Sized),
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
        && !input
            .join_predicates
            .iter()
            .any(|predicate| predicate.lhs_node == node.id || predicate.rhs_node == node.id)
}

fn relationship_proof(
    input: &Input,
    relationship: &InputRelationship,
    proofs: &HashMap<String, ScopeProof>,
    model: &(impl OrbitQueryModel + ?Sized),
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
