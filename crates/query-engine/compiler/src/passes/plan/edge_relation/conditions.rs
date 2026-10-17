use std::collections::{HashMap, HashSet};

use query_data_model::OrbitQueryModel;

use super::{EdgeRelation, NodePlan};
use crate::error::{QueryError, Result};
use crate::input::*;
use crate::passes::plan::helpers::{FilterOwner, bind_filter};
use crate::passes::plan::physical::BindingSource;
use crate::passes::plan::requirements::{Column, Predicate, tag_filter};
use crate::passes::plan::{
    BoundFilter, DenormalizedDirection, DenormalizedKey, DenormalizedProperty,
};

#[derive(Default)]
pub(super) struct ConditionPlacement {
    pub scans: Vec<Option<String>>,
    pub joined_nodes: HashSet<String>,
}

pub(super) fn node_has_uncovered_filter(
    node: &NodePlan,
    relations: &[EdgeRelation],
    denormalized: &HashMap<DenormalizedKey, DenormalizedProperty>,
    model: &(impl OrbitQueryModel + ?Sized),
) -> bool {
    node.filters.iter().any(|(property, filter)| {
        !filter_covered_by_edge(
            &node.alias,
            property,
            filter,
            relations,
            denormalized,
            model,
        )
    })
}

pub(super) fn node_is_relation_source(alias: &str, relations: &[EdgeRelation]) -> bool {
    relations.iter().any(|relation| {
        relation
            .node_source
            .as_ref()
            .is_some_and(|source| source.node == alias)
    })
}

pub(super) fn denorm_hop<'a>(
    filter: &BoundFilter,
    alias: &str,
    relations: &[EdgeRelation],
    denormalized: &'a HashMap<DenormalizedKey, DenormalizedProperty>,
) -> Option<(usize, &'a DenormalizedProperty)> {
    relations
        .iter()
        .enumerate()
        .find_map(|(position, relation)| {
            if relation.node_source.is_some() {
                return None;
            }
            if crate::passes::normalize::is_wildcard(&relation.rel_types) {
                return None;
            }
            let column = relation.endpoint_column(alias)?;
            let direction = if column == ontology::constants::SOURCE_ID_COLUMN {
                DenormalizedDirection::Source
            } else {
                DenormalizedDirection::Target
            };
            let source = relation.source_entity?;
            let target = relation.target_entity?;
            super::super::helpers::lowerable_denorm(filter, direction, denormalized)
                .filter(|facts| {
                    relation.relationships.iter().all(|relationship| {
                        facts.variants.contains(&(*relationship, source, target))
                    })
                })
                .map(|facts| (position, facts))
        })
}

pub(super) fn filter_covered_by_edge(
    alias: &str,
    property: &str,
    filter: &BoundFilter,
    relations: &[EdgeRelation],
    denormalized: &HashMap<DenormalizedKey, DenormalizedProperty>,
    model: &(impl OrbitQueryModel + ?Sized),
) -> bool {
    relations.iter().any(|relation| {
        relation
            .node_source
            .as_ref()
            .is_some_and(|source| source.node == alias)
            || relation.node_source.is_none()
                && (denorm_hop(filter, alias, std::slice::from_ref(relation), denormalized)
                    .is_some()
                    || relation.max_hops == 1
                        && relation.endpoint_column(alias).is_some()
                        && relation
                            .single_edge_table()
                            .and_then(|table| model.table_columns(table))
                            .is_some_and(|columns| columns.contains(property)))
    })
}

pub(super) fn place_conditions(
    input: &Input,
    model: &(impl OrbitQueryModel + ?Sized),
    relations: &[EdgeRelation],
    nodes: &HashMap<String, NodePlan>,
    denormalized: &HashMap<DenormalizedKey, DenormalizedProperty>,
) -> Result<ConditionPlacement> {
    let mut placement = ConditionPlacement::default();
    for root in &input.predicates {
        let mut untagged = HashSet::new();
        for condition in root.leaves() {
            let (column, bound) = bind_leaf(condition.clone(), input, model, relations)?;
            let tagged = matches!(condition.target, PredicateTarget::Node(_))
                && edge_tag(&column.source, &bound, relations, denormalized).is_some();
            if !tagged {
                untagged.extend(
                    condition
                        .node_properties()
                        .filter(|(_, property)| {
                            *property != ontology::constants::DEFAULT_PRIMARY_KEY
                        })
                        .map(|(alias, _)| alias.to_string()),
                );
            } else if let PredicateTarget::Node(alias) = &condition.target
                && node_has_uncovered_filter(&nodes[alias], relations, denormalized, model)
            {
                placement.joined_nodes.insert(alias.clone());
            }
        }
        let scan = root
            .local_node()
            .filter(|_| !untagged.is_empty())
            .map(str::to_string);
        placement
            .joined_nodes
            .extend(scan.iter().cloned().chain(untagged));
        placement.scans.push(scan);
    }
    Ok(placement)
}

pub(super) fn bind_scan_conditions(
    input: &Input,
    model: &(impl OrbitQueryModel + ?Sized),
    relations: &[EdgeRelation],
    denormalized: &HashMap<DenormalizedKey, DenormalizedProperty>,
    placement: &ConditionPlacement,
    nodes: &mut HashMap<String, NodePlan>,
) -> Result<()> {
    for (root, scan) in input.predicates.iter().zip(&placement.scans) {
        if let Some(alias) = scan {
            let predicate = bind_tree(root, input, model, relations, denormalized, None)?;
            nodes
                .get_mut(alias)
                .ok_or_else(|| {
                    QueryError::PipelineInvariant(format!("node '{alias}' has no plan"))
                })?
                .predicates
                .push(predicate);
        }
    }
    Ok(())
}

pub(super) fn bind_tree(
    root: &BooleanExpression<Condition>,
    input: &Input,
    model: &(impl OrbitQueryModel + ?Sized),
    relations: &[EdgeRelation],
    denormalized: &HashMap<DenormalizedKey, DenormalizedProperty>,
    bindings: Option<&[BindingSource]>,
) -> Result<Predicate> {
    let expression = root.clone().try_map(&mut |condition| {
        bind_condition(condition, input, model, relations, denormalized, bindings).map(Box::new)
    })?;
    Ok(match expression {
        BooleanExpression::Leaf(predicate) => *predicate,
        expression => Predicate::Boolean(expression),
    })
}

fn bind_condition(
    condition: Condition,
    input: &Input,
    model: &(impl OrbitQueryModel + ?Sized),
    relations: &[EdgeRelation],
    denormalized: &HashMap<DenormalizedKey, DenormalizedProperty>,
    bindings: Option<&[BindingSource]>,
) -> Result<Predicate> {
    let node = matches!(condition.target, PredicateTarget::Node(_));
    let (column, mut bound) = bind_leaf(condition, input, model, relations)?;
    if node
        && bindings.is_some()
        && let Some(tag) = edge_tag(&column.source, &bound, relations, denormalized)
    {
        return Ok(match resolve_column(bindings, column) {
            Ok(column) => Predicate::Boolean(BooleanExpression::And(vec![
                BooleanExpression::Leaf(Box::new(bound.at(column))),
                BooleanExpression::Leaf(Box::new(tag)),
            ])),
            Err(_) => tag,
        });
    }
    let column = if node {
        resolve_column(bindings, column)?
    } else {
        column
    };
    if let Some((alias, property)) = &bound.filter.rhs_column {
        let rhs = resolve_column(bindings, Column::new(alias, property))?;
        bound.filter.rhs_column = Some((rhs.source, rhs.name));
    }
    Ok(bound.at(column))
}

fn bind_leaf(
    condition: Condition,
    input: &Input,
    model: &(impl OrbitQueryModel + ?Sized),
    relations: &[EdgeRelation],
) -> Result<(Column, BoundFilter)> {
    let (column, owner) = match &condition.target {
        PredicateTarget::Node(alias) => {
            let entity = input
                .entity_of(alias)
                .and_then(|entity| model.graph().entity_id(entity))
                .ok_or_else(|| {
                    QueryError::PipelineInvariant(format!("condition node '{alias}' has no entity"))
                })?;
            (
                Column::new(alias, &condition.property),
                FilterOwner::Entity(entity),
            )
        }
        PredicateTarget::Relationship(index) => {
            let relation = relations
                .iter()
                .find(|relation| relation.input_index == *index)
                .ok_or_else(|| {
                    QueryError::PipelineInvariant("condition relationship has no edge scan".into())
                })?;
            (
                Column::new(&relation.alias, &condition.property),
                FilterOwner::Tables(&relation.edge_tables),
            )
        }
    };
    Ok((
        column,
        bind_filter(&condition.property, condition.filter, &owner, model),
    ))
}

fn edge_tag(
    alias: &str,
    bound: &BoundFilter,
    relations: &[EdgeRelation],
    denormalized: &HashMap<DenormalizedKey, DenormalizedProperty>,
) -> Option<Predicate> {
    let (position, facts) = denorm_hop(bound, alias, relations, denormalized)?;
    let relation = &relations[position];
    if bound.filter.rhs_column.is_some() || facts.nullable || relation.max_hops != 1 {
        return None;
    }
    tag_filter(
        &relation.alias,
        &facts.edge_column,
        &facts.tag_key,
        &bound.filter,
    )
}

fn resolve_column(bindings: Option<&[BindingSource]>, column: Column) -> Result<Column> {
    let Some(bindings) = bindings else {
        return Ok(column);
    };
    bindings
        .iter()
        .find(|binding| binding.node == column.source)
        .and_then(|binding| binding.column(&column.name))
        .ok_or_else(|| {
            QueryError::Lowering(format!(
                "node '{}' has no visible '{}' column",
                column.source, column.name
            ))
        })
}
