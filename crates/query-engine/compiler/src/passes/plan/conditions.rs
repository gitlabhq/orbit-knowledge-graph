use std::collections::{HashMap, HashSet};

use ontology::constants::DEFAULT_PRIMARY_KEY;

use super::context::PlanningContext;
use super::edge_chain::denorm_hop;
use super::helpers::{FilterOwner, bind_filter};
use super::physical::BindingSource;
use super::requirements::{Column, Predicate, tag_filter};
use super::{BoundFilter, DenormalizedKey, DenormalizedProperty, Hop, NodePlan};
use crate::error::{QueryError, Result};
use crate::input::{BooleanExpression, Condition, Input, PredicateTarget};
use query_data_model::OrbitQueryModel;

#[derive(Default)]
pub(super) struct Placement {
    pub scans: Vec<Option<String>>,
    pub joined_nodes: HashSet<String>,
}

pub(super) fn place_conditions(
    input: &Input,
    model: &(impl OrbitQueryModel + ?Sized),
    hops: &[Hop],
    nodes: &HashMap<String, NodePlan>,
    denormalized: &HashMap<DenormalizedKey, DenormalizedProperty>,
) -> Result<Placement> {
    let mut placement = Placement::default();
    for root in &input.predicates {
        let mut untagged = HashSet::new();
        for condition in root.leaves() {
            let (column, bound) = bind_leaf(condition.clone(), input, model, hops)?;
            let tagged = matches!(condition.target, PredicateTarget::Node(_))
                && edge_tag(&column.source, &bound, hops, denormalized).is_some();
            if !tagged {
                untagged.extend(
                    condition
                        .node_properties()
                        .filter(|(_, property)| *property != DEFAULT_PRIMARY_KEY)
                        .map(|(alias, _)| alias.to_string()),
                );
            } else if let PredicateTarget::Node(alias) = &condition.target
                && nodes[alias].has_untagged_filter(hops, denormalized)
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

fn edge_tag(
    alias: &str,
    bound: &BoundFilter,
    hops: &[Hop],
    denormalized: &HashMap<DenormalizedKey, DenormalizedProperty>,
) -> Option<Predicate> {
    let (position, facts) = denorm_hop(bound, alias, hops, denormalized)?;
    let hop = &hops[position];
    if bound.filter.rhs_column.is_some() || facts.nullable || hop.max_hops != 1 || hop.fk.is_some()
    {
        return None;
    }
    tag_filter(
        &hop.alias,
        &facts.edge_column,
        &facts.tag_key,
        &bound.filter,
    )
}

fn bind_leaf(
    condition: Condition,
    input: &Input,
    model: &(impl OrbitQueryModel + ?Sized),
    hops: &[Hop],
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
            let hop = hops
                .iter()
                .find(|hop| hop.input_index == *index)
                .ok_or_else(|| {
                    QueryError::PipelineInvariant("condition relationship has no edge scan".into())
                })?;
            (
                Column::new(&hop.alias, &condition.property),
                FilterOwner::Tables(&hop.edge_tables),
            )
        }
    };
    Ok((
        column,
        bind_filter(&condition.property, condition.filter, &owner, model),
    ))
}

impl<M: OrbitQueryModel + ?Sized> PlanningContext<'_, M> {
    pub(super) fn bind_scan_conditions(
        &self,
        nodes: &mut HashMap<String, NodePlan>,
        placement: &Placement,
    ) -> Result<()> {
        for (root, scan) in self.input.predicates.iter().zip(&placement.scans) {
            if let Some(alias) = scan {
                let predicate = self.bind_tree(root, None)?;
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

    pub(super) fn bind_join_conditions(
        &self,
        bindings: &[BindingSource],
        placement: &Placement,
    ) -> Result<Vec<Predicate>> {
        self.input
            .predicates
            .iter()
            .zip(&placement.scans)
            .filter(|(_, scan)| scan.is_none())
            .map(|(root, _)| self.bind_tree(root, Some(bindings)))
            .collect()
    }

    fn bind_tree(
        &self,
        root: &BooleanExpression<Condition>,
        bindings: Option<&[BindingSource]>,
    ) -> Result<Predicate> {
        let expression = root
            .clone()
            .try_map(&mut |condition| self.bind_condition(condition, bindings).map(Box::new))?;
        Ok(match expression {
            BooleanExpression::Leaf(predicate) => *predicate,
            expression => Predicate::Boolean(expression),
        })
    }

    fn bind_condition(
        &self,
        condition: Condition,
        bindings: Option<&[BindingSource]>,
    ) -> Result<Predicate> {
        let node = matches!(condition.target, PredicateTarget::Node(_));
        let (column, mut bound) = bind_leaf(condition, self.input, self.model, &self.hops)?;
        if node
            && bindings.is_some()
            && let Some(tag) = edge_tag(&column.source, &bound, &self.hops, &self.denormalized)
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
