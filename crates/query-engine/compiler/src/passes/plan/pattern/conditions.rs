//! Places WHERE conditions: on one node's scan when they only touch that node, else on the joined rows.

use crate::passes::codegen::SqlBackend;
use query_data_model::Catalog;
use std::collections::{HashMap, HashSet};

use query_data_model::plan::logical::Expr;
use query_data_model::plan::tracking::Ref;

use super::{EdgeRelation, Pattern};
use crate::error::{QueryError, Result};
use crate::logical::Boolean;
use crate::logical::{Leaf, Query, binding, target};
use crate::passes::plan::filters::{BoundFilter, FilterOwner, bind_filter};
use crate::passes::plan::predicates::{Column, Predicate, property_column, tag_filter};
use crate::passes::plan::rows::BindingSource;
use query_data_model::EdgeField;
use query_data_model::implementations::relational::Target;

/// One leaf of a condition tree, bound to the node or edge column it reads.
#[derive(Clone)]
pub(super) struct Condition {
    column: Column,
    rhs: Option<Column>,
    filter: BoundFilter,
    edge: bool,
}

impl Condition {
    fn is_kind(&self) -> bool {
        self.edge && self.filter.target == Target::Edge(EdgeField::Kind)
    }

    fn compares_aliases(&self) -> bool {
        match (self.edge, &self.rhs) {
            (false, Some(rhs)) => self.column.source != rhs.source,
            (true, rhs) => rhs.is_some(),
            (false, None) => false,
        }
    }

    fn node_properties(&self) -> impl Iterator<Item = &Column> {
        let lhs = (!self.edge).then_some(&self.column);
        lhs.into_iter().chain(&self.rhs)
    }
}

fn local_node(root: &Boolean<Condition>) -> Option<&str> {
    let mut aliases = root.leaves().map(|condition| {
        (!condition.edge && !condition.compares_aliases())
            .then_some(condition.column.source.as_str())
    });
    let first = aliases.next()??;
    aliases.all(|alias| alias == Some(first)).then_some(first)
}

/// The condition trees no single binding's filters absorb: negated groups, negations that do not
/// complement, and comparisons across bindings.
pub(super) fn conditions<B: SqlBackend>(
    query: Query<'_>,
    relations: &[EdgeRelation],
    model: &Catalog<B>,
) -> Result<Vec<Boolean<Condition>>> {
    let edges: HashMap<Ref, &EdgeRelation> = query
        .steps()
        .iter()
        .enumerate()
        .filter_map(|(index, step)| {
            let relation = relations
                .iter()
                .find(|relation| relation.input_index == index)?;
            Some((step.edge, relation))
        })
        .collect();
    let mut roots = Vec::new();
    for tree in query.filters() {
        match tree.try_map(&mut |leaf| condition(query, &edges, model, leaf))? {
            Boolean::Leaf(condition) if !condition.compares_aliases() && !condition.is_kind() => {}
            root => roots.push(root),
        }
    }
    Ok(roots)
}

fn condition<B: SqlBackend>(
    query: Query<'_>,
    edges: &HashMap<Ref, &EdgeRelation>,
    model: &Catalog<B>,
    leaf: Leaf<'_>,
) -> Result<Condition> {
    let graph = query.model.graph();
    let target = target(leaf.left()).ok_or_else(|| {
        QueryError::PipelineInvariant(format!("{:?} is not a filterable column", leaf.left()))
    })?;
    let (column, owner, edge) = match leaf.left() {
        Expr::Property { binding, property } => {
            let alias = query.name(binding);
            let entity = query.entity(binding).ok_or_else(|| {
                QueryError::PipelineInvariant(format!("condition node '{alias}' has no entity"))
            })?;
            let column = property_column(alias, property, graph);
            (column, FilterOwner::Entity(entity), false)
        }
        left => {
            let relation = edges.get(&binding(left)).ok_or_else(|| {
                QueryError::PipelineInvariant("condition relationship has no edge scan".into())
            })?;
            let column = relation.target_column(&relation.alias, target, graph)?;
            (column, FilterOwner::Tables(&relation.edge_tables), true)
        }
    };
    let (rhs, rhs_target) = match leaf.right() {
        Some(Expr::Property { binding, property }) => (
            Some(property_column(query.name(binding), property, graph)),
            Some(Target::Property(property)),
        ),
        Some(_) => {
            return Err(QueryError::PipelineInvariant(
                "relationship columns are never compared with properties".into(),
            ));
        }
        None => (None, None),
    };
    let leaf = leaf.predicate.into_owned();
    let filter = bind_filter(target, leaf, rhs_target, &owner, model);
    Ok(Condition {
        column,
        rhs,
        filter,
        edge,
    })
}

#[derive(Default)]
pub(super) struct ConditionPlacement {
    /// Per condition tree, the node whose scan takes it, or `None` when it binds against the joined rows.
    pub scans: Vec<Option<String>>,
    pub joined_nodes: HashSet<String>,
}
pub(super) fn place_conditions<B>(pattern: &Pattern<'_, B>) -> Result<ConditionPlacement>
where
    B: SqlBackend,
{
    let mut placement = ConditionPlacement::default();
    for root in &pattern.conditions {
        let mut untagged = HashSet::new();
        for condition in root.leaves() {
            let alias = &condition.column.source;
            let tagged = !condition.edge && edge_tag(pattern, alias, &condition.filter).is_some();
            if !tagged {
                untagged.extend(
                    condition
                        .node_properties()
                        .filter(|column| {
                            pattern
                                .nodes
                                .get(&column.source)
                                .is_none_or(|node| column.name != node.id_column)
                        })
                        .map(|column| column.source.clone()),
                );
            } else if pattern.has_uncovered_filter(&pattern.nodes[alias]) {
                placement.joined_nodes.insert(alias.clone());
            }
        }
        let scan = local_node(root)
            .filter(|_| !untagged.is_empty())
            .map(str::to_string);
        placement
            .joined_nodes
            .extend(scan.iter().cloned().chain(untagged));
        placement.scans.push(scan);
    }
    Ok(placement)
}

pub(super) fn bind_scan_conditions<B>(
    pattern: &mut Pattern<'_, B>,
    placement: &ConditionPlacement,
) -> Result<()>
where
    B: SqlBackend,
{
    let bound = pattern
        .conditions
        .iter()
        .zip(&placement.scans)
        .filter_map(|(root, scan)| Some((scan.as_ref()?, root)))
        .map(|(alias, root)| Ok((alias, bind_tree(pattern, root, None)?)))
        .collect::<Result<Vec<_>>>()?;
    for (alias, predicate) in bound {
        pattern
            .nodes
            .get_mut(alias)
            .ok_or_else(|| QueryError::PipelineInvariant(format!("node '{alias}' has no plan")))?
            .predicates
            .push(predicate);
    }
    Ok(())
}

pub(super) fn bind_tree<B>(
    pattern: &Pattern<'_, B>,
    root: &Boolean<Condition>,
    bindings: Option<&[BindingSource]>,
) -> Result<Predicate>
where
    B: SqlBackend,
{
    let expression = root
        .clone()
        .try_map(&mut |condition| bind_condition(pattern, condition, bindings).map(Box::new))?;
    Ok(match expression {
        Boolean::Leaf(predicate) => *predicate,
        expression => Predicate::Boolean(expression),
    })
}

fn bind_condition<B>(
    pattern: &Pattern<'_, B>,
    condition: Condition,
    bindings: Option<&[BindingSource]>,
) -> Result<Predicate>
where
    B: SqlBackend,
{
    let Condition {
        column,
        rhs,
        filter,
        edge,
    } = condition;
    if !edge
        && bindings.is_some()
        && let Some(tag) = edge_tag(pattern, &column.source, &filter)
    {
        return Ok(match resolve_column(bindings, column) {
            Ok(column) => Predicate::Boolean(Boolean::And(vec![
                Boolean::Leaf(Box::new(filter.at(column, None))),
                Boolean::Leaf(Box::new(tag)),
            ])),
            Err(_) => tag,
        });
    }
    let column = match edge {
        true => column,
        false => resolve_column(bindings, column)?,
    };
    let rhs = rhs.map(|rhs| resolve_column(bindings, rhs)).transpose()?;
    Ok(filter.at(column, rhs))
}

fn edge_tag<B>(pattern: &Pattern<'_, B>, node: &str, bound: &BoundFilter) -> Option<Predicate>
where
    B: SqlBackend,
{
    let (position, facts) = pattern.denorm_hop(node, bound)?;
    let relation = &pattern.relations[position];
    if bound.rhs.is_some() || facts.nullable || relation.max_hops != 1 {
        return None;
    }
    tag_filter(&relation.alias, &facts.edge_column, &facts.tag_key, bound)
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
