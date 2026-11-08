//! Places WHERE conditions: on one node's scan when they only touch that node, else on the joined rows.

use std::collections::{HashMap, HashSet};

use query_data_model::OrbitQueryModel;
use query_data_model::plan::logical::{EdgeField, Expr, LogicalOperation, Predicate as Logical};
use query_data_model::plan::tracking::Ref;

use super::Pattern;
use crate::error::{QueryError, Result};
use crate::logical::Query;
use crate::passes::plan::boolean::Boolean;
use crate::passes::plan::filters::{BoundFilter, FilterOwner, Test, bind_filter};
use crate::passes::plan::predicates::{Column, Predicate, tag_filter};
use crate::passes::plan::rows::BindingSource;

#[derive(Clone, PartialEq, Eq)]
pub(super) enum Target {
    Node(String),
    Relationship(usize),
}

/// One leaf of a condition tree: the column it tests and the test, with negation folded in.
#[derive(Clone)]
pub(super) struct Condition {
    target: Target,
    property: String,
    test: Test,
}

impl Condition {
    const KIND: &str = "type()";

    fn is_kind(&self) -> bool {
        matches!(self.target, Target::Relationship(_)) && self.property == Self::KIND
    }

    fn compares_aliases(&self) -> bool {
        match (&self.target, self.test.rhs()) {
            (Target::Node(alias), Some(rhs)) => *alias != rhs.source,
            (Target::Relationship(_), rhs) => rhs.is_some(),
            (Target::Node(_), None) => false,
        }
    }

    fn node_properties(&self) -> impl Iterator<Item = (&str, &str)> {
        let lhs = match &self.target {
            Target::Node(alias) => Some((alias.as_str(), self.property.as_str())),
            Target::Relationship(_) => None,
        };
        lhs.into_iter().chain(
            self.test
                .rhs()
                .map(|rhs| (rhs.source.as_str(), rhs.name.as_str())),
        )
    }
}

fn local_node(root: &Boolean<Condition>) -> Option<&str> {
    let mut aliases = root.leaves().map(|condition| match &condition.target {
        Target::Node(alias) if !condition.compares_aliases() => Some(alias.as_str()),
        _ => None,
    });
    let first = aliases.next()??;
    aliases.all(|alias| alias == Some(first)).then_some(first)
}

/// The condition trees no single binding's filters absorb: negated groups, negations that do not
/// complement, and comparisons across bindings.
pub(super) fn conditions(query: Query<'_>) -> Result<Vec<Boolean<Condition>>> {
    let edges: HashMap<Ref, usize> = query
        .steps()
        .iter()
        .enumerate()
        .map(|(index, step)| (step.edge, index))
        .collect();
    let mut roots = Vec::new();
    for (_, operation) in query.operations() {
        let LogicalOperation::Filter(predicate) = operation else {
            continue;
        };
        for conjunct in polarized(query, &edges, predicate, false)?.conjuncts() {
            match conjunct {
                Boolean::Leaf(condition)
                    if !condition.compares_aliases() && !condition.is_kind() => {}
                root => roots.push(root),
            }
        }
    }
    Ok(roots)
}

fn polarized(
    query: Query<'_>,
    edges: &HashMap<Ref, usize>,
    predicate: &Logical,
    negated: bool,
) -> Result<Boolean<Condition>> {
    Ok(match predicate {
        Logical::Not(inner) => polarized(query, edges, inner, !negated)?,
        Logical::And(all) if all.len() == 1 => polarized(query, edges, &all[0], negated)?,
        Logical::And(all) => {
            let mut children = Vec::new();
            for child in all {
                children.extend(polarized(query, edges, child, false)?.conjuncts());
            }
            let conjunction = Boolean::And(children);
            match negated {
                true => Boolean::Not(Box::new(conjunction)),
                false => conjunction,
            }
        }
        leaf => {
            let complemented = negated && leaf.complementable();
            let condition = Boolean::Leaf(condition(query, edges, leaf, complemented)?);
            match negated && !complemented {
                true => Boolean::Not(Box::new(condition)),
                false => condition,
            }
        }
    })
}

fn condition(
    query: Query<'_>,
    edges: &HashMap<Ref, usize>,
    leaf: &Logical,
    complemented: bool,
) -> Result<Condition> {
    let graph = query.model.graph();
    let unbound = || QueryError::PipelineInvariant("unbound relationship".into());
    let left = *leaf
        .exprs()
        .first()
        .ok_or_else(|| QueryError::PipelineInvariant("a condition without a column".into()))?;
    let (target, property) = match left {
        Expr::Property { binding, property } => (
            Target::Node(query.name(binding).to_string()),
            graph.property(property).name.clone(),
        ),
        Expr::Edge { binding, field } => {
            let property = match field {
                EdgeField::Kind => Condition::KIND,
                EdgeField::Source => "source_id",
                EdgeField::SourceKind => "source_kind",
                EdgeField::Target => "target_id",
                EdgeField::TargetKind => "target_kind",
            };
            let index = *edges.get(&binding).ok_or_else(unbound)?;
            (Target::Relationship(index), property.to_string())
        }
        Expr::Attribute { binding, attribute } => {
            let index = *edges.get(&binding).ok_or_else(unbound)?;
            (
                Target::Relationship(index),
                graph.attribute(attribute).name.clone(),
            )
        }
        other => {
            return Err(QueryError::PipelineInvariant(format!(
                "{other:?} is not a filterable column"
            )));
        }
    };
    let test = Test::of(leaf, complemented, |right| match right {
        Expr::Property { binding, property } => Some(Column::new(
            query.name(binding),
            &graph.property(property).name,
        )),
        _ => None,
    })
    .ok_or_else(|| {
        QueryError::PipelineInvariant(
            "relationship columns are never compared with properties".into(),
        )
    })?;
    Ok(Condition {
        target,
        property,
        test,
    })
}

#[derive(Default)]
pub(super) struct ConditionPlacement {
    /// Per condition tree, the node whose scan takes it, or `None` when it binds against the joined rows.
    pub scans: Vec<Option<String>>,
    pub joined_nodes: HashSet<String>,
}
pub(super) fn place_conditions<M>(pattern: &Pattern<'_, M>) -> Result<ConditionPlacement>
where
    M: OrbitQueryModel + ?Sized,
{
    let mut placement = ConditionPlacement::default();
    for root in &pattern.conditions {
        let mut untagged = HashSet::new();
        for condition in root.leaves() {
            let (column, bound) = bind_leaf(pattern, condition.clone())?;
            let tagged = matches!(condition.target, Target::Node(_))
                && edge_tag(pattern, &column.source, &bound).is_some();
            if !tagged {
                untagged.extend(
                    condition
                        .node_properties()
                        .filter(|(_, property)| {
                            *property != ontology::constants::DEFAULT_PRIMARY_KEY
                        })
                        .map(|(alias, _)| alias.to_string()),
                );
            } else if let Target::Node(alias) = &condition.target
                && pattern.has_uncovered_filter(&pattern.nodes[alias])
            {
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

pub(super) fn bind_scan_conditions<M>(
    pattern: &mut Pattern<'_, M>,
    placement: &ConditionPlacement,
) -> Result<()>
where
    M: OrbitQueryModel + ?Sized,
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

pub(super) fn bind_tree<M>(
    pattern: &Pattern<'_, M>,
    root: &Boolean<Condition>,
    bindings: Option<&[BindingSource]>,
) -> Result<Predicate>
where
    M: OrbitQueryModel + ?Sized,
{
    let expression = root
        .clone()
        .try_map(&mut |condition| bind_condition(pattern, condition, bindings).map(Box::new))?;
    Ok(match expression {
        Boolean::Leaf(predicate) => *predicate,
        expression => Predicate::Boolean(expression),
    })
}

fn bind_condition<M>(
    pattern: &Pattern<'_, M>,
    condition: Condition,
    bindings: Option<&[BindingSource]>,
) -> Result<Predicate>
where
    M: OrbitQueryModel + ?Sized,
{
    let node = matches!(condition.target, Target::Node(_));
    let (column, mut bound) = bind_leaf(pattern, condition)?;
    if node
        && bindings.is_some()
        && let Some(tag) = edge_tag(pattern, &column.source, &bound)
    {
        return Ok(match resolve_column(bindings, column) {
            Ok(column) => Predicate::Boolean(Boolean::And(vec![
                Boolean::Leaf(Box::new(bound.at(column))),
                Boolean::Leaf(Box::new(tag)),
            ])),
            Err(_) => tag,
        });
    }
    let column = if node {
        resolve_column(bindings, column)?
    } else {
        column
    };
    if let Some(rhs) = bound.test.rhs_mut() {
        bound.null_as_epoch.rhs = pattern
            .nodes
            .get(&rhs.source)
            .and_then(|node| node.null_as_epoch.get(&rhs.name).copied());
        *rhs = resolve_column(bindings, rhs.clone())?;
    }
    Ok(bound.at(column))
}

fn bind_leaf<M>(pattern: &Pattern<'_, M>, condition: Condition) -> Result<(Column, BoundFilter)>
where
    M: OrbitQueryModel + ?Sized,
{
    let (column, owner) = match &condition.target {
        Target::Node(alias) => {
            let entity = pattern
                .query
                .binding(alias)
                .map(|binding| binding.entity)
                .ok_or_else(|| {
                    QueryError::PipelineInvariant(format!("condition node '{alias}' has no entity"))
                })?;
            (
                Column::new(alias, &condition.property),
                FilterOwner::Entity(entity),
            )
        }
        Target::Relationship(index) => {
            let relation = pattern
                .relations
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
        bind_filter(&condition.property, condition.test, &owner, pattern.model),
    ))
}

fn edge_tag<M>(pattern: &Pattern<'_, M>, node: &str, bound: &BoundFilter) -> Option<Predicate>
where
    M: OrbitQueryModel + ?Sized,
{
    let (position, facts) = pattern.denorm_hop(node, bound)?;
    let relation = &pattern.relations[position];
    if bound.test.rhs().is_some() || facts.nullable || relation.max_hops != 1 {
        return None;
    }
    tag_filter(
        &relation.alias,
        &facts.edge_column,
        &facts.tag_key,
        &bound.test,
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
