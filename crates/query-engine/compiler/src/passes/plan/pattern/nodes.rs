//! Node planning: a `NodePlan` per input node, and the decision whether each node joins the result
//! as a table read or only narrows an edge scan.

use std::collections::{HashMap, HashSet};

use ontology::constants::DEFAULT_PRIMARY_KEY;
use query_data_model::OrbitQueryModel;

use super::{NodePlan, Pattern};
use crate::error::{QueryError, Result};
use crate::input::*;
use crate::passes::plan::filters::{FilterOwner, ordered_filters};

impl NodePlan {
    fn from_input<M>(node: &InputNode, model: &M, strip_virtuals: bool) -> Option<Self>
    where
        M: OrbitQueryModel + ?Sized,
    {
        let entity = node.entity.as_deref()?;
        let entity_id = model.graph().entity_id(entity)?;
        let stored_filters = strip_virtuals.then(|| {
            node.filters
                .iter()
                .filter(|(property, _)| !model.property_is_virtual(entity_id, property))
                .map(|(property, filters)| (property.clone(), filters.clone()))
                .collect()
        });
        let filters = stored_filters.as_ref().unwrap_or(&node.filters);
        let columns = node.columns.as_ref().map(|selection| match selection {
            ColumnSelection::All => ColumnSelection::All,
            ColumnSelection::List(columns) if strip_virtuals => ColumnSelection::List(
                columns
                    .iter()
                    .filter(|column| !model.property_is_virtual(entity_id, column))
                    .cloned()
                    .collect(),
            ),
            ColumnSelection::List(columns) => ColumnSelection::List(columns.clone()),
        });
        Some(Self {
            alias: node.id.clone(),
            entity: node.entity.clone(),
            table: model.entity_table(entity).map(String::from),
            joined: false,
            filters: ordered_filters(filters, FilterOwner::Entity(entity_id), model),
            predicates: Vec::new(),
            node_ids: node.node_ids.clone(),
            id_range: node.id_range.clone(),
            has_traversal_path: model.entity_has_traversal_path(entity),
            redaction_id_column: model
                .redaction_id_column_named(entity)
                .unwrap_or(DEFAULT_PRIMARY_KEY)
                .to_string(),
            columns,
            emit_select: true,
            narrowed: false,
        })
    }

    /// A node outside a pattern keeps its virtual filters; the lowerer for its query type handles them.
    pub(crate) fn resolve<M>(node: &InputNode, model: &M) -> Result<Self>
    where
        M: OrbitQueryModel + ?Sized,
    {
        Self::from_input(node, model, false).ok_or_else(|| {
            QueryError::Lowering(format!("node '{}' has an unknown entity", node.id))
        })
    }

    pub fn is_id_pinned_only(&self) -> bool {
        !self.node_ids.is_empty() && self.filters.is_empty() && self.id_range.is_none()
    }

    pub fn uses_default_pk(&self) -> bool {
        self.redaction_id_column == DEFAULT_PRIMARY_KEY
    }

    pub(crate) fn is_pinned(&self) -> bool {
        !self.node_ids.is_empty() || self.id_range.is_some()
    }

    pub(crate) fn requested_columns(&self) -> Vec<String> {
        match &self.columns {
            Some(ColumnSelection::List(columns)) => columns.clone(),
            Some(ColumnSelection::All) => vec!["*".to_string()],
            None => vec![],
        }
    }
}

pub(super) fn build_nodes<M>(input: &Input, model: &M) -> HashMap<String, NodePlan>
where
    M: OrbitQueryModel + ?Sized,
{
    input
        .nodes
        .iter()
        .filter_map(|node| Some((node.id.clone(), NodePlan::from_input(node, model, true)?)))
        .collect()
}

/// Decides which nodes join the result as rows (`joined`) and which only narrow an edge scan (`narrowed`).
pub(super) fn assign_node_access<M>(pattern: &mut Pattern<'_, M>, condition_nodes: &HashSet<String>)
where
    M: OrbitQueryModel + ?Sized,
{
    let narrowing = narrowing_nodes(pattern);
    let access: HashMap<String, (bool, bool)> = pattern
        .nodes
        .values()
        .map(|node| {
            (
                node.alias.clone(),
                node_access(pattern, node, condition_nodes, &narrowing),
            )
        })
        .collect();
    for node in pattern.nodes.values_mut() {
        (node.joined, node.narrowed) = access[&node.alias];
    }
}

fn narrowing_nodes<M: OrbitQueryModel + ?Sized>(pattern: &Pattern<'_, M>) -> HashSet<String> {
    match pattern.relations.as_slice() {
        [relation]
            if pattern.input.query_type == QueryType::Traversal
                && relation.max_hops == 1
                && relation.fk.is_none()
                && relation.direction != Direction::Both =>
        {
            [
                (&relation.from_node, &relation.to_node),
                (&relation.to_node, &relation.from_node),
            ]
            .into_iter()
            .filter(|(_, partner)| {
                pattern
                    .nodes
                    .get(partner.as_str())
                    .is_some_and(NodePlan::is_pinned)
            })
            .map(|(node, _)| node.clone())
            .collect()
        }
        _ => HashSet::new(),
    }
}

fn node_access<M: OrbitQueryModel + ?Sized>(
    pattern: &Pattern<'_, M>,
    node: &NodePlan,
    condition_nodes: &HashSet<String>,
    narrowing: &HashSet<String>,
) -> (bool, bool) {
    let input = pattern.input;
    let group_target = input
        .aggregation
        .group_by
        .iter()
        .any(|group| group.node() == node.alias);
    let metric_target = input.aggregation.metrics.iter().any(|metric| {
        metric.expr.node() == node.alias
            && metric.expr.property().is_some_and(|property| {
                !matches!(metric.expr.function(), AggFunction::Count)
                    || node
                        .entity
                        .as_deref()
                        .is_some_and(|entity| pattern.model.property_is_nullable(entity, property))
            })
    });
    let ordered = input
        .order_by
        .as_ref()
        .is_some_and(|order| order.node == node.alias);
    let local_predicate = condition_nodes.contains(&node.alias) && !pattern.holds_fk(&node.alias);
    let uncovered_filter = pattern.has_uncovered_filter(node);
    let filter_only = pattern.aggregate()
        && uncovered_filter
        && !group_target
        && !metric_target
        && !ordered
        && !local_predicate;
    let joined = input.relationships.is_empty()
        || !pattern.model.supports_deferred_node_loading()
        || group_target
        || metric_target
        || ordered
        || local_predicate
        || uncovered_filter && !filter_only;
    let narrowed = !joined && narrowing.contains(&node.alias) && !node.uses_default_pk();
    (joined || narrowed, narrowed)
}
