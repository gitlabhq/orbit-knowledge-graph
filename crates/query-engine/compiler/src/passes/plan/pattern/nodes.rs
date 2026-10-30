//! Node planning: a `NodePlan` per input node, and the decision whether each node joins the result
//! as a table read or only narrows an edge scan.

use std::collections::{HashMap, HashSet};

use ontology::constants::DEFAULT_PRIMARY_KEY;
use query_data_model::OrbitQueryModel;

use super::{NodePlan, Pattern};
use crate::error::{QueryError, Result};
use crate::input::*;
use crate::passes::plan::filters::{FilterOwner, ordered_filters};
use crate::passes::plan::graph::ReadScope;
use crate::passes::plan::predicates::{
    Column, OutputValue, Predicate, Projection, live, property_filter,
};

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
        let null_as_epoch = model.null_as_epoch_properties(entity_id);
        let mut filters = ordered_filters(filters, FilterOwner::Entity(entity_id), model);
        for (_, bound) in &mut filters {
            if let Some((alias, property)) = &bound.filter.rhs_column
                && *alias == node.id
            {
                bound.null_as_epoch.rhs = null_as_epoch.get(property).copied();
            }
        }
        Some(Self {
            alias: node.id.clone(),
            entity: node.entity.clone(),
            table: model.entity_table(entity).map(String::from),
            joined: false,
            filters,
            predicates: Vec::new(),
            node_ids: node.node_ids.clone(),
            id_range: node.id_range.clone(),
            has_traversal_path: model.entity_has_traversal_path(entity),
            gated_rows: model.entity_owner_column(entity).is_some()
                || model
                    .entity_minimum_access_level(entity)
                    .is_some_and(|level| level > crate::types::DEFAULT_PATH_ACCESS_LEVEL),
            read_scope: match (
                model.entity_has_traversal_path(entity),
                model.entity_high_fan_out(entity),
            ) {
                (false, _) => ReadScope::Unscoped,
                (true, false) => ReadScope::Scoped,
                (true, true) => ReadScope::HighFanOut,
            },
            redaction_id_column: model
                .redaction_id_column_named(entity)
                .unwrap_or(DEFAULT_PRIMARY_KEY)
                .to_string(),
            columns,
            emit_select: true,
            null_as_epoch,
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

    pub(crate) fn id_predicates(&self, column: Column) -> Vec<Predicate> {
        let mut predicates = Vec::new();
        if !self.node_ids.is_empty() {
            predicates.push(Predicate::Ids {
                column: column.clone(),
                values: self.node_ids.clone(),
            });
        }
        if let Some(range) = &self.id_range {
            predicates.push(Predicate::IdRange {
                column,
                start: range.start,
                end: range.end,
            });
        }
        predicates
    }

    /// Every predicate the node's table read applies: filters, bound conditions, ids, and liveness.
    pub fn conditions(&self) -> Vec<Predicate> {
        let mut predicates: Vec<_> = self
            .filters
            .iter()
            .map(|(property, filter)| property_filter(&self.alias, property, filter))
            .collect();
        predicates.extend(self.predicates.iter().cloned());
        predicates.extend(self.id_predicates(Column::new(&self.alias, DEFAULT_PRIMARY_KEY)));
        predicates.push(live(&self.alias));
        predicates
    }

    pub fn outputs(&self) -> Vec<Projection> {
        if !self.emit_select {
            return vec![];
        }
        self.requested_columns()
            .into_iter()
            .map(|column| {
                Projection::new(
                    OutputValue::property(&self.null_as_epoch, Column::new(&self.alias, &column)),
                    format!("{}_{column}", self.alias),
                )
            })
            .collect()
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

/// Decides which nodes join the result as rows and which join only to carry their authorization columns.
pub(super) fn assign_node_access<M>(pattern: &mut Pattern<'_, M>, condition_nodes: &HashSet<String>)
where
    M: OrbitQueryModel + ?Sized,
{
    let access: HashMap<String, (bool, bool)> = pattern
        .nodes
        .values()
        .map(|node| {
            (
                node.alias.clone(),
                node_access(pattern, node, condition_nodes),
            )
        })
        .collect();
    for node in pattern.nodes.values_mut() {
        let authorization_only;
        (node.joined, authorization_only) = access[&node.alias];
        if authorization_only {
            node.columns = Some(ColumnSelection::List(vec![]));
        }
    }
}

fn node_access<M: OrbitQueryModel + ?Sized>(
    pattern: &Pattern<'_, M>,
    node: &NodePlan,
    condition_nodes: &HashSet<String>,
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
                metric.distinct
                    || !matches!(metric.expr.function(), AggFunction::Count)
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
    let returned = !pattern.aggregate()
        || input
            .aggregation_row_nodes(pattern.model)
            .contains(node.alias.as_str());
    let authorized_from_table =
        returned && !node.uses_default_pk() || node.gated_rows && !pattern.holds_fk(&node.alias);
    let filter_only = pattern.aggregate()
        && uncovered_filter
        && !group_target
        && !metric_target
        && !ordered
        && !local_predicate;
    let joined_as_rows = input.relationships.is_empty()
        || !pattern.model.supports_deferred_node_loading()
        || group_target
        || metric_target
        || ordered
        || local_predicate
        || uncovered_filter && !filter_only;
    let joined = joined_as_rows || authorized_from_table;
    (joined, joined && !joined_as_rows)
}
