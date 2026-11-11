//! Node planning: a `NodePlan` per binding, and the decision whether each node joins the result
//! as a table read or only narrows an edge scan.

use crate::passes::codegen::SqlBackend;
use query_data_model::Catalog;
use std::collections::{HashMap, HashSet};

use query_data_model::plan::logical::{Columns, Expr, Measure};

use super::{NodePlan, Pattern};
use crate::logical::anchors::Anchor;
use crate::logical::columns::{free_text, identity_column, redaction_column, returned, selections};
use crate::logical::{Query, binding};
use crate::passes::plan::filters::{bind_facts, unindex_if_key_narrowed};
use crate::passes::plan::predicates::{
    Column, OutputValue, Predicate, Projection, live, property_filter,
};
use crate::passes::plan::rows::ReadScope;

impl NodePlan {
    fn new<B>(
        query: Query<'_>,
        anchor: &Anchor<'_>,
        selection: Option<&Columns>,
        model: &Catalog<B>,
    ) -> crate::error::Result<Self>
    where
        B: SqlBackend,
    {
        let binding = anchor.binding;
        let entity = binding.entity;
        let id_column = identity_column(model, entity)?.to_string();
        let graph = model.graph();
        let stored = |name: &str| {
            graph
                .property_id(entity, name)
                .and_then(|property| model.lookup(property))
                .is_none()
        };
        let mut filters = bind_facts(query, anchor, binding.name, model);
        filters.retain(|(property, _)| stored(property));
        unindex_if_key_narrowed(&mut filters);
        let has_traversal_path = model.has_path_column(entity);
        Ok(Self {
            alias: binding.name.to_string(),
            entity: Some(graph.entity(entity).name.clone()),
            table: model.table_of(entity).map(String::from),
            joined: false,
            filters,
            predicates: Vec::new(),
            node_ids: anchor.pinned.clone(),
            id_range: anchor.range,
            has_traversal_path,
            read_scope: match (has_traversal_path, model.high_fan_out(entity)) {
                (false, _) => ReadScope::Unscoped,
                (true, false) => ReadScope::Scoped,
                (true, true) => ReadScope::HighFanOut,
            },
            redaction_id_column: redaction_column(model, entity)
                .unwrap_or(&id_column)
                .to_string(),
            id_column,
            columns: returned(model, entity, selection)
                .into_iter()
                .filter(|name| stored(name))
                .collect(),
            emit_select: true,
            null_as_epoch: graph
                .entity(entity)
                .properties
                .iter()
                .filter(|property| model.null_as_epoch(**property))
                .map(|property| {
                    let property = graph.property(*property);
                    (property.name.clone(), property.data_type)
                })
                .collect(),
        })
    }

    pub fn is_id_pinned_only(&self) -> bool {
        self.node_ids.is_some() && self.filters.is_empty() && self.id_range.is_none()
    }

    pub fn uses_default_pk(&self) -> bool {
        self.redaction_id_column == self.id_column
    }

    pub(crate) fn is_pinned(&self) -> bool {
        self.node_ids.is_some() || self.id_range.is_some()
    }

    pub(crate) fn id_predicates(&self, column: Column) -> Vec<Predicate> {
        let mut predicates = Vec::new();
        if let Some(ids) = &self.node_ids {
            predicates.push(Predicate::Ids {
                column: column.clone(),
                values: ids.clone(),
            });
        }
        if let Some((start, end)) = self.id_range {
            predicates.push(Predicate::IdRange { column, start, end });
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
        predicates.extend(self.id_predicates(Column::new(&self.alias, &self.id_column)));
        predicates.push(live(&self.alias));
        predicates
    }

    pub fn outputs(&self) -> Vec<Projection> {
        if !self.emit_select {
            return vec![];
        }
        self.columns
            .iter()
            .map(|column| {
                Projection::new(
                    OutputValue::property(&self.null_as_epoch, Column::new(&self.alias, column)),
                    format!("{}_{column}", self.alias),
                )
            })
            .collect()
    }
}

pub(super) fn build_nodes<B>(
    query: Query<'_>,
    anchors: &[Anchor<'_>],
    model: &Catalog<B>,
) -> crate::error::Result<HashMap<String, NodePlan>>
where
    B: SqlBackend,
{
    let selected = selections(query);
    anchors
        .iter()
        .map(|anchor| {
            let selection = selected.get(&anchor.binding.output).copied();
            let node = NodePlan::new(query, anchor, selection, model)?;
            Ok((node.alias.clone(), node))
        })
        .collect()
}

/// Decides which nodes join the result as rows and which join only to carry their authorization columns.
pub(super) fn assign_node_access<B>(pattern: &mut Pattern<'_, B>, condition_nodes: &HashSet<String>)
where
    B: SqlBackend,
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
            node.columns = Vec::new();
        }
    }
}

/// The nodes an aggregation returns one row per: node groups, and nodes grouped by free text.
fn aggregation_row_nodes<'p>(
    query: Query<'p>,
    model: &Catalog<impl SqlBackend>,
) -> HashSet<&'p str> {
    let (groups, _) = query.aggregate().unwrap_or_default();
    groups
        .iter()
        .filter(|group| match group {
            Expr::Node(_) => true,
            Expr::Property { property, .. } | Expr::Truncate { property, .. } => {
                free_text(model, *property)
            }
            _ => false,
        })
        .map(|group| query.name(binding(*group)))
        .collect()
}

fn node_access<B: SqlBackend>(
    pattern: &Pattern<'_, B>,
    node: &NodePlan,
    condition_nodes: &HashSet<String>,
) -> (bool, bool) {
    let query = pattern.query;
    let model = pattern.model;
    let on_node = |expr: &Expr| query.name(binding(*expr)) == node.alias;
    let (groups, measures) = query.aggregate().unwrap_or_default();
    let group_target = groups.iter().any(on_node);
    let metric_target = measures.iter().any(|measure| {
        let (expr, distinct, count) = match measure {
            Measure::Count(Some(expr)) => (expr, false, true),
            Measure::CountDistinct(expr) => (expr, true, true),
            Measure::Sum(expr) | Measure::Avg(expr) | Measure::Min(expr) | Measure::Max(expr) => {
                (expr, false, false)
            }
            Measure::Count(None) => return false,
        };
        on_node(expr)
            && matches!(expr, Expr::Property { property, .. }
                if distinct || !count || model.graph().property(*property).nullable)
    });
    let ordered = query
        .ordered()
        .into_iter()
        .rev()
        .find(|key| matches!(key, Expr::Property { .. }))
        .is_some_and(|key| on_node(&key));
    let local_predicate = condition_nodes.contains(&node.alias) && !pattern.holds_fk(&node.alias);
    let uncovered_filter = pattern.has_uncovered_filter(node);
    let returned =
        !pattern.aggregate() || aggregation_row_nodes(query, model).contains(node.alias.as_str());
    let role_floor = query
        .binding(&node.alias)
        .is_some_and(|binding| crate::passes::guards::stricter_than_edges(model, binding.entity));
    let authorized_from_table =
        returned && !node.uses_default_pk() || role_floor && !pattern.holds_fk(&node.alias);
    let filter_only = pattern.aggregate()
        && uncovered_filter
        && !group_target
        && !metric_target
        && !ordered
        && !local_predicate;
    let joined_as_rows = query.steps().is_empty()
        || !B::CAPABILITIES.deferred_nodes
        || group_target
        || metric_target
        || ordered
        || local_predicate
        || uncovered_filter && !filter_only;
    let joined = joined_as_rows || authorized_from_table;
    (joined, joined && !joined_as_rows)
}
