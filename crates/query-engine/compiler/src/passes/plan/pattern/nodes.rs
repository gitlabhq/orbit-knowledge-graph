//! Node planning: a `NodePlan` per binding, and the decision whether each node joins the result
//! as a table read or only narrows an edge scan.

use std::collections::{HashMap, HashSet};

use ontology::constants::DEFAULT_PRIMARY_KEY;
use query_data_model::plan::logical::{
    Columns, Expr, LogicalOperation, Measure, Projection as Item,
};
use query_data_model::plan::tracking::Ref;
use query_data_model::{OrbitQueryModel, PropertyId};

use super::{NodePlan, Pattern};
use crate::logical::facts::Anchor;
use crate::logical::{Query, binding};
use crate::passes::plan::filters::{bind_facts, unindex_if_key_narrowed};
use crate::passes::plan::graph::ReadScope;
use crate::passes::plan::predicates::{
    Column, OutputValue, Predicate, Projection, live, property_filter,
};

impl NodePlan {
    fn new<M>(query: Query<'_>, anchor: &Anchor<'_>, selection: Option<&Columns>, model: &M) -> Self
    where
        M: OrbitQueryModel + ?Sized,
    {
        let binding = anchor.binding;
        let entity = binding.entity;
        let graph = model.graph();
        let mut filters = bind_facts(query, anchor, binding.name, model);
        filters.retain(|(property, _)| !model.property_is_virtual(entity, property));
        filters.sort_by(|(left, _), (right, _)| left.cmp(right));
        unindex_if_key_narrowed(&mut filters);
        let has_traversal_path = model.has_traversal_path(entity);
        Self {
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
            redaction_id_column: model
                .redaction_id_column(entity)
                .unwrap_or(DEFAULT_PRIMARY_KEY)
                .to_string(),
            columns: columns(model, entity, selection),
            emit_select: true,
            null_as_epoch: model.null_as_epoch_properties(entity),
        }
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
        predicates.extend(self.id_predicates(Column::new(&self.alias, DEFAULT_PRIMARY_KEY)));
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

/// The columns a node returns: the listed ones, else the defaults or every readable property.
fn columns<M>(
    model: &M,
    entity: query_data_model::EntityId,
    selection: Option<&Columns>,
) -> Vec<String>
where
    M: OrbitQueryModel + ?Sized,
{
    let graph = model.graph();
    let properties: Vec<PropertyId> = match selection {
        Some(Columns::Listed(listed)) => listed.clone(),
        selection => {
            let defaults = model.default_properties(entity);
            let defaulted = matches!(selection, None | Some(Columns::Default));
            let properties = match defaulted && !defaults.is_empty() {
                true => defaults,
                false => &graph.entity(entity).properties,
            };
            properties
                .iter()
                .copied()
                .filter(|property| model.property_realization(*property).is_some())
                .filter(|property| !model.property_is_hidden(*property))
                .filter(|property| !model.property_is_admin_only(*property))
                .collect()
        }
    };
    properties
        .into_iter()
        .map(|property| graph.property(property).name.clone())
        .filter(|name| !model.property_is_virtual(entity, name))
        .collect()
}

/// The first graph projection of each binding, through the group outputs that stand for a node.
fn selections<'p>(query: Query<'p>) -> HashMap<Ref, &'p Columns> {
    let mut nodes = HashMap::new();
    for (node, operation) in query.operations() {
        if let LogicalOperation::Aggregate { groups, .. } = operation {
            for (group, output) in groups.iter().zip(query.plan.outputs(node)) {
                if let Expr::Node(binding) = group {
                    nodes.insert(output, *binding);
                }
            }
        }
    }
    let mut selected = HashMap::new();
    for item in query.selected() {
        if let Item::Graph { binding, columns } = item {
            let binding = nodes.get(binding).copied().unwrap_or(*binding);
            selected.entry(binding).or_insert(columns);
        }
    }
    selected
}

pub(super) fn build_nodes<M>(
    query: Query<'_>,
    anchors: &[Anchor<'_>],
    model: &M,
) -> HashMap<String, NodePlan>
where
    M: OrbitQueryModel + ?Sized,
{
    let selected = selections(query);
    anchors
        .iter()
        .map(|anchor| {
            let selection = selected.get(&anchor.binding.output).copied();
            let node = NodePlan::new(query, anchor, selection, model);
            (node.alias.clone(), node)
        })
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
            node.columns = Vec::new();
        }
    }
}

/// The nodes an aggregation returns one row per: node groups, and nodes grouped by free text.
fn aggregation_row_nodes<'p>(
    query: Query<'p>,
    model: &(impl OrbitQueryModel + ?Sized),
) -> HashSet<&'p str> {
    let (groups, _) = query.aggregate().unwrap_or_default();
    groups
        .iter()
        .filter(|group| match group {
            Expr::Node(_) => true,
            Expr::Property { property, .. } | Expr::Truncate { property, .. } => {
                model.property_free_text(*property)
            }
            _ => false,
        })
        .map(|group| query.name(binding(*group)))
        .collect()
}

fn node_access<M: OrbitQueryModel + ?Sized>(
    pattern: &Pattern<'_, M>,
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
    let role_floor = query.binding(&node.alias).is_some_and(|binding| {
        model
            .required_access_level(binding.entity)
            .is_some_and(|level| level > crate::types::DEFAULT_PATH_ACCESS_LEVEL)
    });
    let authorized_from_table =
        returned && !node.uses_default_pk() || role_floor && !pattern.holds_fk(&node.alias);
    let filter_only = pattern.aggregate()
        && uncovered_filter
        && !group_target
        && !metric_target
        && !ordered
        && !local_predicate;
    let joined_as_rows = query.steps().is_empty()
        || !model.supports_deferred_node_loading()
        || group_target
        || metric_target
        || ordered
        || local_predicate
        || uncovered_filter && !filter_only;
    let joined = joined_as_rows || authorized_from_table;
    (joined, joined && !joined_as_rows)
}
