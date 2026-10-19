use std::collections::{HashMap, HashSet};

use ontology::constants::*;
use query_data_model::OrbitQueryModel;

mod binding;
mod conditions;
mod expansion;

use super::BoundFilter;
use super::context::PlanningContext;
use super::helpers::{FilterOwner, ordered_filters};
use super::physical::{BindingSource, ExecutionPlan, RelationGraph, RelationId};
use super::requirements::{
    Column, OutputValue, Predicate, Projection, id_list, live, membership, node_outputs,
    node_predicates, property_filter, relationship_kinds, tag_filter,
};
use crate::constants::*;
use crate::error::{QueryError, Result};
use crate::input::*;
use binding::{
    assign_node_access, build_nodes, build_relations, linearize_relations, select_edge_layouts,
};
use conditions::{
    bind_scan_conditions, bind_tree, denorm_hop, filter_covered_by_edge, node_has_uncovered_filter,
    place_conditions,
};
use expansion::multi_hop;

#[derive(Clone)]
pub struct NodePlan {
    pub alias: String,
    pub entity: Option<String>,
    pub table: Option<String>,
    pub joined: bool,
    pub filters: Vec<(String, BoundFilter)>,
    pub predicates: Vec<Predicate>,
    pub node_ids: Vec<i64>,
    pub id_range: Option<InputIdRange>,
    pub has_traversal_path: bool,
    pub redaction_id_column: String,
    pub columns: Option<ColumnSelection>,
    pub emit_select: bool,
    pub narrowed: bool,
}

impl NodePlan {
    pub(super) fn from_input<M>(node: &InputNode, model: &M, strip_virtuals: bool) -> Option<Self>
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

    pub fn is_id_pinned_only(&self) -> bool {
        !self.node_ids.is_empty() && self.filters.is_empty() && self.id_range.is_none()
    }

    pub fn uses_default_pk(&self) -> bool {
        self.redaction_id_column == DEFAULT_PRIMARY_KEY
    }
}

#[derive(Clone)]
pub struct EdgeRelation {
    pub input_index: usize,
    pub rel_types: Vec<String>,
    pub relationships: Vec<query_data_model::RelationshipId>,
    pub edge_tables: Vec<String>,
    pub alias: String,
    pub source_entity: Option<query_data_model::EntityId>,
    pub target_entity: Option<query_data_model::EntityId>,
    pub from_node: String,
    pub to_node: String,
    pub direction: Direction,
    pub min_hops: u32,
    pub max_hops: u32,
    pub filters: Vec<(String, BoundFilter)>,
    pub node_source: Option<NodeRelationSource>,
}

#[derive(Clone)]
pub struct NodeRelationSource {
    pub node: String,
    pub column: String,
}

impl EdgeRelation {
    fn reversed(&self) -> Self {
        let mut relation = self.clone();
        std::mem::swap(&mut relation.from_node, &mut relation.to_node);
        relation.direction = match relation.direction {
            Direction::Outgoing => Direction::Incoming,
            Direction::Incoming => Direction::Outgoing,
            Direction::Both => Direction::Both,
        };
        relation
    }

    pub fn single_edge_table(&self) -> Option<&str> {
        let [table] = self.edge_tables.as_slice() else {
            return None;
        };
        Some(table)
    }

    pub fn column_prefix(&self) -> String {
        if self.max_hops > 1 {
            format!("hop_e{}_", self.input_index)
        } else {
            format!("e{}_", self.input_index)
        }
    }

    fn endpoint_column(&self, node: &str) -> Option<&'static str> {
        let (start, end) = self.direction.edge_columns();
        if self.from_node == node {
            Some(start)
        } else if self.to_node == node {
            Some(end)
        } else {
            None
        }
    }
}

pub(super) struct PlannedRelations<'a, M: OrbitQueryModel + ?Sized> {
    pub context: PlanningContext<'a, M>,
    pub execution: ExecutionPlan,
}

pub(super) fn plan<'a, M>(mut context: PlanningContext<'a, M>) -> Result<PlannedRelations<'a, M>>
where
    M: OrbitQueryModel + ?Sized,
{
    let input = context.input;
    let model = context.model;
    let mut relations = build_relations(input, model);
    let mut nodes = build_nodes(input, model);
    let denormalized = super::denormalized_facts(input, model);
    let placement = place_conditions(input, model, &relations, &nodes, &denormalized)?;
    bind_scan_conditions(
        input,
        model,
        &relations,
        &denormalized,
        &placement,
        &mut nodes,
    )?;
    assign_node_access(
        input,
        model,
        &relations,
        &denormalized,
        &placement.joined_nodes,
        &mut nodes,
    );
    linearize_relations(&mut relations, &nodes)?;
    if input.query_type == QueryType::Aggregation {
        let groups: HashSet<&str> = node_group_ids(&input.aggregation.group_by).collect();
        for node in nodes.values_mut() {
            node.emit_select = groups.contains(node.alias.as_str());
        }
    }

    context.relations = relations;
    context.nodes = nodes;
    context.denormalized = denormalized;

    let mut execution = RelationBuilder::new(&context).build()?;
    let conditions = input
        .predicates
        .iter()
        .zip(&placement.scans)
        .filter(|(_, scan)| scan.is_none())
        .map(|(root, _)| {
            bind_tree(
                root,
                input,
                model,
                &context.relations,
                &context.denormalized,
                Some(&execution.bindings),
            )
        })
        .collect::<Result<_>>()?;
    execution.root = execution
        .graph
        .from(execution.root)
        .filter(conditions)
        .finish();
    context.node_edge_mappings = execution
        .bindings
        .iter()
        .map(|binding| {
            (
                binding.node.clone(),
                (binding.alias.clone(), binding.column.clone()),
            )
        })
        .collect();

    Ok(PlannedRelations { context, execution })
}

struct RelationBuilder<'a, M: OrbitQueryModel + ?Sized> {
    facts: &'a PlanningContext<'a, M>,
    root: Option<RelationId>,
    outputs: Vec<Projection>,
    nodes: HashMap<String, PlannedNode>,
    graph: RelationGraph,
    tagged: HashSet<(String, String, usize)>,
}

#[derive(Default)]
struct PlannedNode {
    binding: Option<BindingSource>,
    ids: Option<(RelationId, String)>,
    bounded_read: Option<RelationId>,
}

impl<'a, M: OrbitQueryModel + ?Sized> RelationBuilder<'a, M> {
    fn new(facts: &'a PlanningContext<'a, M>) -> Self {
        Self {
            facts,
            root: None,
            outputs: Vec::new(),
            nodes: HashMap::new(),
            graph: RelationGraph::default(),
            tagged: HashSet::new(),
        }
    }

    fn build(mut self) -> Result<ExecutionPlan> {
        if self.facts.relations.is_empty() {
            return self.facts.single_node();
        }
        for (index, relation) in self.facts.relations.iter().enumerate() {
            self.add_edge(index, relation)?;
        }
        self.attach_nodes()?;
        let bindings = self
            .facts
            .input
            .nodes
            .iter()
            .filter_map(|node| self.nodes.get(&node.id)?.binding.clone())
            .collect();
        Ok(ExecutionPlan {
            root: self
                .root
                .ok_or_else(|| QueryError::Lowering("traversal has no physical source".into()))?,
            graph: self.graph,
            outputs: self.outputs,
            bindings,
        })
    }

    fn attach_nodes(&mut self) -> Result<()> {
        let aliases: Vec<String> = self
            .facts
            .input
            .nodes
            .iter()
            .map(|node| node.id.clone())
            .filter(|alias| self.nodes.contains_key(alias) && self.facts.nodes[alias].joined)
            .collect();
        for alias in aliases {
            self.ensure_node(&alias)?;
        }
        Ok(())
    }

    fn binding(&self, node: &str) -> Option<&BindingSource> {
        self.nodes.get(node)?.binding.as_ref()
    }

    fn ids(&self, node: &str) -> Option<&(RelationId, String)> {
        self.nodes.get(node)?.ids.as_ref()
    }

    fn reached_ids(&self, node: &str) -> Option<(RelationId, String)> {
        if self.endpoint_is_selective(node) {
            return None;
        }
        self.ids(node).cloned()
    }

    fn set_ids(&mut self, node: &str, ids: (RelationId, String)) {
        self.nodes.entry(node.to_string()).or_default().ids = Some(ids);
    }

    fn set_binding(
        &mut self,
        node: &str,
        alias: &str,
        column: &str,
        property_source: Option<&str>,
        traversal_path: Option<Column>,
    ) {
        let planned = self.nodes.entry(node.to_string()).or_default();
        if let Some(binding) = &mut planned.binding {
            binding.alias = alias.to_string();
            binding.column = column.to_string();
            binding.property_source = property_source.map(str::to_string);
            binding.traversal_path = traversal_path;
        } else {
            planned.binding = Some(BindingSource {
                node: node.to_string(),
                alias: alias.to_string(),
                column: column.to_string(),
                property_source: property_source.map(str::to_string),
                traversal_path,
            });
        }
    }

    fn add_edge(&mut self, index: usize, relation: &EdgeRelation) -> Result<()> {
        if relation.node_source.is_some() {
            return self.add_node_relation(relation);
        }
        let alias = &relation.alias;
        let mut physical_relation = relation.clone();
        for node in [&relation.from_node, &relation.to_node] {
            if self.ids(node).is_none() && self.has_selective_node_filter(node) {
                self.node_ids(node)?;
            }
        }
        let endpoint_ids: Vec<_> = [&relation.from_node, &relation.to_node]
            .into_iter()
            .filter_map(|node| self.ids(node).cloned().map(|ids| (node.clone(), ids)))
            .collect();
        let narrowed_columns: Vec<_> = endpoint_ids
            .iter()
            .filter_map(|(node, _)| relation.endpoint_column(node))
            .collect();
        if relation.max_hops == 1 {
            select_edge_layouts(
                self.facts.model,
                &mut physical_relation.edge_tables,
                &narrowed_columns,
            );
        }
        let mut predicates = edge_predicates(self.facts, alias, &physical_relation, false);
        predicates.extend(
            relation
                .filters
                .iter()
                .map(|(property, filter)| property_filter(alias, property, filter)),
        );
        push_denorm_tags(
            self.facts,
            &mut predicates,
            relation,
            alias,
            &mut self.tagged,
        );
        predicates.extend(node_id_predicates(self.facts, alias, relation));
        for (node, (ids, key)) in &endpoint_ids {
            if let Some(column) = relation.endpoint_column(node) {
                predicates.push(Predicate::Membership {
                    columns: vec![Column::new(alias, column)],
                    relation: *ids,
                    keys: vec![key.clone()],
                });
            }
        }
        for node in [&relation.from_node, &relation.to_node] {
            if self.facts.nodes[node].joined
                || !node_has_uncovered_filter(
                    &self.facts.nodes[node],
                    &self.facts.relations,
                    &self.facts.denormalized,
                    self.facts.model,
                )
            {
                continue;
            }
            let ids = self.node_ids(node)?;
            let column = relation.endpoint_column(node).expect("relation endpoint");
            predicates.push(membership(Column::new(alias, column), ids));
        }

        let mut source = if relation.max_hops > 1 {
            let mut expansion_relation = physical_relation.clone();
            let mut source_ids = self.ids(&relation.from_node).cloned();
            let mut target_ids = self.ids(&relation.to_node).cloned();
            let mut reverse_output = false;
            if relation.direction != Direction::Both
                && !self.endpoint_is_selective(&relation.from_node)
                && self.endpoint_is_selective(&relation.to_node)
            {
                expansion_relation = expansion_relation.reversed();
                std::mem::swap(&mut source_ids, &mut target_ids);
                reverse_output = true;
            }
            let endpoint = expansion_relation.direction.edge_columns().0;
            select_edge_layouts(
                self.facts.model,
                &mut expansion_relation.edge_tables,
                &[endpoint],
            );
            multi_hop(
                &mut self.graph,
                self.facts,
                &expansion_relation,
                alias,
                [source_ids, target_ids],
                reverse_output,
            )
        } else {
            let current_rows = !(self.facts.aggregate() && self.facts.relations.len() == 1);
            if self.facts.aggregate() && self.facts.relations.len() == 1 {
                if let Some(table) = physical_relation.single_edge_table() {
                    let edge = self.edge_read(&physical_relation, alias, false)?;
                    let sort_key = self.facts.latest_row_key(table)?;
                    let identity = predicates
                        .iter()
                        .filter(|predicate| filters_sort_key(predicate, sort_key))
                        .cloned()
                        .collect();
                    self.graph
                        .from(edge)
                        .filter(identity)
                        .latest(alias, sort_key.to_vec(), predicates)
                        .finish()
                } else {
                    self.read_relation(&physical_relation, alias, current_rows, predicates)?
                }
            } else {
                self.read_relation(&physical_relation, alias, current_rows, predicates)?
            }
        };
        self.bind_relation_ids(index, relation, source);
        if relation.max_hops == 1
            && (self.facts.relations.len() > 1
                || [&relation.from_node, &relation.to_node]
                    .into_iter()
                    .any(|node| self.facts.nodes[node].narrowed))
        {
            source = self.graph.from(source).bind(alias).finish();
        }
        let (start, end) = relation.direction.edge_columns();
        let endpoints = [(&relation.from_node, start), (&relation.to_node, end)];
        if let Some(previous) = self.root.take() {
            let (node, column, binding) = endpoints
                .iter()
                .find_map(|(node, column)| {
                    self.binding(node)
                        .cloned()
                        .map(|binding| (*node, *column, binding))
                })
                .ok_or_else(|| QueryError::Lowering("stored relation is disconnected".into()))?;
            let mut join_predicates = Vec::new();
            for (other, other_column) in endpoints {
                if other == node {
                    continue;
                }
                if let Some(other_binding) = self.binding(other) {
                    join_predicates.push(Predicate::ColumnEq {
                        left: Column::new(&other_binding.alias, &other_binding.column),
                        right: Column::new(alias, other_column),
                    });
                }
            }
            self.root = Some(
                self.graph
                    .from(previous)
                    .join(
                        source.alias(alias),
                        (
                            Column::new(&binding.alias, &binding.column),
                            Column::new(alias, column),
                        ),
                        join_predicates,
                    )
                    .finish(),
            );
        } else {
            self.root = Some(source);
        }
        for (node, column) in endpoints {
            if self.binding(node).is_none() {
                self.set_binding(node, alias, column, None, None);
            }
        }
        self.push_edge_outputs(relation, alias);
        Ok(())
    }

    fn add_node_relation(&mut self, relation: &EdgeRelation) -> Result<()> {
        let node_source = relation.node_source.as_ref().expect("node relation source");
        let joined_through_endpoint = self.ensure_relation_node(relation, node_source)?;

        let source_binding = self
            .binding(&node_source.node)
            .cloned()
            .expect("node relation source binding");
        let alias = source_binding
            .property_source
            .as_deref()
            .expect("node relation property source");
        let endpoint = [&relation.from_node, &relation.to_node]
            .into_iter()
            .find(|node| **node != node_source.node)
            .ok_or_else(|| QueryError::Lowering("node relation has no other endpoint".into()))?;
        let endpoint_column = Column::new(alias, &node_source.column);
        let mut predicates = Vec::new();
        let endpoint_plan = &self.facts.nodes[endpoint];
        if !endpoint_plan.node_ids.is_empty() {
            predicates.push(Predicate::Ids {
                column: endpoint_column.clone(),
                values: endpoint_plan.node_ids.clone(),
            });
        }
        if let Some(range) = &endpoint_plan.id_range {
            predicates.push(Predicate::IdRange {
                column: endpoint_column.clone(),
                start: range.start,
                end: range.end,
            });
        }
        if self.binding(endpoint).is_none() {
            let reached = if self.endpoint_is_selective(endpoint) {
                None
            } else {
                self.ids_through_holder(&node_source.node, &node_source.column)
            };
            if let Some(reached) = reached.clone()
                && self.facts.nodes[endpoint].joined
            {
                self.set_ids(endpoint, reached);
            } else {
                let mut endpoint = self.facts.nodes[endpoint].clone();
                endpoint.columns = Some(ColumnSelection::List(vec![]));
                if let Some(reached) = reached {
                    endpoint.predicates.push(membership(
                        Column::new(&endpoint.alias, DEFAULT_PRIMARY_KEY),
                        reached,
                    ));
                }
                let ids = self.node_ids_from(&endpoint)?;
                predicates.push(membership(endpoint_column.clone(), ids));
            }
        } else if let Some(ids) = self.ids(endpoint).cloned() {
            predicates.push(membership(endpoint_column.clone(), ids));
        }
        if let Some(binding) = self.binding(endpoint) {
            if !joined_through_endpoint {
                predicates.push(Predicate::ColumnEq {
                    left: Column::new(&binding.alias, &binding.column),
                    right: endpoint_column,
                });
            }
        } else {
            self.set_binding(endpoint, alias, &node_source.column, None, None);
        }
        if let Some(root) = self.root.take() {
            self.root = Some(self.graph.from(root).filter(predicates).finish());
        }
        self.push_node_relation_outputs(relation, node_source, alias);
        Ok(())
    }

    fn ensure_relation_node(
        &mut self,
        relation: &EdgeRelation,
        node_source: &NodeRelationSource,
    ) -> Result<bool> {
        if self
            .binding(&node_source.node)
            .is_some_and(|binding| binding.property_source.is_some())
        {
            return Ok(false);
        }
        let mut node = self.facts.nodes[&node_source.node].clone();
        let outputs = node_outputs(&node);
        let endpoint = [&relation.from_node, &relation.to_node]
            .into_iter()
            .find(|endpoint| **endpoint != node_source.node)
            .expect("node relation endpoint");
        let endpoint_plan = &self.facts.nodes[endpoint];
        if !endpoint_plan.node_ids.is_empty() {
            node.predicates.push(id_list(
                &node.alias,
                &node_source.column,
                &endpoint_plan.node_ids,
            ));
        }
        if let Some(range) = &endpoint_plan.id_range {
            node.predicates.push(Predicate::IdRange {
                column: Column::new(&node.alias, &node_source.column),
                start: range.start,
                end: range.end,
            });
        }
        if let Some(ids) = self.endpoint_ids(endpoint) {
            node.predicates.push(membership(
                Column::new(&node.alias, &node_source.column),
                ids,
            ));
        }
        if let Some(ids) = self.reached_ids(&node_source.node) {
            node.predicates.push(membership(
                Column::new(&node.alias, DEFAULT_PRIMARY_KEY),
                ids,
            ));
        }
        let scan = self.node_read(&node)?;
        self.record_read(&node, scan);
        let mut joined_through_endpoint = false;
        self.root = Some(
            match (self.root.take(), self.binding(&node_source.node).cloned()) {
                (Some(root), Some(binding)) => self.graph.join(
                    root,
                    scan.alias(&node.alias),
                    (
                        Column::new(&node.alias, DEFAULT_PRIMARY_KEY),
                        Column::new(&binding.alias, &binding.column),
                    ),
                    vec![],
                ),
                (Some(root), None) => {
                    let endpoint = [&relation.from_node, &relation.to_node]
                        .into_iter()
                        .find(|endpoint| **endpoint != node_source.node)
                        .and_then(|endpoint| self.binding(endpoint))
                        .cloned()
                        .ok_or_else(|| {
                            QueryError::Lowering("node relation is disconnected".into())
                        })?;
                    joined_through_endpoint = true;
                    self.graph.join(
                        root,
                        scan.alias(&node.alias),
                        (
                            Column::new(&node.alias, &node_source.column),
                            Column::new(&endpoint.alias, &endpoint.column),
                        ),
                        vec![],
                    )
                }
                (None, _) => scan,
            },
        );
        self.outputs.extend(outputs);
        self.set_binding(
            &node_source.node,
            &node.alias,
            DEFAULT_PRIMARY_KEY,
            Some(&node.alias),
            node.has_traversal_path
                .then(|| Column::new(&node.alias, TRAVERSAL_PATH_COLUMN)),
        );
        Ok(joined_through_endpoint)
    }

    fn push_node_relation_outputs(
        &mut self,
        relation: &EdgeRelation,
        node_source: &NodeRelationSource,
        alias: &str,
    ) {
        if self.facts.aggregate() {
            return;
        }
        let (source, target) = match relation.direction {
            Direction::Outgoing | Direction::Both => (&relation.from_node, &relation.to_node),
            Direction::Incoming => (&relation.to_node, &relation.from_node),
        };
        let node_is_source = node_source.node == *source;
        let source_id = if node_is_source {
            DEFAULT_PRIMARY_KEY
        } else {
            &node_source.column
        };
        let target_id = if node_is_source {
            &node_source.column
        } else {
            DEFAULT_PRIMARY_KEY
        };
        let prefix = relation.column_prefix();
        self.outputs.extend([
            Projection::new(
                OutputValue::Text(relation.rel_types.first().cloned().unwrap_or_default()),
                format!("{prefix}{EDGE_TYPE_SUFFIX}"),
            ),
            Projection::new(
                OutputValue::Column(Column::new(alias, source_id)),
                format!("{prefix}{EDGE_SRC_SUFFIX}"),
            ),
            Projection::new(
                OutputValue::Text(self.facts.nodes[source].entity.clone().unwrap_or_default()),
                format!("{prefix}{EDGE_SRC_TYPE_SUFFIX}"),
            ),
            Projection::new(
                OutputValue::Column(Column::new(alias, target_id)),
                format!("{prefix}{EDGE_DST_SUFFIX}"),
            ),
            Projection::new(
                OutputValue::Text(self.facts.nodes[target].entity.clone().unwrap_or_default()),
                format!("{prefix}{EDGE_DST_TYPE_SUFFIX}"),
            ),
        ]);
    }

    fn node_ids(&mut self, alias: &str) -> Result<(RelationId, String)> {
        if let Some(ids) = self.ids(alias) {
            return Ok(ids.clone());
        }
        let node = self.facts.nodes[alias].clone();
        self.node_ids_from(&node)
    }

    fn node_ids_from(&mut self, node: &NodePlan) -> Result<(RelationId, String)> {
        if let Some(ids) = self.ids(&node.alias) {
            return Ok(ids.clone());
        }
        let source = self.node_read(node)?;
        self.record_read(node, source);
        let id = self.graph.project(
            source,
            vec![Projection::new(
                OutputValue::Column(Column::new(&node.alias, DEFAULT_PRIMARY_KEY)),
                DEFAULT_PRIMARY_KEY,
            )],
        );
        let key = DEFAULT_PRIMARY_KEY.to_string();
        self.set_ids(&node.alias, (id, key.clone()));
        Ok((id, key))
    }

    fn bind_relation_ids(&mut self, index: usize, relation: &EdgeRelation, source: RelationId) {
        let (start, end) = relation.direction.edge_columns();
        for (node, column) in [(&relation.from_node, start), (&relation.to_node, end)] {
            if self.ids(node).is_some() {
                continue;
            }
            let other = if node == &relation.from_node {
                &relation.to_node
            } else {
                &relation.from_node
            };
            let used_later = self.facts.relations[index + 1..]
                .iter()
                .any(|next| next.endpoint_column(node).is_some())
                || self.facts.nodes[node].narrowed
                || self.facts.model.supports_selective_latest_rows()
                    && self.facts.nodes[node].joined
                    && (self.endpoint_is_selective(other) || self.ids(other).is_some());
            if !used_later {
                continue;
            }
            let ids = self.ids_reached_by(source, column);
            self.set_ids(node, ids);
        }
    }

    fn record_read(&mut self, node: &NodePlan, read: RelationId) {
        if node_predicates(node).iter().any(Predicate::is_selective) {
            self.nodes
                .entry(node.alias.clone())
                .or_default()
                .bounded_read = Some(read);
        }
    }

    fn endpoint_ids(&mut self, node: &str) -> Option<(RelationId, String)> {
        if let Some(ids) = self.ids(node) {
            return Some(ids.clone());
        }
        let ids = self.ids_through_holder(node, DEFAULT_PRIMARY_KEY)?;
        self.set_ids(node, ids.clone());
        Some(ids)
    }

    fn ids_through_holder(&mut self, holder: &str, column: &str) -> Option<(RelationId, String)> {
        if !self.facts.model.supports_selective_latest_rows() {
            return None;
        }
        let read = self.nodes.get(holder)?.bounded_read?;
        Some(self.ids_reached_by(read, column))
    }

    fn ids_reached_by(&mut self, source: RelationId, column: &str) -> (RelationId, String) {
        let key = DEFAULT_PRIMARY_KEY.to_string();
        let id = self
            .graph
            .from(source)
            .bind("_id_source")
            .project(vec![Projection::new(
                OutputValue::Column(Column::new("_id_source", column)),
                &key,
            )])
            .finish();
        (id, key)
    }

    fn ensure_node(&mut self, alias: &str) -> Result<()> {
        if self
            .binding(alias)
            .is_some_and(|binding| binding.property_source.is_some())
        {
            return Ok(());
        }
        let mut node = self.uncovered_node(alias);
        if let Some(ids) = self.reached_ids(alias) {
            node.predicates
                .push(membership(Column::new(alias, DEFAULT_PRIMARY_KEY), ids));
        } else if node.narrowed {
            return Err(QueryError::Lowering(format!(
                "node '{alias}' has no narrowing relation"
            )));
        }
        let outputs = node_outputs(&node);
        let scan = self.node_read(&node)?;
        self.record_read(&node, scan);
        self.root = Some(match (self.root.take(), self.binding(alias).cloned()) {
            (Some(root), Some(binding)) => self.graph.join(
                root,
                scan.alias(alias),
                (
                    Column::new(alias, DEFAULT_PRIMARY_KEY),
                    Column::new(&binding.alias, &binding.column),
                ),
                vec![],
            ),
            (Some(_), None) => {
                return Err(QueryError::Lowering(format!(
                    "node '{alias}' is not connected to the planned relations"
                )));
            }
            (None, _) => scan,
        });
        self.outputs.extend(outputs);
        self.set_binding(
            alias,
            alias,
            DEFAULT_PRIMARY_KEY,
            Some(alias),
            node.has_traversal_path
                .then(|| Column::new(alias, TRAVERSAL_PATH_COLUMN)),
        );
        Ok(())
    }

    fn push_edge_outputs(&mut self, relation: &EdgeRelation, alias: &str) {
        if self.facts.aggregate() {
            return;
        }
        let prefix = relation.column_prefix();
        self.outputs.extend(
            [
                (RELATIONSHIP_KIND_COLUMN, EDGE_TYPE_SUFFIX),
                (SOURCE_ID_COLUMN, EDGE_SRC_SUFFIX),
                (SOURCE_KIND_COLUMN, EDGE_SRC_TYPE_SUFFIX),
                (TARGET_ID_COLUMN, EDGE_DST_SUFFIX),
                (TARGET_KIND_COLUMN, EDGE_DST_TYPE_SUFFIX),
            ]
            .into_iter()
            .map(|(column, suffix)| {
                Projection::new(
                    OutputValue::Column(Column::new(alias, column)),
                    format!("{prefix}{suffix}"),
                )
            }),
        );
        if relation.max_hops > 1 {
            self.outputs.push(Projection::new(
                OutputValue::Column(Column::new(alias, PATH_NODES_COLUMN)),
                format!("{prefix}{PATH_NODES_COLUMN}"),
            ));
        }
    }

    fn node_read(&mut self, node: &NodePlan) -> Result<RelationId> {
        self.facts.node_read(&mut self.graph, node)
    }

    fn has_selective_node_filter(&self, alias: &str) -> bool {
        let node = &self.facts.nodes[alias];
        node.filters.iter().any(|(property, filter)| {
            filter.is_selective()
                && !filter_covered_by_edge(
                    alias,
                    property,
                    filter,
                    &self.facts.relations,
                    &self.facts.denormalized,
                    self.facts.model,
                )
        })
    }

    fn endpoint_is_selective(&self, alias: &str) -> bool {
        let node = &self.facts.nodes[alias];
        !node.node_ids.is_empty()
            || node.id_range.is_some()
            || self.has_selective_node_filter(alias)
    }

    fn uncovered_node(&self, alias: &str) -> NodePlan {
        let mut node = self.facts.nodes[alias].clone();
        node.filters.retain(|(property, filter)| {
            !filter_covered_by_edge(
                alias,
                property,
                filter,
                &self.facts.relations,
                &self.facts.denormalized,
                self.facts.model,
            )
        });
        node
    }

    fn edge_read(
        &mut self,
        relation: &EdgeRelation,
        alias: &str,
        final_: bool,
    ) -> Result<RelationId> {
        if let Some(table) = relation.single_edge_table() {
            let read = self.graph.read(table, alias, final_);
            return Ok(self.graph.owned(
                read,
                super::physical::ReadOwner::Relationship(relation.input_index),
                alias,
            ));
        }
        let mut routes = Vec::new();
        for table in &relation.edge_tables {
            let route_alias = format!("_{alias}");
            let read = self.graph.read(table, &route_alias, final_);
            let owned = self.graph.owned(
                read,
                super::physical::ReadOwner::Relationship(relation.input_index),
                &route_alias,
            );
            let outputs = EDGE_RESERVED_COLUMNS
                .iter()
                .chain([&SOURCE_TAGS_COLUMN, &TARGET_TAGS_COLUMN])
                .chain([&DELETED_COLUMN])
                .map(|column| Projection::col(&route_alias, column))
                .collect();
            routes.push(self.graph.from(owned).project(outputs).finish().into());
        }
        Ok(self.graph.union_all(routes, alias))
    }

    fn read_relation(
        &mut self,
        relation: &EdgeRelation,
        alias: &str,
        final_: bool,
        predicates: Vec<Predicate>,
    ) -> Result<RelationId> {
        if final_ && let Some(table) = relation.single_edge_table() {
            return Ok(self.graph.current_rows(
                table,
                alias,
                super::physical::ReadOwner::Relationship(relation.input_index),
                self.facts.latest_row_key(table)?,
                predicates,
                self.facts.model.supports_selective_latest_rows(),
            ));
        }
        if final_
            && self.facts.model.supports_selective_latest_rows()
            && predicates.iter().any(Predicate::is_selective)
        {
            let routes = relation
                .edge_tables
                .iter()
                .map(|table| {
                    let current = self.graph.current_rows(
                        table,
                        alias,
                        super::physical::ReadOwner::Relationship(relation.input_index),
                        self.facts.latest_row_key(table)?,
                        predicates.clone(),
                        true,
                    );
                    let outputs = EDGE_RESERVED_COLUMNS
                        .iter()
                        .chain([&SOURCE_TAGS_COLUMN, &TARGET_TAGS_COLUMN])
                        .map(|column| Projection::col(alias, column))
                        .collect();
                    Ok(self.graph.from(current).project(outputs).finish().into())
                })
                .collect::<Result<Vec<_>>>()?;
            return Ok(self.graph.union_all(routes, alias));
        }
        let read = self.edge_read(relation, alias, final_)?;
        Ok(self.graph.from(read).filter(predicates).finish())
    }
}

fn filters_sort_key(predicate: &Predicate, sort_key: &[String]) -> bool {
    let in_key = |column: &str| sort_key.iter().any(|key| key == column);
    match predicate {
        Predicate::RelationshipKinds { .. } => in_key(RELATIONSHIP_KIND_COLUMN),
        Predicate::EntityKind { column, .. }
        | Predicate::Ids { column, .. }
        | Predicate::IdRange { column, .. } => in_key(&column.name),
        Predicate::Membership { columns, .. } => columns.iter().all(|column| in_key(&column.name)),
        _ => false,
    }
}

fn edge_predicates<M: OrbitQueryModel + ?Sized>(
    facts: &PlanningContext<'_, M>,
    alias: &str,
    relation: &EdgeRelation,
    skip_deleted: bool,
) -> Vec<Predicate> {
    let mut predicates = Vec::new();
    if let Some(filter) = relationship_kinds(alias, &relation.rel_types) {
        predicates.push(filter);
    }
    let (source_node, target_node) = match relation.direction {
        Direction::Outgoing | Direction::Both => (&relation.from_node, &relation.to_node),
        Direction::Incoming => (&relation.to_node, &relation.from_node),
    };
    for (node_alias, column) in [
        (source_node, SOURCE_ID_COLUMN),
        (target_node, TARGET_ID_COLUMN),
    ] {
        if let Some(entity) = facts.nodes[node_alias].entity.as_ref() {
            predicates.push(Predicate::EntityKind {
                column: Column::new(
                    alias,
                    if column == SOURCE_ID_COLUMN {
                        SOURCE_KIND_COLUMN
                    } else {
                        TARGET_KIND_COLUMN
                    },
                ),
                entity: entity.clone(),
            });
        }
    }
    if !skip_deleted {
        predicates.push(live(alias));
    }
    if let Some(columns) = relation
        .single_edge_table()
        .and_then(|table| facts.model.table_columns(table))
    {
        let mut seen = HashSet::new();
        for node_alias in [&relation.from_node, &relation.to_node] {
            for (property, filter) in &facts.nodes[node_alias].filters {
                if columns.contains(property)
                    && !EDGE_RESERVED_COLUMNS.contains(&property.as_str())
                    && seen.insert(property)
                {
                    predicates.push(property_filter(alias, property, filter));
                }
            }
        }
    }
    predicates
}

fn node_id_predicates<M: OrbitQueryModel + ?Sized>(
    facts: &PlanningContext<'_, M>,
    alias: &str,
    relation: &EdgeRelation,
) -> Vec<Predicate> {
    let mut predicates = Vec::new();
    for node in [&relation.from_node, &relation.to_node] {
        let column = relation.endpoint_column(node).unwrap();
        let plan = &facts.nodes[node];
        if let Some(range) = &plan.id_range {
            predicates.push(Predicate::IdRange {
                column: Column::new(alias, column),
                start: range.start,
                end: range.end,
            });
        }
        if !plan.node_ids.is_empty() {
            predicates.push(id_list(alias, column, &plan.node_ids));
        }
    }
    predicates
}

fn push_denorm_tags<M: OrbitQueryModel + ?Sized>(
    facts: &PlanningContext<'_, M>,
    predicates: &mut Vec<Predicate>,
    relation: &EdgeRelation,
    alias: &str,
    tagged: &mut HashSet<(String, String, usize)>,
) {
    if crate::passes::normalize::is_wildcard(&relation.rel_types) {
        return;
    }
    for node_alias in [&relation.from_node, &relation.to_node] {
        for (filter_index, (property_name, filter)) in
            facts.nodes[node_alias].filters.iter().enumerate()
        {
            let tag = (node_alias.clone(), property_name.clone(), filter_index);
            if tagged.contains(&tag) {
                continue;
            }
            if let Some((_, denormalized)) = denorm_hop(
                filter,
                node_alias,
                std::slice::from_ref(relation),
                &facts.denormalized,
            ) && let Some(predicate) = tag_filter(
                alias,
                &denormalized.edge_column,
                &denormalized.tag_key,
                &filter.filter,
            ) {
                predicates.push(predicate);
                tagged.insert(tag);
            }
        }
    }
}
