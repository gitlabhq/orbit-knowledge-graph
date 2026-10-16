use std::collections::{HashMap, HashSet};

use ontology::constants::*;
use query_data_model::OrbitQueryModel;

mod binding;
mod conditions;
mod expansion;

use super::BoundFilter;
use super::context::PlanningContext;
use super::helpers::{FilterOwner, ordered_filters};
use super::physical::{
    BindingSource, ExecutionPlan, PhysicalPlan, PhysicalSource, Subplan, SubplanId,
};
use super::requirements::{
    Column, OutputValue, Predicate, Projection, id_list, live, property_filter, relationship_kinds,
    tag_filter,
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
    pub fn single_edge_table(&self) -> Option<&str> {
        let [table] = self.edge_tables.as_slice() else {
            return None;
        };
        Some(table)
    }

    pub fn column_prefix(&self, index: usize) -> String {
        if self.max_hops > 1 {
            format!("hop_e{index}_")
        } else {
            format!("e{index}_")
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

pub(super) fn plan<'a, M>(
    mut context: PlanningContext<'a, M>,
    table_scans: &HashSet<String>,
) -> Result<PlannedRelations<'a, M>>
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
        table_scans,
        &mut nodes,
    );
    linearize_relations(&mut relations)?;
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
    execution.source = execution.source.filter(conditions);
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
    source: Option<PhysicalSource>,
    outputs: Vec<Projection>,
    bindings: Vec<BindingSource>,
    subplans: Vec<Subplan>,
    frontiers: HashMap<String, (SubplanId, String)>,
    exact_keys: HashMap<String, (SubplanId, String)>,
    tagged: HashSet<(String, String, usize)>,
}

impl<'a, M: OrbitQueryModel + ?Sized> RelationBuilder<'a, M> {
    fn new(facts: &'a PlanningContext<'a, M>) -> Self {
        Self {
            facts,
            source: None,
            outputs: Vec::new(),
            bindings: Vec::new(),
            subplans: Vec::new(),
            frontiers: HashMap::new(),
            exact_keys: HashMap::new(),
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
        Ok(ExecutionPlan {
            source: self
                .source
                .ok_or_else(|| QueryError::Lowering("traversal has no physical source".into()))?,
            subplans: self.subplans,
            outputs: self.outputs,
            bindings: self.bindings,
        })
    }

    fn attach_nodes(&mut self) -> Result<()> {
        let aliases: Vec<_> = self
            .bindings
            .iter()
            .map(|binding| binding.node.clone())
            .collect();
        for alias in aliases {
            if self.facts.nodes[&alias].joined {
                self.ensure_node(&alias)?;
            }
        }
        Ok(())
    }

    fn binding(&self, node: &str) -> Option<&BindingSource> {
        self.bindings.iter().find(|binding| binding.node == node)
    }

    fn set_binding(
        &mut self,
        node: &str,
        alias: &str,
        column: &str,
        property_source: Option<&str>,
        traversal_path: Option<Column>,
    ) {
        if let Some(binding) = self
            .bindings
            .iter_mut()
            .find(|binding| binding.node == node)
        {
            binding.alias = alias.to_string();
            binding.column = column.to_string();
            binding.property_source = property_source.map(str::to_string);
            binding.traversal_path = traversal_path;
        } else {
            self.bindings.push(BindingSource {
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
            return self.add_node_relation(index, relation);
        }
        let alias = &relation.alias;
        let mut physical_relation = relation.clone();
        let frontiers: Vec<_> = [&relation.from_node, &relation.to_node]
            .into_iter()
            .filter_map(|node| {
                self.frontiers
                    .get(node)
                    .cloned()
                    .map(|frontier| (node.clone(), frontier))
            })
            .collect();
        let frontier_columns: Vec<_> = frontiers
            .iter()
            .filter_map(|(node, _)| relation.endpoint_column(node))
            .collect();
        if relation.max_hops == 1 {
            select_edge_layouts(
                self.facts.model,
                &mut physical_relation.edge_tables,
                &frontier_columns,
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
        for (node, (frontier, key)) in &frontiers {
            if let Some(column) = relation.endpoint_column(node) {
                predicates.push(Predicate::Membership {
                    column: Column::new(alias, column),
                    definition: format!("_orbit_subplan_{}", frontier.0),
                    key: key.clone(),
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
            let keys = self.exact_node_keys(node)?;
            let column = relation.endpoint_column(node).expect("relation endpoint");
            predicates.push(Predicate::Membership {
                column: Column::new(alias, column),
                definition: format!("_orbit_subplan_{}", keys.0.0),
                key: keys.1,
            });
        }

        let mut source = if relation.max_hops > 1 {
            multi_hop(self.facts, &physical_relation, alias).filter(predicates)
        } else if self.facts.aggregate() && self.facts.relations.len() == 1 {
            if let Some(table) = physical_relation.single_edge_table() {
                PhysicalSource::Latest {
                    sort_key: self.facts.latest_row_key(table)?.to_vec(),
                    alias: alias.clone(),
                    aggregate_condition: predicates,
                    input: Box::new(PhysicalSource::edge(&physical_relation, alias, false)),
                }
            } else {
                PhysicalSource::edge(&physical_relation, alias, true).filter(predicates)
            }
        } else {
            PhysicalSource::edge(&physical_relation, alias, true).filter(predicates)
        };
        let needs_new_frontier = [&relation.from_node, &relation.to_node]
            .into_iter()
            .any(|node| {
                !self.frontiers.contains_key(node)
                    && self.facts.relations[index + 1..]
                        .iter()
                        .any(|next| next.endpoint_column(node).is_some())
            });
        let should_reuse = relation.max_hops == 1
            && needs_new_frontier
            && (!frontiers.is_empty()
                || self.source.is_none() && relation_is_selective(self.facts, relation));
        if should_reuse {
            let id = SubplanId(self.subplans.len());
            self.subplans.push(Subplan {
                id,
                plan: PhysicalPlan::Project {
                    source,
                    outputs: edge_relation_outputs(alias),
                },
                materialized: true,
            });
            source = PhysicalSource::Use {
                id,
                alias: alias.clone(),
            };
            self.add_relation_frontiers(index, relation, id);
        }
        if self.facts.relations.len() > 1 && relation.max_hops == 1 {
            source = PhysicalSource::Scope {
                alias: alias.clone(),
                input: Box::new(source),
            };
        }
        let (start, end) = relation.direction.edge_columns();
        let endpoints = [(&relation.from_node, start), (&relation.to_node, end)];
        if let Some(previous) = self.source.take() {
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
            self.source = Some(PhysicalSource::Join {
                endpoints: (
                    Column::new(&binding.alias, &binding.column),
                    Column::new(alias, column),
                ),
                predicates: join_predicates,
                left: Box::new(previous),
                right: Box::new(source),
            });
        } else {
            self.source = Some(source);
        }
        for (node, column) in endpoints {
            if self.binding(node).is_none() {
                self.set_binding(node, alias, column, None, None);
            }
        }
        self.push_edge_outputs(index, relation, alias);
        Ok(())
    }

    fn add_node_relation(&mut self, index: usize, relation: &EdgeRelation) -> Result<()> {
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
        if !endpoint_plan.joined
            && node_has_uncovered_filter(
                endpoint_plan,
                &self.facts.relations,
                &self.facts.denormalized,
                self.facts.model,
            )
        {
            let keys = self.exact_node_keys(endpoint)?;
            predicates.push(Predicate::Membership {
                column: endpoint_column.clone(),
                definition: format!("_orbit_subplan_{}", keys.0.0),
                key: keys.1,
            });
        }
        if let Some((frontier, key)) = self.frontiers.get(endpoint) {
            predicates.push(Predicate::Membership {
                column: endpoint_column.clone(),
                definition: format!("_orbit_subplan_{}", frontier.0),
                key: key.clone(),
            });
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
        self.source = self.source.take().map(|source| source.filter(predicates));
        self.push_node_relation_outputs(index, relation, node_source, alias);
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
        let node = &self.facts.nodes[&node_source.node];
        let scan = PhysicalPlan::node_scan(node)?;
        let outputs = scan.outputs().to_vec();
        let mut scan = scan.into_source();
        if let Some((frontier, key)) = self.frontiers.get(&node_source.node) {
            scan = scan.filter(vec![Predicate::Membership {
                column: Column::new(&node.alias, DEFAULT_PRIMARY_KEY),
                definition: format!("_orbit_subplan_{}", frontier.0),
                key: key.clone(),
            }]);
        }
        let mut joined_through_endpoint = false;
        self.source = Some(
            match (self.source.take(), self.binding(&node_source.node).cloned()) {
                (Some(source), Some(binding)) => source.inner_join(
                    scan,
                    (
                        Column::new(&node.alias, DEFAULT_PRIMARY_KEY),
                        Column::new(&binding.alias, &binding.column),
                    ),
                ),
                (Some(source), None) => {
                    let endpoint = [&relation.from_node, &relation.to_node]
                        .into_iter()
                        .find(|endpoint| **endpoint != node_source.node)
                        .and_then(|endpoint| self.binding(endpoint))
                        .cloned()
                        .ok_or_else(|| {
                            QueryError::Lowering("node relation is disconnected".into())
                        })?;
                    joined_through_endpoint = true;
                    source.inner_join(
                        scan,
                        (
                            Column::new(&node.alias, &node_source.column),
                            Column::new(&endpoint.alias, &endpoint.column),
                        ),
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
        index: usize,
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
        let prefix = relation.column_prefix(index);
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

    fn exact_node_keys(&mut self, alias: &str) -> Result<(SubplanId, String)> {
        if let Some(keys) = self.exact_keys.get(alias) {
            return Ok(keys.clone());
        }
        let node = &self.facts.nodes[alias];
        let candidate = if let Some(plan) = PhysicalPlan::candidate_keys(node)? {
            let id = SubplanId(self.subplans.len());
            self.subplans.push(Subplan {
                id,
                plan,
                materialized: true,
            });
            Some((id, DEFAULT_PRIMARY_KEY.to_string()))
        } else {
            None
        };
        let id = SubplanId(self.subplans.len());
        let key = DEFAULT_PRIMARY_KEY.to_string();
        self.subplans.push(Subplan {
            id,
            plan: PhysicalPlan::node_keys(node, candidate)?,
            materialized: true,
        });
        self.exact_keys.insert(alias.to_string(), (id, key.clone()));
        Ok((id, key))
    }

    fn add_relation_frontiers(&mut self, index: usize, relation: &EdgeRelation, source: SubplanId) {
        let (start, end) = relation.direction.edge_columns();
        for (node, column) in [(&relation.from_node, start), (&relation.to_node, end)] {
            if self.frontiers.contains_key(node) {
                continue;
            }
            let used_later = self.facts.relations[index + 1..]
                .iter()
                .any(|next| next.endpoint_column(node).is_some());
            if !used_later {
                continue;
            }
            let id = SubplanId(self.subplans.len());
            let key = "id".to_string();
            self.subplans.push(Subplan {
                id,
                plan: PhysicalPlan::Distinct {
                    source: PhysicalSource::Use {
                        id: source,
                        alias: "_frontier_source".into(),
                    },
                    outputs: vec![Projection::new(
                        OutputValue::Column(Column::new("_frontier_source", column)),
                        &key,
                    )],
                },
                materialized: false,
            });
            self.frontiers.insert(node.clone(), (id, key));
        }
    }

    fn ensure_node(&mut self, alias: &str) -> Result<()> {
        if self
            .binding(alias)
            .is_some_and(|binding| binding.property_source.is_some())
        {
            return Ok(());
        }
        let mut node = self.facts.nodes[alias].clone();
        node.filters.retain(|(property, filter)| {
            !filter_covered_by_edge(
                &node.alias,
                property,
                filter,
                &self.facts.relations,
                &self.facts.denormalized,
                self.facts.model,
            )
        });
        let scan = PhysicalPlan::node_scan(&node)?;
        let outputs = scan.outputs().to_vec();
        let scan = scan.into_source();
        self.source = Some(match (self.source.take(), self.binding(alias).cloned()) {
            (Some(source), Some(binding)) => source.inner_join(
                scan,
                (
                    Column::new(alias, DEFAULT_PRIMARY_KEY),
                    Column::new(&binding.alias, &binding.column),
                ),
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

    fn push_edge_outputs(&mut self, index: usize, relation: &EdgeRelation, alias: &str) {
        if self.facts.aggregate() {
            return;
        }
        let prefix = relation.column_prefix(index);
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

fn edge_relation_outputs(alias: &str) -> Vec<Projection> {
    EDGE_RESERVED_COLUMNS
        .iter()
        .chain([&SOURCE_TAGS_COLUMN, &TARGET_TAGS_COLUMN])
        .chain([&DELETED_COLUMN])
        .map(|column| Projection::col(alias, column))
        .collect()
}

fn relation_is_selective<M: OrbitQueryModel + ?Sized>(
    facts: &PlanningContext<'_, M>,
    relation: &EdgeRelation,
) -> bool {
    [&relation.from_node, &relation.to_node]
        .into_iter()
        .any(|alias| {
            let node = &facts.nodes[alias];
            !node.node_ids.is_empty() || node.id_range.is_some() || !node.filters.is_empty()
        })
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
