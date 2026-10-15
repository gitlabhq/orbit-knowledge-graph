use std::collections::{HashMap, HashSet};

use ontology::constants::*;
use query_data_model::OrbitQueryModel;

use super::context::PlanningContext;
use super::helpers::{FilterOwner, bind_filter, ordered_filters};
use super::physical::{
    BindingSource, ExecutionPlan, PhysicalPlan, PhysicalSource, Subplan, SubplanId,
};
use super::requirements::{
    Column, OutputValue, Predicate, Projection, id_list, live, property_filter, relationship_kinds,
    tag_filter,
};
use super::{BoundFilter, DenormalizedDirection, DenormalizedKey, DenormalizedProperty};
use crate::constants::*;
use crate::error::{QueryError, Result};
use crate::input::*;

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
    pub foreign_key: Option<ForeignKeyEdge>,
}

#[derive(Clone)]
pub struct ForeignKeyEdge {
    pub table: String,
    pub holder: String,
    pub holder_column: String,
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

#[derive(Default)]
struct ConditionPlacement {
    scans: Vec<Option<String>>,
    joined_nodes: HashSet<String>,
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

fn linearize_relations(relations: &mut [EdgeRelation]) -> Result<()> {
    let Some(first) = relations.first() else {
        return Ok(());
    };
    let mut reached = HashSet::from([first.from_node.clone(), first.to_node.clone()]);
    for index in 1..relations.len() {
        let next = (index..relations.len())
            .find(|candidate| {
                reached.contains(&relations[*candidate].from_node)
                    || reached.contains(&relations[*candidate].to_node)
            })
            .ok_or_else(|| {
                QueryError::Validation("relationships must form a connected tree".into())
            })?;
        relations[index..=next].rotate_right(1);
        reached.insert(relations[index].from_node.clone());
        reached.insert(relations[index].to_node.clone());
    }
    Ok(())
}

fn build_nodes<M>(input: &Input, model: &M) -> HashMap<String, NodePlan>
where
    M: OrbitQueryModel + ?Sized,
{
    input
        .nodes
        .iter()
        .filter_map(|node| Some((node.id.clone(), NodePlan::from_input(node, model, true)?)))
        .collect()
}

fn build_relations<M>(input: &Input, model: &M) -> Vec<EdgeRelation>
where
    M: OrbitQueryModel + ?Sized,
{
    let entities: HashMap<&str, &str> = input
        .nodes
        .iter()
        .filter_map(|node| Some((node.id.as_str(), node.entity.as_deref()?)))
        .collect();
    input
        .relationships
        .iter()
        .enumerate()
        .map(|(input_index, relationship)| {
            let mut edge_tables = model.relationship_tables(&relationship.types);
            if edge_tables.is_empty() {
                edge_tables.push(model.default_edge_table().to_string());
            }
            let from_entity = entities
                .get(relationship.from.as_str())
                .copied()
                .unwrap_or_default();
            let to_entity = entities
                .get(relationship.to.as_str())
                .copied()
                .unwrap_or_default();
            let endpoint_entities = match relationship.direction {
                Direction::Outgoing | Direction::Both => (from_entity, to_entity),
                Direction::Incoming => (to_entity, from_entity),
            };
            let (start, end) = relationship.direction.edge_columns();
            let endpoints: Vec<_> = [(&relationship.from, start), (&relationship.to, end)]
                .into_iter()
                .filter(|(node, _)| node_ids_are_selective(input, node))
                .map(|(_, column)| column)
                .collect();
            select_edge_layouts(model, &mut edge_tables, &endpoints);
            let source_entity = model.graph().entity_id(endpoint_entities.0);
            let target_entity = model.graph().entity_id(endpoint_entities.1);
            let foreign_key = if relationship.hops.max == 1
                && relationship.filters.is_empty()
                && !input.references(&PredicateTarget::Relationship(input_index))
                && relationship.direction != Direction::Both
            {
                model
                    .foreign_key(
                        &relationship.types,
                        endpoint_entities.0,
                        endpoint_entities.1,
                    )
                    .and_then(|foreign_key| {
                        let holder_column =
                            model.property_column(foreign_key.property)?.to_string();
                        model.property_column(foreign_key.referenced_key)?;
                        let (source_node, target_node) = match relationship.direction {
                            Direction::Outgoing | Direction::Both => {
                                (&relationship.from, &relationship.to)
                            }
                            Direction::Incoming => (&relationship.to, &relationship.from),
                        };
                        let holder = match foreign_key.holder {
                            query_data_model::Endpoint::Source => source_node.clone(),
                            query_data_model::Endpoint::Target => target_node.clone(),
                        };
                        let entity = input.entity_of(&holder)?;
                        Some(ForeignKeyEdge {
                            table: model.entity_table(entity)?.to_string(),
                            holder,
                            holder_column,
                        })
                    })
            } else {
                None
            };
            EdgeRelation {
                input_index,
                rel_types: relationship.types.clone(),
                relationships: relationship
                    .types
                    .iter()
                    .filter_map(|kind| model.graph().relationship_id(kind))
                    .collect(),
                edge_tables: edge_tables.clone(),
                alias: format!("{}edge_{input_index}", internal_column_prefix()),
                source_entity,
                target_entity,
                from_node: relationship.from.clone(),
                to_node: relationship.to.clone(),
                direction: relationship.direction,
                min_hops: relationship.hops.min,
                max_hops: relationship.hops.max,
                filters: ordered_filters(
                    &relationship.filters,
                    FilterOwner::Tables(&edge_tables),
                    model,
                ),
                foreign_key,
            }
        })
        .collect()
}

fn node_ids_are_selective(input: &Input, alias: &str) -> bool {
    input
        .nodes
        .iter()
        .find(|node| node.id == alias)
        .is_some_and(|node| !node.node_ids.is_empty() || node.id_range.is_some())
}

fn select_edge_layouts(
    model: &(impl OrbitQueryModel + ?Sized),
    tables: &mut [String],
    endpoints: &[&str],
) {
    if endpoints.is_empty() {
        return;
    }
    for table in tables {
        let selected = std::iter::once(table.as_str())
            .chain(model.equivalent_layouts(table))
            .min_by_key(|layout| {
                model.table_sort_key(layout).map_or(usize::MAX, |key| {
                    endpoints
                        .iter()
                        .filter_map(|endpoint| key.iter().position(|column| column == endpoint))
                        .min()
                        .unwrap_or(usize::MAX)
                })
            });
        if let Some(selected) = selected {
            *table = selected.to_string();
        }
    }
}

fn assign_node_access(
    input: &Input,
    model: &(impl OrbitQueryModel + ?Sized),
    relations: &[EdgeRelation],
    denormalized: &HashMap<DenormalizedKey, DenormalizedProperty>,
    condition_nodes: &HashSet<String>,
    table_scans: &HashSet<String>,
    nodes: &mut HashMap<String, NodePlan>,
) {
    for node in nodes.values_mut() {
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
                            .is_some_and(|entity| model.property_is_nullable(entity, property))
                })
        });
        let ordered = input
            .order_by
            .as_ref()
            .is_some_and(|order| order.node == node.alias);
        let local_predicate = condition_nodes.contains(&node.alias);
        let uncovered_filter = node_has_uncovered_filter(node, relations, denormalized, model);
        let filter_only = input.query_type == QueryType::Aggregation
            && uncovered_filter
            && !group_target
            && !metric_target
            && !ordered
            && !local_predicate
            && !table_scans.contains(&node.alias);
        node.joined = input.relationships.is_empty()
            || !model.supports_deferred_node_loading()
            || group_target
            || metric_target
            || ordered
            || local_predicate
            || table_scans.contains(&node.alias)
            || uncovered_filter && !filter_only;
    }
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

    fn set_binding(&mut self, node: &str, alias: &str, column: &str, joined: bool) {
        if let Some(binding) = self
            .bindings
            .iter_mut()
            .find(|binding| binding.node == node)
        {
            binding.alias = alias.to_string();
            binding.column = column.to_string();
            binding.joined = joined;
        } else {
            self.bindings.push(BindingSource {
                node: node.to_string(),
                alias: alias.to_string(),
                column: column.to_string(),
                joined,
            });
        }
    }

    fn add_edge(&mut self, index: usize, relation: &EdgeRelation) -> Result<()> {
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
        select_edge_layouts(
            self.facts.model,
            &mut physical_relation.edge_tables,
            &frontier_columns,
        );
        let mut predicates = if physical_relation.foreign_key.is_some() {
            Vec::new()
        } else {
            edge_predicates(self.facts, alias, &physical_relation, false)
        };
        predicates.extend(
            relation
                .filters
                .iter()
                .map(|(property, filter)| property_filter(alias, property, filter)),
        );
        if physical_relation.foreign_key.is_none() {
            push_denorm_tags(
                self.facts,
                &mut predicates,
                relation,
                alias,
                &mut self.tagged,
            );
        }
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

        let mut source = if let Some(foreign_key) = &physical_relation.foreign_key {
            foreign_key_source(self.facts, &physical_relation, foreign_key)?.filter(predicates)
        } else if relation.max_hops > 1 {
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
                self.set_binding(node, alias, column, false);
            }
        }
        self.push_edge_outputs(index, relation, alias);
        Ok(())
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
        if self.binding(alias).is_some_and(|binding| binding.joined) {
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
        self.set_binding(alias, alias, DEFAULT_PRIMARY_KEY, true);
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

fn foreign_key_source<M: OrbitQueryModel + ?Sized>(
    facts: &PlanningContext<'_, M>,
    relation: &EdgeRelation,
    foreign_key: &ForeignKeyEdge,
) -> Result<PhysicalSource> {
    let alias = &relation.alias;
    let (source_node, target_node) = match relation.direction {
        Direction::Outgoing | Direction::Both => (&relation.from_node, &relation.to_node),
        Direction::Incoming => (&relation.to_node, &relation.from_node),
    };
    let holder_is_source = foreign_key.holder == *source_node;
    let source_id = if holder_is_source {
        Column::new(alias, DEFAULT_PRIMARY_KEY)
    } else {
        Column::new(alias, &foreign_key.holder_column)
    };
    let target_id = if holder_is_source {
        Column::new(alias, &foreign_key.holder_column)
    } else {
        Column::new(alias, DEFAULT_PRIMARY_KEY)
    };
    let source_kind = facts.nodes[source_node].entity.clone().unwrap_or_default();
    let target_kind = facts.nodes[target_node].entity.clone().unwrap_or_default();
    let holder = &facts.nodes[&foreign_key.holder];
    let mut predicates = holder
        .filters
        .iter()
        .map(|(property, filter)| property_filter(alias, property, filter))
        .collect::<Vec<_>>();
    predicates.push(live(alias));
    let scan = PhysicalSource::Scan {
        table: foreign_key.table.clone(),
        alias: alias.clone(),
        final_: true,
        relationship: Some(relation.input_index),
    }
    .filter(predicates);
    let outputs = vec![
        Projection::new(OutputValue::Column(source_id), SOURCE_ID_COLUMN),
        Projection::new(OutputValue::Text(source_kind), SOURCE_KIND_COLUMN),
        Projection::new(
            OutputValue::Text(relation.rel_types.first().cloned().unwrap_or_default()),
            RELATIONSHIP_KIND_COLUMN,
        ),
        Projection::new(OutputValue::Column(target_id), TARGET_ID_COLUMN),
        Projection::new(OutputValue::Text(target_kind), TARGET_KIND_COLUMN),
        Projection::new(OutputValue::Null, TRAVERSAL_PATH_COLUMN),
        Projection::new(OutputValue::EmptyTextArray, SOURCE_TAGS_COLUMN),
        Projection::new(OutputValue::EmptyTextArray, TARGET_TAGS_COLUMN),
        Projection::new(OutputValue::Null, DELETED_COLUMN),
    ];
    Ok(PhysicalSource::Project {
        alias: alias.clone(),
        outputs,
        input: Box::new(scan),
    })
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

fn node_has_uncovered_filter(
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

fn filter_covered_by_edge(
    alias: &str,
    property: &str,
    filter: &BoundFilter,
    relations: &[EdgeRelation],
    denormalized: &HashMap<DenormalizedKey, DenormalizedProperty>,
    model: &(impl OrbitQueryModel + ?Sized),
) -> bool {
    relations.iter().any(|relation| {
        relation
            .foreign_key
            .as_ref()
            .is_some_and(|foreign_key| foreign_key.holder == alias)
            || relation.foreign_key.is_none()
                && (denorm_hop(filter, alias, std::slice::from_ref(relation), denormalized)
                    .is_some()
                    || relation.endpoint_column(alias).is_some()
                        && relation
                            .single_edge_table()
                            .and_then(|table| model.table_columns(table))
                            .is_some_and(|columns| columns.contains(property)))
    })
}

fn denorm_hop<'a>(
    filter: &BoundFilter,
    alias: &str,
    relations: &[EdgeRelation],
    denormalized: &'a HashMap<DenormalizedKey, DenormalizedProperty>,
) -> Option<(usize, &'a DenormalizedProperty)> {
    relations
        .iter()
        .enumerate()
        .find_map(|(position, relation)| {
            if relation.foreign_key.is_some() {
                return None;
            }
            if crate::passes::normalize::is_wildcard(&relation.rel_types) {
                return None;
            }
            let column = relation.endpoint_column(alias)?;
            let direction = if column == SOURCE_ID_COLUMN {
                DenormalizedDirection::Source
            } else {
                DenormalizedDirection::Target
            };
            let source = relation.source_entity?;
            let target = relation.target_entity?;
            super::helpers::lowerable_denorm(filter, direction, denormalized)
                .filter(|facts| {
                    relation.relationships.iter().all(|relationship| {
                        facts.variants.contains(&(*relationship, source, target))
                    })
                })
                .map(|facts| (position, facts))
        })
}

fn multi_hop<M: OrbitQueryModel + ?Sized>(
    facts: &PlanningContext<'_, M>,
    relation: &EdgeRelation,
    alias: &str,
) -> PhysicalSource {
    let edge_columns = relation
        .single_edge_table()
        .and_then(|table| facts.model.table_columns(table));
    let mut required_columns = Vec::new();
    for node in [&relation.from_node, &relation.to_node] {
        for (property, _) in &facts.nodes[node].filters {
            if edge_columns.is_some_and(|columns| columns.contains(property))
                && !EDGE_RESERVED_COLUMNS.contains(&property.as_str())
                && !required_columns.contains(property)
            {
                required_columns.push(property.clone());
            }
        }
    }
    let arms = (relation.min_hops.max(1)..=relation.max_hops)
        .map(|depth| depth_arm(relation, depth, &required_columns))
        .collect();
    let (from_kind, to_kind) = match relation.direction {
        Direction::Outgoing | Direction::Both => (SOURCE_KIND_COLUMN, TARGET_KIND_COLUMN),
        Direction::Incoming => (TARGET_KIND_COLUMN, SOURCE_KIND_COLUMN),
    };
    let mut predicates = Vec::new();
    for (node, column) in [
        (&relation.from_node, from_kind),
        (&relation.to_node, to_kind),
    ] {
        if let Some(entity) = facts.nodes[node].entity.as_ref() {
            predicates.push(Predicate::EntityKind {
                column: Column::new(alias, column),
                entity: entity.clone(),
            });
        }
    }
    predicates.push(live(alias));
    PhysicalSource::Filter {
        predicates,
        input: Box::new(PhysicalSource::Union {
            alias: alias.into(),
            arms,
            relationship: relation.input_index,
        }),
    }
}

fn depth_arm(relation: &EdgeRelation, depth: u32, required_columns: &[String]) -> PhysicalPlan {
    let (start, end) = relation.direction.edge_columns();
    let end_kind = match relation.direction {
        Direction::Outgoing | Direction::Both => TARGET_KIND_COLUMN,
        Direction::Incoming => SOURCE_KIND_COLUMN,
    };
    let scan = |alias: &str| PhysicalSource::edge(relation, alias, true);
    let mut root_predicates = relationship_kinds("e1", &relation.rel_types)
        .into_iter()
        .collect();
    let mut source = scan("e1");
    for index in 2..=depth {
        let previous = format!("e{}", index - 1);
        let current = format!("e{index}");
        let mut predicates: Vec<_> = relationship_kinds(&current, &relation.rel_types)
            .into_iter()
            .collect();
        predicates.push(Predicate::ColumnEq {
            left: Column::new(&previous, end_kind),
            right: Column::new(
                &current,
                match relation.direction {
                    Direction::Outgoing | Direction::Both => SOURCE_KIND_COLUMN,
                    Direction::Incoming => TARGET_KIND_COLUMN,
                },
            ),
        });
        source = PhysicalSource::Join {
            endpoints: (Column::new(&previous, end), Column::new(&current, start)),
            predicates,
            left: Box::new(source),
            right: Box::new(scan(&current)),
        };
    }
    let last = format!("e{depth}");
    let (source_alias, target_alias, kind_alias) = match relation.direction {
        Direction::Outgoing | Direction::Both => ("e1", last.as_str(), "e1"),
        Direction::Incoming => (last.as_str(), "e1", last.as_str()),
    };
    let path = OutputValue::Path(
        (1..=depth)
            .map(|index| {
                let alias = format!("e{index}");
                (Column::new(&alias, end), Column::new(&alias, end_kind))
            })
            .collect(),
    );
    let mut outputs = vec![
        Projection::col("e1", start),
        Projection::col(&last, end),
        Projection::col(kind_alias, RELATIONSHIP_KIND_COLUMN),
        Projection::col(source_alias, SOURCE_ID_COLUMN),
        Projection::col(source_alias, SOURCE_KIND_COLUMN),
        Projection::col(source_alias, SOURCE_TAGS_COLUMN),
        Projection::col(target_alias, TARGET_ID_COLUMN),
        Projection::col(target_alias, TARGET_KIND_COLUMN),
        Projection::col(target_alias, TARGET_TAGS_COLUMN),
        Projection::new(path, PATH_NODES_COLUMN),
        Projection::new(OutputValue::Depth(depth), DEPTH_COLUMN),
        Projection::col("e1", DELETED_COLUMN),
        Projection::col("e1", TRAVERSAL_PATH_COLUMN),
    ];
    outputs.extend(
        required_columns
            .iter()
            .map(|column| Projection::col("e1", column)),
    );
    PhysicalPlan::Project {
        source: PhysicalSource::Filter {
            predicates: std::mem::take(&mut root_predicates),
            input: Box::new(source),
        },
        outputs,
    }
}

fn place_conditions(
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
                        .filter(|(_, property)| *property != DEFAULT_PRIMARY_KEY)
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

fn bind_scan_conditions(
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

fn bind_tree(
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
        return Ok(tag);
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
