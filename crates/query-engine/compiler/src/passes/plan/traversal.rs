use std::collections::{HashMap, HashSet};

use ontology::constants::*;
use query_data_model::OrbitQueryModel;

use super::context::PlanningContext;
use super::helpers::{FilterOwner, bind_filter, ordered_filters};
use super::physical::{BindingSource, ExecutionPlan, PhysicalPlan, PhysicalSource};
use super::requirements::{
    Column, OutputValue, Predicate, Projection, id_list, live, property_filter, relationship_kinds,
    tag_filter,
};
use super::{
    Aggregation, BoundFilter, DenormalizedDirection, DenormalizedKey, DenormalizedProperty,
    QueryPlan, Traversal,
};
use crate::constants::*;
use crate::error::{QueryError, Result};
use crate::input::*;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum NodeAccess {
    Identity,
    SemiJoin,
    Join,
}

pub struct NodePlan {
    pub alias: String,
    pub entity: Option<String>,
    pub table: Option<String>,
    pub access: NodeAccess,
    pub filters: Vec<(String, BoundFilter)>,
    pub predicates: Vec<Predicate>,
    pub node_ids: Vec<i64>,
    pub id_range: Option<InputIdRange>,
    pub has_traversal_path: bool,
    pub is_global: bool,
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
            access: NodeAccess::Identity,
            filters: ordered_filters(filters, FilterOwner::Entity(entity_id), model),
            predicates: Vec::new(),
            node_ids: node.node_ids.clone(),
            id_range: node.id_range.clone(),
            has_traversal_path: model.entity_has_traversal_path(entity),
            is_global: model.entity_is_global(entity),
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
pub struct ForeignKeyRelation {
    pub holder: String,
    pub column: String,
    pub referenced: String,
    pub referenced_column: String,
}

#[derive(Clone)]
pub enum RelationSource {
    Stored,
    ForeignKey(ForeignKeyRelation),
}

pub struct Hop {
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
    pub source: RelationSource,
    pub filters: Vec<(String, BoundFilter)>,
    pub scope_preserving: bool,
}

impl Hop {
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

pub(super) fn plan<M>(
    mut context: PlanningContext<'_, M>,
    table_scans: &HashSet<String>,
) -> Result<QueryPlan>
where
    M: OrbitQueryModel + ?Sized,
{
    let input = context.input;
    let model = context.model;
    let hops = build_hops(input, model);
    let mut nodes = build_nodes(input, model);
    let denormalized = super::denormalized_facts(input, model);
    let placement = place_conditions(input, model, &hops, &nodes, &denormalized)?;
    bind_scan_conditions(input, model, &hops, &denormalized, &placement, &mut nodes)?;
    assign_node_access(
        input,
        model,
        &hops,
        &denormalized,
        &placement,
        table_scans,
        &mut nodes,
    );
    if input.query_type == QueryType::Aggregation {
        let groups: HashSet<&str> = node_group_ids(&input.aggregation.group_by).collect();
        for node in nodes.values_mut() {
            node.emit_select = groups.contains(node.alias.as_str());
        }
    }

    context.hops = hops;
    context.nodes = nodes;
    context.denormalized = denormalized;

    let mut execution = RelationBuilder::new(&context).build()?;
    let conditions = bind_join_conditions(
        input,
        model,
        &context.hops,
        &context.denormalized,
        &execution.bindings,
        &placement,
    )?;
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

    Ok(if input.query_type == QueryType::Aggregation {
        let result = context.aggregation(&execution);
        QueryPlan::Aggregation(context.finish(Aggregation { execution, result }))
    } else {
        QueryPlan::Traversal(context.finish(Traversal { execution }))
    })
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

fn build_hops<M>(input: &Input, model: &M) -> Vec<Hop>
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
            let source_entity = model.graph().entity_id(endpoint_entities.0);
            let target_entity = model.graph().entity_id(endpoint_entities.1);
            let foreign_key = if relationship.hops.max == 1
                && relationship.filters.is_empty()
                && !input.references(&PredicateTarget::Relationship(input_index))
                && relationship.direction != Direction::Both
            {
                match relationship.direction {
                    Direction::Outgoing => {
                        model.foreign_key(&relationship.types, from_entity, to_entity)
                    }
                    Direction::Incoming => {
                        model.foreign_key(&relationship.types, to_entity, from_entity)
                    }
                    Direction::Both => None,
                }
                .and_then(|foreign_key| {
                    let column = model.property_column(foreign_key.property)?.to_string();
                    let referenced_column = model
                        .property_column(foreign_key.referenced_key)?
                        .to_string();
                    let holder_is_from = matches!(
                        (relationship.direction, foreign_key.holder),
                        (Direction::Outgoing, query_data_model::Endpoint::Source)
                            | (Direction::Incoming, query_data_model::Endpoint::Target)
                    );
                    let (holder, referenced) = if holder_is_from {
                        (relationship.from.clone(), relationship.to.clone())
                    } else {
                        (relationship.to.clone(), relationship.from.clone())
                    };
                    Some(ForeignKeyRelation {
                        holder,
                        column,
                        referenced,
                        referenced_column,
                    })
                })
            } else {
                None
            };
            let scope_preserving = !relationship.types.is_empty()
                && relationship.types.iter().all(|kind| {
                    model
                        .variant_scope(kind, from_entity, to_entity)
                        .is_some_and(ontology::EdgeVariantScope::is_scope_preserving)
                        || model
                            .variant_scope(kind, to_entity, from_entity)
                            .is_some_and(ontology::EdgeVariantScope::is_scope_preserving)
                });
            Hop {
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
                source: foreign_key.map_or(RelationSource::Stored, RelationSource::ForeignKey),
                filters: ordered_filters(
                    &relationship.filters,
                    FilterOwner::Tables(&edge_tables),
                    model,
                ),
                scope_preserving,
            }
        })
        .collect()
}

fn assign_node_access(
    input: &Input,
    model: &(impl OrbitQueryModel + ?Sized),
    hops: &[Hop],
    denormalized: &HashMap<DenormalizedKey, DenormalizedProperty>,
    placement: &ConditionPlacement,
    table_scans: &HashSet<String>,
    nodes: &mut HashMap<String, NodePlan>,
) {
    let holder_nodes: HashSet<&str> = hops
        .iter()
        .filter_map(|hop| match &hop.source {
            RelationSource::ForeignKey(foreign_key) => Some(foreign_key.holder.as_str()),
            RelationSource::Stored => None,
        })
        .collect();
    let fk_bridges: HashSet<String> = nodes
        .keys()
        .filter(|alias| {
            hops.iter()
                .filter(|hop| {
                    matches!(hop.source, RelationSource::ForeignKey(_))
                        && (hop.from_node == ***alias || hop.to_node == ***alias)
                })
                .count()
                > 1
        })
        .cloned()
        .collect();
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
        let requires_projection = input.query_type == QueryType::Traversal
            && node
                .columns
                .as_ref()
                .is_some_and(|columns| match columns {
                    ColumnSelection::All => true,
                    ColumnSelection::List(columns) => {
                        columns.iter().any(|column| column != DEFAULT_PRIMARY_KEY)
                    }
                })
            && hops.iter().any(|hop| {
                matches!(hop.source, RelationSource::ForeignKey(_))
                    && (hop.from_node == node.alias || hop.to_node == node.alias)
            });
        let cross_scope_aggregate = input.query_type == QueryType::Aggregation
            && hops.iter().any(|hop| {
                !hop.scope_preserving
                    && matches!(hop.source, RelationSource::ForeignKey(_))
                    && (hop.from_node == node.alias || hop.to_node == node.alias)
                    && node.has_traversal_path
            });
        let local_predicate = placement.joined_nodes.contains(&node.alias);
        node.access = if input.relationships.is_empty()
            || !model.supports_deferred_node_loading()
            || holder_nodes.contains(node.alias.as_str())
            || fk_bridges.contains(node.alias.as_str())
            || group_target
            || metric_target
            || ordered
            || requires_projection
            || local_predicate
            || table_scans.contains(&node.alias)
            || cross_scope_aggregate
        {
            NodeAccess::Join
        } else if node_has_uncovered_filter(node, hops, denormalized, model) {
            if input.query_type == QueryType::Traversal {
                NodeAccess::Join
            } else {
                NodeAccess::SemiJoin
            }
        } else {
            NodeAccess::Identity
        };
    }
}

struct RelationBuilder<'a, M: OrbitQueryModel + ?Sized> {
    facts: &'a PlanningContext<'a, M>,
    source: Option<PhysicalSource>,
    outputs: Vec<Projection>,
    bindings: Vec<BindingSource>,
    tagged: HashSet<(String, String, usize)>,
}

impl<'a, M: OrbitQueryModel + ?Sized> RelationBuilder<'a, M> {
    fn new(facts: &'a PlanningContext<'a, M>) -> Self {
        Self {
            facts,
            source: None,
            outputs: Vec::new(),
            bindings: Vec::new(),
            tagged: HashSet::new(),
        }
    }

    fn build(mut self) -> Result<ExecutionPlan> {
        if self.facts.hops.is_empty() {
            return self.facts.single_node();
        }
        for (index, hop) in self.facts.hops.iter().enumerate() {
            match &hop.source {
                RelationSource::Stored => self.add_stored(index, hop)?,
                RelationSource::ForeignKey(foreign_key) => {
                    self.add_foreign_key(index, hop, foreign_key)?
                }
            }
        }
        self.attach_nodes()?;
        Ok(ExecutionPlan {
            source: self
                .source
                .ok_or_else(|| QueryError::Lowering("traversal has no physical source".into()))?,
            outputs: self.outputs,
            bindings: self.bindings,
        })
    }

    fn attach_nodes(&mut self) -> Result<()> {
        let aliases: Vec<_> = self.bindings.iter().map(|binding| binding.node.clone()).collect();
        for alias in aliases {
            match self.facts.nodes[&alias].access {
                NodeAccess::Identity => {}
                NodeAccess::SemiJoin => {
                    let binding = self.binding(&alias).cloned().unwrap();
                    self.apply_node_filter(&alias, &binding.alias, &binding.column)?;
                }
                NodeAccess::Join => self.ensure_node(&alias)?,
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

    fn add_stored(&mut self, index: usize, hop: &Hop) -> Result<()> {
        let alias = &hop.alias;
        let mut predicates = edge_predicates(self.facts, alias, hop, false);
        predicates.extend(
            hop.filters
                .iter()
                .map(|(property, filter)| property_filter(alias, property, filter)),
        );
        push_denorm_tags(self.facts, &mut predicates, hop, alias, &mut self.tagged);
        predicates.extend(node_id_predicates(self.facts, alias, hop));

        let mut relation = if hop.max_hops > 1 {
            multi_hop(self.facts, hop, alias).filter(predicates)
        } else if self.facts.aggregate() && self.facts.hops.len() == 1 {
            if let Some(table) = hop.single_edge_table() {
                PhysicalSource::Latest {
                    sort_key: self.facts.latest_row_key(table)?.to_vec(),
                    alias: alias.clone(),
                    aggregate_condition: predicates,
                    input: Box::new(PhysicalSource::edge(hop, alias, false)),
                }
            } else {
                PhysicalSource::edge(hop, alias, true).filter(predicates)
            }
        } else {
            PhysicalSource::edge(hop, alias, true).filter(predicates)
        };
        if self.facts.hops.len() > 1 && hop.max_hops == 1 {
            relation = PhysicalSource::Scope {
                alias: alias.clone(),
                input: Box::new(relation),
            };
        }
        let (start, end) = hop.direction.edge_columns();
        let endpoints = [(&hop.from_node, start), (&hop.to_node, end)];
        if let Some(source) = self.source.take() {
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
                left: Box::new(source),
                right: Box::new(relation),
            });
        } else {
            self.source = Some(relation);
        }
        for (node, column) in endpoints {
            if self.binding(node).is_none() {
                self.set_binding(node, alias, column, false);
            }
        }
        self.push_edge_outputs(index, hop, alias);
        Ok(())
    }

    fn add_foreign_key(
        &mut self,
        index: usize,
        hop: &Hop,
        foreign_key: &ForeignKeyRelation,
    ) -> Result<()> {
        let holder_bound = self.binding(&foreign_key.holder).is_some();
        let referenced_bound = self.binding(&foreign_key.referenced).is_some();
        if !holder_bound && referenced_bound {
            self.join_holder(foreign_key)?;
        } else {
            self.ensure_node(&foreign_key.holder)?;
        }

        let holder = self
            .binding(&foreign_key.holder)
            .cloned()
            .ok_or_else(|| QueryError::Lowering("FK holder has no binding".into()))?;
        if holder_bound
            && referenced_bound
            && let Some(referenced) = self.binding(&foreign_key.referenced).cloned()
        {
            if holder.alias != referenced.alias {
                self.source = self.source.take().map(|source| {
                    source.filter(vec![Predicate::ColumnEq {
                        left: Column::new(&holder.alias, &foreign_key.column),
                        right: Column::new(&referenced.alias, &referenced.column),
                    }])
                });
            }
        } else if !referenced_bound {
            self.apply_node_filter(&foreign_key.referenced, &holder.alias, &foreign_key.column)?;
            self.set_binding(
                &foreign_key.referenced,
                &holder.alias,
                &foreign_key.column,
                false,
            );
        }
        if self.facts.aggregate()
            || self.facts.nodes[&foreign_key.referenced].access != NodeAccess::Identity
        {
            self.join_referenced(foreign_key)?;
        }
        if !self.facts.aggregate() {
            let from = self
                .binding(&hop.from_node)
                .ok_or_else(|| QueryError::Lowering("FK source has no binding".into()))?;
            let to = self
                .binding(&hop.to_node)
                .ok_or_else(|| QueryError::Lowering("FK target has no binding".into()))?;
            self.outputs.extend(edge_outputs(
                hop,
                index,
                &self.facts.nodes,
                Column::new(&from.alias, &from.column),
                Column::new(&to.alias, &to.column),
            ));
        }
        Ok(())
    }

    fn join_holder(&mut self, foreign_key: &ForeignKeyRelation) -> Result<()> {
        let referenced = self
            .binding(&foreign_key.referenced)
            .cloned()
            .ok_or_else(|| QueryError::Lowering("referenced FK node has no binding".into()))?;
        let holder = &self.facts.nodes[&foreign_key.holder];
        let scan = PhysicalPlan::node_scan(holder)?;
        let source = self
            .source
            .take()
            .ok_or_else(|| QueryError::Lowering("FK join has no source".into()))?;
        self.source = Some(source.inner_join(
            scan.source,
            (
                Column::new(&holder.alias, &foreign_key.column),
                Column::new(&referenced.alias, &referenced.column),
            ),
        ));
        self.outputs.extend(scan.outputs);
        self.set_binding(
            &foreign_key.holder,
            &holder.alias,
            DEFAULT_PRIMARY_KEY,
            true,
        );
        Ok(())
    }

    fn join_referenced(&mut self, foreign_key: &ForeignKeyRelation) -> Result<()> {
        if self
            .binding(&foreign_key.referenced)
            .is_some_and(|binding| binding.joined)
        {
            return Ok(());
        }
        let holder = self
            .binding(&foreign_key.holder)
            .cloned()
            .ok_or_else(|| QueryError::Lowering("FK holder has no binding".into()))?;
        let referenced = &self.facts.nodes[&foreign_key.referenced];
        let scan = PhysicalPlan::node_scan(referenced)?;
        let source = self
            .source
            .take()
            .ok_or_else(|| QueryError::Lowering("FK join has no source".into()))?;
        let endpoints = (
            Column::new(&holder.alias, &foreign_key.column),
            Column::new(&referenced.alias, &foreign_key.referenced_column),
        );
        self.source = Some(source.inner_join(scan.source, endpoints));
        self.outputs.extend(scan.outputs);
        self.set_binding(
            &foreign_key.referenced,
            &referenced.alias,
            &foreign_key.referenced_column,
            true,
        );
        Ok(())
    }

    fn ensure_node(&mut self, alias: &str) -> Result<()> {
        if self.binding(alias).is_some_and(|binding| binding.joined) {
            return Ok(());
        }
        let node = &self.facts.nodes[alias];
        let scan = PhysicalPlan::node_scan(node)?;
        self.source = Some(match (self.source.take(), self.binding(alias).cloned()) {
            (Some(source), Some(binding)) => source.inner_join(
                scan.source,
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
            (None, _) => scan.source,
        });
        self.outputs.extend(scan.outputs);
        self.set_binding(alias, alias, DEFAULT_PRIMARY_KEY, true);
        Ok(())
    }

    fn apply_node_filter(&mut self, node: &str, alias: &str, column: &str) -> Result<()> {
        let plan = &self.facts.nodes[node];
        if plan.access != NodeAccess::SemiJoin {
            if !plan.node_ids.is_empty() {
                self.source = self
                    .source
                    .take()
                    .map(|source| source.filter(vec![id_list(alias, column, &plan.node_ids)]));
            }
            if let Some(range) = &plan.id_range {
                self.source = self.source.take().map(|source| {
                    source.filter(vec![Predicate::IdRange {
                        column: Column::new(alias, column),
                        start: range.start,
                        end: range.end,
                    }])
                });
            }
            return Ok(());
        }
        let keys = PhysicalPlan::exact_keys(plan, DEFAULT_PRIMARY_KEY)?;
        self.source = self.source.take().map(|source| PhysicalSource::KeyFilter {
            value: Column::new(alias, column),
            keys: Box::new(keys),
            input: Box::new(source),
        });
        Ok(())
    }

    fn push_edge_outputs(&mut self, index: usize, hop: &Hop, alias: &str) {
        if self.facts.aggregate() {
            return;
        }
        let prefix = hop.column_prefix(index);
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
        if hop.max_hops > 1 {
            self.outputs.push(Projection::new(
                OutputValue::Column(Column::new(alias, PATH_NODES_COLUMN)),
                format!("{prefix}{PATH_NODES_COLUMN}"),
            ));
        }
    }
}

fn edge_outputs(
    hop: &Hop,
    index: usize,
    nodes: &HashMap<String, NodePlan>,
    from_id: Column,
    to_id: Column,
) -> [Projection; 5] {
    let (source, source_id, target, target_id) = match hop.direction {
        Direction::Incoming => (&hop.to_node, to_id, &hop.from_node, from_id),
        Direction::Outgoing | Direction::Both => (&hop.from_node, from_id, &hop.to_node, to_id),
    };
    [
        (
            EDGE_TYPE_SUFFIX,
            OutputValue::Text(hop.rel_types.first().cloned().unwrap_or_default()),
        ),
        (EDGE_SRC_SUFFIX, OutputValue::Column(source_id)),
        (
            EDGE_SRC_TYPE_SUFFIX,
            OutputValue::Text(
                nodes
                    .get(source)
                    .and_then(|node| node.entity.clone())
                    .unwrap_or_default(),
            ),
        ),
        (EDGE_DST_SUFFIX, OutputValue::Column(target_id)),
        (
            EDGE_DST_TYPE_SUFFIX,
            OutputValue::Text(
                nodes
                    .get(target)
                    .and_then(|node| node.entity.clone())
                    .unwrap_or_default(),
            ),
        ),
    ]
    .map(|(suffix, value)| Projection::new(value, format!("e{index}_{suffix}")))
}

fn edge_predicates<M: OrbitQueryModel + ?Sized>(
    facts: &PlanningContext<'_, M>,
    alias: &str,
    hop: &Hop,
    skip_deleted: bool,
) -> Vec<Predicate> {
    let mut predicates = Vec::new();
    let (start, end) = hop.direction.edge_columns();
    if let Some(filter) = relationship_kinds(alias, &hop.rel_types) {
        predicates.push(filter);
    }
    for (node_alias, column) in [(&hop.from_node, start), (&hop.to_node, end)] {
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
    if let Some(columns) = hop
        .single_edge_table()
        .and_then(|table| facts.model.table_columns(table))
    {
        let mut seen = HashSet::new();
        for node_alias in [&hop.from_node, &hop.to_node] {
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
    hop: &Hop,
) -> Vec<Predicate> {
    let mut predicates = Vec::new();
    for node in [&hop.from_node, &hop.to_node] {
        let column = hop.endpoint_column(node).unwrap();
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
    hop: &Hop,
    alias: &str,
    tagged: &mut HashSet<(String, String, usize)>,
) {
    if crate::passes::normalize::is_wildcard(&hop.rel_types) {
        return;
    }
    for node_alias in [&hop.from_node, &hop.to_node] {
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
                std::slice::from_ref(hop),
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
    hops: &[Hop],
    denormalized: &HashMap<DenormalizedKey, DenormalizedProperty>,
    model: &(impl OrbitQueryModel + ?Sized),
) -> bool {
    node.filters.iter().any(|(property, filter)| {
        !denorm_hop(filter, &node.alias, hops, denormalized)
            .is_some_and(|(position, _)| matches!(hops[position].source, RelationSource::Stored))
            && !hops.iter().any(|hop| {
                matches!(hop.source, RelationSource::Stored)
                    && hop.endpoint_column(&node.alias).is_some()
                    && hop
                        .single_edge_table()
                        .and_then(|table| model.table_columns(table))
                        .is_some_and(|columns| columns.contains(property))
            })
    })
}

fn denorm_hop<'a>(
    filter: &BoundFilter,
    alias: &str,
    hops: &[Hop],
    denormalized: &'a HashMap<DenormalizedKey, DenormalizedProperty>,
) -> Option<(usize, &'a DenormalizedProperty)> {
    hops.iter().enumerate().find_map(|(position, hop)| {
        if crate::passes::normalize::is_wildcard(&hop.rel_types) {
            return None;
        }
        let column = hop.endpoint_column(alias)?;
        let direction = if column == SOURCE_ID_COLUMN {
            DenormalizedDirection::Source
        } else {
            DenormalizedDirection::Target
        };
        let source = hop.source_entity?;
        let target = hop.target_entity?;
        super::helpers::lowerable_denorm(filter, direction, denormalized)
            .filter(|facts| {
                hop.relationships
                    .iter()
                    .all(|relationship| facts.variants.contains(&(*relationship, source, target)))
            })
            .map(|facts| (position, facts))
    })
}

fn multi_hop<M: OrbitQueryModel + ?Sized>(
    facts: &PlanningContext<'_, M>,
    hop: &Hop,
    alias: &str,
) -> PhysicalSource {
    let edge_columns = hop
        .single_edge_table()
        .and_then(|table| facts.model.table_columns(table));
    let mut required_columns = Vec::new();
    for node in [&hop.from_node, &hop.to_node] {
        for (property, _) in &facts.nodes[node].filters {
            if edge_columns.is_some_and(|columns| columns.contains(property))
                && !EDGE_RESERVED_COLUMNS.contains(&property.as_str())
                && !required_columns.contains(property)
            {
                required_columns.push(property.clone());
            }
        }
    }
    let arms = (hop.min_hops.max(1)..=hop.max_hops)
        .map(|depth| depth_arm(hop, depth, &required_columns))
        .collect();
    let (from_kind, to_kind) = match hop.direction {
        Direction::Outgoing | Direction::Both => (SOURCE_KIND_COLUMN, TARGET_KIND_COLUMN),
        Direction::Incoming => (TARGET_KIND_COLUMN, SOURCE_KIND_COLUMN),
    };
    let mut predicates = Vec::new();
    for (node, column) in [(&hop.from_node, from_kind), (&hop.to_node, to_kind)] {
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
            relationship: hop.input_index,
        }),
    }
}

fn depth_arm(hop: &Hop, depth: u32, required_columns: &[String]) -> PhysicalPlan {
    let (start, end) = hop.direction.edge_columns();
    let end_kind = match hop.direction {
        Direction::Outgoing | Direction::Both => TARGET_KIND_COLUMN,
        Direction::Incoming => SOURCE_KIND_COLUMN,
    };
    let scan = |alias: &str| PhysicalSource::edge(hop, alias, true);
    let mut root_predicates = relationship_kinds("e1", &hop.rel_types)
        .into_iter()
        .collect();
    let mut source = scan("e1");
    for index in 2..=depth {
        let previous = format!("e{}", index - 1);
        let current = format!("e{index}");
        let mut predicates: Vec<_> = relationship_kinds(&current, &hop.rel_types)
            .into_iter()
            .collect();
        predicates.push(Predicate::ColumnEq {
            left: Column::new(&previous, end_kind),
            right: Column::new(
                &current,
                match hop.direction {
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
    let (source_alias, target_alias, kind_alias) = match hop.direction {
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
    PhysicalPlan {
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
    hops: &[Hop],
    nodes: &HashMap<String, NodePlan>,
    denormalized: &HashMap<DenormalizedKey, DenormalizedProperty>,
) -> Result<ConditionPlacement> {
    let mut placement = ConditionPlacement::default();
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
                && node_has_uncovered_filter(&nodes[alias], hops, denormalized, model)
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
    hops: &[Hop],
    denormalized: &HashMap<DenormalizedKey, DenormalizedProperty>,
    placement: &ConditionPlacement,
    nodes: &mut HashMap<String, NodePlan>,
) -> Result<()> {
    for (root, scan) in input.predicates.iter().zip(&placement.scans) {
        if let Some(alias) = scan {
            let predicate = bind_tree(root, input, model, hops, denormalized, None)?;
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

fn bind_join_conditions(
    input: &Input,
    model: &(impl OrbitQueryModel + ?Sized),
    hops: &[Hop],
    denormalized: &HashMap<DenormalizedKey, DenormalizedProperty>,
    bindings: &[BindingSource],
    placement: &ConditionPlacement,
) -> Result<Vec<Predicate>> {
    input
        .predicates
        .iter()
        .zip(&placement.scans)
        .filter(|(_, scan)| scan.is_none())
        .map(|(root, _)| bind_tree(root, input, model, hops, denormalized, Some(bindings)))
        .collect()
}

fn bind_tree(
    root: &BooleanExpression<Condition>,
    input: &Input,
    model: &(impl OrbitQueryModel + ?Sized),
    hops: &[Hop],
    denormalized: &HashMap<DenormalizedKey, DenormalizedProperty>,
    bindings: Option<&[BindingSource]>,
) -> Result<Predicate> {
    let expression = root.clone().try_map(&mut |condition| {
        bind_condition(condition, input, model, hops, denormalized, bindings).map(Box::new)
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
    hops: &[Hop],
    denormalized: &HashMap<DenormalizedKey, DenormalizedProperty>,
    bindings: Option<&[BindingSource]>,
) -> Result<Predicate> {
    let node = matches!(condition.target, PredicateTarget::Node(_));
    let (column, mut bound) = bind_leaf(condition, input, model, hops)?;
    if node
        && bindings.is_some()
        && let Some(tag) = edge_tag(&column.source, &bound, hops, denormalized)
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

fn edge_tag(
    alias: &str,
    bound: &BoundFilter,
    hops: &[Hop],
    denormalized: &HashMap<DenormalizedKey, DenormalizedProperty>,
) -> Option<Predicate> {
    let (position, facts) = denorm_hop(bound, alias, hops, denormalized)?;
    let hop = &hops[position];
    if bound.filter.rhs_column.is_some()
        || facts.nullable
        || hop.max_hops != 1
        || matches!(hop.source, RelationSource::ForeignKey(_))
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
