mod conditions;
mod coverage;
mod nodes;
mod relations;
mod steps;

use std::collections::{HashMap, HashSet};

use query_data_model::OrbitQueryModel;

use super::filters::BoundFilter;
use super::graph::{ExecutionPlan, ReadOwner, RelationGraph, RelationId};
use super::predicates::{Column, Predicate, kind_column, node_predicates};
use super::{DenormalizedKey, DenormalizedProperty, Plan, sort_key};
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
    pub narrowed: bool,
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
    pub fk: Option<Fk>,
}

/// A relationship stored as a column on one of its endpoint nodes instead of an edge row.
#[derive(Clone)]
pub struct Fk {
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

    /// The endpoints as the edge row stores them: the node in `source_id`, then the node in `target_id`.
    fn stored_endpoints(&self) -> (&str, &str) {
        self.direction
            .stored_order(self.from_node.as_str(), self.to_node.as_str())
    }

    fn other_end(&self, node: &str) -> &str {
        if self.from_node == node {
            &self.to_node
        } else {
            &self.from_node
        }
    }
}

pub(super) struct Pattern<'a, M: OrbitQueryModel + ?Sized> {
    pub input: &'a Input,
    pub model: &'a M,
    pub nodes: HashMap<String, NodePlan>,
    pub relations: Vec<EdgeRelation>,
    pub denormalized: HashMap<DenormalizedKey, DenormalizedProperty>,
}

impl<M: OrbitQueryModel + ?Sized> Pattern<'_, M> {
    fn aggregate(&self) -> bool {
        self.input.query_type == QueryType::Aggregation
    }

    fn node_read(&self, graph: &mut RelationGraph, node: &NodePlan) -> Result<RelationId> {
        let table = node
            .table
            .as_ref()
            .ok_or_else(|| QueryError::Lowering(format!("node '{}' has no table", node.alias)))?;
        Ok(graph.current_rows(
            table,
            &node.alias,
            ReadOwner::Node(node.alias.clone()),
            sort_key(self.model, table)?,
            node_predicates(node),
            self.model.supports_selective_latest_rows(),
        ))
    }

    fn entity_kind(&self, edge_alias: &str, node: &str, id_column: &str) -> Option<Predicate> {
        Some(Predicate::EntityKind {
            column: Column::new(edge_alias, kind_column(id_column)),
            entity: self.nodes[node].entity.clone()?,
        })
    }
}

pub(super) struct Planned<'a, M: OrbitQueryModel + ?Sized> {
    pattern: Pattern<'a, M>,
    execution: ExecutionPlan,
    node_edge_mappings: HashMap<String, (String, String)>,
}

impl<'a, M: OrbitQueryModel + ?Sized> Planned<'a, M> {
    pub fn finish<T>(self, operation: impl FnOnce(&Pattern<'a, M>, ExecutionPlan) -> T) -> Plan<T> {
        let operation = operation(&self.pattern, self.execution);
        Plan {
            nodes: self.pattern.nodes,
            relations: self.pattern.relations,
            node_edge_mappings: self.node_edge_mappings,
            denormalized: self.pattern.denormalized,
            operation,
        }
    }
}

pub(super) fn plan<'a, M>(input: &'a Input, model: &'a M) -> Result<Planned<'a, M>>
where
    M: OrbitQueryModel + ?Sized,
{
    let nodes = nodes::build_nodes(input, model);
    let mut pattern = Pattern {
        input,
        model,
        relations: relations::build_relations(input, model, &nodes),
        nodes,
        denormalized: super::denormalized_facts(input, model),
    };
    let placement = conditions::place_conditions(&pattern)?;
    conditions::bind_scan_conditions(&mut pattern, &placement)?;
    nodes::assign_node_access(&mut pattern, &placement.joined_nodes);
    relations::linearize_relations(&mut pattern.relations, &pattern.nodes)?;
    if pattern.aggregate() {
        let groups: HashSet<&str> = node_group_ids(&input.aggregation.group_by).collect();
        for node in pattern.nodes.values_mut() {
            node.emit_select = groups.contains(node.alias.as_str());
        }
    }
    let mut execution = steps::RelationBuilder::new(&pattern).build()?;
    let conditions = input
        .predicates
        .iter()
        .zip(&placement.scans)
        .filter(|(_, scan)| scan.is_none())
        .map(|(root, _)| conditions::bind_tree(&pattern, root, Some(&execution.bindings)))
        .collect::<Result<_>>()?;
    execution.root = execution.graph.filter(execution.root, conditions);
    let node_edge_mappings = execution
        .bindings
        .iter()
        .map(|binding| {
            (
                binding.node.clone(),
                (binding.alias.clone(), binding.column.clone()),
            )
        })
        .collect();
    Ok(Planned {
        pattern,
        execution,
        node_edge_mappings,
    })
}
