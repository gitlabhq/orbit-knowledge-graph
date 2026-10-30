//! The MATCH pattern of a traversal or aggregation: planned nodes, planned relations, and the
//! denormalized-column facts. `Pattern::new` runs the planning steps in order (nodes, relations,
//! condition placement, node access, relation order); `Pattern::execution` assembles the relation graph.

mod conditions;
mod coverage;
mod nodes;
mod relations;
mod steps;

use std::collections::{HashMap, HashSet};

use query_data_model::OrbitQueryModel;

use super::filters::{BoundFilter, NullAsEpochColumns};
use super::graph::{ExecutionPlan, ReadOwner, ReadScope, RelationGraph, RelationId};
use super::predicates::{Column, Predicate, kind_column};
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
    pub owner_scoped: bool,
    pub read_scope: ReadScope,
    pub redaction_id_column: String,
    pub columns: Option<ColumnSelection>,
    pub emit_select: bool,
    pub null_as_epoch: NullAsEpochColumns,
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

pub(super) struct Pattern<'a, M: OrbitQueryModel + ?Sized> {
    pub input: &'a Input,
    pub model: &'a M,
    pub nodes: HashMap<String, NodePlan>,
    pub relations: Vec<EdgeRelation>,
    pub denormalized: HashMap<DenormalizedKey, DenormalizedProperty>,
    pub owner_gates: Vec<String>,
    scans: Vec<Option<String>>,
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
            self.node_conditions(node),
            node.read_scope,
        ))
    }

    fn node_conditions(&self, node: &NodePlan) -> Vec<Predicate> {
        let mut conditions = node.conditions();
        conditions.extend(self.shared_column_predicates(node));
        conditions
    }

    fn entity_kind(&self, edge_alias: &str, node: &str, id_column: &str) -> Option<Predicate> {
        Some(Predicate::EntityKind {
            column: Column::new(edge_alias, kind_column(id_column)),
            entity: self.nodes[node].entity.clone()?,
        })
    }
}

impl<'a, M: OrbitQueryModel + ?Sized> Pattern<'a, M> {
    pub(super) fn new(input: &'a Input, model: &'a M, owner_gates: &[String]) -> Result<Self> {
        let nodes = nodes::build_nodes(input, model);
        let mut pattern = Self {
            input,
            model,
            owner_gates: owner_gates.to_vec(),
            relations: relations::build_relations(input, model, &nodes),
            nodes,
            denormalized: super::filters::denormalized_facts(input, model),
            scans: Vec::new(),
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
        pattern.scans = placement.scans;
        Ok(pattern)
    }

    /// The relation graph for the pattern, with the conditions that span nodes applied at its root.
    pub(super) fn execution(&self) -> Result<ExecutionPlan> {
        let mut execution = steps::RelationBuilder::new(self).build()?;
        let conditions = self
            .input
            .predicates
            .iter()
            .zip(&self.scans)
            .filter(|(_, scan)| scan.is_none())
            .map(|(root, _)| conditions::bind_tree(self, root, Some(&execution.bindings)))
            .collect::<Result<_>>()?;
        execution.root = execution.graph.filter(execution.root, conditions);
        Ok(execution)
    }

    pub(super) fn into_plan<T>(
        self,
        execution: ExecutionPlan,
        operation: impl FnOnce(ExecutionPlan) -> T,
    ) -> Plan<T> {
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
        Plan {
            nodes: self.nodes,
            relations: self.relations,
            node_edge_mappings,
            denormalized: self.denormalized,
            owner_gates: self.owner_gates,
            operation: operation(execution),
        }
    }
}
