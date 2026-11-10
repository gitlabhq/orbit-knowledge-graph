//! The MATCH pattern of a traversal or aggregation: planned nodes, planned relations, and the
//! denormalized-column facts. `Pattern::new` runs the planning steps in order (nodes, relations,
//! condition placement, node access, relation order); `Pattern::execution` assembles the relation graph.

mod conditions;
mod coverage;
mod nodes;
mod relations;
mod steps;

pub use steps::Matched;

use crate::passes::codegen::RelationalModel;
use crate::passes::plan::build::stored;
use std::collections::{HashMap, HashSet};

use query_data_model::implementations::relational::RelationalBackend;
use query_data_model::implementations::relational::plan::RelationalBuilder;

use super::filters::{BoundFilter, NullAsEpochColumns};
use super::predicates::{Column, Predicate, kind_column};
use super::rows::{ReadScope, Reads, Rel};
use super::{DenormalizedKey, DenormalizedProperty, sort_key};
use crate::error::{QueryError, Result};
use crate::logical::Kind;
use crate::logical::Query;
use crate::logical::facts::{anchors, facts};
use crate::passes::plan::boolean::Boolean;
use query_data_model::Direction;
use query_data_model::plan::logical::Expr;

#[derive(Clone)]
pub struct NodePlan {
    pub alias: String,
    pub entity: Option<String>,
    pub table: Option<String>,
    pub joined: bool,
    pub filters: Vec<(String, BoundFilter)>,
    pub predicates: Vec<Predicate>,
    pub node_ids: Option<Vec<i64>>,
    pub id_range: Option<(i64, i64)>,
    pub has_traversal_path: bool,
    pub read_scope: ReadScope,
    pub redaction_id_column: String,
    pub columns: Vec<String>,
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

pub(crate) struct Pattern<'a, M: RelationalModel> {
    pub query: Query<'a>,
    conditions: Vec<Boolean<conditions::Condition>>,
    pub model: &'a M,
    pub nodes: HashMap<String, NodePlan>,
    pub relations: Vec<EdgeRelation>,
    pub denormalized: HashMap<DenormalizedKey, DenormalizedProperty>,
    scans: Vec<Option<String>>,
}

impl<M: RelationalModel> Pattern<'_, M> {
    fn aggregate(&self) -> bool {
        self.query.kind() == Kind::Aggregation
    }

    fn node_read<B: RelationalBackend>(
        &self,
        reads: &mut Reads<'_, '_, B>,
        node: &NodePlan,
    ) -> Result<Rel> {
        let table = node
            .table
            .as_ref()
            .ok_or_else(|| QueryError::Lowering(format!("node '{}' has no table", node.alias)))?;
        let bound = reads.node_bound(&node.alias);
        reads.current_rows(
            table,
            &node.alias,
            bound,
            sort_key(self.model, table)?,
            self.node_conditions(node),
            node.read_scope,
        )
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

impl<'a, M: RelationalModel> Pattern<'a, M> {
    pub(crate) fn new(query: Query<'a>, model: &'a M) -> Result<Self> {
        let facts = facts(query);
        let nodes = nodes::build_nodes(query, &anchors(query, &facts), model);
        let mut pattern = Self {
            query,
            conditions: conditions::conditions(query)?,
            model,
            relations: relations::build_relations(query, &facts, model, &nodes),
            nodes,
            denormalized: super::filters::denormalized_facts(query, model),
            scans: Vec::new(),
        };
        let placement = conditions::place_conditions(&pattern)?;
        conditions::bind_scan_conditions(&mut pattern, &placement)?;
        nodes::assign_node_access(&mut pattern, &placement.joined_nodes);
        relations::linearize_relations(&mut pattern.relations, &pattern.nodes)?;
        if pattern.aggregate() {
            let (groups, _) = query.aggregate().unwrap_or_default();
            let groups: HashSet<&str> = groups
                .iter()
                .filter_map(|group| match group {
                    Expr::Node(binding) => Some(query.name(*binding)),
                    _ => None,
                })
                .collect();
            for node in pattern.nodes.values_mut() {
                node.emit_select = groups.contains(node.alias.as_str());
            }
        }
        pattern.scans = placement.scans;
        Ok(pattern)
    }

    /// The pattern's joined rows, with the conditions that span nodes applied on top.
    pub(crate) fn execution<B: RelationalBackend>(
        &self,
        q: &mut RelationalBuilder<'_, B>,
        tp_bounds: Option<&crate::tp_bounds::TpBounds>,
    ) -> Result<Matched> {
        let reads = Reads::new(q, tp_bounds, !stored::<B>());
        steps::RelationBuilder::new(self, reads).build(|bindings| {
            self.conditions
                .iter()
                .zip(&self.scans)
                .filter(|(_, scan)| scan.is_none())
                .map(|(root, _)| conditions::bind_tree(self, root, Some(bindings)))
                .collect()
        })
    }
}
