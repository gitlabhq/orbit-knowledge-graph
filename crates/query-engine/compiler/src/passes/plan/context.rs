use std::collections::HashMap;

use query_data_model::OrbitQueryModel;

use crate::error::{QueryError, Result};
use crate::input::{Input, InputNode, QueryType};

use super::physical::{BindingSource, ExecutionPlan, ReadOwner, RelationGraph, RelationId};
use super::requirements::node_predicates;
use super::{DenormalizedKey, DenormalizedProperty, EdgeRelation, NodePlan, Plan};

pub(super) struct PlanningContext<'a, M: OrbitQueryModel + ?Sized> {
    pub input: &'a Input,
    pub model: &'a M,
    pub relations: Vec<EdgeRelation>,
    pub nodes: HashMap<String, NodePlan>,
    pub denormalized: HashMap<DenormalizedKey, DenormalizedProperty>,
    pub node_edge_mappings: HashMap<String, (String, String)>,
}

impl<M: OrbitQueryModel + ?Sized> PlanningContext<'_, M> {
    pub fn finish<T>(self, operation: T) -> Plan<T> {
        Plan {
            nodes: self.nodes,
            relations: self.relations,
            denormalized: self.denormalized,
            node_edge_mappings: self.node_edge_mappings,
            operation,
        }
    }

    pub fn resolve_node(&self, node: &InputNode) -> Result<NodePlan> {
        NodePlan::from_input(node, self.model, false).ok_or_else(|| {
            QueryError::Lowering(format!("node '{}' has an unknown entity", node.id))
        })
    }

    pub fn single_node(&self) -> Result<ExecutionPlan> {
        let node = self
            .nodes
            .values()
            .next()
            .ok_or_else(|| QueryError::Lowering("no nodes in plan".into()))?;
        let mut graph = RelationGraph::default();
        let root = self.node_read(&mut graph, node)?;
        Ok(ExecutionPlan {
            graph,
            root,
            outputs: super::requirements::node_outputs(node),
            bindings: vec![BindingSource::table(&node.alias)],
        })
    }

    pub fn aggregate(&self) -> bool {
        self.input.query_type == QueryType::Aggregation
    }

    pub fn latest_row_key(&self, table: &str) -> Result<&[String]> {
        self.model
            .table_sort_key(table)
            .filter(|key| !key.is_empty())
            .ok_or_else(|| {
                QueryError::Lowering(format!(
                    "table '{table}' has no sort key for latest-row resolution"
                ))
            })
    }

    pub fn node_read(&self, graph: &mut RelationGraph, node: &NodePlan) -> Result<RelationId> {
        let table = node
            .table
            .as_ref()
            .ok_or_else(|| QueryError::Lowering(format!("node '{}' has no table", node.alias)))?;
        Ok(graph.current_rows(
            table,
            &node.alias,
            ReadOwner::Node(node.alias.clone()),
            self.latest_row_key(table)?,
            node_predicates(node),
            self.model.supports_selective_latest_rows(),
        ))
    }
}
