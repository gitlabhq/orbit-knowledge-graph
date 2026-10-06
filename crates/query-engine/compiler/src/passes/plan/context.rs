use std::collections::HashMap;

use query_data_model::QueryDataModel;

use crate::error::{QueryError, Result};
use crate::input::{Input, InputNode, QueryType};

use super::physical::{BindingSource, ExecutionPlan, PhysicalPlan};
use super::{DenormalizedKey, DenormalizedProperty, Hop, NodePlan, Plan};

pub(super) struct PlanningContext<'a, M: QueryDataModel + ?Sized> {
    pub bindings: query_data_model::bindings::QueryBindings,
    pub input: &'a Input,
    pub model: &'a M,
    pub hops: Vec<Hop>,
    pub nodes: HashMap<String, NodePlan>,
    pub denormalized: HashMap<DenormalizedKey, DenormalizedProperty>,
    pub node_edge_mappings: HashMap<String, (String, String)>,
}

impl<M: QueryDataModel + ?Sized> PlanningContext<'_, M> {
    pub fn finish<T>(self, operation: T) -> Plan<T> {
        Plan {
            bindings: self.bindings,
            nodes: self.nodes,
            hops: self.hops,
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

    pub fn single_node(&mut self) -> Result<ExecutionPlan> {
        let node = self
            .nodes
            .values()
            .next()
            .ok_or_else(|| QueryError::Lowering("no nodes in plan".into()))?;
        let root = PhysicalPlan::single_node(&mut self.bindings, self.model, node)?;
        Ok(ExecutionPlan {
            source: root.source,
            outputs: root.outputs,
            bindings: vec![BindingSource::table(&node.alias)],
            definitions: vec![],
        })
    }

    pub fn aggregate(&self) -> bool {
        self.input.query_type == QueryType::Aggregation
    }
}
