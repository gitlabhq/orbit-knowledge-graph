use std::collections::HashMap;

use query_data_model::OrbitQueryModel;

use crate::error::{QueryError, Result};
use crate::input::{Input, InputNode, QueryType};

use super::physical::{BindingSource, ExecutionPlan, PhysicalPlan};
use super::{
    Aggregation, DenormalizedKey, DenormalizedProperty, Hop, NodePlan, Plan, QueryPlan, Traversal,
};

pub(super) struct PlanningContext<'a, M: OrbitQueryModel + ?Sized> {
    pub input: &'a Input,
    pub model: &'a M,
    pub hops: Vec<Hop>,
    pub nodes: HashMap<String, NodePlan>,
    pub denormalized: HashMap<DenormalizedKey, DenormalizedProperty>,
    pub node_edge_mappings: HashMap<String, (String, String)>,
}

impl<M: OrbitQueryModel + ?Sized> PlanningContext<'_, M> {
    pub fn finish_execution(mut self, execution: ExecutionPlan) -> QueryPlan {
        for binding in &execution.bindings {
            self.node_edge_mappings
                .entry(binding.node.clone())
                .or_insert_with(|| (binding.alias.clone(), binding.column.clone()));
        }
        if self.aggregate() {
            let result = self.aggregation(&execution);
            QueryPlan::Aggregation(self.finish(Aggregation { execution, result }))
        } else {
            QueryPlan::Traversal(self.finish(Traversal { execution }))
        }
    }

    pub fn finish<T>(self, operation: T) -> Plan<T> {
        Plan {
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

    pub fn single_node(&self) -> Result<ExecutionPlan> {
        let node = self
            .nodes
            .values()
            .next()
            .ok_or_else(|| QueryError::Lowering("no nodes in plan".into()))?;
        let root = PhysicalPlan::single_node(node)?;
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

    pub fn node(&self, alias: &str) -> Result<&NodePlan> {
        self.nodes
            .get(alias)
            .ok_or_else(|| QueryError::Lowering(format!("node '{alias}' not found")))
    }

    pub fn node_sort_key(&self, node: &NodePlan) -> Result<&[String]> {
        let table = node
            .table
            .as_ref()
            .ok_or_else(|| QueryError::Lowering(format!("node '{}' has no table", node.alias)))?;
        self.model
            .table_sort_key(table)
            .ok_or_else(|| QueryError::Lowering(format!("no sort key for node table '{table}'")))
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
}

impl<'a, M: OrbitQueryModel + ?Sized> PlanningContext<'a, M> {
    pub(super) fn new(input: &'a Input, model: &'a M) -> Self {
        Self {
            input,
            model,
            hops: Vec::new(),
            nodes: HashMap::new(),
            denormalized: HashMap::new(),
            node_edge_mappings: HashMap::new(),
        }
    }
}
