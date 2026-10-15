use std::collections::HashMap;

use query_data_model::OrbitQueryModel;

use crate::error::{QueryError, Result};
use crate::input::{Input, InputNode, QueryType};

use super::physical::{BindingSource, ExecutionPlan, PhysicalPlan};
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
        let root = PhysicalPlan::single_node(node)?;
        Ok(ExecutionPlan {
            source: root.into_source(),
            subplans: vec![],
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
}
