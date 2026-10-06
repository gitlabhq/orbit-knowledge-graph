use std::collections::HashMap;

use query_data_model::QueryDataModel;

use crate::error::{QueryError, Result};
use crate::input::{Input, InputNode, QueryType};

use super::physical::ExecutionPlan;
use super::{DenormalizedKey, DenormalizedProperty, Hop, NodePlan, Plan};

pub(super) struct PlanningContext<'a, M: QueryDataModel + ?Sized> {
    pub bindings: &'a mut query_data_model::bindings::QueryBindings,
    pub names: &'a mut crate::config::BindingNames,
    pub node_relations: HashMap<String, query_data_model::bindings::RelationId>,
    pub input: &'a Input,
    pub model: &'a M,
    pub hops: Vec<Hop>,
    pub nodes: HashMap<String, NodePlan>,
    pub denormalized: HashMap<DenormalizedKey, DenormalizedProperty>,
}

impl<M: QueryDataModel + ?Sized> PlanningContext<'_, M> {
    pub fn finish<T>(self, operation: T) -> Plan<T> {
        Plan {
            nodes: self.nodes,
            hops: self.hops,
            denormalized: self.denormalized,
            operation,
        }
    }

    pub fn resolve_node(&self, node: &InputNode) -> Result<NodePlan> {
        NodePlan::from_input(node, self.model, false).ok_or_else(|| {
            QueryError::Lowering(format!("node '{}' has an unknown entity", node.id))
        })
    }

    pub fn single_node(&mut self) -> Result<ExecutionPlan> {
        let alias = self
            .nodes
            .keys()
            .next()
            .cloned()
            .ok_or_else(|| QueryError::Lowering("no nodes in plan".into()))?;
        let root = self.node_plan(self.bindings.root(), &alias)?;
        let relation = root.source.relation();
        Ok(ExecutionPlan {
            source: root.source,
            outputs: root.outputs,
            bindings: HashMap::from([self.node_binding(
                &alias,
                self.column(relation, ontology::DEFAULT_PRIMARY_KEY)?,
                Some(relation),
            )?]),
            definitions: vec![],
        })
    }

    pub fn aggregate(&self) -> bool {
        self.input.query_type == QueryType::Aggregation
    }
}
