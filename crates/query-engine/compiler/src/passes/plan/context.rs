use std::collections::HashMap;

use query_data_model::QueryDataModel;

use crate::error::{QueryError, Result};
use crate::input::{Input, QueryType};

use super::physical::{BindingSource, ExecutionPlan, PhysicalPlan};
use super::{DenormalizedKey, DenormalizedProperty, Hop, NodePlan};

pub(super) struct PlanningContext<'a, M: QueryDataModel + ?Sized> {
    pub input: &'a Input,
    pub model: &'a M,
    pub hops: &'a [Hop],
    pub nodes: &'a HashMap<String, NodePlan>,
    pub denormalized: &'a HashMap<DenormalizedKey, DenormalizedProperty>,
}

impl<M: QueryDataModel + ?Sized> PlanningContext<'_, M> {
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
            edge_aliases: vec![],
            edge_if_predicates: None,
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
}
