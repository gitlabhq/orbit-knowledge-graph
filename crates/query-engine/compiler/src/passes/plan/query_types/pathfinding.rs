//! Path finding: start and end nodes, the forward and backward depth split, and the first-hop kind
//! filters.

use query_data_model::OrbitQueryModel;

use super::EdgeTableConfig;
use crate::error::{QueryError, Result};
use crate::input::*;
use crate::passes::plan::{NodePlan, Plan};

pub struct PathFinding {
    pub start: String,
    pub end: String,
    pub max_depth: u32,
    pub forward_depth: u32,
    pub backward_depth: u32,
    pub edge: EdgeTableConfig,
    pub scoped_by_tp: bool,
}

pub(crate) fn plan<M: OrbitQueryModel + ?Sized>(
    input: &Input,
    model: &M,
    owner_gates: &[String],
) -> Result<Plan<PathFinding>> {
    let path = input
        .path
        .as_ref()
        .ok_or_else(|| QueryError::Lowering("path config missing".into()))?;
    let node = |alias: &str| {
        input
            .node(alias)
            .ok_or_else(|| QueryError::Lowering(format!("node '{alias}' not found")))
    };
    let start_node = node(&path.from)?;
    let end_node = node(&path.to)?;
    let start = NodePlan::resolve(start_node, model)?;
    let end = NodePlan::resolve(end_node, model)?;

    let max_depth = path.max_depth;
    let mut plan = Plan::new(
        PathFinding {
            start: start.alias.clone(),
            end: end.alias.clone(),
            max_depth,
            forward_depth: max_depth / 2 + max_depth % 2,
            backward_depth: if max_depth >= 2 { max_depth / 2 } else { 0 },
            edge: EdgeTableConfig::from_model(model, &path.rel_types),
            scoped_by_tp: start.has_traversal_path && end.has_traversal_path,
        },
        owner_gates,
    );
    plan.nodes.insert(start.alias.clone(), start);
    plan.nodes.insert(end.alias.clone(), end);
    Ok(plan)
}
