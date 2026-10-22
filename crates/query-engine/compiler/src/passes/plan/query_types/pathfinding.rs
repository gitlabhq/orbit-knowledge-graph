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
    pub forward_first_hop_filter: Option<Vec<String>>,
    pub backward_first_hop_filter: Option<Vec<String>>,
    pub scoped_by_tp: bool,
    pub forward_first_hop_carries_path: bool,
    pub backward_first_hop_carries_path: bool,
}

pub(crate) fn plan<M: OrbitQueryModel + ?Sized>(
    input: &Input,
    model: &M,
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

    let endpoint_kinds = |entity: &str, source: bool| {
        if !crate::passes::normalize::is_wildcard(&path.rel_types) {
            return None;
        }
        let relationships = if source {
            model.graph().relationship_names(Some(entity), None)
        } else {
            model.graph().relationship_names(None, Some(entity))
        };
        (!crate::passes::normalize::is_wildcard(&relationships)).then_some(relationships)
    };
    let forward_first_hop_filter = start_node
        .entity
        .as_deref()
        .and_then(|entity| endpoint_kinds(entity, true));
    let backward_first_hop_filter = end_node
        .entity
        .as_deref()
        .and_then(|entity| endpoint_kinds(entity, false));

    let carries_path = |entity: Option<&str>, kinds: Option<&Vec<String>>, source: bool| {
        let Some(entity) = entity else {
            return false;
        };
        let kinds = kinds
            .or(Some(&path.rel_types))
            .filter(|kinds| !crate::passes::normalize::is_wildcard(kinds) && !kinds.is_empty());
        let Some(kinds) = kinds else {
            return false;
        };
        let graph = model.graph();
        let mut found = false;
        let carries = kinds.iter().all(|kind| {
            model.relationship_routes(kind).iter().all(|route| {
                route
                    .variants()
                    .filter(|variant| {
                        let end = if source {
                            variant.source
                        } else {
                            variant.target
                        };
                        graph.entity(end).name == entity
                    })
                    .all(|variant| {
                        found = true;
                        model
                            .variant_scope(
                                kind,
                                &graph.entity(variant.source).name,
                                &graph.entity(variant.target).name,
                            )
                            .is_some_and(|scope| {
                                scope.is_scope_preserving()
                                    || source && scope.edge_path_is_source_path()
                                    || !source && scope.edge_path_is_target_path()
                            })
                    })
            })
        });
        found && carries
    };
    let forward_first_hop_carries_path = carries_path(
        start_node.entity.as_deref(),
        forward_first_hop_filter.as_ref(),
        true,
    );
    let backward_first_hop_carries_path = carries_path(
        end_node.entity.as_deref(),
        backward_first_hop_filter.as_ref(),
        false,
    );

    let max_depth = path.max_depth;
    let mut plan = Plan::new(PathFinding {
        start: start.alias.clone(),
        end: end.alias.clone(),
        max_depth,
        forward_depth: max_depth / 2 + max_depth % 2,
        backward_depth: if max_depth >= 2 { max_depth / 2 } else { 0 },
        edge: EdgeTableConfig::from_model(model, &path.rel_types),
        forward_first_hop_filter,
        backward_first_hop_filter,
        scoped_by_tp: start.has_traversal_path && end.has_traversal_path,
        forward_first_hop_carries_path,
        backward_first_hop_carries_path,
    });
    plan.nodes.insert(start.alias.clone(), start);
    plan.nodes.insert(end.alias.clone(), end);
    Ok(plan)
}
