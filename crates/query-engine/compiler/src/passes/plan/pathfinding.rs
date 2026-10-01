use super::context::PlanningContext;
use super::{EdgeTableConfig, PathFinding, Plan, find_node};
use crate::error::Result;
use query_data_model::QueryDataModel;

pub(super) fn plan_pathfinding<M>(mut context: PlanningContext<'_, M>) -> Result<Plan<PathFinding>>
where
    M: QueryDataModel + ?Sized,
{
    let input = context.input;
    let model = context.model;
    let path = input
        .path
        .as_ref()
        .ok_or_else(|| crate::error::QueryError::Lowering("path config missing".into()))?;

    let start_node = find_node(input, &path.from)?;
    let end_node = find_node(input, &path.to)?;
    let start_alias = start_node.id.clone();
    let end_alias = end_node.id.clone();

    let start_np = context.resolve_node(start_node)?;
    let end_np = context.resolve_node(end_node)?;

    let scoped_by_tp = start_np.has_traversal_path && end_np.has_traversal_path;
    let edge = EdgeTableConfig::from_model(model, &path.rel_types);

    let endpoint_kinds = |entity: &str, source: bool| {
        if !crate::passes::normalize::is_wildcard(&path.rel_types) {
            return None;
        }
        let relationships = if source {
            model.graph().relationship_names(Some(entity), None)
        } else {
            model.graph().relationship_names(None, Some(entity))
        };
        crate::passes::shared::rel_kind_filter_values(&relationships)
    };
    let forward_first_hop_filter = start_node
        .entity
        .as_deref()
        .and_then(|entity| endpoint_kinds(entity, true));
    let backward_first_hop_filter = end_node
        .entity
        .as_deref()
        .and_then(|entity| endpoint_kinds(entity, false));

    let max_depth = path.max_depth;
    let forward_depth = max_depth / 2 + max_depth % 2;
    let backward_depth = if max_depth >= 2 { max_depth / 2 } else { 0 };

    context.nodes.insert(start_alias.clone(), start_np);
    context.nodes.insert(end_alias.clone(), end_np);
    Ok(context.finish(PathFinding {
        start: start_alias,
        end: end_alias,
        max_depth,
        forward_depth,
        backward_depth,
        edge,
        forward_first_hop_filter,
        backward_first_hop_filter,
        scoped_by_tp,
    }))
}
