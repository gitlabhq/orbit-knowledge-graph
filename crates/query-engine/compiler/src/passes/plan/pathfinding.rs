use std::collections::HashMap;

use crate::error::Result;
use crate::input::*;

use super::{
    EdgeTableConfig, HydrationStrategy, NodePlan, PathFindingBody, Plan, PlanBody, Selectivity,
    Strategy, find_node,
};
use query_data_model::{QueryBackendCatalog, QueryDataModel};

pub fn plan_pathfinding<M>(input: &Input, model: &M) -> Result<Plan>
where
    M: QueryDataModel + crate::data_model::QueryModel + ?Sized,
{
    let path = input
        .path
        .as_ref()
        .ok_or_else(|| crate::error::QueryError::Lowering("path config missing".into()))?;

    let start_node = find_node(input, &path.from)?;
    let end_node = find_node(input, &path.to)?;
    let start_alias = start_node.id.clone();
    let end_alias = end_node.id.clone();

    let start_np = node_plan_from(start_node, model)?;
    let end_np = node_plan_from(end_node, model)?;

    let scoped_by_tp = start_np.has_traversal_path && end_np.has_traversal_path;
    let edge = EdgeTableConfig::from_model(model, &path.rel_types);

    let endpoint_kinds = |entity: &str, source: bool| {
        let relationships: Vec<String> = model
            .graph()
            .relationships()
            .filter(|relationship| {
                let entities = if source {
                    super::relationship_entities(model.graph(), &relationship.name, |v| v.source)
                } else {
                    super::relationship_entities(model.graph(), &relationship.name, |v| v.target)
                };
                entities.iter().any(|kind| kind == entity)
            })
            .map(|relationship| relationship.name.clone())
            .collect();
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

    let mut nodes = HashMap::new();
    nodes.insert(start_alias.clone(), start_np);
    nodes.insert(end_alias.clone(), end_np);

    Ok(Plan {
        scope_requirements: Vec::new(),
        nodes,
        hops: vec![],
        strategy: Strategy::SingleNode,
        node_edge_mappings: HashMap::new(),
        denorm_columns: HashMap::new(),
        denorm_rel_kinds: HashMap::new(),
        table_columns: HashMap::new(),
        table_sort_keys: HashMap::new(),
        body: PlanBody::PathFinding(PathFindingBody {
            start: start_alias,
            end: end_alias,
            max_depth,
            forward_depth,
            backward_depth,
            edge,
            forward_first_hop_filter,
            backward_first_hop_filter,
            scoped_by_tp,
        }),
    })
}

fn node_plan_from<M>(node: &InputNode, model: &M) -> Result<NodePlan>
where
    M: QueryDataModel + crate::data_model::QueryModel + ?Sized,
{
    let entity = node
        .entity
        .as_deref()
        .ok_or_else(|| crate::error::QueryError::Lowering("path node has no entity".into()))?;
    let entity_id = model
        .graph()
        .entity_id(entity)
        .ok_or_else(|| crate::error::QueryError::Lowering("path node entity is unknown".into()))?;
    Ok(NodePlan {
        alias: node.id.clone(),
        entity: node.entity.clone(),
        table: model
            .query_backend()
            .entity_table(entity_id)
            .map(String::from),
        selectivity: Selectivity::from_node(node),
        hydration: HydrationStrategy::Skip,
        filters: crate::passes::shared::ordered_filters(&node.filters, Some(entity_id), model),
        node_ids: node.node_ids.clone(),
        id_range: node.id_range.clone(),
        has_traversal_path: model.query_backend().entity_has_traversal_path(entity_id),
        is_global: model.query_backend().entity_is_global(entity_id),
        redaction_id_column: ontology::constants::DEFAULT_PRIMARY_KEY.to_string(),
        columns: node.columns.clone(),
        use_narrowing: false,
        fk_needs_join: false,
        emit_select: true,
    })
}
