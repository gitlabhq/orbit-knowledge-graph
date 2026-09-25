use std::collections::HashMap;

use ontology::constants::*;

use crate::error::Result;
use crate::input::*;

use super::{EdgeTableConfig, HydrationStrategy, NodePlan, Plan, PlanBody, Selectivity, Strategy};
use crate::passes::shared::has_non_denorm_filters;
use query_data_model::{QueryBackendCatalog, QueryDataModel};

pub fn plan_neighbors<M>(input: &Input, model: &M) -> Result<Plan>
where
    M: QueryDataModel + crate::data_model::QueryModel + ?Sized,
{
    let config = input
        .neighbors
        .as_ref()
        .ok_or_else(|| crate::error::QueryError::Lowering("neighbors config missing".into()))?;

    let [center_node] = input.nodes.as_slice() else {
        return Err(crate::error::QueryError::Lowering(
            "neighbors query requires exactly one node".into(),
        ));
    };
    let center_alias = center_node.id.clone();
    let center_entity = center_node.entity.as_deref().ok_or_else(|| {
        crate::error::QueryError::Lowering("neighbors center has no entity".into())
    })?;
    let center_entity_id = model.graph().entity_id(center_entity).ok_or_else(|| {
        crate::error::QueryError::Lowering("neighbors center entity is unknown".into())
    })?;

    let center_np = NodePlan {
        alias: center_node.id.clone(),
        entity: center_node.entity.clone(),
        table: model
            .query_backend()
            .entity_table(center_entity_id)
            .map(String::from),
        selectivity: Selectivity::from_node(center_node),
        hydration: HydrationStrategy::Skip,
        filters: crate::passes::shared::ordered_filters(
            &center_node.filters,
            Some(center_entity_id),
            model,
        ),
        node_ids: center_node.node_ids.clone(),
        id_range: center_node.id_range.clone(),
        has_traversal_path: model
            .query_backend()
            .entity_has_traversal_path(center_entity_id),
        is_global: model.query_backend().entity_is_global(center_entity_id),
        redaction_id_column: ontology::constants::DEFAULT_PRIMARY_KEY.to_string(),
        columns: center_node.columns.clone(),
        use_narrowing: false,
        fk_needs_join: false,
        emit_select: true,
    };

    let denormalized = model.query_backend().denormalized();
    let denorm_columns = denormalized.columns.clone();
    let denorm_rel_kinds = denormalized.relationships.clone();
    let has_non_denorm = has_non_denorm_filters(
        center_np.entity.as_deref().unwrap_or(""),
        &center_np.filters,
        &denorm_columns,
    ) || center_np.id_range.is_some();

    let mut edge = EdgeTableConfig::from_model(model, &config.rel_types);
    {
        let center_entity = center_node.entity.as_deref().unwrap_or_default();
        let rels: Vec<&str> = if config.rel_types.is_empty() {
            model
                .graph()
                .relationships()
                .map(|relationship| relationship.name.as_str())
                .collect()
        } else {
            config.rel_types.iter().map(String::as_str).collect()
        };
        let tables_for = |source: bool| -> Vec<String> {
            let mut t: Vec<String> = rels
                .iter()
                .filter(|r| {
                    let kinds = if source {
                        super::relationship_entities(model.graph(), r, |v| v.source)
                    } else {
                        super::relationship_entities(model.graph(), r, |v| v.target)
                    };
                    kinds.iter().any(|kind| kind == center_entity)
                })
                .map(|r| {
                    model
                        .graph()
                        .relationship_id(r)
                        .and_then(|relationship| {
                            model.query_backend().relationship_table(relationship)
                        })
                        .unwrap_or_else(|| model.query_backend().default_edge_table())
                        .to_string()
                })
                .collect();
            t.sort();
            t.dedup();
            t
        };
        let outgoing = tables_for(true);
        let incoming = tables_for(false);
        if !outgoing.is_empty() {
            edge.outgoing_tables = outgoing;
        }
        if !incoming.is_empty() {
            edge.incoming_tables = incoming;
        }
    }

    let node_edge_mappings = HashMap::from([(
        center_alias.clone(),
        ("e".to_string(), SOURCE_ID_COLUMN.to_string()),
    )]);

    let mut nodes = HashMap::new();
    nodes.insert(center_alias.clone(), center_np);

    Ok(Plan {
        scope_requirements: Vec::new(),
        nodes,
        hops: vec![],
        strategy: Strategy::SingleNode,
        node_edge_mappings,
        denorm_columns,
        denorm_rel_kinds,
        table_columns: HashMap::new(),
        table_sort_keys: HashMap::new(),
        body: PlanBody::Neighbors {
            center: center_alias,
            direction: config.direction,
            edge,
            has_non_denorm,
            center_tp_lookup: center_node
                .entity
                .as_deref()
                .and_then(|entity| model.graph().entity_id(entity))
                .and_then(|entity| {
                    let lookup = model
                        .query_backend()
                        .traversal_path_lookup(entity, ontology::TraversalPathKind::Id)?;
                    Some((
                        lookup.table.clone(),
                        model
                            .query_backend()
                            .property_column(lookup.property)?
                            .to_string(),
                    ))
                }),
        },
    })
}
