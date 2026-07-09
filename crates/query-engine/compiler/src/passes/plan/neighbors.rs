use std::collections::HashMap;

use ontology::constants::*;

use crate::error::Result;
use crate::input::*;

use super::{EdgeTableConfig, HydrationStrategy, NodePlan, Plan, PlanBody, Selectivity, Strategy};
use crate::passes::shared::has_non_denorm_filters;

pub fn plan_neighbors(input: &Input) -> Result<Plan> {
    let config = input
        .neighbors
        .as_ref()
        .ok_or_else(|| crate::error::QueryError::Lowering("neighbors config missing".into()))?;

    let center_node = input.nodes.first().ok_or_else(|| {
        crate::error::QueryError::Lowering("neighbors query requires a node selector".into())
    })?;
    let center_alias = center_node.id.clone();

    let center_np = NodePlan {
        alias: center_node.id.clone(),
        entity: center_node.entity.clone(),
        table: center_node.table.clone(),
        selectivity: Selectivity::from_node(center_node),
        hydration: HydrationStrategy::Skip,
        filters: center_node
            .filters
            .iter()
            .flat_map(|(k, v)| v.iter().map(move |f| (k.clone(), f.clone())))
            .collect(),
        node_ids: center_node.node_ids.clone(),
        id_range: center_node.id_range.clone(),
        has_traversal_path: center_node.has_traversal_path,
        is_global: center_node.is_global,
        redaction_id_column: center_node.redaction_id_column.clone(),
        columns: center_node.columns.clone(),
        dedup_columns: vec![],
        use_narrowing: false,
        needs_elevated_filter: false,
        fk_needs_join: false,
        emit_select: true,
    };

    let has_non_denorm = has_non_denorm_filters(
        center_np.entity.as_deref().unwrap_or(""),
        &center_np.filters,
        &input.compiler.denormalized_columns,
    ) || center_np.id_range.is_some();

    let mut edge = EdgeTableConfig::from_input(&input.compiler, &config.rel_types);
    {
        let center_entity = center_node.entity.as_deref().unwrap_or_default();
        let rels: Vec<&String> = if config.rel_types.is_empty() {
            input.compiler.edge_table_for_rel.keys().collect()
        } else {
            config.rel_types.iter().collect()
        };
        let tables_for = |kinds: &HashMap<String, Vec<String>>| -> Vec<String> {
            let mut t: Vec<String> = rels
                .iter()
                .filter(|r| {
                    kinds
                        .get(**r)
                        .is_some_and(|ks| ks.iter().any(|k| k == center_entity))
                })
                .map(|r| {
                    input
                        .compiler
                        .edge_table_for_rel
                        .get(*r)
                        .cloned()
                        .unwrap_or_else(|| input.compiler.default_edge_table.clone())
                })
                .collect();
            t.sort();
            t.dedup();
            t
        };
        let outgoing = tables_for(&input.compiler.edge_source_kinds);
        let incoming = tables_for(&input.compiler.edge_target_kinds);
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
        nodes,
        hops: vec![],
        strategy: Strategy::SingleNode,
        limit: input.fetch_limit(),
        order_by: input.order_by.clone(),
        cursor: input.cursor.clone(),
        node_edge_mappings,
        denorm_columns: input.compiler.denormalized_columns.clone(),
        denorm_rel_kinds: input.compiler.denorm_rel_kinds.clone(),
        table_columns: input.compiler.table_columns.clone(),
        table_sort_keys: input.compiler.table_sort_keys.clone(),
        body: PlanBody::Neighbors {
            center: center_alias,
            direction: config.direction,
            edge,
            has_non_denorm,
            center_tp_lookup: center_node
                .entity
                .as_deref()
                .and_then(|e| input.compiler.tp_id_lookup.get(e).cloned()),
        },
    })
}
