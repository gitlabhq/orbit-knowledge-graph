//! Neighbors plan: single-hop edge scan for adjacent entities.

use std::collections::HashMap;

use ontology::constants::*;

use crate::error::{QueryError, Result};
use crate::input::*;

pub struct NeighborsPlan {
    pub center_id: String,
    pub center_entity: String,
    pub center_table: String,
    pub center_uses_default_pk: bool,
    pub center_redaction_col: String,
    pub center_node_ids: Vec<i64>,
    pub center_filters: Vec<(String, InputFilter)>,
    pub center_id_range: Option<InputIdRange>,
    pub has_non_denorm: bool,
    pub direction: Direction,
    pub edge_tables: Vec<String>,
    pub rel_type_filter: Option<Vec<String>>,
    /// Denorm column map snapshot from CompilerMetadata.
    pub denorm_columns: HashMap<(String, String, String), (String, String)>,
    pub node_edge_mappings: HashMap<String, (String, String)>,
    pub order_by: Option<InputOrderBy>,
    pub cursor: Option<InputCursor>,
    pub limit: u32,
}

pub fn plan_neighbors(input: &Input) -> Result<NeighborsPlan> {
    let config = input
        .neighbors
        .as_ref()
        .ok_or_else(|| QueryError::Lowering("neighbors config missing".into()))?;

    let center_node = input
        .nodes
        .iter()
        .find(|n| n.id == config.node)
        .ok_or_else(|| QueryError::Lowering(format!("center node '{}' not found", config.node)))?;
    let center_entity = center_node
        .entity
        .as_ref()
        .ok_or_else(|| QueryError::Lowering("center node entity missing".into()))?
        .clone();
    let center_table = center_node
        .table
        .as_ref()
        .ok_or_else(|| QueryError::Lowering("center node table missing".into()))?
        .clone();

    let center_filters: Vec<(String, InputFilter)> =
        center_node.filters.clone().into_iter().collect();

    let has_non_denorm = center_filters.iter().any(|(prop, _)| {
        let src = input.compiler.denormalized_columns.contains_key(&(
            center_entity.clone(),
            prop.clone(),
            "source".to_string(),
        ));
        let tgt = input.compiler.denormalized_columns.contains_key(&(
            center_entity.clone(),
            prop.clone(),
            "target".to_string(),
        ));
        !src && !tgt
    }) || center_node.id_range.is_some();

    let node_edge_mappings = HashMap::from([(
        center_node.id.clone(),
        ("e".to_string(), SOURCE_ID_COLUMN.to_string()),
    )]);

    Ok(NeighborsPlan {
        center_id: center_node.id.clone(),
        center_entity,
        center_table,
        center_uses_default_pk: center_node.redaction_id_column == DEFAULT_PRIMARY_KEY,
        center_redaction_col: center_node.redaction_id_column.clone(),
        center_node_ids: center_node.node_ids.clone(),
        center_filters,
        center_id_range: center_node.id_range.clone(),
        has_non_denorm,
        direction: config.direction,
        edge_tables: input.compiler.resolve_edge_tables(&config.rel_types),
        rel_type_filter: if config.rel_types.is_empty() {
            None
        } else {
            Some(config.rel_types.clone())
        },
        denorm_columns: input.compiler.denormalized_columns.clone(),
        node_edge_mappings,
        order_by: input.order_by.clone(),
        cursor: input.cursor,
        limit: input.limit,
    })
}
