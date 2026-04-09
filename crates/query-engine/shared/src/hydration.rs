//! Shared hydration helpers used by both the server and local pipelines.
//!
//! These functions handle ID collection, input construction, batch parsing,
//! and property merging. The compile + execute step is left to the caller.

use std::collections::HashMap;

use arrow::datatypes::Int64Type;
use arrow::record_batch::RecordBatch;
use compiler::constants::MAX_DYNAMIC_HYDRATION_RESULTS;
use compiler::constants::{HYDRATION_NODE_ALIAS, primary_key_column, redaction_id_column};
use compiler::{
    ColumnSelection, DynamicEntityColumns, HydrationTemplate, Input, InputNode, QueryType,
};
use gkg_utils::arrow::{ArrowUtils, ColumnValue};
use pipeline::PipelineError;
use types::QueryResult;

pub use crate::content::PropertyMap;

/// Collect entity primary keys from authorized rows for static hydration.
///
/// Tries the PK column first (`_gkg_{alias}_pk`), falling back to the
/// redaction ID column (`_gkg_{alias}_id`).
pub fn collect_static_ids(result: &QueryResult, template: &HydrationTemplate) -> Vec<i64> {
    let pk_col = primary_key_column(&template.node_alias);
    let id_col = redaction_id_column(&template.node_alias);
    let mut ids: Vec<i64> = result
        .authorized_rows()
        .filter_map(|row| {
            row.get_column_i64(&pk_col)
                .or_else(|| row.get_column_i64(&id_col))
        })
        .collect();
    ids.sort_unstable();
    ids.dedup();
    ids
}

/// Extract (entity_type, id) pairs from dynamic nodes (path finding, neighbors).
pub fn extract_dynamic_refs(result: &QueryResult) -> HashMap<String, Vec<i64>> {
    let mut refs: HashMap<String, Vec<i64>> = HashMap::new();
    for row in result.authorized_rows() {
        for node_ref in row.dynamic_nodes() {
            refs.entry(node_ref.entity_type.clone())
                .or_default()
                .push(node_ref.id);
        }
    }
    for ids in refs.values_mut() {
        ids.sort_unstable();
        ids.dedup();
    }
    refs
}

/// Build `InputNode`s for static hydration from templates and collected IDs.
pub fn build_static_nodes(
    templates: &[HydrationTemplate],
    result: &QueryResult,
) -> (Vec<InputNode>, usize) {
    let mut nodes = Vec::new();
    let mut total_ids: usize = 0;

    for template in templates {
        if template.columns.is_empty() {
            continue;
        }
        let ids = collect_static_ids(result, template);
        if ids.is_empty() {
            continue;
        }
        total_ids += ids.len();
        nodes.push(InputNode {
            id: HYDRATION_NODE_ALIAS.to_string(),
            entity: Some(template.entity_type.clone()),
            table: Some(template.destination_table.clone()),
            columns: Some(ColumnSelection::List(template.columns.clone())),
            node_ids: ids,
            ..InputNode::default()
        });
    }

    (nodes, total_ids)
}

/// Build `InputNode`s for dynamic hydration from entity specs and discovered refs.
pub fn build_dynamic_nodes(
    entity_specs: &[DynamicEntityColumns],
    refs: &HashMap<String, Vec<i64>>,
) -> (Vec<InputNode>, usize) {
    let mut nodes = Vec::new();
    let mut total_ids: usize = 0;

    for (entity_type, ids) in refs {
        let Some(spec) = entity_specs.iter().find(|s| s.entity_type == *entity_type) else {
            continue;
        };
        if spec.columns.is_empty() || ids.is_empty() {
            continue;
        }
        total_ids += ids.len();
        nodes.push(InputNode {
            id: HYDRATION_NODE_ALIAS.to_string(),
            entity: Some(entity_type.clone()),
            table: Some(spec.destination_table.clone()),
            columns: Some(ColumnSelection::List(spec.columns.clone())),
            node_ids: ids.clone(),
            ..InputNode::default()
        });
    }

    (nodes, total_ids)
}

/// Build the hydration `Input` from collected nodes.
/// Caps limit at `u32::MAX` to prevent truncation.
pub fn build_hydration_input(nodes: Vec<InputNode>, total_ids: usize) -> Input {
    Input {
        query_type: QueryType::Hydration,
        nodes,
        limit: total_ids.min(u32::MAX as usize) as u32,
        ..Input::default()
    }
}

/// Parse hydration result batches into a `PropertyMap`.
///
/// Expects columns: `{alias}_id`, `{alias}_entity_type`, `{alias}_props`
/// where props is a JSON-encoded object.
pub fn parse_hydration_batches(batches: &[RecordBatch]) -> Result<PropertyMap, PipelineError> {
    let alias = HYDRATION_NODE_ALIAS;
    let entity_type_col = format!("{alias}_entity_type");
    let props_col = format!("{alias}_props");
    let id_col = format!("{alias}_id");

    let mut result = HashMap::new();

    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let Some(id) = ArrowUtils::get_column::<Int64Type>(batch, &id_col, row_idx) else {
                continue;
            };

            let row_data = ArrowUtils::extract_row(batch, row_idx);

            let entity_type = row_data
                .iter()
                .find(|(name, _)| name.as_str() == entity_type_col)
                .and_then(|(_, v)| v.as_string().cloned());

            let Some(entity_type) = entity_type else {
                continue;
            };

            let props: HashMap<String, ColumnValue> = row_data
                .iter()
                .find(|(name, _)| name.as_str() == props_col)
                .and_then(|(_, v)| v.as_string())
                .and_then(|json_str| {
                    serde_json::from_str::<HashMap<String, serde_json::Value>>(json_str).ok()
                })
                .map(|m| {
                    m.into_iter()
                        .filter_map(|(k, v)| {
                            let cv = ColumnValue::from(v);
                            if cv == ColumnValue::Null {
                                None
                            } else {
                                Some((k, cv))
                            }
                        })
                        .collect()
                })
                .unwrap_or_default();

            result.insert((entity_type, id), props);
        }
    }

    Ok(result)
}

/// Merge hydrated properties into static (traversal) query result rows.
pub fn merge_static_properties(
    result: &mut QueryResult,
    property_map: &PropertyMap,
    templates: &[HydrationTemplate],
) {
    for row in result.authorized_rows_mut() {
        for template in templates {
            let pk_col = primary_key_column(&template.node_alias);
            let id_col = redaction_id_column(&template.node_alias);
            let id = row
                .get_column_i64(&pk_col)
                .or_else(|| row.get_column_i64(&id_col));
            if let Some(id) = id
                && let Some(props) = property_map.get(&(template.entity_type.clone(), id))
            {
                for (key, value) in props {
                    let col_name = format!("{}_{key}", template.node_alias);
                    row.set_column(col_name, value.clone());
                }
            }
        }
    }
}

/// Merge hydrated properties into dynamic (path finding, neighbors) node refs.
pub fn merge_dynamic_properties(result: &mut QueryResult, property_map: &PropertyMap) {
    for row in result.authorized_rows_mut() {
        for node_ref in row.dynamic_nodes_mut() {
            if let Some(props) = property_map.get(&(node_ref.entity_type.clone(), node_ref.id)) {
                node_ref.properties = props.clone();
            }
        }
    }
}

/// Remove columns that were injected as dependencies for virtual column
/// resolvers but not explicitly requested by the user.
pub fn strip_injected_columns<'a>(
    property_map: &mut PropertyMap,
    specs: impl Iterator<Item = (&'a str, &'a Vec<String>)>,
) {
    for (entity_type, injected) in specs {
        if injected.is_empty() {
            continue;
        }
        for ((et, _), props) in property_map.iter_mut() {
            if et == entity_type {
                for col in injected {
                    props.remove(col);
                }
            }
        }
    }
}

pub fn hydrate_static(
    templates: &[HydrationTemplate],
    query_result: &QueryResult,
) -> Result<(Vec<InputNode>, usize), PipelineError> {
    let mut nodes = Vec::new();
    let mut total_ids: usize = 0;

    for template in templates {
        if template.columns.is_empty() {
            continue;
        }

        let ids = collect_static_ids(query_result, template);
        if ids.is_empty() {
            continue;
        }

        total_ids += ids.len();
        nodes.push(InputNode {
            id: HYDRATION_NODE_ALIAS.to_string(),
            entity: Some(template.entity_type.clone()),
            table: Some(template.destination_table.clone()),
            columns: Some(ColumnSelection::List(template.columns.clone())),
            node_ids: ids,
            ..InputNode::default()
        });
    }

    Ok((nodes, total_ids))
}

/// Dynamic hydration: builds an `Input` with one node per
/// discovered entity type using pre-resolved column specs from the
/// compilation plan. No ontology lookups at runtime.
pub fn hydrate_dynamic(
    entity_specs: &[DynamicEntityColumns],
    refs: &HashMap<String, Vec<i64>>,
) -> Result<(Vec<InputNode>, usize), PipelineError> {
    let mut nodes = Vec::new();
    let mut total_ids: usize = 0;

    for (entity_type, ids) in refs {
        if ids.is_empty() {
            continue;
        }

        let spec = match entity_specs.iter().find(|s| s.entity_type == *entity_type) {
            Some(s) => s,
            None => continue,
        };

        if spec.columns.is_empty() {
            continue;
        }

        let capped_ids: Vec<i64> = ids
            .iter()
            .copied()
            .take(MAX_DYNAMIC_HYDRATION_RESULTS)
            .collect();
        total_ids += capped_ids.len();

        nodes.push(InputNode {
            id: HYDRATION_NODE_ALIAS.to_string(),
            entity: Some(entity_type.clone()),
            table: Some(spec.destination_table.clone()),
            columns: Some(ColumnSelection::List(spec.columns.clone())),
            node_ids: capped_ids,
            ..InputNode::default()
        });
    }

    Ok((nodes, total_ids))
}
