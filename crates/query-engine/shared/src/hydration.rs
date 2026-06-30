//! Shared hydration helpers used by both the server and local pipelines.
//! The compile + execute step is left to the caller.

use std::collections::HashMap;

use arrow::datatypes::Int64Type;
use arrow::record_batch::RecordBatch;
use compiler::constants::MAX_DYNAMIC_HYDRATION_RESULTS;
use compiler::constants::{
    HYDRATION_NODE_ALIAS, primary_key_column, redaction_id_column, traversal_path_column,
};
use compiler::{
    ColumnSelection, DynamicEntityColumns, FilterOp, HydrationTemplate, Input, InputFilter,
    InputNode, QueryType,
};
use gkg_utils::arrow::{ArrowUtils, ColumnValue};
use pipeline::PipelineError;
use types::QueryResult;

pub use crate::content::PropertyMap;

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

/// Reads the `_gkg_{alias}_tp` column emitted by the enforce pass.
pub fn collect_traversal_paths(result: &QueryResult, alias: &str) -> Vec<String> {
    let tp_col = traversal_path_column(alias);
    let mut paths: Vec<String> = result
        .authorized_rows()
        .filter_map(|row| row.get_column_string(&tp_col))
        .filter(|p| !p.is_empty())
        .collect();
    paths.sort_unstable();
    paths.dedup();
    paths
}

/// Fallback for nodes whose own TP column is absent (FK-elided pinned nodes
/// where the alias was absorbed into a literal). The TPs from sibling nodes in
/// the same result rows are valid narrowing candidates because all entities
/// share the same namespace hierarchy.
fn collect_all_traversal_paths(
    result: &QueryResult,
    templates: &[HydrationTemplate],
) -> Vec<String> {
    let mut all: Vec<String> = Vec::new();
    for t in templates {
        if !t.has_traversal_path {
            continue;
        }
        let tp_col = traversal_path_column(&t.node_alias);
        for row in result.authorized_rows() {
            if let Some(tp) = row.get_column_string(&tp_col)
                && !tp.is_empty()
            {
                all.push(tp);
            }
        }
    }
    all.sort_unstable();
    all.dedup();
    all
}

/// When `static_nodes` is provided (e.g. the center node in a neighbors query),
/// their IDs are also collected so properties can be hydrated for them.
pub fn extract_dynamic_refs(
    result: &QueryResult,
    static_nodes: &[compiler::RedactionNode],
) -> HashMap<String, Vec<i64>> {
    let mut refs: HashMap<String, Vec<i64>> = HashMap::new();
    for row in result.authorized_rows() {
        for node_ref in row.dynamic_nodes() {
            refs.entry(node_ref.entity_type.clone())
                .or_default()
                .push(node_ref.id);
        }
        for node in static_nodes {
            if let Some(id) = row.get_public_id(node)
                && let Some(entity_type) = row.get_type(node)
            {
                refs.entry(entity_type.to_string()).or_default().push(id);
            }
        }
    }
    for ids in refs.values_mut() {
        ids.sort_unstable();
        ids.dedup();
    }
    refs
}

pub fn build_static_nodes(
    templates: &[HydrationTemplate],
    result: &QueryResult,
) -> (Vec<InputNode>, usize) {
    let all_tps = collect_all_traversal_paths(result, templates);
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
        let traversal_paths = if template.has_traversal_path {
            let own = collect_traversal_paths(result, &template.node_alias);
            if own.is_empty() { all_tps.clone() } else { own }
        } else {
            Vec::new()
        };
        total_ids += ids.len();
        nodes.push(InputNode {
            id: HYDRATION_NODE_ALIAS.to_string(),
            entity: Some(template.entity_type.clone()),
            table: Some(template.destination_table.clone()),
            columns: Some(ColumnSelection::List(template.columns.clone())),
            node_ids: ids,
            traversal_paths,
            ..InputNode::default()
        });
    }

    (nodes, total_ids)
}

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

/// Caps limit at `u32::MAX` to prevent truncation.
///
/// Callers set `Input.hydration_dynamic` from the originating query's type
/// (Neighbors/PathFinding, derived from the pipeline ctx) before passing
/// the input to `compile_input`. The compiler reads that flag during
/// lowering to pick the `traversal_path` filter shape: dynamic emits a
/// single `arrayExists` (constant AST depth, safe at hundreds of paths)
/// and static emits OR-of-`startsWith` (per-leaf PK pushdown).
pub fn build_hydration_input(nodes: Vec<InputNode>, total_ids: usize) -> Input {
    Input {
        query_type: QueryType::Hydration,
        nodes,
        limit: total_ids.min(u32::MAX as usize) as u32,
        ..Input::default()
    }
}

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
                        .filter_map(|(k, v)| match ColumnValue::from(v) {
                            ColumnValue::Null => None,
                            ColumnValue::String(s) if s.is_empty() => None,
                            cv => Some((k, cv)),
                        })
                        .collect()
                })
                .unwrap_or_default();

            result.insert((entity_type, id), props);
        }
    }

    Ok(result)
}

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

pub fn merge_dynamic_properties(result: &mut QueryResult, property_map: &PropertyMap) {
    for row in result.authorized_rows_mut() {
        for node_ref in row.dynamic_nodes_mut() {
            if let Some(props) = property_map.get(&(node_ref.entity_type.clone(), node_ref.id)) {
                node_ref.properties = props.clone();
            }
        }
    }
}

/// Writes row columns named `{alias}_{column}` so formatters can extract them.
pub fn merge_static_node_properties(
    result: &mut QueryResult,
    property_map: &PropertyMap,
    static_nodes: &[compiler::RedactionNode],
) {
    for row in result.authorized_rows_mut() {
        for node in static_nodes {
            let Some(id) = row.get_public_id(node) else {
                continue;
            };
            let Some(entity_type) = row.get_type(node) else {
                continue;
            };
            if let Some(props) = property_map.get(&(entity_type.to_string(), id)) {
                for (key, value) in props {
                    let col_name = format!("{}_{key}", node.alias);
                    row.set_column(col_name, value.clone());
                }
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
    let all_tps = collect_all_traversal_paths(query_result, templates);
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

        let traversal_paths = if template.has_traversal_path {
            let own = collect_traversal_paths(query_result, &template.node_alias);
            if own.is_empty() { all_tps.clone() } else { own }
        } else {
            Vec::new()
        };

        total_ids += ids.len();
        nodes.push(InputNode {
            id: HYDRATION_NODE_ALIAS.to_string(),
            entity: Some(template.entity_type.clone()),
            table: Some(template.destination_table.clone()),
            columns: Some(ColumnSelection::List(template.columns.clone())),
            node_ids: ids,
            traversal_paths,
            ..InputNode::default()
        });
    }

    Ok((nodes, total_ids))
}

/// For PathFinding queries where no static nodes have TP columns, returns an
/// empty vec — the caller can fall back to security context TPs.
pub fn collect_dynamic_traversal_paths(
    result: &QueryResult,
    static_nodes: &[compiler::RedactionNode],
) -> Vec<String> {
    let mut paths: Vec<String> = Vec::new();
    for node in static_nodes {
        let tp_col = traversal_path_column(&node.alias);
        for row in result.authorized_rows() {
            if let Some(tp) = row.get_column_string(&tp_col)
                && !tp.is_empty()
            {
                paths.push(tp);
            }
        }
    }
    paths.sort_unstable();
    paths.dedup();
    paths
}

/// Uses pre-resolved column specs from the compilation plan; no ontology
/// lookups at runtime.
///
/// `traversal_paths` narrows hydration scans for entities that have
/// `traversal_path` in their table. Pass TPs collected from static nodes
/// or the security context.
pub fn hydrate_dynamic(
    entity_specs: &[DynamicEntityColumns],
    refs: &HashMap<String, Vec<i64>>,
    traversal_paths: &[String],
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

        let tps = if spec.has_traversal_path {
            traversal_paths.to_vec()
        } else {
            Vec::new()
        };

        nodes.push(InputNode {
            id: HYDRATION_NODE_ALIAS.to_string(),
            entity: Some(entity_type.clone()),
            table: Some(spec.destination_table.clone()),
            columns: Some(ColumnSelection::List(spec.columns.clone())),
            node_ids: capped_ids,
            traversal_paths: tps,
            ..InputNode::default()
        });
    }

    Ok((nodes, total_ids))
}

fn eval_virtual_filter(value: Option<&ColumnValue>, filter: &InputFilter) -> bool {
    let op = filter.op.unwrap_or(FilterOp::Eq);
    match op {
        FilterOp::IsNull => value.is_none() || matches!(value, Some(ColumnValue::Null)),
        FilterOp::IsNotNull => value.is_some() && !matches!(value, Some(ColumnValue::Null)),
        _ => {
            let Some(ColumnValue::String(cv_str)) = value else {
                return false;
            };
            let filter_str = filter.value.as_ref().and_then(|v| v.as_str()).unwrap_or("");
            match op {
                FilterOp::Eq => cv_str == filter_str,
                FilterOp::Contains => cv_str.to_lowercase().contains(&filter_str.to_lowercase()),
                FilterOp::StartsWith => cv_str.starts_with(filter_str),
                FilterOp::EndsWith => cv_str.ends_with(filter_str),
                _ => true,
            }
        }
    }
}

pub fn apply_virtual_filters_static(result: &mut QueryResult, templates: &[HydrationTemplate]) {
    let filters: Vec<_> = templates
        .iter()
        .flat_map(|t| {
            t.virtual_filters
                .iter()
                .map(move |(col, f)| (format!("{}_{col}", t.node_alias), f))
        })
        .collect();
    if filters.is_empty() {
        return;
    }
    result.retain_rows(|row| {
        filters
            .iter()
            .all(|(col_key, filter)| eval_virtual_filter(row.get(col_key), filter))
    });
}
