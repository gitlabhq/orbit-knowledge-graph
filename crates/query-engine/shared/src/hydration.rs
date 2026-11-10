//! Shared hydration helpers used by both the server and local pipelines.
//! The compile + execute step is left to the caller.

use std::collections::HashMap;

use arrow::datatypes::Int64Type;
use arrow::record_batch::RecordBatch;
use compiler::constants::MAX_DYNAMIC_HYDRATION_RESULTS;
use compiler::constants::{
    hydration_node_alias, neighbor_traversal_path_column, primary_key_column, redaction_id_column,
    traversal_path_column,
};
use compiler::{
    ColumnSelection, DynamicEntityColumns, FilterOp, HydrationTemplate, Input, InputFilter,
    InputNode, QueryType,
};
use ontology::{DataType, Ontology};
use orbit_arrow::{ArrowUtils, ColumnValue};
use orbit_utils::traversal_path::TraversalPath;
use pipeline::PipelineError;
use types::QueryResult;

pub use crate::content::PropertyMap;

/// Tries the PK column first (`_orbit_{alias}_pk`), falling back to the
/// redaction ID column (`_orbit_{alias}_id`).
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

/// Reads the `_orbit_{alias}_tp` column emitted by the result-contract pass.
pub fn collect_traversal_paths(result: &QueryResult, alias: &str) -> Vec<TraversalPath> {
    let tp_col = traversal_path_column(alias);
    let mut paths: Vec<TraversalPath> = result
        .authorized_rows()
        .filter_map(|row| row.get_column_string(&tp_col))
        .filter(|p| !p.is_empty())
        .map(TraversalPath::new_unchecked)
        .collect();
    paths.sort_unstable();
    paths.dedup();
    paths
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

pub fn collect_dynamic_ref_paths(
    result: &QueryResult,
    static_nodes: &[compiler::RedactionNode],
) -> HashMap<String, Vec<TraversalPath>> {
    let mut paths: HashMap<String, Option<Vec<TraversalPath>>> = HashMap::new();
    let mut record = |entity_type: &str, path: Option<String>| {
        let entry = paths
            .entry(entity_type.to_string())
            .or_insert_with(|| Some(Vec::new()));
        match (entry.as_mut(), path.filter(|p| !p.is_empty())) {
            (Some(list), Some(path)) => list.push(TraversalPath::new_unchecked(path)),
            _ => *entry = None,
        }
    };
    for row in result.authorized_rows() {
        let neighbor_path = row.get_column_string(neighbor_traversal_path_column());
        for node_ref in row.dynamic_nodes() {
            record(&node_ref.entity_type, neighbor_path.clone());
        }
        for node in static_nodes {
            if row.get_public_id(node).is_some()
                && let Some(entity_type) = row.get_type(node)
            {
                record(
                    entity_type,
                    row.get_column_string(&traversal_path_column(&node.alias)),
                );
            }
        }
    }
    paths
        .into_iter()
        .filter_map(|(entity_type, list)| {
            let mut list = list?;
            list.sort_unstable();
            list.dedup();
            Some((entity_type, list))
        })
        .collect()
}

/// Caps limit at `u32::MAX` to prevent truncation.
pub fn build_hydration_input(nodes: Vec<InputNode>, total_ids: usize) -> Input {
    Input {
        query_type: QueryType::Hydration,
        nodes,
        limit: total_ids.min(u32::MAX as usize) as u32,
        ..Input::default()
    }
}

/// `{alias}_props` arrives as a JSON object of strings, so typed fields are re-typed from the ontology.
pub fn parse_hydration_batches(
    batches: &[RecordBatch],
    ontology: &Ontology,
) -> Result<PropertyMap, PipelineError> {
    let alias = hydration_node_alias();
    let entity_type_col = format!("{alias}_entity_type");
    let props_col = format!("{alias}_props");
    let id_col = format!("{alias}_id");

    let mut result = HashMap::new();
    let mut field_types_cache: HashMap<String, HashMap<String, DataType>> = HashMap::new();

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

            let field_types = field_types_cache
                .entry(entity_type.clone())
                .or_insert_with(|| {
                    ontology
                        .get_node(&entity_type)
                        .map(|node| {
                            node.fields
                                .iter()
                                .map(|f| (f.name.clone(), f.data_type))
                                .collect()
                        })
                        .unwrap_or_default()
                });

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
                            cv @ ColumnValue::String(_) => {
                                let typed = typed_string(cv, field_types.get(&k));
                                Some((k, typed))
                            }
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

fn typed_string(cv: ColumnValue, data_type: Option<&DataType>) -> ColumnValue {
    match data_type {
        Some(DataType::Bool) => cv.coerce::<bool>().map_or(cv, ColumnValue::Bool),
        Some(DataType::Int) => cv.coerce::<i64>().map_or(cv, ColumnValue::Int64),
        Some(DataType::Float) => cv.coerce::<f64>().map_or(cv, ColumnValue::Float64),
        Some(DataType::DateTime) => cv
            .as_string()
            .and_then(|s| ColumnValue::parse_datetime(s))
            .unwrap_or(cv),
        _ => cv,
    }
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
            collect_traversal_paths(query_result, &template.node_alias)
        } else {
            Vec::new()
        };

        total_ids += ids.len();
        nodes.push(InputNode {
            id: hydration_node_alias().to_string(),
            entity: Some(template.entity_type.clone()),
            columns: Some(ColumnSelection::List(template.columns.clone())),
            node_ids: ids,
            traversal_paths,
            ..InputNode::default()
        });
    }

    Ok((nodes, total_ids))
}

/// Uses pre-resolved column specs from the compilation plan; no ontology
/// lookups at runtime.
///
/// `traversal_paths` is the authorized scope; `ref_paths` narrows an entity type to the paths
/// its own rows carry. A neighbor can sit above the center node, so the center path is no fallback.
pub fn hydrate_dynamic(
    entity_specs: &[DynamicEntityColumns],
    refs: &HashMap<String, Vec<i64>>,
    traversal_paths: &[TraversalPath],
    ref_paths: &HashMap<String, Vec<TraversalPath>>,
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
            ref_paths
                .get(entity_type)
                .map_or_else(|| traversal_paths.to_vec(), Clone::clone)
        } else {
            Vec::new()
        };

        nodes.push(InputNode {
            id: hydration_node_alias().to_string(),
            entity: Some(entity_type.clone()),
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
            let filter_str = filter.value_str().unwrap_or("");
            match op {
                FilterOp::Eq => cv_str == filter_str,
                FilterOp::Contains | FilterOp::StartsWith | FilterOp::EndsWith => {
                    let (value, needle) =
                        (cv_str.to_ascii_lowercase(), filter_str.to_ascii_lowercase());
                    match op {
                        FilterOp::Contains => value.contains(&needle),
                        FilterOp::StartsWith => value.starts_with(&needle),
                        _ => value.ends_with(&needle),
                    }
                }
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

/// Must run after [`apply_virtual_filters_static`]: filter-injected columns
/// are resolved solely for the filter comparison and were never requested,
/// so they are dropped from the rows that survived it.
pub fn strip_filter_injected_static(result: &mut QueryResult, templates: &[HydrationTemplate]) {
    let col_keys: Vec<String> = templates
        .iter()
        .flat_map(|t| {
            t.filter_injected_columns
                .iter()
                .map(move |col| format!("{}_{col}", t.node_alias))
        })
        .collect();
    if col_keys.is_empty() {
        return;
    }
    for row in result.authorized_rows_mut() {
        for key in &col_keys {
            row.remove_column(key);
        }
    }
}
