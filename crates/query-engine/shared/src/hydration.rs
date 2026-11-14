//! Shared hydration helpers used by both the server and local pipelines.
//! The compile + execute step is left to the caller.

use std::collections::HashMap;

use arrow::record_batch::RecordBatch;
use compiler::constants::{
    hydration_id_column, hydration_node_alias, identity_part_column,
    neighbor_traversal_path_column, traversal_path_column,
};
use compiler::orbit_authorization::{Grants, Key, ScopeSet};
use compiler::{
    DynamicEntityColumns, HydrationArm, HydrationTemplate, Operator, QueryLimits, VirtualFilter,
};
use orbit_arrow::{ArrowUtils, ColumnValue};
use orbit_utils::traversal_path::TraversalPath;
use pipeline::PipelineError;
use query_data_model::catalog::Check;
use query_data_model::{DataType, EntityId, Model, PropertyId};
use types::identity;
use types::{KeyLookup, QueryResult};

pub use crate::content::{PropertyMap, PropertyRow};

/// Tries the PK column first (`_orbit_{alias}_pk`), falling back to the
/// redaction ID column (`_orbit_{alias}_id`).
pub fn collect_static_ids(result: &QueryResult, template: &HydrationTemplate) -> Vec<Key> {
    let Some(node) = result.ctx().get(&template.node_alias).cloned() else {
        return Vec::new();
    };
    let mut ids: Vec<Key> = result
        .authorized_rows()
        .filter_map(|row| row.get_public_id(&node))
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

/// The entity a stored kind value names. Every kind a row carries decodes here, and an unknown
/// value fails the request rather than dropping the row.
pub fn decode_kind(model: &Model, value: &str, column: &str) -> Result<EntityId, PipelineError> {
    model
        .graph()
        .entity_kind(value)
        .map(|entity| entity.id)
        .ok_or_else(|| {
            PipelineError::custom(format!(
                "column {column} holds unknown entity kind {value:?}"
            ))
        })
}

/// When `static_nodes` is provided (e.g. the center node in a neighbors query),
/// their IDs are also collected so properties can be hydrated for them.
pub fn extract_dynamic_refs(
    model: &Model,
    result: &QueryResult,
    static_nodes: &[compiler::RedactionNode],
) -> Result<HashMap<EntityId, Vec<Key>>, PipelineError> {
    let mut refs: HashMap<EntityId, Vec<Key>> = HashMap::new();
    for row in result.authorized_rows() {
        for node_ref in row.dynamic_nodes() {
            let entity = decode_kind(model, &node_ref.entity_type, "type")?;
            refs.entry(entity).or_default().push(node_ref.id.clone());
        }
        for node in static_nodes {
            if let Some(id) = row.get_public_id(node)
                && let Some(entity_type) = row.get_type(node)
            {
                let entity = decode_kind(model, entity_type, &node.type_column)?;
                refs.entry(entity).or_default().push(id);
            }
        }
    }
    for ids in refs.values_mut() {
        ids.sort_unstable();
        ids.dedup();
    }
    Ok(refs)
}

pub fn collect_dynamic_ref_paths(
    model: &Model,
    result: &QueryResult,
    static_nodes: &[compiler::RedactionNode],
) -> Result<HashMap<EntityId, Vec<TraversalPath>>, PipelineError> {
    let mut paths: HashMap<EntityId, Option<Vec<TraversalPath>>> = HashMap::new();
    let mut record = |entity: EntityId, path: Option<String>| {
        let entry = paths.entry(entity).or_insert_with(|| Some(Vec::new()));
        match (entry.as_mut(), path.filter(|p| !p.is_empty())) {
            (Some(list), Some(path)) => list.push(TraversalPath::new_unchecked(path)),
            _ => *entry = None,
        }
    };
    for row in result.authorized_rows() {
        let neighbor_path = row.get_column_string(neighbor_traversal_path_column());
        for node_ref in row.dynamic_nodes() {
            let entity = decode_kind(model, &node_ref.entity_type, "type")?;
            record(entity, neighbor_path.clone());
        }
        for node in static_nodes {
            if row.get_public_id(node).is_some()
                && let Some(entity_type) = row.get_type(node)
            {
                let entity = decode_kind(model, entity_type, &node.type_column)?;
                record(
                    entity,
                    row.get_column_string(&traversal_path_column(&node.alias)),
                );
            }
        }
    }
    Ok(paths
        .into_iter()
        .filter_map(|(entity, list)| {
            let mut list = list?;
            list.sort_unstable();
            list.dedup();
            Some((entity, list))
        })
        .collect())
}

/// The subtrees in which the caller holds the entity's permission, which bound its hydration
/// read; a scope answered with keys or with `All` bounds no path, and security still scopes the read.
pub fn granted_paths(
    grants: &Grants,
    model: &Model,
    entity: EntityId,
) -> Result<Vec<TraversalPath>, PipelineError> {
    let access = model.access();
    let declared = access.entity(entity);
    let set = grants
        .scope_set(
            &access.scope(declared.scope).name,
            &access.permission(declared.permission).name,
        )
        .map_err(|error| PipelineError::Authorization(error.to_string()))?;
    Ok(match set {
        ScopeSet::Subtrees(roots) => roots
            .iter()
            .map(|root| TraversalPath::new_unchecked(root.clone()))
            .collect(),
        ScopeSet::All | ScopeSet::Keys(_) => Vec::new(),
    })
}

pub fn key_lookup_arm(
    model: &Model,
    limits: &QueryLimits,
    grants: &Grants,
    lookup: &KeyLookup,
) -> Result<HydrationArm, PipelineError> {
    let entity = decode_kind(model, &lookup.entity, "type")?;
    let graph = model.graph();
    let keys: Vec<PropertyId> = model
        .access()
        .entity(entity)
        .checks
        .iter()
        .filter_map(|check| match check {
            Check::Rows { id, .. } => Some(*id),
            Check::Delegated { .. } => None,
        })
        .filter(|key| lookup.properties.contains(&graph.property(*key).name))
        .collect();
    let scope = granted_paths(grants, model, entity)?;
    hydration_arm(model, limits, entity, &lookup.ids, &keys, scope)
}

/// Hydrated properties keyed by entity name, as the result's row checks read them.
pub fn by_kind_name(
    model: &Model,
    property_map: &PropertyMap,
) -> HashMap<(String, Key), PropertyRow> {
    let graph = model.graph();
    property_map
        .iter()
        .map(|((entity, key), row)| {
            (
                (graph.entity(*entity).name.clone(), key.clone()),
                row.clone(),
            )
        })
        .collect()
}

pub fn hydration_arm(
    model: &Model,
    limits: &QueryLimits,
    entity: EntityId,
    ids: &[Key],
    columns: &[PropertyId],
    traversal_paths: Vec<TraversalPath>,
) -> Result<HydrationArm, PipelineError> {
    HydrationArm::new(model, limits, entity, ids, columns, traversal_paths).map_err(|error| {
        PipelineError::Compile {
            client_safe: false,
            reason: error.rejection_reason(),
            message: error.to_string(),
        }
    })
}

/// `{alias}_props` arrives as a JSON object of strings, so typed fields are re-typed from the model.
pub fn parse_hydration_batches(
    batches: &[RecordBatch],
    model: &Model,
) -> Result<PropertyMap, PipelineError> {
    let alias = hydration_node_alias();
    let entity_type_col = format!("{alias}_entity_type");
    let props_col = format!("{alias}_props");
    let id_col = hydration_id_column();

    let mut result = HashMap::new();
    let mut field_types_cache: HashMap<EntityId, HashMap<String, DataType>> = HashMap::new();
    let graph = model.graph();

    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let row_data = ArrowUtils::extract_row(batch, row_idx);

            let entity_type = row_data
                .iter()
                .find(|(name, _)| name.as_str() == entity_type_col)
                .and_then(|(_, v)| v.as_string().cloned());

            let Some(entity_type) = entity_type else {
                continue;
            };
            let entity = decode_kind(model, &entity_type, &entity_type_col)?;

            let field_types = field_types_cache.entry(entity).or_insert_with(|| {
                graph
                    .entity(entity)
                    .properties
                    .iter()
                    .map(|property| graph.property(*property))
                    .map(|property| (property.name.clone(), property.data_type))
                    .collect()
            });

            let mut props: HashMap<String, ColumnValue> = row_data
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
            let id = match graph.entity(entity).identity.as_slice() {
                [_] => row_data.get(&id_col).and_then(identity),
                parts => parts
                    .iter()
                    .enumerate()
                    .map(|(index, part)| {
                        let value = props.remove(&identity_part_column(&id_col, index))?;
                        identity(&typed_string(value, Some(&graph.property(*part).data_type)))
                    })
                    .collect::<Option<Vec<_>>>()
                    .map(Key::Composite),
            };
            let Some(id) = id else {
                continue;
            };

            result.insert((entity, id), props);
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
    let nodes: Vec<Option<compiler::RedactionNode>> = templates
        .iter()
        .map(|template| result.ctx().get(&template.node_alias).cloned())
        .collect();
    for row in result.authorized_rows_mut() {
        for (template, node) in templates.iter().zip(&nodes) {
            if let Some(id) = node.as_ref().and_then(|node| row.get_public_id(node))
                && let Some(props) = property_map.get(&(template.entity, id))
            {
                for (key, value) in props {
                    let col_name = format!("{}_{key}", template.node_alias);
                    row.set_column(col_name, value.clone());
                }
            }
        }
    }
}

pub fn merge_dynamic_properties(
    model: &Model,
    result: &mut QueryResult,
    property_map: &PropertyMap,
) -> Result<(), PipelineError> {
    for row in result.authorized_rows_mut() {
        for node_ref in row.dynamic_nodes_mut() {
            let entity = decode_kind(model, &node_ref.entity_type, "type")?;
            if let Some(props) = property_map.get(&(entity, node_ref.id.clone())) {
                node_ref.properties = props.clone();
            }
        }
    }
    Ok(())
}

/// Writes row columns named `{alias}_{column}` so formatters can extract them.
pub fn merge_static_node_properties(
    model: &Model,
    result: &mut QueryResult,
    property_map: &PropertyMap,
    static_nodes: &[compiler::RedactionNode],
) -> Result<(), PipelineError> {
    for row in result.authorized_rows_mut() {
        for node in static_nodes {
            let Some(id) = row.get_public_id(node) else {
                continue;
            };
            let Some(entity_type) = row.get_type(node) else {
                continue;
            };
            let entity = decode_kind(model, entity_type, &node.type_column)?;
            if let Some(props) = property_map.get(&(entity, id)) {
                for (key, value) in props {
                    let col_name = format!("{}_{key}", node.alias);
                    row.set_column(col_name, value.clone());
                }
            }
        }
    }
    Ok(())
}

/// Remove columns that were injected as dependencies for virtual column
/// resolvers but not explicitly requested by the user.
pub fn strip_injected_columns<'a>(
    model: &Model,
    property_map: &mut PropertyMap,
    specs: impl Iterator<Item = (EntityId, &'a Vec<PropertyId>)>,
) {
    let graph = model.graph();
    for (entity, injected) in specs {
        if injected.is_empty() {
            continue;
        }
        for ((et, _), props) in property_map.iter_mut() {
            if *et == entity {
                for property in injected {
                    props.remove(&graph.property(*property).name);
                }
            }
        }
    }
}

pub fn hydrate_static(
    model: &Model,
    limits: &QueryLimits,
    templates: &[HydrationTemplate],
    query_result: &QueryResult,
) -> Result<Vec<HydrationArm>, PipelineError> {
    let mut arms = Vec::new();

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

        arms.push(hydration_arm(
            model,
            limits,
            template.entity,
            &ids,
            &template.columns,
            traversal_paths,
        )?);
    }

    Ok(arms)
}

/// Uses pre-resolved column specs from the compilation plan; no model
/// lookups at runtime.
///
/// `traversal_paths` is the authorized scope; `ref_paths` narrows an entity type to the paths
/// its own rows carry. A neighbor can sit above the center node, so the center path is no fallback.
pub fn hydrate_dynamic(
    model: &Model,
    limits: &QueryLimits,
    entity_specs: &[DynamicEntityColumns],
    refs: &HashMap<EntityId, Vec<Key>>,
    grants: &Grants,
    ref_paths: &HashMap<EntityId, Vec<TraversalPath>>,
) -> Result<Vec<HydrationArm>, PipelineError> {
    let mut arms = Vec::new();

    for (entity, ids) in refs {
        if ids.is_empty() {
            continue;
        }

        let spec = match entity_specs.iter().find(|s| s.entity == *entity) {
            Some(s) => s,
            None => continue,
        };

        if spec.columns.is_empty() {
            continue;
        }

        let capped_ids: Vec<Key> = ids.iter().take(limits.hydration_rows).cloned().collect();

        let tps = match (spec.has_traversal_path, ref_paths.get(entity)) {
            (false, _) => Vec::new(),
            (true, Some(paths)) => paths.clone(),
            (true, None) => granted_paths(grants, model, *entity)?,
        };

        arms.push(hydration_arm(
            model,
            limits,
            *entity,
            &capped_ids,
            &spec.columns,
            tps,
        )?);
    }

    Ok(arms)
}

fn eval_virtual_filter(value: Option<&ColumnValue>, filter: &VirtualFilter) -> bool {
    let op = filter.op;
    match op {
        Operator::IsNull => value.is_none() || matches!(value, Some(ColumnValue::Null)),
        Operator::IsNotNull => value.is_some() && !matches!(value, Some(ColumnValue::Null)),
        _ => {
            let Some(ColumnValue::String(cv_str)) = value else {
                return false;
            };
            let filter_str = filter.value_str().unwrap_or("");
            match op {
                Operator::Eq => cv_str == filter_str,
                Operator::Contains | Operator::StartsWith | Operator::EndsWith => {
                    let (value, needle) =
                        (cv_str.to_ascii_lowercase(), filter_str.to_ascii_lowercase());
                    match op {
                        Operator::Contains => value.contains(&needle),
                        Operator::StartsWith => value.starts_with(&needle),
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

#[cfg(test)]
mod tests {
    use compiler::orbit_authorization::{Caller, Grants, ScopeSet};
    use query_data_model::Packages;

    use super::{TraversalPath, granted_paths};

    #[test]
    fn lookups_read_within_the_subtrees_the_caller_holds_the_permission_in() {
        let model = Packages::embedded().deployment("server").unwrap();
        let grants = Grants::new(Caller::new("user:1", false, ())).with(
            "namespace",
            "read",
            ScopeSet::Subtrees(vec!["1/9970/".into()]),
        );
        let paths = granted_paths(
            &grants,
            &model,
            model.names().entity("MergeRequest").unwrap(),
        )
        .unwrap();
        assert_eq!(paths, [TraversalPath::new_unchecked("1/9970/".to_string())]);
        let unanswered = granted_paths(
            &grants,
            &model,
            model.names().entity("Vulnerability").unwrap(),
        )
        .unwrap_err();
        assert!(
            unanswered.to_string().contains("no scope answer"),
            "{unanswered}"
        );
    }
}
