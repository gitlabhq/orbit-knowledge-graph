//! Input normalization.
//!
//! Transforms validated input into a canonical form before lowering.
//! After normalization:
//! - Entity names are resolved to table names
//! - Filter values are coerced to match ontology types
//! - Wildcard column selections are expanded to explicit column lists

use crate::error::{QueryError, Result};
use crate::input::{ColumnSelection, Direction, EntityAuthConfig, Input, QueryType, TextIndexMeta};
use crate::passes::hydrate::VirtualColumnRequest;
use ontology::constants::DEFAULT_PRIMARY_KEY;
use ontology::{EnumType, Ontology};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};

/// Build the entity auth map for every entity type in the ontology that has a
/// redaction config. This is the single source of truth consumed by both the
/// compilation pipeline (via `normalize`) and tests that construct `ResultContext`
/// directly without going through `compile()`.
pub fn build_entity_auth(ontology: &Ontology) -> HashMap<String, EntityAuthConfig> {
    let owners: std::collections::HashMap<&str, &str> = ontology
        .nodes()
        .filter_map(|n| {
            n.redaction.as_ref().and_then(|r| {
                if r.id_column == DEFAULT_PRIMARY_KEY {
                    Some((r.resource_type.as_str(), n.name.as_str()))
                } else {
                    None
                }
            })
        })
        .collect();

    ontology
        .nodes()
        .filter_map(|n| {
            n.redaction.as_ref().map(|r| {
                let owner_entity = if r.id_column != DEFAULT_PRIMARY_KEY {
                    owners.get(r.resource_type.as_str()).map(|&s| s.to_string())
                } else {
                    None
                };
                (
                    n.name.clone(),
                    EntityAuthConfig {
                        resource_type: r.resource_type.clone(),
                        ability: r.ability.clone(),
                        auth_id_column: r.id_column.clone(),
                        owner_entity,
                        required_access_level: r.required_role.as_access_level(),
                    },
                )
            })
        })
        .collect()
}

/// Normalize validated input.
///
/// Performs the following transformations:
/// - Resolves entity names to ClickHouse table names
/// - Coerces filter values to match ontology field types (e.g., enum int → string)
/// - Expands wildcard column selections ("*") to explicit column lists
pub fn normalize(mut input: Input, ontology: &Ontology) -> Result<Input> {
    input.entity_auth = build_entity_auth(ontology);
    input.compiler.edge_tables = ontology
        .edge_tables()
        .into_iter()
        .map(|s| s.to_string())
        .collect();
    input.compiler.default_edge_table = ontology.edge_table().to_string();
    input.compiler.edge_table_for_rel = ontology
        .edge_names()
        .map(|name| {
            (
                name.to_string(),
                ontology.edge_table_for_relationship(name).to_string(),
            )
        })
        .collect();

    // Populate text index metadata from the ontology's StorageIndex entries.
    for node_entity in ontology.nodes() {
        for idx in &node_entity.storage.indexes {
            if let Some(tokenizer) = ontology.text_index_tokenizer(&node_entity.name, &idx.column) {
                input.compiler.text_indexes.insert(
                    (node_entity.destination_table.clone(), idx.column.clone()),
                    TextIndexMeta {
                        tokenizer: tokenizer.to_string(),
                    },
                );
            }
        }
    }

    for node in &mut input.nodes {
        let Some(entity) = node.entity.as_deref() else {
            continue;
        };

        node.table = Some(
            ontology
                .table_name(entity)
                .map_err(|_| {
                    QueryError::AllowlistRejected(format!(
                        "entity '{entity}' passed schema validation but has no table mapping"
                    ))
                })?
                .to_owned(),
        );

        let node_entity = ontology.get_node(entity).ok_or_else(|| {
            QueryError::AllowlistRejected(format!(
                "entity '{entity}' passed schema validation but is not in the ontology"
            ))
        })?;

        node.redaction_id_column = node_entity
            .redaction
            .as_ref()
            .map(|r| r.id_column.clone())
            .unwrap_or_else(|| DEFAULT_PRIMARY_KEY.to_string());

        // Expand column selections to explicit lists. Strip virtual columns
        // into node.virtual_columns for the hydration plan.
        // PathFinding/Neighbors handle virtuals in build_dynamic_specs.
        let strip_virtual = !matches!(
            input.query_type,
            QueryType::PathFinding | QueryType::Neighbors
        );
        match &mut node.columns {
            Some(ColumnSelection::All) => {
                let columns: Vec<String> =
                    node_entity.fields.iter().map(|f| f.name.clone()).collect();
                node.columns = Some(ColumnSelection::List(columns));
            }
            Some(ColumnSelection::List(_)) => {}
            None => {
                let columns = if node_entity.default_columns.is_empty() {
                    node_entity.fields.iter().map(|f| f.name.clone()).collect()
                } else {
                    node_entity.default_columns.clone()
                };
                node.columns = Some(ColumnSelection::List(columns));
            }
        }

        if strip_virtual && let Some(ColumnSelection::List(cols)) = &mut node.columns {
            let mut virtual_cols = Vec::new();
            cols.retain(|col_name| {
                if let Some(field) = node_entity.fields.iter().find(|f| f.name == *col_name)
                    && let ontology::FieldSource::Virtual(vs) = &field.source
                {
                    if !vs.disabled {
                        virtual_cols.push(VirtualColumnRequest {
                            column_name: col_name.clone(),
                            service: vs.service.clone(),
                            lookup: vs.lookup.clone(),
                        });
                    }
                    return false;
                }
                true
            });
            node.virtual_columns = virtual_cols;
        }

        // Coerce filter values to match ontology field types (e.g., enum int → string)
        for (column, filter) in &mut node.filters {
            let Some(value) = &filter.value else {
                continue;
            };
            let Some(field) = node_entity.fields.iter().find(|f| f.name == *column) else {
                continue;
            };
            // Only coerce int-based enums; string enums are already strings in the source
            if field.enum_type != EnumType::Int {
                continue;
            }
            let Some(enum_values) = field.enum_values.as_ref() else {
                continue;
            };
            filter.value = Some(coerce_value(value, enum_values));
        }
    }
    infer_wildcard_relationship_kinds(&mut input, ontology);
    Ok(input)
}

fn is_wildcard(types: &[String]) -> bool {
    types.is_empty() || (types.len() == 1 && types[0] == "*")
}

fn infer_wildcard_relationship_kinds(input: &mut Input, ontology: &Ontology) {
    let entity_for: HashMap<&str, &str> = input
        .nodes
        .iter()
        .filter_map(|n| Some((n.id.as_str(), n.entity.as_deref()?)))
        .collect();
    let infer = |direction, outgoing, incoming| match direction {
        Direction::Outgoing => ontology.relationship_kinds_matching([outgoing]),
        Direction::Incoming => ontology.relationship_kinds_matching([incoming]),
        Direction::Both => ontology.relationship_kinds_matching([outgoing, incoming]),
    };

    for rel in &mut input.relationships {
        let Some((from_entity, to_entity)) = entity_for
            .get(rel.from.as_str())
            .copied()
            .zip(entity_for.get(rel.to.as_str()).copied())
        else {
            continue;
        };
        specialize_wildcard(
            &mut rel.types,
            infer(
                rel.direction,
                (Some(from_entity), Some(to_entity)),
                (Some(to_entity), Some(from_entity)),
            ),
        );
    }

    if let Some(neighbors) = input.neighbors.as_mut()
        && let Some(center_entity) = entity_for.get(neighbors.node.as_str()).copied()
    {
        specialize_wildcard(
            &mut neighbors.rel_types,
            infer(
                neighbors.direction,
                (Some(center_entity), None),
                (None, Some(center_entity)),
            ),
        );
    }

    if let Some(path) = input.path.as_mut()
        && is_wildcard(&path.rel_types)
    {
        if let Some(start_entity) = entity_for.get(path.from.as_str()).copied() {
            path.forward_first_hop_rel_types =
                ontology.relationship_kinds_matching([(Some(start_entity), None)]);
        }
        if let Some(end_entity) = entity_for.get(path.to.as_str()).copied() {
            path.backward_first_hop_rel_types =
                ontology.relationship_kinds_matching([(None, Some(end_entity))]);
        }
    }
}

fn specialize_wildcard(types: &mut Vec<String>, inferred: Vec<String>) {
    if is_wildcard(types) && !inferred.is_empty() {
        *types = inferred;
    }
}

fn coerce_value(value: &Value, enum_values: &BTreeMap<i64, String>) -> Value {
    match value {
        Value::Number(n) => {
            if let Some(key) = n.as_i64()
                && let Some(label) = enum_values.get(&key)
            {
                return Value::String(label.clone());
            }
            value.clone()
        }
        Value::Array(arr) => {
            let coerced: Vec<Value> = arr.iter().map(|v| coerce_value(v, enum_values)).collect();
            Value::Array(coerced)
        }
        _ => value.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::parse_input;
    use serde_json::json;

    fn normalize_query(json: &str) -> Input {
        let input = parse_input(json).unwrap();
        let ontology = Ontology::load_embedded().unwrap();
        normalize(input, &ontology).unwrap()
    }

    #[test]
    fn enum_coercion_all_variants() {
        // Single int → string
        let r = normalize_query(
            r#"{"query_type": "traversal", "node": {"id": "mr", "entity": "MergeRequest", "filters": {"state": 1}}}"#,
        );
        assert_eq!(
            r.nodes[0].filters.get("state").unwrap().value,
            Some(json!("opened"))
        );

        // Array of ints → array of strings
        let r = normalize_query(
            r#"{"query_type": "traversal", "node": {"id": "mr", "entity": "MergeRequest", "filters": {"state": {"op": "in", "value": [1, 2, 3, 4]}}}}"#,
        );
        assert_eq!(
            r.nodes[0].filters.get("state").unwrap().value,
            Some(json!(["opened", "closed", "merged", "locked"]))
        );

        // Mixed valid/invalid ints in array - unknown values pass through
        let r = normalize_query(
            r#"{"query_type": "traversal", "node": {"id": "mr", "entity": "MergeRequest", "filters": {"state": {"op": "in", "value": [1, 999, 3]}}}}"#,
        );
        assert_eq!(
            r.nodes[0].filters.get("state").unwrap().value,
            Some(json!(["opened", 999, "merged"]))
        );

        // String values pass through unchanged
        let r = normalize_query(
            r#"{"query_type": "traversal", "node": {"id": "mr", "entity": "MergeRequest", "filters": {"state": "opened"}}}"#,
        );
        assert_eq!(
            r.nodes[0].filters.get("state").unwrap().value,
            Some(json!("opened"))
        );

        // Unknown int passes through
        let r = normalize_query(
            r#"{"query_type": "traversal", "node": {"id": "mr", "entity": "MergeRequest", "filters": {"state": 999}}}"#,
        );
        assert_eq!(
            r.nodes[0].filters.get("state").unwrap().value,
            Some(json!(999))
        );

        // Null filter value (is_null op) unchanged
        let r = normalize_query(
            r#"{"query_type": "traversal", "node": {"id": "mr", "entity": "MergeRequest", "filters": {"state": {"op": "is_null"}}}}"#,
        );
        assert_eq!(r.nodes[0].filters.get("state").unwrap().value, None);
    }

    #[test]
    fn full_traversal_normalization() {
        let result = normalize_query(
            r#"{
                "query_type": "traversal",
                "nodes": [
                    {"id": "u", "entity": "User", "filters": {"username": "admin", "id": 42}},
                    {"id": "mr", "entity": "MergeRequest", "filters": {"state": 3, "draft": false, "title": {"op": "contains", "value": "fix"}}},
                    {"id": "p", "entity": "Pipeline", "filters": {"source": 10, "failure_reason": 1}},
                    {"id": "wi", "entity": "WorkItem", "filters": {"state": 2, "work_item_type": 8}},
                    {"id": "n"}
                ],
                "relationships": [
                    {"type": "AUTHORED", "from": "u", "to": "mr"},
                    {"type": "TRIGGERED", "from": "mr", "to": "p"}
                ]
            }"#,
        );

        // User: table resolved, non-enum filters unchanged
        assert_eq!(result.nodes[0].table, Some("gl_user".into()));
        assert_eq!(
            result.nodes[0].filters.get("username").unwrap().value,
            Some(json!("admin"))
        );
        assert_eq!(
            result.nodes[0].filters.get("id").unwrap().value,
            Some(json!(42))
        );

        // MergeRequest: table + enum coercion + non-enum passthrough
        assert_eq!(result.nodes[1].table, Some("gl_merge_request".into()));
        assert_eq!(
            result.nodes[1].filters.get("state").unwrap().value,
            Some(json!("merged"))
        );
        assert_eq!(
            result.nodes[1].filters.get("draft").unwrap().value,
            Some(json!(false))
        );
        assert_eq!(
            result.nodes[1].filters.get("title").unwrap().value,
            Some(json!("fix"))
        );

        // Pipeline: multiple enum fields coerced
        assert_eq!(result.nodes[2].table, Some("gl_pipeline".into()));
        assert_eq!(
            result.nodes[2].filters.get("source").unwrap().value,
            Some(json!("merge_request_event"))
        );
        assert_eq!(
            result.nodes[2].filters.get("failure_reason").unwrap().value,
            Some(json!("config_error"))
        );

        // WorkItem: different entity with same enum field name (state) + work_item_type
        assert_eq!(result.nodes[3].table, Some("gl_work_item".into()));
        assert_eq!(
            result.nodes[3].filters.get("state").unwrap().value,
            Some(json!("closed"))
        );
        assert_eq!(
            result.nodes[3].filters.get("work_item_type").unwrap().value,
            Some(json!("epic"))
        );

        // Node without entity: no table
        assert_eq!(result.nodes[4].table, None);
    }

    #[test]
    fn edge_cases() {
        // Unknown field on known entity - unchanged
        let r = normalize_query(
            r#"{"query_type": "traversal", "node": {"id": "mr", "entity": "MergeRequest", "filters": {"nonexistent_field": 42}}}"#,
        );
        assert_eq!(
            r.nodes[0].filters.get("nonexistent_field").unwrap().value,
            Some(json!(42))
        );

        // Non-enum int field not coerced (User.id is int, not enum)
        let r = normalize_query(
            r#"{"query_type": "traversal", "node": {"id": "u", "entity": "User", "filters": {"id": 1}}}"#,
        );
        assert_eq!(r.nodes[0].filters.get("id").unwrap().value, Some(json!(1)));

        // Boolean field unchanged
        let r = normalize_query(
            r#"{"query_type": "traversal", "node": {"id": "mr", "entity": "MergeRequest", "filters": {"squash": true}}}"#,
        );
        assert_eq!(
            r.nodes[0].filters.get("squash").unwrap().value,
            Some(json!(true))
        );

        // String array on non-enum field unchanged
        let r = normalize_query(
            r#"{"query_type": "traversal", "node": {"id": "mr", "entity": "MergeRequest", "filters": {"source_branch": {"op": "in", "value": ["main", "develop"]}}}}"#,
        );
        assert_eq!(
            r.nodes[0].filters.get("source_branch").unwrap().value,
            Some(json!(["main", "develop"]))
        );

        // Unknown entity rejected
        let input = parse_input(
            r#"{"query_type": "traversal", "node": {"id": "x", "entity": "UnknownEntity", "filters": {"foo": 123}}}"#,
        ).unwrap();
        let ontology = Ontology::load_embedded().unwrap();
        let err = normalize(input, &ontology).unwrap_err();
        assert!(
            matches!(err, QueryError::AllowlistRejected(_)),
            "unknown entity should be AllowlistRejected, got: {err}"
        );
    }
}
