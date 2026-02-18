//! Input normalization.
//!
//! Transforms validated input into a canonical form before lowering.
//! After normalization:
//! - Entity names are resolved to table names
//! - Filter values are coerced to match ontology types
//! - Wildcard column selections are expanded to explicit column lists

use crate::input::{ColumnSelection, Input};
use ontology::{EnumType, Ontology, NODE_RESERVED_COLUMNS};
use serde_json::Value;
use std::collections::BTreeMap;

/// Normalize validated input.
///
/// Performs the following transformations:
/// - Resolves entity names to ClickHouse table names
/// - Coerces filter values to match ontology field types (e.g., enum int → string)
/// - Expands wildcard column selections ("*") to explicit column lists
pub fn normalize(mut input: Input, ontology: &Ontology) -> Input {
    for node in &mut input.nodes {
        let Some(entity) = node.entity.as_deref() else {
            continue;
        };

        // Resolve entity to table name
        if let Ok(table) = ontology.table_name(entity) {
            node.table = Some(table);
        }

        let Some(node_entity) = ontology.get_node(entity) else {
            continue;
        };

        // "id" must always be retained, for a list, wildcard, and empty selection.
        match &mut node.columns {
            Some(ColumnSelection::All) => {
                let mut columns: Vec<String> = NODE_RESERVED_COLUMNS
                    .iter()
                    .map(|s| s.to_string())
                    .collect();
                columns.extend(node_entity.fields.iter().map(|f| f.name.clone()));
                node.columns = Some(ColumnSelection::List(columns));
            }
            Some(ColumnSelection::List(cols)) => {
                for reserved in NODE_RESERVED_COLUMNS {
                    if !cols.contains(&reserved.to_string()) {
                        cols.push(reserved.to_string());
                    }
                }
            }
            None => {
                node.columns = Some(ColumnSelection::List(
                    NODE_RESERVED_COLUMNS
                        .iter()
                        .map(|s| s.to_string())
                        .collect(),
                ));
            }
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
    input
}

fn coerce_value(value: &Value, enum_values: &BTreeMap<i64, String>) -> Value {
    match value {
        Value::Number(n) => {
            if let Some(key) = n.as_i64() {
                if let Some(label) = enum_values.get(&key) {
                    return Value::String(label.clone());
                }
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
        normalize(input, &ontology)
    }

    #[test]
    fn enum_coercion_all_variants() {
        // Single int → string
        let r = normalize_query(
            r#"{"query_type": "search", "node": {"id": "mr", "entity": "MergeRequest", "filters": {"state": 1}}}"#,
        );
        assert_eq!(
            r.nodes[0].filters.get("state").unwrap().value,
            Some(json!("opened"))
        );

        // Array of ints → array of strings
        let r = normalize_query(
            r#"{"query_type": "search", "node": {"id": "mr", "entity": "MergeRequest", "filters": {"state": {"op": "in", "value": [1, 2, 3, 4]}}}}"#,
        );
        assert_eq!(
            r.nodes[0].filters.get("state").unwrap().value,
            Some(json!(["opened", "closed", "merged", "locked"]))
        );

        // Mixed valid/invalid ints in array - unknown values pass through
        let r = normalize_query(
            r#"{"query_type": "search", "node": {"id": "mr", "entity": "MergeRequest", "filters": {"state": {"op": "in", "value": [1, 999, 3]}}}}"#,
        );
        assert_eq!(
            r.nodes[0].filters.get("state").unwrap().value,
            Some(json!(["opened", 999, "merged"]))
        );

        // String values pass through unchanged
        let r = normalize_query(
            r#"{"query_type": "search", "node": {"id": "mr", "entity": "MergeRequest", "filters": {"state": "opened"}}}"#,
        );
        assert_eq!(
            r.nodes[0].filters.get("state").unwrap().value,
            Some(json!("opened"))
        );

        // Unknown int passes through
        let r = normalize_query(
            r#"{"query_type": "search", "node": {"id": "mr", "entity": "MergeRequest", "filters": {"state": 999}}}"#,
        );
        assert_eq!(
            r.nodes[0].filters.get("state").unwrap().value,
            Some(json!(999))
        );

        // Null filter value (is_null op) unchanged
        let r = normalize_query(
            r#"{"query_type": "search", "node": {"id": "mr", "entity": "MergeRequest", "filters": {"state": {"op": "is_null"}}}}"#,
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
                    {"id": "x", "entity": "UnknownEntity", "filters": {"foo": 123}},
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

        // Unknown entity: no table, filters unchanged
        assert_eq!(result.nodes[4].table, None);
        assert_eq!(
            result.nodes[4].filters.get("foo").unwrap().value,
            Some(json!(123))
        );

        // Node without entity: no table
        assert_eq!(result.nodes[5].table, None);
    }

    #[test]
    fn edge_cases() {
        // Unknown field on known entity - unchanged
        let r = normalize_query(
            r#"{"query_type": "search", "node": {"id": "mr", "entity": "MergeRequest", "filters": {"nonexistent_field": 42}}}"#,
        );
        assert_eq!(
            r.nodes[0].filters.get("nonexistent_field").unwrap().value,
            Some(json!(42))
        );

        // Non-enum int field not coerced (User.id is int, not enum)
        let r = normalize_query(
            r#"{"query_type": "search", "node": {"id": "u", "entity": "User", "filters": {"id": 1}}}"#,
        );
        assert_eq!(r.nodes[0].filters.get("id").unwrap().value, Some(json!(1)));

        // Boolean field unchanged
        let r = normalize_query(
            r#"{"query_type": "search", "node": {"id": "mr", "entity": "MergeRequest", "filters": {"squash": true}}}"#,
        );
        assert_eq!(
            r.nodes[0].filters.get("squash").unwrap().value,
            Some(json!(true))
        );

        // String array on non-enum field unchanged
        let r = normalize_query(
            r#"{"query_type": "search", "node": {"id": "mr", "entity": "MergeRequest", "filters": {"source_branch": {"op": "in", "value": ["main", "develop"]}}}}"#,
        );
        assert_eq!(
            r.nodes[0].filters.get("source_branch").unwrap().value,
            Some(json!(["main", "develop"]))
        );
    }
}
