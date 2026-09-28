use crate::error::{QueryError, Result};
use crate::input::{ColumnSelection, Direction, Input, QueryType};
use ontology::EnumType;
#[cfg(test)]
use ontology::Ontology;
use query_data_model::ClickHouseDataModel;
use query_data_model::EntityAuthConfig;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};

/// Build the entity auth map for every entity type in the ontology that has a
/// redaction config. This is the single source of truth consumed by both the
/// compilation pipeline (via `normalize`) and tests that construct `ResultContext`
/// directly without going through `compile()`.
pub fn build_entity_auth(ontology: &ontology::Ontology) -> HashMap<String, EntityAuthConfig> {
    ClickHouseDataModel::derive(std::sync::Arc::new(ontology.clone()))
        .map(|model| model.authorization().entity_auth().clone())
        .unwrap_or_default()
}

pub fn normalize<M: query_data_model::QueryDataModel>(input: Input, model: &M) -> Result<Input> {
    let mut input = input;
    for node in &mut input.nodes {
        let Some(entity) = node.entity.as_deref() else {
            continue;
        };

        let entity_record = model
            .entity(entity)
            .ok_or_else(|| QueryError::AllowlistRejected(format!("unknown entity '{entity}'")))?;
        if model.entity_table(entity).is_none() {
            return Err(QueryError::AllowlistRejected(format!(
                "entity '{entity}' is not available in this data model"
            )));
        }

        match &mut node.columns {
            Some(ColumnSelection::All) => {
                let columns = model
                    .entity(entity)
                    .expect("entity resolved above")
                    .properties
                    .iter()
                    .map(|property| model.graph().property(*property).name.clone())
                    .collect();
                node.columns = Some(ColumnSelection::List(columns));
            }
            Some(ColumnSelection::List(_)) => {}
            None => {
                let columns = if model.default_properties(entity_record.id).is_empty() {
                    model
                        .entity(entity)
                        .expect("entity resolved above")
                        .properties
                        .iter()
                        .map(|property| model.graph().property(*property).name.clone())
                        .collect()
                } else {
                    model
                        .default_properties(entity_record.id)
                        .iter()
                        .map(|property| model.graph().property(*property).name.clone())
                        .collect()
                };
                node.columns = Some(ColumnSelection::List(columns));
            }
        }

        for (column, filters) in &mut node.filters {
            let Some(property) = model.property(entity, column) else {
                continue;
            };
            // Only coerce int-based enums; string enums are already strings in the source
            if property.enum_type != EnumType::Int {
                continue;
            }
            let Some(enum_values) = property.enum_values.as_ref() else {
                continue;
            };
            for filter in filters {
                let Some(value) = &filter.value else {
                    continue;
                };
                filter.value = Some(coerce_value(value, enum_values));
            }
        }
    }
    infer_wildcard_relationship_kinds(&mut input, model);
    Ok(input)
}

pub(crate) fn is_wildcard(types: &[String]) -> bool {
    types.is_empty() || (types.len() == 1 && types[0] == "*")
}

fn infer_wildcard_relationship_kinds(
    input: &mut Input,
    model: &(impl query_data_model::QueryDataModel + ?Sized),
) {
    let entity_for: HashMap<&str, &str> = input
        .nodes
        .iter()
        .filter_map(|n| Some((n.id.as_str(), n.entity.as_deref()?)))
        .collect();
    let matching = |source: Option<&str>, target: Option<&str>| {
        model.graph().relationship_names(source, target)
    };
    let infer = |direction: Direction,
                 outgoing: (Option<&str>, Option<&str>),
                 incoming: (Option<&str>, Option<&str>)| match direction {
        Direction::Outgoing => matching(outgoing.0, outgoing.1),
        Direction::Incoming => matching(incoming.0, incoming.1),
        Direction::Both => matching(outgoing.0, outgoing.1)
            .into_iter()
            .chain(matching(incoming.0, incoming.1))
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect(),
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

    if input.query_type == QueryType::Neighbors
        && let Some(neighbors) = input.neighbors.as_mut()
        && let [center] = input.nodes.as_slice()
        && let Some(center_entity) = center.entity.as_deref()
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
        let model = crate::data_model::clickhouse(std::sync::Arc::new(ontology)).unwrap();
        normalize(input, model.as_ref()).unwrap()
    }

    #[test]
    fn enum_coercion_all_variants() {
        let r = normalize_query(
            r#"{"query_type": "traversal", "nodes": [{"id": "mr", "entity": "MergeRequest", "filters": {"state": 1}}]}"#,
        );
        assert_eq!(
            r.nodes[0].filters.get("state").unwrap()[0].value,
            Some(json!("opened"))
        );

        let r = normalize_query(
            r#"{"query_type": "traversal", "nodes": [{"id": "mr", "entity": "MergeRequest", "filters": {"state": {"in": [1, 2, 3, 4]}}}]}"#,
        );
        assert_eq!(
            r.nodes[0].filters.get("state").unwrap()[0].value,
            Some(json!(["opened", "closed", "merged", "locked"]))
        );

        // Mixed valid/invalid ints in array - unknown values pass through
        let r = normalize_query(
            r#"{"query_type": "traversal", "nodes": [{"id": "mr", "entity": "MergeRequest", "filters": {"state": {"in": [1, 999, 3]}}}]}"#,
        );
        assert_eq!(
            r.nodes[0].filters.get("state").unwrap()[0].value,
            Some(json!(["opened", 999, "merged"]))
        );

        let r = normalize_query(
            r#"{"query_type": "traversal", "nodes": [{"id": "mr", "entity": "MergeRequest", "filters": {"state": "opened"}}]}"#,
        );
        assert_eq!(
            r.nodes[0].filters.get("state").unwrap()[0].value,
            Some(json!("opened"))
        );

        let r = normalize_query(
            r#"{"query_type": "traversal", "nodes": [{"id": "mr", "entity": "MergeRequest", "filters": {"state": 999}}]}"#,
        );
        assert_eq!(
            r.nodes[0].filters.get("state").unwrap()[0].value,
            Some(json!(999))
        );

        let r = normalize_query(
            r#"{"query_type": "traversal", "nodes": [{"id": "mr", "entity": "MergeRequest", "filters": {"state": {"is_null": true}}}]}"#,
        );
        assert_eq!(r.nodes[0].filters.get("state").unwrap()[0].value, None);
    }

    #[test]
    fn full_traversal_normalization() {
        let result = normalize_query(
            r#"{
                "query_type": "traversal",
                "nodes": [
                    {"id": "u", "entity": "User", "filters": {"username": "admin", "id": 42}},
                    {"id": "mr", "entity": "MergeRequest", "filters": {"state": 3, "draft": false, "title": {"contains": "fix"}}},
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

        assert_eq!(
            result.nodes[0].filters.get("username").unwrap()[0].value,
            Some(json!("admin"))
        );
        assert_eq!(
            result.nodes[0].filters.get("id").unwrap()[0].value,
            Some(json!(42))
        );

        assert_eq!(
            result.nodes[1].filters.get("state").unwrap()[0].value,
            Some(json!("merged"))
        );
        assert_eq!(
            result.nodes[1].filters.get("draft").unwrap()[0].value,
            Some(json!(false))
        );
        assert_eq!(
            result.nodes[1].filters.get("title").unwrap()[0].value,
            Some(json!("fix"))
        );

        assert_eq!(
            result.nodes[2].filters.get("source").unwrap()[0].value,
            Some(json!("merge_request_event"))
        );
        assert_eq!(
            result.nodes[2].filters.get("failure_reason").unwrap()[0].value,
            Some(json!("config_error"))
        );

        assert_eq!(
            result.nodes[3].filters.get("state").unwrap()[0].value,
            Some(json!("closed"))
        );
        assert_eq!(
            result.nodes[3].filters.get("work_item_type").unwrap()[0].value,
            Some(json!("epic"))
        );
    }

    #[test]
    fn edge_cases() {
        let r = normalize_query(
            r#"{"query_type": "traversal", "nodes": [{"id": "mr", "entity": "MergeRequest", "filters": {"nonexistent_field": 42}}]}"#,
        );
        assert_eq!(
            r.nodes[0].filters.get("nonexistent_field").unwrap()[0].value,
            Some(json!(42))
        );

        let r = normalize_query(
            r#"{"query_type": "traversal", "nodes": [{"id": "u", "entity": "User", "filters": {"id": 1}}]}"#,
        );
        assert_eq!(
            r.nodes[0].filters.get("id").unwrap()[0].value,
            Some(json!(1))
        );

        let r = normalize_query(
            r#"{"query_type": "traversal", "nodes": [{"id": "mr", "entity": "MergeRequest", "filters": {"squash": true}}]}"#,
        );
        assert_eq!(
            r.nodes[0].filters.get("squash").unwrap()[0].value,
            Some(json!(true))
        );

        let r = normalize_query(
            r#"{"query_type": "traversal", "nodes": [{"id": "mr", "entity": "MergeRequest", "filters": {"source_branch": {"in": ["main", "develop"]}}}]}"#,
        );
        assert_eq!(
            r.nodes[0].filters.get("source_branch").unwrap()[0].value,
            Some(json!(["main", "develop"]))
        );

        let input = parse_input(
            r#"{"query_type": "traversal", "nodes": [{"id": "x", "entity": "UnknownEntity", "filters": {"foo": 123}}]}"#,
        ).unwrap();
        let ontology = Ontology::load_embedded().unwrap();
        let model = crate::data_model::clickhouse(std::sync::Arc::new(ontology)).unwrap();
        let err = normalize(input, model.as_ref()).unwrap_err();
        assert!(
            matches!(err, QueryError::AllowlistRejected(_)),
            "unknown entity should be AllowlistRejected, got: {err}"
        );
    }
}
