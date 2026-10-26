#[cfg(test)]
mod schema_contract;

use crate::error::Result;
use crate::input::Input;
use crate::passes::validate;
use query_data_model::OrbitQueryModel;

pub fn parse(json: &str, model: &impl OrbitQueryModel) -> Result<Input> {
    let value: serde_json::Value = serde_json::from_str(json)?;
    validate::collect_schema_errors(validate::base_validator(), validate::base_schema(), &value)?;
    let schema = derive_json_schema(model)?;
    let validator = jsonschema::validator_for(&schema).map_err(|error| {
        crate::error::QueryError::Validation(format!("invalid derived schema: {error}"))
    })?;
    validate::collect_schema_errors(&validator, &schema, &value).map_err(|error| match error {
        crate::error::QueryError::Validation(message) => {
            crate::error::QueryError::AllowlistRejected(message)
        }
        other => other,
    })?;
    let mut input: Input = serde_json::from_value(value)?;
    input.lift_negated_filters()?;
    input.promote_id_filters()?;
    Ok(input)
}

fn derive_json_schema(model: &impl OrbitQueryModel) -> Result<serde_json::Value> {
    let mut schema: serde_json::Value = serde_json::from_str(validate::BASE_SCHEMA_JSON)?;
    let definitions = &mut schema["$defs"];
    definitions["EntityType"]["enum"] = model
        .graph()
        .entities()
        .map(|entity| entity.name.clone())
        .collect();
    definitions["RelationshipTypeName"]["enum"] = model
        .graph()
        .relationships()
        .map(|relationship| relationship.name.clone())
        .chain(std::iter::once("*".into()))
        .collect();
    let selectors = model.graph().entities().map(|entity| {
        let allowlist = |filterable_only: bool| {
            let mut listed: Vec<_> = ontology::constants::NODE_RESERVED_COLUMNS
                .iter()
                .map(|name| (*name).to_string())
                .collect();
            for id in &entity.properties {
                let property = model.graph().property(*id);
                if model.property_is_hidden(*id)
                    || filterable_only
                        && !model.property_is_filterable(&entity.name, &property.name)
                {
                    continue;
                }
                listed.push(property.name.clone());
            }
            serde_json::json!({"enum": listed})
        };
        serde_json::json!({
            "if": { "properties": { "entity": { "const": entity.name } } },
            "then": {
                "properties": {
                    "columns": {
                        "oneOf": [
                            { "const": "*" },
                            { "type": "array", "items": allowlist(false), "minItems": 1 }
                        ]
                    },
                    "filters": { "propertyNames": allowlist(true) },
                    "not": { "propertyNames": allowlist(true) }
                }
            }
        })
    });
    definitions["NodeSelector"]["allOf"] = selectors.collect();
    Ok(schema)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn accepts_only_visible_columns_and_filterable_fields() {
        let ontology = Arc::new(ontology::Ontology::load_embedded().unwrap());
        let model = query_data_model::ClickHouseDataModel::derive(ontology.clone()).unwrap();
        for entity in model.graph().entities() {
            for property in entity
                .properties
                .iter()
                .map(|id| model.graph().property(*id).name.as_str())
                .chain(std::iter::once("unknown_property"))
            {
                let declared = ontology
                    .get_node(&entity.name)
                    .unwrap()
                    .fields
                    .iter()
                    .find(|field| field.name == property);
                let reserved = ontology::constants::NODE_RESERVED_COLUMNS.contains(&property);
                let visible = declared.filter(|field| !field.hidden);
                for (selector, expected) in [
                    (
                        serde_json::json!({"columns": [property]}),
                        reserved || visible.is_some(),
                    ),
                    (
                        serde_json::json!({"filters": {property: "value"}}),
                        reserved || visible.is_some_and(|field| field.filterable),
                    ),
                ] {
                    let mut node =
                        serde_json::json!({"id": "n", "entity": entity.name, "node_ids": [1]});
                    node.as_object_mut()
                        .unwrap()
                        .extend(selector.as_object().unwrap().clone());
                    let query = serde_json::json!({"query_type": "traversal", "nodes": [node], "limit": 10});
                    assert_eq!(
                        parse(&query.to_string(), &model).is_ok(),
                        expected,
                        "{query}"
                    );
                }
            }
        }
    }
}
