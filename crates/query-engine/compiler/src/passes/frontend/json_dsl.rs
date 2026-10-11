use crate::error::Result;
use crate::input::Input;
use crate::passes::{cursor, validate};
use query_data_model::OrbitQueryModel;

pub fn parse(json: &str, model: &impl OrbitQueryModel) -> Result<(Input, u64)> {
    let value: serde_json::Value = serde_json::from_str(json)?;
    validate::collect_schema_errors(validate::base_validator(), &value)?;
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
    definitions["NodeSelector"]["allOf"] = model.graph().entities().map(|entity| {
        let allowlist = |filterable_only: bool| {
            let mut listed: Vec<_> = ontology::constants::NODE_RESERVED_COLUMNS.iter().map(|name| (*name).to_string()).collect();
            let mut hidden = Vec::new();
            for id in &entity.properties {
                let property = model.graph().property(*id);
                if filterable_only && !model.property_is_filterable(&entity.name, &property.name)
                    && property.name != ontology::TRAVERSAL_PATH_COLUMN { continue; }
                if model.property_is_hidden(*id) { hidden.push(property.name.clone()); }
                else { listed.push(property.name.clone()); }
            }
            serde_json::json!({"anyOf": [{"enum": listed}, {"enum": hidden}]})
        };
        serde_json::json!({
            "if": { "properties": { "entity": { "const": entity.name } } },
            "then": { "properties": {
                "columns": { "oneOf": [ { "const": "*" }, { "type": "array", "items": allowlist(false), "minItems": 1 } ] },
                "filters": { "propertyNames": allowlist(true) }
            } }
        })
    }).collect();
    let validator = jsonschema::validator_for(&schema).map_err(|error| {
        crate::error::QueryError::Validation(format!("invalid derived schema: {error}"))
    })?;
    validate::collect_schema_errors(&validator, &value).map_err(|error| match error {
        crate::error::QueryError::Validation(message) => {
            crate::error::QueryError::AllowlistRejected(message)
        }
        other => other,
    })?;
    let query_hash = cursor::canonical_hash(&value);
    Ok((serde_json::from_value(value)?, query_hash))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn catalog_allowlist_matches_ontology_allowlist() {
        let ontology = Arc::new(ontology::Ontology::load_embedded().unwrap());
        let model = query_data_model::ClickHouseDataModel::derive(ontology.clone()).unwrap();
        let schema = ontology
            .derive_json_schema(validate::BASE_SCHEMA_JSON)
            .unwrap();
        let validator = jsonschema::validator_for(&schema).unwrap();
        for entity in model.graph().entities() {
            for property in entity
                .properties
                .iter()
                .map(|id| model.graph().property(*id).name.as_str())
                .chain(std::iter::once("unknown_property"))
            {
                for selector in [
                    serde_json::json!({"columns": [property]}),
                    serde_json::json!({"filters": {property: "value"}}),
                ] {
                    let mut node =
                        serde_json::json!({"id": "n", "entity": entity.name, "node_ids": [1]});
                    node.as_object_mut()
                        .unwrap()
                        .extend(selector.as_object().unwrap().clone());
                    let query = serde_json::json!({"query_type": "traversal", "nodes": [node], "limit": 10});
                    assert_eq!(
                        parse(&query.to_string(), &model).is_ok(),
                        validator.is_valid(&query),
                        "{query}"
                    );
                }
            }
        }
    }
}
