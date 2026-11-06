use jsonschema::ValidationError;
use jsonschema::error::ValidationErrorKind;
use jsonschema::paths::{Location, LocationSegment};
use ontology::errors::format_candidate_list;
use serde_json::Value;

pub(super) fn describe(error: &ValidationError<'_>, schema: &Value) -> String {
    let path = error.instance_path().to_string();
    if let Some(options) = enum_options(error) {
        return format!(
            "value is not an allowed value{} at {path}",
            candidates(options)
        );
    }
    if is_group_by_entry(error.instance_path()) {
        return format!(
            "invalid group_by entry at {path}. Expected shapes: \"<node-id>\" (group by node), \
             \"<node-id>.<property>\" (group by property), or \
             {{\"key\": \"<node-id>.<property>\", \"truncate\"?: \"<unit>\", \"as\"?: \"<name>\"}}"
        );
    }
    if let Some(description) = one_of_description(error, schema) {
        return if path.is_empty() {
            description.to_string()
        } else {
            format!(
                "invalid value at {path}: {}",
                description.trim_end_matches('.')
            )
        };
    }
    format!("{} at {path}", error.masked())
}

fn one_of_description<'s>(error: &ValidationError<'_>, schema: &'s Value) -> Option<&'s str> {
    if !matches!(error.kind(), ValidationErrorKind::OneOfNotValid { .. }) {
        return None;
    }
    let keyword = error.schema_path().to_string();
    let parent = keyword.strip_suffix("/oneOf")?;
    schema.pointer(parent)?.get("description")?.as_str()
}

fn enum_options<'a>(error: &'a ValidationError<'a>) -> Option<&'a Value> {
    match error.kind() {
        ValidationErrorKind::Enum { options } => Some(options),
        ValidationErrorKind::PropertyNames { error: inner } => match inner.kind() {
            ValidationErrorKind::Enum { options } => Some(options),
            _ => None,
        },
        ValidationErrorKind::OneOfNotValid { context } => inner_enum_options(context),
        _ => None,
    }
}

fn inner_enum_options<'a>(context: &'a [Vec<ValidationError<'a>>]) -> Option<&'a Value> {
    context
        .iter()
        .flatten()
        .find_map(|error| match error.kind() {
            ValidationErrorKind::Enum { options } => Some(options),
            ValidationErrorKind::OneOfNotValid { context }
            | ValidationErrorKind::AnyOf { context } => inner_enum_options(context),
            _ => None,
        })
}

fn candidates(options: &Value) -> String {
    let mut unique: Vec<String> = Vec::new();
    for option in options.as_array().into_iter().flatten() {
        let name = match option {
            Value::String(name) => name.clone(),
            other => other.to_string(),
        };
        if !unique.contains(&name) {
            unique.push(name);
        }
    }
    format_candidate_list("values", &unique)
}

fn is_group_by_entry(path: &Location) -> bool {
    let mut segments = path.into_iter();
    matches!(
        (segments.next(), segments.next(), segments.next()),
        (Some(LocationSegment::Property(key)), Some(LocationSegment::Index(_)), None)
            if key.as_ref() == "group_by"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn first(schema: &Value, value: &Value) -> String {
        let validator = jsonschema::validator_for(schema).expect("valid schema");
        let error = validator.iter_errors(value).next().expect("rejected");
        describe(&error, schema)
    }

    #[test]
    fn enum_rejections_list_the_candidates_without_the_value() {
        let schema = serde_json::json!({
            "properties": { "x": { "oneOf": [{ "const": "*" }, { "enum": ["alpha", "beta", "alpha"] }] } }
        });
        let message = first(&schema, &serde_json::json!({ "x": "private-literal" }));
        assert_eq!(
            message,
            "value is not an allowed value. Valid values: alpha, beta at /x"
        );
    }

    #[test]
    fn group_by_rejections_name_the_shapes_without_the_value() {
        let schema = serde_json::json!({
            "properties": { "group_by": { "items": { "type": "string" } } }
        });
        let message = first(
            &schema,
            &serde_json::json!({ "group_by": [{ "private-literal": 1 }] }),
        );
        assert!(
            message.starts_with("invalid group_by entry at /group_by/0"),
            "{message}"
        );
        assert!(!message.contains("private-literal"), "{message}");
    }

    #[test]
    fn one_of_rejections_report_the_schema_description() {
        let schema = serde_json::json!({
            "properties": {
                "x": {
                    "description": "A number or a boolean.",
                    "oneOf": [{ "type": "number" }, { "type": "boolean" }]
                }
            }
        });
        let message = first(&schema, &serde_json::json!({ "x": "private-literal" }));
        assert_eq!(message, "invalid value at /x: A number or a boolean");
    }

    #[test]
    fn other_rejections_mask_the_value() {
        let schema = serde_json::json!({
            "properties": { "x": { "type": "object", "maxProperties": 1 } }
        });
        let typed = first(&schema, &serde_json::json!({ "x": "private-literal" }));
        assert_eq!(typed, "value is not of type \"object\" at /x");
        let counted = first(
            &schema,
            &serde_json::json!({ "x": { "a": "private-literal", "b": "private-literal" } }),
        );
        assert_eq!(counted, "value has more than 1 property at /x");
    }
}
