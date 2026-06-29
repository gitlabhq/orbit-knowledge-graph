//! Error formatting for JSON Schema validation rejections.
//!
//! Enriches `jsonschema`'s default error messages so validator rejections list
//! valid candidates and steer the agent toward `get_graph_schema`.

use ontology::errors::format_candidate_list;

/// Render a JSON Schema validation error into an actionable message.
///
/// jsonschema's own `Display` truncates enum rejections to three candidates
/// and renders `oneOf` rejections as opaque prose, both of which leave an agent
/// guessing the valid values. Reading the structured `ValidationErrorKind`
/// recovers the full allowlist instead.
pub(crate) fn format_schema_error(error: &jsonschema::ValidationError<'_>) -> String {
    let path = error.instance_path().to_string();

    if let Some((instance, options)) = extract_enum_options(error) {
        return format_enum_rejection(&instance, options, &path);
    }

    let instance = error.instance().to_string();

    if is_group_by_path(error.instance_path()) {
        return format_group_by_hint(&instance, &path);
    }

    format!("{error} at {path}")
}

/// Try to extract an enum allowlist from an error.
///
/// For `PropertyNames`, the returned instance is the offending key name, not
/// the whole object.
fn extract_enum_options<'a>(
    error: &'a jsonschema::ValidationError<'a>,
) -> Option<(String, &'a serde_json::Value)> {
    use jsonschema::error::ValidationErrorKind;

    match error.kind() {
        ValidationErrorKind::Enum { options } => Some((error.instance().to_string(), options)),
        ValidationErrorKind::PropertyNames { error: inner } => match inner.kind() {
            ValidationErrorKind::Enum { options } => Some((inner.instance().to_string(), options)),
            _ => None,
        },
        ValidationErrorKind::OneOfNotValid { context } => {
            let options = find_inner_enum_options(context)?;
            Some((error.instance().to_string(), options))
        }
        _ => None,
    }
}

fn format_group_by_hint(instance: &str, path: &str) -> String {
    format!(
        "{instance} is not a valid group_by entry at {path}. \
         Each group_by entry must be an object with a \"kind\" field. \
         Expected shapes: by property \
         [{{\"kind\": \"property\", \"node\": \"<node-id>\", \"property\": \"<property>\"}}], \
         or by node [{{\"kind\": \"node\", \"node\": \"<node-id>\"}}]",
    )
}

fn format_enum_rejection(instance: &str, options: &serde_json::Value, path: &str) -> String {
    let candidates = options
        .as_array()
        .map(|opts| format_candidates(opts))
        .unwrap_or_default();
    format!("{instance} is not an allowed value{candidates} at {path}")
}

/// Filter operators nest a `oneOf` inside the property-filter `oneOf`, so the
/// search must recurse. Every `oneOf` the ontology derives today (`columns`,
/// relationship `type`, filter `op`) carries exactly one allowlist enum, so
/// returning the first enum found is unambiguous.
fn find_inner_enum_options<'a>(
    context: &'a [Vec<jsonschema::ValidationError<'a>>],
) -> Option<&'a serde_json::Value> {
    use jsonschema::error::ValidationErrorKind;

    for branch in context {
        for err in branch {
            match err.kind() {
                ValidationErrorKind::Enum { options } => return Some(options),
                ValidationErrorKind::OneOfNotValid { context: inner }
                | ValidationErrorKind::AnyOf { context: inner } => {
                    if let Some(found) = find_inner_enum_options(inner) {
                        return Some(found);
                    }
                }
                _ => {}
            }
        }
    }
    None
}

fn format_candidates(options: &[serde_json::Value]) -> String {
    use std::collections::HashSet;

    if options.is_empty() {
        return String::new();
    }

    let mut seen = HashSet::new();
    let unique: Vec<String> = options
        .iter()
        .map(|v| match v {
            serde_json::Value::String(s) => s.clone(),
            other => other.to_string(),
        })
        .filter(|c| seen.insert(c.clone()))
        .collect();

    format_candidate_list("values", &unique, None)
}

/// Uses the structured segment iterator rather than matching the `Display`
/// string, so a jsonschema serialization change can't silently fall through.
fn is_group_by_path(path: &jsonschema::paths::Location) -> bool {
    use jsonschema::paths::LocationSegment;
    let mut segments = path.into_iter();
    matches!(
        (segments.next(), segments.next(), segments.next()),
        (
            Some(LocationSegment::Property(key)),
            Some(LocationSegment::Index(_)),
            None,
        ) if key.as_ref() == "group_by"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enum_candidates_are_capped_and_deduplicated() {
        let options: Vec<serde_json::Value> = ["id", "id", "iid", "title"]
            .iter()
            .map(|s| serde_json::Value::String((*s).to_string()))
            .collect();
        let rendered = format_candidates(&options);
        assert_eq!(rendered, ". Valid values: id, iid, title", "{rendered}");

        let many: Vec<serde_json::Value> = (0..25)
            .map(|i| serde_json::Value::String(format!("c{i}")))
            .collect();
        let rendered = format_candidates(&many);
        assert!(rendered.contains("Valid values include:"), "{rendered}");
        assert!(rendered.contains("and 15 more"), "{rendered}");
        assert!(rendered.contains("get_graph_schema"), "{rendered}");
    }

    #[test]
    fn group_by_path_matches_only_group_by_array_elements() {
        use jsonschema::paths::{Location, LocationSegment};
        use std::borrow::Cow;

        let prop = |s: &'static str| LocationSegment::Property(Cow::Borrowed(s));
        let idx = LocationSegment::Index;

        let mk = |segs: Vec<LocationSegment<'static>>| segs.into_iter().collect::<Location>();

        assert!(is_group_by_path(&mk(vec![prop("group_by"), idx(0)])));
        assert!(is_group_by_path(&mk(vec![prop("group_by"), idx(12)])));

        assert!(!is_group_by_path(&mk(vec![prop("group_by")])));
        assert!(!is_group_by_path(&mk(vec![
            prop("group_by"),
            idx(0),
            prop("kind")
        ])));
        assert!(!is_group_by_path(&mk(vec![prop("node"), prop("filters")])));
        assert!(!is_group_by_path(&mk(vec![prop("group_by_extra"), idx(0)])));
    }

    #[test]
    fn one_of_rejection_unwraps_to_inner_enum() {
        let schema = serde_json::json!({
            "oneOf": [
                { "const": "*" },
                { "oneOf": [ { "enum": ["alpha", "beta", "gamma"] } ] }
            ]
        });
        let validator = jsonschema::validator_for(&schema).expect("valid schema");
        let value = serde_json::json!("delta");
        let err = validator
            .iter_errors(&value)
            .next()
            .expect("delta must be rejected");
        let msg = format_schema_error(&err);
        assert!(msg.contains("is not an allowed value"), "{msg}");
        assert!(msg.contains("alpha, beta, gamma"), "{msg}");
    }

    #[test]
    fn property_names_non_enum_fallback() {
        let schema = serde_json::json!({
            "type": "object",
            "propertyNames": { "minLength": 3 }
        });
        let validator = jsonschema::validator_for(&schema).expect("valid schema");
        let value = serde_json::json!({"ab": 1});
        let err = validator
            .iter_errors(&value)
            .next()
            .expect("short key must be rejected");
        let msg = format_schema_error(&err);
        assert!(msg.contains("ab"), "{msg}");
        assert!(msg.contains("at"), "{msg}");
    }
}
