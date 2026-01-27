use serde_json::Value;

use super::ResourceAuthorization;

pub struct RedactionFilter;

impl RedactionFilter {
    /// Apply redaction to a result based on authorization.
    /// Returns the filtered value and count of redacted resources.
    ///
    /// TODO: Implement targeted filtering based on known response structure
    /// instead of recursive tree walking.
    pub fn apply(value: Value, authorizations: &[ResourceAuthorization]) -> (Value, usize) {
        let unauthorized_count = authorizations
            .iter()
            .flat_map(|auth| auth.authorized.iter())
            .filter(|(_, authorized)| !**authorized)
            .count();

        // Placeholder: return value unchanged, actual filtering to be implemented
        (value, unauthorized_count)
    }
}

pub fn map_node_type_to_resource(node_type: &str) -> String {
    match node_type {
        "gl_issue" => "issues",
        "gl_mr" | "gl_merge_request" => "merge_requests",
        "gl_project" => "projects",
        "gl_milestone" => "milestones",
        "gl_snippet" => "snippets",
        _ => node_type,
    }
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn test_apply_returns_unchanged_value() {
        let input = json!({"nodes": [{"id": 1}]});
        let authorizations = vec![];

        let (result, count) = RedactionFilter::apply(input.clone(), &authorizations);
        assert_eq!(result, input);
        assert_eq!(count, 0);
    }

    #[test]
    fn test_apply_counts_unauthorized() {
        let input = json!({"nodes": []});

        let mut auth = HashMap::new();
        auth.insert(101, true);
        auth.insert(102, false);
        auth.insert(103, false);

        let authorizations = vec![ResourceAuthorization {
            resource_type: "issues".to_string(),
            authorized: auth,
        }];

        let (_, count) = RedactionFilter::apply(input, &authorizations);
        assert_eq!(count, 2);
    }

    #[test]
    fn test_map_node_type_to_resource() {
        assert_eq!(map_node_type_to_resource("gl_issue"), "issues");
        assert_eq!(map_node_type_to_resource("gl_mr"), "merge_requests");
        assert_eq!(
            map_node_type_to_resource("gl_merge_request"),
            "merge_requests"
        );
        assert_eq!(map_node_type_to_resource("gl_project"), "projects");
        assert_eq!(map_node_type_to_resource("unknown"), "unknown");
    }
}
