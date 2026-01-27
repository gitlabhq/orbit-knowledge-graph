use serde_json::{Value, json};

use crate::redaction::{RedactionFilter, ResourceAuthorization};

#[derive(Debug, Clone, Default)]
pub struct ContextEngine;

impl ContextEngine {
    pub fn new() -> Self {
        Self
    }

    pub fn prepare_response(&self, raw_result: Value) -> Value {
        self.optimize_for_llm(raw_result)
    }

    pub fn apply_redaction_and_prepare(
        &self,
        raw_result: Value,
        authorizations: &[ResourceAuthorization],
    ) -> Value {
        let (redacted, redacted_count) = RedactionFilter::apply(raw_result, authorizations);

        if redacted_count == 0 {
            return self.optimize_for_llm(redacted);
        }

        self.add_redaction_metadata(redacted, redacted_count)
    }

    fn add_redaction_metadata(&self, mut result: Value, redacted_count: usize) -> Value {
        if let Value::Object(ref mut map) = result {
            map.insert(
                "_context".to_string(),
                json!({
                    "redacted_count": redacted_count,
                    "note": "Some results were filtered due to access permissions"
                }),
            );
        }
        self.optimize_for_llm(result)
    }

    fn optimize_for_llm(&self, value: Value) -> Value {
        value
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_prepare_response() {
        let engine = ContextEngine::new();
        let input = json!({"nodes": [{"id": 1}]});
        let result = engine.prepare_response(input.clone());
        assert_eq!(result, input);
    }

    #[test]
    fn test_apply_redaction_adds_metadata() {
        let engine = ContextEngine::new();
        let input = json!({
            "nodes": [
                {"id": 101, "type": "gl_issue"},
                {"id": 102, "type": "gl_issue"}
            ]
        });

        let mut auth = HashMap::new();
        auth.insert(101, true);
        auth.insert(102, false);

        let authorizations = vec![ResourceAuthorization {
            resource_type: "issues".to_string(),
            authorized: auth,
        }];

        let result = engine.apply_redaction_and_prepare(input, &authorizations);

        let context = result.get("_context").unwrap();
        assert_eq!(context.get("redacted_count").unwrap().as_u64().unwrap(), 1);
    }

    #[test]
    fn test_no_metadata_when_all_authorized() {
        let engine = ContextEngine::new();
        let input = json!({"nodes": [{"id": 101, "type": "gl_issue"}]});

        let mut auth = HashMap::new();
        auth.insert(101, true);

        let authorizations = vec![ResourceAuthorization {
            resource_type: "issues".to_string(),
            authorized: auth,
        }];

        let result = engine.apply_redaction_and_prepare(input, &authorizations);
        assert!(result.get("_context").is_none());
    }
}
