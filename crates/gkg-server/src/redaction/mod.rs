mod extractor;
mod query_result;
mod stream;
mod types;
mod validator;

pub use extractor::RedactionExtractor;
pub use query_result::{ColumnValue, NodeRef, QueryResult, QueryResultRow, RedactableNodes};
pub use stream::{RedactionExchangeError, RedactionExchangeResult, RedactionService};
pub use types::{ResourceAuthorization, ResourceCheck};
pub use validator::{SchemaValidationError, SchemaValidator};

use serde_json::Value;

pub struct ResourceExtractor;

impl ResourceExtractor {
    pub fn extract(result: &Value) -> Vec<ResourceCheck> {
        let mut checks = Vec::new();

        if let Some(rows) = result.as_array() {
            let mut project_ids = Vec::new();
            let mut issue_ids = Vec::new();
            let mut mr_ids = Vec::new();
            let mut user_ids = Vec::new();
            let mut group_ids = Vec::new();

            for row in rows {
                if let Some(id) = row.get("project_id").and_then(|v| v.as_i64()) {
                    project_ids.push(id);
                }
                if let Some(id) = row.get("issue_id").and_then(|v| v.as_i64()) {
                    issue_ids.push(id);
                }
                if let Some(id) = row.get("merge_request_id").and_then(|v| v.as_i64()) {
                    mr_ids.push(id);
                }
                if let Some(id) = row.get("user_id").and_then(|v| v.as_i64()) {
                    user_ids.push(id);
                }
                if let Some(id) = row.get("group_id").and_then(|v| v.as_i64()) {
                    group_ids.push(id);
                }
            }

            if !project_ids.is_empty() {
                checks.push(ResourceCheck {
                    resource_type: "projects".to_string(),
                    ids: project_ids,
                    ability: "read_project".to_string(),
                });
            }
            if !issue_ids.is_empty() {
                checks.push(ResourceCheck {
                    resource_type: "issues".to_string(),
                    ids: issue_ids,
                    ability: "read_issue".to_string(),
                });
            }
            if !mr_ids.is_empty() {
                checks.push(ResourceCheck {
                    resource_type: "merge_requests".to_string(),
                    ids: mr_ids,
                    ability: "read_merge_request".to_string(),
                });
            }
            if !user_ids.is_empty() {
                checks.push(ResourceCheck {
                    resource_type: "users".to_string(),
                    ids: user_ids,
                    ability: "read_user".to_string(),
                });
            }
            if !group_ids.is_empty() {
                checks.push(ResourceCheck {
                    resource_type: "groups".to_string(),
                    ids: group_ids,
                    ability: "read_group".to_string(),
                });
            }
        }

        checks
    }
}
