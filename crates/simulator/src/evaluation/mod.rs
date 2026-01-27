//! Query evaluation framework for correctness testing.
//!
//! This module loads SDLC queries, samples valid parameter values from the database,
//! executes queries, and collects statistics.

mod error;
mod executor;
mod report;
mod sampler;

pub use error::{ErrorCategory, ParsedError};
pub use executor::{ExecutionResult, QueryExecutor, SampleRow};
pub use report::{Report, ReportFormat};
pub use sampler::ParameterSampler;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

/// A single query definition from the SDLC queries file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryDefinition {
    pub query_type: String,
    pub nodes: Vec<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub relationships: Vec<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub aggregations: Vec<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order_by: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aggregation_sort: Option<serde_json::Value>,
}

/// Load queries from a JSON file.
pub fn load_queries(path: &Path) -> Result<HashMap<String, QueryDefinition>> {
    let content = std::fs::read_to_string(path)?;
    let queries: HashMap<String, QueryDefinition> = serde_json::from_str(&content)?;
    Ok(queries)
}

/// Parameters extracted from a query that need sampling.
#[derive(Debug, Clone, Default)]
pub struct QueryParameters {
    /// Entity type -> list of node_ids fields that need sampling
    pub node_ids: HashMap<String, Vec<String>>,
}

/// Extract parameters that need sampling from a query definition.
pub fn extract_parameters(query: &QueryDefinition) -> QueryParameters {
    let mut params = QueryParameters::default();

    for node in &query.nodes {
        if let Some(obj) = node.as_object() {
            // Check if this node has node_ids that are placeholders
            if obj.contains_key("node_ids")
                && let Some(entity) = obj.get("entity").and_then(|e| e.as_str())
                && let Some(id) = obj.get("id").and_then(|i| i.as_str())
            {
                // Store the node alias and entity type
                params
                    .node_ids
                    .entry(entity.to_string())
                    .or_default()
                    .push(id.to_string());
            }
        }
    }

    params
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_parameters() {
        let query: QueryDefinition = serde_json::from_str(
            r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "mr", "entity": "MergeRequest", "filters": {"state": "merged"}},
                {"id": "merger", "entity": "User", "node_ids": [42]}
            ],
            "relationships": [
                {"type": "MERGED_BY", "from": "mr", "to": "merger"}
            ],
            "limit": 30
        }"#,
        )
        .unwrap();

        let params = extract_parameters(&query);
        assert!(params.node_ids.contains_key("User"));
        assert_eq!(params.node_ids["User"], vec!["merger".to_string()]);
    }
}
