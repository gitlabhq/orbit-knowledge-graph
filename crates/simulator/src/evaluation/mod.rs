//! Query evaluation framework for correctness testing.
//!
//! This module loads SDLC queries, samples valid parameter values from the database,
//! executes queries, and collects statistics.

mod error;
mod executor;
mod metadata;
mod report;
mod sampler;

pub use error::{ErrorCategory, ParsedError};
pub use executor::{ExecutionResult, QueryExecutor, SamplingInfo};
pub use metadata::{
    ErrorInfo, QueryMetadata, QueryMetadataBuilder, QueryPlan, RunConfig, RunMetadata,
    RuntimeStats, SampleData,
};
pub use report::{Report, ReportFormat};
pub use sampler::ParameterSampler;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

/// A query entry from the queries YAML file.
///
/// Each entry has a stable key (`q1`..`qN`), a human-readable description,
/// and the raw GKG query DSL as an inline JSON string.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryEntry {
    /// Human-readable description of what this query tests.
    pub desc: String,
    /// Raw JSON query DSL string, passed to the query engine compiler.
    pub query: String,
}

impl QueryEntry {
    /// Parse the `query` JSON string into a `serde_json::Value`.
    pub fn parse_query(&self) -> Result<serde_json::Value> {
        serde_json::from_str(&self.query).map_err(Into::into)
    }
}

/// Load queries from a YAML file.
///
/// ```yaml
/// q1:
///   desc: List active users
///   query: |
///     {"query_type": "search", ...}
/// ```
pub fn load_queries(path: &Path) -> Result<HashMap<String, QueryEntry>> {
    use anyhow::Context;

    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read queries file: {}", path.display()))?;

    let queries: HashMap<String, QueryEntry> = serde_yaml::from_str(&content)
        .with_context(|| format!("Failed to parse queries file: {}", path.display()))?;

    for (key, entry) in &queries {
        entry
            .parse_query()
            .with_context(|| format!("Invalid JSON in query '{}' ({})", key, entry.desc))?;
    }
    Ok(queries)
}

/// Parameters extracted from a query that need sampling.
#[derive(Debug, Clone, Default)]
pub struct QueryParameters {
    /// Entity type -> list of node_ids fields that need sampling
    pub node_ids: HashMap<String, Vec<String>>,
}

/// Extract parameters that need sampling from a query's JSON value.
pub fn extract_parameters(query_value: &serde_json::Value) -> QueryParameters {
    let mut params = QueryParameters::default();

    let mut all_nodes: Vec<&serde_json::Value> = Vec::new();
    if let Some(nodes) = query_value.get("nodes").and_then(|n| n.as_array()) {
        all_nodes.extend(nodes);
    }
    if let Some(node) = query_value.get("node") {
        all_nodes.push(node);
    }

    for node in all_nodes {
        if let Some(obj) = node.as_object()
            && obj.contains_key("node_ids")
            && let Some(entity) = obj.get("entity").and_then(|e| e.as_str())
            && let Some(id) = obj.get("id").and_then(|i| i.as_str())
        {
            params
                .node_ids
                .entry(entity.to_string())
                .or_default()
                .push(id.to_string());
        }
    }

    params
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_parameters() {
        let json = r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "mr", "entity": "MergeRequest", "filters": {"state": "merged"}},
                {"id": "merger", "entity": "User", "node_ids": [42]}
            ],
            "relationships": [
                {"type": "MERGED_BY", "from": "mr", "to": "merger"}
            ],
            "limit": 30
        }"#;
        let value: serde_json::Value = serde_json::from_str(json).unwrap();

        let params = extract_parameters(&value);
        assert!(params.node_ids.contains_key("User"));
        assert_eq!(params.node_ids["User"], vec!["merger".to_string()]);
    }

    #[test]
    fn test_query_entry_parse() {
        let entry = QueryEntry {
            desc: "test query".into(),
            query: r#"{"query_type": "search", "node": {"id": "u", "entity": "User"}}"#.into(),
        };
        let value = entry.parse_query().unwrap();
        assert_eq!(value["query_type"], "search");
    }
}
