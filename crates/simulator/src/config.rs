//! Configuration for data generation.

use std::collections::HashMap;

/// Configuration for the simulator.
#[derive(Debug, Clone)]
pub struct Config {
    /// ClickHouse connection URL.
    pub clickhouse_url: String,
    /// Number of organizations to generate.
    pub num_organizations: u32,
    /// Number of traversal IDs to generate per organization.
    pub traversal_ids_per_org: usize,
    /// Maximum depth of traversal ID hierarchy.
    pub max_traversal_depth: usize,
    /// Default number of nodes per type (if not overridden).
    pub default_nodes_per_type: usize,
    /// Override counts for specific node types.
    pub node_counts: HashMap<String, usize>,
    /// Number of edges to generate per source node.
    pub edges_per_source: usize,
    /// Batch size for ClickHouse inserts.
    pub batch_size: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            clickhouse_url: "http://localhost:8123".to_string(),
            num_organizations: 2,
            traversal_ids_per_org: 1000,
            max_traversal_depth: 5,
            default_nodes_per_type: 100,
            node_counts: HashMap::new(),
            edges_per_source: 3,
            batch_size: 10_000,
        }
    }
}

impl Config {
    /// Get the count for a specific node type.
    pub fn count_for(&self, node_type: &str) -> usize {
        self.node_counts
            .get(node_type)
            .copied()
            .unwrap_or(self.default_nodes_per_type)
    }
}
