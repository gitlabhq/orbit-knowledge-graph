//! Deterministic ID generation for plugin nodes and edges.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub fn generate_node_id(
    plugin_id: &str,
    namespace_id: i64,
    node_kind: &str,
    external_id: &str,
) -> i64 {
    let mut hasher = DefaultHasher::new();

    plugin_id.hash(&mut hasher);
    namespace_id.hash(&mut hasher);
    node_kind.hash(&mut hasher);
    external_id.hash(&mut hasher);

    let hash = hasher.finish();
    hash as i64
}

pub fn generate_edge_id(
    plugin_id: &str,
    namespace_id: i64,
    relationship_kind: &str,
    external_id: &str,
) -> i64 {
    let mut hasher = DefaultHasher::new();

    plugin_id.hash(&mut hasher);
    namespace_id.hash(&mut hasher);
    "edge".hash(&mut hasher);
    relationship_kind.hash(&mut hasher);
    external_id.hash(&mut hasher);

    let hash = hasher.finish();
    hash as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generates_deterministic_ids() {
        let id1 = generate_node_id("plugin", 42, "Vulnerability", "vuln-001");
        let id2 = generate_node_id("plugin", 42, "Vulnerability", "vuln-001");

        assert_eq!(id1, id2);
    }

    #[test]
    fn different_inputs_produce_different_ids() {
        let id1 = generate_node_id("plugin", 42, "Vulnerability", "vuln-001");
        let id2 = generate_node_id("plugin", 42, "Vulnerability", "vuln-002");
        let id3 = generate_node_id("plugin", 43, "Vulnerability", "vuln-001");

        assert_ne!(id1, id2);
        assert_ne!(id1, id3);
    }

    #[test]
    fn node_and_edge_ids_differ() {
        let node_id = generate_node_id("plugin", 42, "Type", "id-001");
        let edge_id = generate_edge_id("plugin", 42, "Type", "id-001");

        assert_ne!(node_id, edge_id);
    }
}
