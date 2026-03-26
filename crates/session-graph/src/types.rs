use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub id: String,
    pub kind: String,
    pub properties: Value,
    pub created_at: String,
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Edge {
    pub source_id: String,
    pub source_kind: String,
    pub relationship_kind: String,
    pub target_id: String,
    pub target_kind: String,
    pub properties: Value,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeKind {
    pub kind: String,
    pub description: String,
    pub property_keys: Vec<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraversalResult {
    pub node: Node,
    pub depth: i32,
    pub via_relationship: String,
    /// The edge properties (e.g. reason, link_type) connecting to this node.
    pub edge_properties: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbStats {
    pub node_count: i64,
    pub edge_count: i64,
    pub kind_count: i64,
    pub kinds: Vec<KindStat>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KindStat {
    pub kind: String,
    pub count: i64,
}
