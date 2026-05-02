//! Input types for JSON query deserialization.
//!
//! Security validation (identifiers, SQL injection) is handled by JSON Schema in lib.rs.

use ontology::constants::{DEFAULT_PRIMARY_KEY, SOURCE_ID_COLUMN, TARGET_ID_COLUMN};
use serde::{Deserialize, Deserializer};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

// ─────────────────────────────────────────────────────────────────────────────
// Top-level input
// ─────────────────────────────────────────────────────────────────────────────

/// Controls which columns are fetched for dynamically-discovered entities
/// during hydration (PathFinding, Neighbors).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
pub enum DynamicColumnMode {
    /// Fetch all columns from the ontology for each entity type.
    #[serde(rename = "*")]
    All,
    /// Fetch only the entity's `default_columns` from the ontology.
    #[default]
    #[serde(rename = "default")]
    Default,
}

/// Consumer-level preferences that affect result presentation, not query semantics.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct QueryOptions {
    /// Columns fetched for dynamically-discovered entities during hydration.
    /// `All` returns every column; `Default` returns the entity's `default_columns`.
    #[serde(default)]
    pub dynamic_columns: DynamicColumnMode,
    /// When true, includes compiled ClickHouse SQL in the response metadata.
    /// Only honored for authorized users (instance admins and direct GitLab
    /// org members with Reporter+ access).
    #[serde(default)]
    pub include_debug_sql: bool,
    /// When true, skips the ReplacingMergeTree deduplication pass for node
    /// tables. Rows are still filtered by `_deleted = false` but stale
    /// duplicates from un-merged parts may appear in results.
    ///
    /// Not allowed for aggregation queries (would produce incorrect counts).
    /// Useful for traversal/neighbors/path_finding where the caller tolerates
    /// eventual consistency in exchange for lower latency.
    #[serde(default)]
    pub skip_dedup: bool,
    /// When true, marks multi-referenced CTEs as `MATERIALIZED` so
    /// ClickHouse evaluates them once instead of inlining at every
    /// reference site. Reduces redundant scans for cascade and hop
    /// frontier CTEs in multi-relationship queries.
    #[serde(default)]
    pub materialize_ctes: bool,
    /// When true, rewrites `IN (SELECT id FROM cte)` SIP patterns into
    /// explicit `LEFT SEMI JOIN` for early termination and reduced hash-set
    /// materialization in ClickHouse.
    #[serde(default)]
    pub use_semi_join: bool,
    /// When true, forces auth-scoped cascade seeding on every query,
    /// regardless of whether any node has `node_ids`. When false (default),
    /// auth-scoped cascades are only used when no node has `node_ids` —
    /// pinned-node cascades provide better narrowing and avoid redundant
    /// full-table _nf_* scans.
    #[serde(default)]
    pub auth_scope_cascade: bool,
    /// When true, emits `SELECT DISTINCT` on cascade and hop frontier CTEs.
    /// When false (default), CTEs emit plain `SELECT` — ClickHouse's `IN`
    /// operator already deduplicates internally, and `DISTINCT` adds a
    /// blocking hash aggregation barrier that prevents pipelining.
    #[serde(default)]
    pub cascade_distinct: bool,
}

/// Authorization config for an entity type, derived from the ontology and carried
/// through the compilation pipeline so the server never re-consults the ontology at
/// request time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntityAuthConfig {
    /// Rails resource type sent to the authorization service (e.g. "projects").
    pub resource_type: String,
    /// Ability to check (e.g. "read_code").
    pub ability: String,
    /// DB column whose value is used as the authorization ID.
    /// "id" for most entities; e.g. "project_id" for Definition/File/Branch.
    pub auth_id_column: String,
    /// For indirect-auth entities (auth_id_column != "id"): the entity type that
    /// owns this resource, used to resolve the auth ID from edge columns for
    /// dynamic (path/neighbor) nodes.
    pub owner_entity: Option<String>,
    /// Minimum GitLab role required on a traversal path for rows of this entity
    /// to survive the security pass. Stored as an access-level integer so the
    /// compiler can compare against per-path roles carried by `SecurityContext`
    /// without pulling the ontology crate into `types.rs`.
    pub required_access_level: u32,
}

impl Default for EntityAuthConfig {
    fn default() -> Self {
        Self {
            resource_type: String::new(),
            ability: String::new(),
            auth_id_column: ontology::constants::DEFAULT_PRIMARY_KEY.to_string(),
            owner_entity: None,
            // Reporter mirrors the pre-fix access gate and is the right
            // default for tests that do not care about role scoping.
            required_access_level: crate::types::DEFAULT_PATH_ACCESS_LEVEL,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Input {
    pub query_type: QueryType,
    #[serde(flatten, deserialize_with = "deserialize_nodes_or_node")]
    pub nodes: Vec<InputNode>,
    #[serde(default)]
    pub relationships: Vec<InputRelationship>,
    #[serde(default)]
    pub aggregations: Vec<InputAggregation>,
    pub path: Option<InputPath>,
    pub neighbors: Option<InputNeighbors>,
    #[serde(default = "default_limit")]
    pub limit: u32,
    pub cursor: Option<InputCursor>,
    pub order_by: Option<InputOrderBy>,
    pub aggregation_sort: Option<InputAggSort>,
    #[serde(default)]
    pub options: QueryOptions,
    /// Auth config for every entity type with redaction configured. Populated by
    /// normalization; covers all ontology entities (not just those in this query)
    /// so dynamic nodes (path/neighbors) can be resolved without re-consulting the ontology.
    #[serde(skip)]
    pub entity_auth: HashMap<String, EntityAuthConfig>,
    /// Metadata accumulated across compiler passes (lowering, optimize, etc.).
    #[serde(skip)]
    pub compiler: CompilerMetadata,
}

/// Text index metadata for a column, used by the optimizer to rewrite
/// LIKE patterns to ClickHouse text-index-aware functions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextIndexMeta {
    /// The tokenizer strategy, e.g. `"splitByNonAlpha"`, `"splitByString(['/'])"`.
    pub tokenizer: String,
}

/// Metadata accumulated across compiler passes.
///
/// Written by normalize/lowering, read by downstream passes (deduplicate,
/// optimize, enforce, SIP, fold, etc.).
#[derive(Debug, Clone)]
pub struct CompilerMetadata {
    /// Maps node alias → (edge_alias, edge_column) for edge-only nodes.
    /// Written by lower, read by enforce to emit `_gkg_*` redaction columns
    /// from edge columns instead of node table columns. Also used by SIP
    /// and fold passes to skip edge-only targets.
    pub node_edge_col: HashMap<String, (String, String)>,
    /// All edge table names from the ontology. Used by dedup and optimizer
    /// passes to identify edge scans without needing the full ontology.
    pub edge_tables: HashSet<String>,
    /// Default edge table name for creating new edge scans.
    pub default_edge_table: String,
    /// Maps relationship kind → edge table name. Populated by normalize from
    /// `EdgeEntity.destination_table`. Used by lower/optimize to route each
    /// relationship's scan to the correct physical table.
    pub edge_table_for_rel: HashMap<String, String>,
    /// Maps (node_kind, property_name, direction_prefix) → (edge_column, tag_key).
    /// Populated by normalize from ontology denormalized properties.
    /// Example: ("Pipeline", "status", "source") → ("source_tags", "status")
    pub denormalized_columns: HashMap<(String, String, String), (String, String)>,
    /// `_nf_*` CTEs created by the lowerer from user-supplied filters or
    /// node_ids. Distinguished from `_nf_*` CTEs synthesized by
    /// `narrow_joined_nodes_via_pinned_neighbors` (reverse cascades).
    /// The hop frontier optimizer uses this to decide whether a CTE is safe
    /// to forward-chain from.
    pub lowerer_nf_ctes: HashSet<String>,
    /// Maps (table_name, column_name) → text index metadata. Populated by
    /// normalize from the ontology's `StorageIndex` entries. Used by the
    /// optimizer to rewrite `LIKE` patterns to `hasToken`/`hasAllTokens`.
    pub text_indexes: HashMap<(String, String), TextIndexMeta>,
    /// Physical table columns from the ontology. Used by lowering to emit
    /// internal predicates only when a table is known to carry that column.
    pub table_columns: HashMap<String, HashSet<String>>,
}

/// Defaults to `gl_edge` for test convenience. In production, `normalize()`
/// always overwrites `edge_tables` and `default_edge_table` from the ontology.
impl Default for CompilerMetadata {
    fn default() -> Self {
        Self {
            node_edge_col: HashMap::new(),
            edge_tables: HashSet::from([ontology::constants::EDGE_TABLE.to_string()]),
            default_edge_table: ontology::constants::EDGE_TABLE.to_string(),
            edge_table_for_rel: HashMap::new(),
            denormalized_columns: HashMap::new(),
            lowerer_nf_ctes: HashSet::new(),
            text_indexes: HashMap::new(),
            table_columns: HashMap::new(),
        }
    }
}

impl CompilerMetadata {
    pub fn table_has_column(&self, table: &str, column: &str) -> bool {
        self.table_columns
            .get(table)
            .is_some_and(|columns| columns.contains(column))
    }

    /// Resolve the edge table(s) for a relationship's type list.
    ///
    /// Returns a deduplicated list of physical tables that need to be scanned.
    /// - Single table → caller emits a normal `edge_scan`
    /// - Multiple tables → caller emits a UNION ALL across tables
    ///
    /// Wildcards and empty type lists resolve to all declared edge tables.
    pub fn resolve_edge_tables(&self, types: &[String]) -> Vec<String> {
        if types.is_empty() || (types.len() == 1 && types[0] == "*") {
            let mut tables: Vec<String> = self.edge_tables.iter().cloned().collect();
            tables.sort();
            return tables;
        }
        let mut seen = std::collections::BTreeSet::new();
        for t in types {
            let table = self
                .edge_table_for_rel
                .get(t)
                .map(|s| s.as_str())
                .unwrap_or(&self.default_edge_table);
            seen.insert(table.to_string());
        }
        seen.into_iter().collect()
    }
}

impl Input {
    /// Whether this query has the "search shape": a single-node table scan
    /// with no relationships (traversal with 1 node + 0 relationships).
    pub fn is_search(&self) -> bool {
        self.query_type == QueryType::Traversal
            && self.nodes.len() == 1
            && self.relationships.is_empty()
    }
}

impl Default for Input {
    fn default() -> Self {
        Self {
            query_type: QueryType::Traversal,
            nodes: vec![],
            relationships: vec![],
            aggregations: vec![],
            path: None,
            neighbors: None,
            limit: default_limit(),
            cursor: None,
            order_by: None,
            aggregation_sort: None,
            options: QueryOptions::default(),
            entity_auth: HashMap::new(),
            compiler: CompilerMetadata::default(),
        }
    }
}

fn deserialize_nodes_or_node<'de, D>(deserializer: D) -> Result<Vec<InputNode>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    struct Helper {
        #[serde(default)]
        node: Option<InputNode>,
        #[serde(default)]
        nodes: Option<Vec<InputNode>>,
    }

    let helper = Helper::deserialize(deserializer)?;

    match (helper.node, helper.nodes) {
        (Some(node), None) => Ok(vec![node]),
        (None, Some(nodes)) => Ok(nodes),
        (Some(_), Some(_)) => Err(serde::de::Error::custom(
            "cannot specify both 'node' and 'nodes'",
        )),
        (None, None) => Err(serde::de::Error::custom(
            "must specify either 'node' or 'nodes'",
        )),
    }
}

fn default_limit() -> u32 {
    30
}

/// Agent-driven pagination cursor. Slices the authorized (post-redaction)
/// result set by `offset` and `page_size`. The server re-runs the query,
/// authorizes all rows up to `limit`, and returns `[offset..offset+page_size]`.
///
/// This model avoids SQL-level keyset pagination, which only generalizes to
/// Search queries and breaks when redaction removes rows from the LIMIT window.
// TODO: Server-side query caching with TTL to avoid re-running the same query on page 2+
#[derive(Debug, Clone, Copy, Deserialize)]
pub struct InputCursor {
    pub offset: u32,
    pub page_size: u32,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Deserialize,
    strum::Display,
    strum::IntoStaticStr,
    strum::VariantNames,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum QueryType {
    Traversal,
    Aggregation,
    PathFinding,
    Neighbors,
    /// Internal-only: consolidated hydration for multiple entity types.
    /// Generates a UNION ALL of search-like arms, one per node. Skips
    /// security context injection (IDs are pre-authorized by the pipeline).
    #[serde(skip)]
    Hydration,
}

// ─────────────────────────────────────────────────────────────────────────────
// Nodes
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct InputNode {
    pub id: String,
    /// Entity type (e.g., "User", "Project"). Determines which table to query.
    #[serde(default)]
    pub entity: Option<String>,
    /// Resolved table name (e.g., "gl_user"). Populated during normalization.
    #[serde(skip)]
    pub table: Option<String>,
    /// Columns to return for this node. Use `ColumnSelection::All` for all columns,
    /// or `ColumnSelection::List` for specific columns. If not specified, only
    /// mandatory columns (id, type) are returned.
    #[serde(default, deserialize_with = "deserialize_columns")]
    pub columns: Option<ColumnSelection>,
    #[serde(default, deserialize_with = "deserialize_filters")]
    pub filters: HashMap<String, InputFilter>,
    #[serde(default, deserialize_with = "deserialize_id_vec")]
    pub node_ids: Vec<i64>,
    pub id_range: Option<InputIdRange>,
    pub id_property: String,
    /// Which DB column to select as the auth ID for this node. Populated unconditionally
    /// during normalization ("id" for most entities, e.g. "project_id" for Definition).
    /// Always set before enforce.rs runs; do not add fallbacks in downstream code.
    #[serde(skip)]
    pub redaction_id_column: String,
    /// Virtual columns stripped by normalize, consumed by the hydration plan.
    #[serde(skip)]
    pub virtual_columns: Vec<crate::passes::hydrate::VirtualColumnRequest>,
    /// Whether the node table has a traversal_path column. Set during normalization.
    #[serde(skip)]
    pub has_traversal_path: bool,
}

impl Default for InputNode {
    fn default() -> Self {
        Self {
            id: String::new(),
            entity: None,
            table: None,
            columns: None,
            filters: HashMap::new(),
            node_ids: Vec::new(),
            id_range: None,
            id_property: DEFAULT_PRIMARY_KEY.to_string(),
            redaction_id_column: DEFAULT_PRIMARY_KEY.to_string(),
            virtual_columns: Vec::new(),
            has_traversal_path: false,
        }
    }
}

/// Column selection for a node's result set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnSelection {
    /// Select all columns for this entity ("*")
    All,
    /// Select specific columns by name
    List(Vec<String>),
}

fn deserialize_columns<'de, D>(deserializer: D) -> Result<Option<ColumnSelection>, D::Error>
where
    D: Deserializer<'de>,
{
    let value: Option<Value> = Option::deserialize(deserializer)?;
    match value {
        None => Ok(None),
        Some(Value::String(s)) if s == "*" => Ok(Some(ColumnSelection::All)),
        Some(Value::Array(arr)) => {
            let cols: Result<Vec<String>, _> = arr
                .into_iter()
                .map(|v| {
                    v.as_str()
                        .map(String::from)
                        .ok_or_else(|| serde::de::Error::custom("column names must be strings"))
                })
                .collect();
            Ok(Some(ColumnSelection::List(cols?)))
        }
        Some(_) => Err(serde::de::Error::custom(
            "columns must be '*' or an array of column names",
        )),
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct InputIdRange {
    #[serde(deserialize_with = "deserialize_id")]
    pub start: i64,
    #[serde(deserialize_with = "deserialize_id")]
    pub end: i64,
}

/// Accepts either a JSON integer or a JSON string of digits. Supports the
/// server response convention (IDs serialized as strings to avoid JavaScript
/// precision loss) so consumers can round-trip IDs without casting.
fn deserialize_id<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: Deserializer<'de>,
{
    match Value::deserialize(deserializer)? {
        Value::Number(n) => n
            .as_i64()
            .ok_or_else(|| serde::de::Error::custom("id out of i64 range")),
        Value::String(s) => s.parse::<i64>().map_err(serde::de::Error::custom),
        _ => Err(serde::de::Error::custom("id must be an integer or string")),
    }
}

fn deserialize_id_vec<'de, D>(deserializer: D) -> Result<Vec<i64>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw: Vec<Value> = Vec::deserialize(deserializer)?;
    raw.into_iter()
        .map(|v| match v {
            Value::Number(n) => n
                .as_i64()
                .ok_or_else(|| serde::de::Error::custom("id out of i64 range")),
            Value::String(s) => s.parse::<i64>().map_err(serde::de::Error::custom),
            _ => Err(serde::de::Error::custom("id must be an integer or string")),
        })
        .collect()
}

// ─────────────────────────────────────────────────────────────────────────────
// Filters
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default)]
pub struct InputFilter {
    pub op: Option<FilterOp>,
    pub value: Option<Value>,
    /// Populated by the validate pass; lets the lowerer bind temporal columns
    /// with their typed CH param.
    pub data_type: Option<ontology::DataType>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterOp {
    Eq,
    Gt,
    Lt,
    Gte,
    Lte,
    In,
    Contains,
    StartsWith,
    EndsWith,
    IsNull,
    IsNotNull,
    /// Token-boundary match via `hasToken()`. Requires a text index on the column.
    TokenMatch,
    /// All tokens present via `hasAllTokens()`. Requires a text index on the column.
    AllTokens,
    /// Any token present via `hasAnyTokens()`. Requires a text index on the column.
    AnyTokens,
}

fn deserialize_filters<'de, D>(deserializer: D) -> Result<HashMap<String, InputFilter>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw: HashMap<String, Value> = HashMap::deserialize(deserializer)?;
    Ok(raw.into_iter().map(|(k, v)| (k, parse_filter(v))).collect())
}

fn parse_filter(value: Value) -> InputFilter {
    if let Value::Object(ref obj) = value
        && let Some(op_val) = obj.get("op")
        && let Ok(op) = serde_json::from_value::<FilterOp>(op_val.clone())
    {
        return InputFilter {
            op: Some(op),
            value: obj.get("value").cloned(),
            ..Default::default()
        };
    }
    InputFilter {
        op: None,
        value: Some(value),
        ..Default::default()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Relationships
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct InputRelationship {
    #[serde(rename = "type", deserialize_with = "deserialize_rel_types")]
    pub types: Vec<String>,
    pub from: String,
    pub to: String,
    #[serde(default = "default_hops")]
    pub min_hops: u32,
    #[serde(default = "default_hops")]
    pub max_hops: u32,
    #[serde(default)]
    pub direction: Direction,
    #[serde(default, deserialize_with = "deserialize_filters")]
    pub filters: HashMap<String, InputFilter>,
}

fn default_hops() -> u32 {
    1
}

fn deserialize_rel_types<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    match Value::deserialize(deserializer)? {
        Value::String(s) => Ok(vec![s]),
        Value::Array(arr) => arr
            .into_iter()
            .map(|v| {
                v.as_str()
                    .map(String::from)
                    .ok_or_else(|| serde::de::Error::custom("expected string"))
            })
            .collect(),
        _ => Err(serde::de::Error::custom("type must be string or array")),
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    #[default]
    Outgoing,
    Incoming,
    Both,
}

impl Direction {
    /// Returns (start_col, end_col) for edge traversal.
    pub fn edge_columns(self) -> (&'static str, &'static str) {
        match self {
            Direction::Outgoing | Direction::Both => (SOURCE_ID_COLUMN, TARGET_ID_COLUMN),
            Direction::Incoming => (TARGET_ID_COLUMN, SOURCE_ID_COLUMN),
        }
    }

    /// Returns (from_col, to_col) for union subquery joins.
    pub fn union_columns(self) -> (&'static str, &'static str) {
        use crate::constants::{END_ID_COLUMN, START_ID_COLUMN};
        match self {
            Direction::Outgoing | Direction::Both => (START_ID_COLUMN, END_ID_COLUMN),
            Direction::Incoming => (END_ID_COLUMN, START_ID_COLUMN),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Aggregations
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct InputAggregation {
    pub function: AggFunction,
    #[serde(default)]
    pub target: Option<String>,
    #[serde(default)]
    pub group_by: Option<String>,
    #[serde(default)]
    pub property: Option<String>,
    #[serde(default)]
    pub alias: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, strum::Display)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
pub enum AggFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    Collect,
}

impl AggFunction {
    pub fn as_sql(&self) -> &'static str {
        match self {
            Self::Count => "COUNT",
            Self::Sum => "SUM",
            Self::Avg => "AVG",
            Self::Min => "MIN",
            Self::Max => "MAX",
            Self::Collect => "groupArray",
        }
    }

    /// ClickHouse `-If` combinator variant (e.g. `countIf`, `sumIf`).
    /// Returns `None` for functions that don't support the combinator.
    pub fn as_sql_if(&self) -> Option<&'static str> {
        match self {
            Self::Count => Some("countIf"),
            Self::Sum => Some("sumIf"),
            Self::Avg => Some("avgIf"),
            Self::Min => Some("minIf"),
            Self::Max => Some("maxIf"),
            Self::Collect => None,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Path finding
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct InputPath {
    #[serde(rename = "type")]
    pub path_type: PathType,
    pub from: String,
    pub to: String,
    pub max_depth: u32,
    #[serde(default)]
    pub rel_types: Vec<String>,
    #[serde(skip)]
    pub forward_first_hop_rel_types: Vec<String>,
    #[serde(skip)]
    pub backward_first_hop_rel_types: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PathType {
    Shortest,
    AllShortest,
    Any,
}

// ─────────────────────────────────────────────────────────────────────────────
// Neighbors
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct InputNeighbors {
    pub node: String,
    #[serde(default)]
    pub direction: Direction,
    #[serde(default)]
    pub rel_types: Vec<String>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Ordering
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct InputOrderBy {
    pub node: String,
    pub property: String,
    #[serde(default)]
    pub direction: OrderDirection,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum OrderDirection {
    #[default]
    Asc,
    Desc,
}

#[derive(Debug, Clone, Deserialize)]
pub struct InputAggSort {
    pub agg_index: usize,
    #[serde(default)]
    pub direction: OrderDirection,
}

// ─────────────────────────────────────────────────────────────────────────────
// Public API
// ─────────────────────────────────────────────────────────────────────────────

/// Parse JSON into Input structure.
#[must_use = "the parsed input should be used"]
pub fn parse_input(json: &str) -> Result<Input, serde_json::Error> {
    serde_json::from_str(json)
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_traversal() {
        let input = parse_input(
            r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "n", "entity": "Note", "filters": {"system": false}},
                {"id": "u", "entity": "User"}
            ],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "n"}],
            "limit": 25
        }"#,
        )
        .unwrap();

        assert_eq!(input.query_type, QueryType::Traversal);
        assert_eq!(input.nodes.len(), 2);
        assert_eq!(input.nodes[0].entity, Some("Note".into()));
        assert_eq!(input.relationships.len(), 1);
        assert_eq!(input.limit, 25);
    }

    #[test]
    fn operator_filter() {
        let input = parse_input(
            r#"{
            "query_type": "traversal",
            "nodes": [{
                "id": "u", "entity": "User",
                "filters": {
                    "created_at": {"op": "gte", "value": "2024-01-01"},
                    "state": {"op": "in", "value": ["active", "blocked"]}
                }
            }]
        }"#,
        )
        .unwrap();

        let filters = &input.nodes[0].filters;
        assert_eq!(filters.get("created_at").unwrap().op, Some(FilterOp::Gte));
        assert_eq!(filters.get("state").unwrap().op, Some(FilterOp::In));
    }

    #[test]
    fn multiple_rel_types() {
        let input = parse_input(
            r#"{
            "query_type": "traversal",
            "nodes": [{"id": "a"}, {"id": "b"}],
            "relationships": [{"type": ["BLOCKS", "RELATES_TO"], "from": "a", "to": "b"}]
        }"#,
        )
        .unwrap();

        assert_eq!(input.relationships[0].types, vec!["BLOCKS", "RELATES_TO"]);
    }

    #[test]
    fn aggregation() {
        let input = parse_input(r#"{
            "query_type": "aggregation",
            "nodes": [{"id": "n"}, {"id": "u"}],
            "aggregations": [{"function": "count", "target": "n", "group_by": "u", "alias": "note_count"}],
            "aggregation_sort": {"agg_index": 0, "direction": "DESC"}
        }"#).unwrap();

        assert_eq!(input.query_type, QueryType::Aggregation);
        assert_eq!(input.aggregations[0].function, AggFunction::Count);
        assert!(input.aggregation_sort.is_some());
    }

    #[test]
    fn path_finding() {
        let input = parse_input(
            r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "entity": "Project", "node_ids": [100]},
                {"id": "end", "entity": "Project", "node_ids": [200]}
            ],
            "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3}
        }"#,
        )
        .unwrap();

        assert_eq!(input.query_type, QueryType::PathFinding);
        let path = input.path.unwrap();
        assert_eq!(path.path_type, PathType::Shortest);
        assert_eq!(path.max_depth, 3);
    }

    #[test]
    fn default_values() {
        let input = parse_input(r#"{"query_type": "traversal", "nodes": [{"id": "n"}]}"#).unwrap();
        assert_eq!(input.limit, 30);
        assert!(input.relationships.is_empty());
        assert!(input.aggregations.is_empty());
    }

    #[test]
    fn traversal_with_single_node() {
        let input = parse_input(
            r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "filters": {"username": "admin"}}
        }"#,
        )
        .unwrap();

        assert_eq!(input.query_type, QueryType::Traversal);
        assert_eq!(input.nodes.len(), 1);
        assert_eq!(input.nodes[0].id, "u");
        assert!(input.is_search());
    }

    #[test]
    fn columns_wildcard() {
        let input = parse_input(
            r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "columns": "*"}
        }"#,
        )
        .unwrap();

        assert_eq!(input.nodes[0].columns, Some(ColumnSelection::All));
    }

    #[test]
    fn columns_list() {
        let input = parse_input(
            r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "columns": ["username", "email", "created_at"]}
        }"#,
        )
        .unwrap();

        assert_eq!(
            input.nodes[0].columns,
            Some(ColumnSelection::List(vec![
                "username".into(),
                "email".into(),
                "created_at".into()
            ]))
        );
    }

    #[test]
    fn columns_not_specified() {
        let input = parse_input(
            r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User"}
        }"#,
        )
        .unwrap();

        assert_eq!(input.nodes[0].columns, None);
    }

    #[test]
    fn columns_in_traversal() {
        let input = parse_input(
            r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "columns": ["username"]},
                {"id": "p", "entity": "Project", "columns": "*"}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "p"}]
        }"#,
        )
        .unwrap();

        assert_eq!(
            input.nodes[0].columns,
            Some(ColumnSelection::List(vec!["username".into()]))
        );
        assert_eq!(input.nodes[1].columns, Some(ColumnSelection::All));
    }

    #[test]
    fn neighbors_query() {
        let input = parse_input(
            r#"{
            "query_type": "neighbors",
            "node": {"id": "u", "entity": "User", "node_ids": [100]},
            "neighbors": {"node": "u", "direction": "both"}
        }"#,
        )
        .unwrap();

        assert_eq!(input.query_type, QueryType::Neighbors);
        let neighbors = input.neighbors.unwrap();
        assert_eq!(neighbors.node, "u");
        assert_eq!(neighbors.direction, Direction::Both);
    }

    #[test]
    fn options_default_when_omitted() {
        let input =
            parse_input(r#"{"query_type": "traversal", "node": {"id": "u", "entity": "User"}}"#)
                .unwrap();

        assert_eq!(input.options.dynamic_columns, DynamicColumnMode::Default);
    }

    #[test]
    fn options_dynamic_columns_all() {
        let input = parse_input(
            r#"{
            "query_type": "neighbors",
            "node": {"id": "u", "entity": "User", "node_ids": [1]},
            "neighbors": {"node": "u"},
            "options": {"dynamic_columns": "*"}
        }"#,
        )
        .unwrap();

        assert_eq!(input.options.dynamic_columns, DynamicColumnMode::All);
    }

    #[test]
    fn options_dynamic_columns_default() {
        let input = parse_input(
            r#"{
            "query_type": "neighbors",
            "node": {"id": "u", "entity": "User", "node_ids": [1]},
            "neighbors": {"node": "u"},
            "options": {"dynamic_columns": "default"}
        }"#,
        )
        .unwrap();

        assert_eq!(input.options.dynamic_columns, DynamicColumnMode::Default);
    }

    #[test]
    fn options_empty_object_uses_defaults() {
        let input = parse_input(
            r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User"},
            "options": {}
        }"#,
        )
        .unwrap();

        assert_eq!(input.options.dynamic_columns, DynamicColumnMode::Default);
        assert!(!input.options.include_debug_sql);
    }

    #[test]
    fn options_include_debug_sql_true() {
        let input = parse_input(
            r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User"},
            "options": {"include_debug_sql": true}
        }"#,
        )
        .unwrap();

        assert!(input.options.include_debug_sql);
    }

    #[test]
    fn options_include_debug_sql_defaults_false() {
        let input = parse_input(
            r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User"},
            "options": {"dynamic_columns": "*"}
        }"#,
        )
        .unwrap();

        assert!(!input.options.include_debug_sql);
    }

    #[test]
    fn options_skip_dedup_true() {
        let input = parse_input(
            r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User"},
            "options": {"skip_dedup": true}
        }"#,
        )
        .unwrap();

        assert!(input.options.skip_dedup);
    }

    #[test]
    fn options_skip_dedup_defaults_false() {
        let input = parse_input(
            r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User"}
        }"#,
        )
        .unwrap();

        assert!(!input.options.skip_dedup);
    }

    #[test]
    fn node_ids_accepts_integers_and_strings() {
        let input = parse_input(
            r#"{
            "query_type": "traversal",
            "node": {
                "id": "u",
                "entity": "User",
                "node_ids": [1, "9007199254740993", -42]
            }
        }"#,
        )
        .unwrap();

        assert_eq!(input.nodes[0].node_ids, vec![1, 9_007_199_254_740_993, -42]);
    }

    #[test]
    fn id_range_accepts_integers_and_strings() {
        let input = parse_input(
            r#"{
            "query_type": "traversal",
            "node": {
                "id": "u",
                "entity": "User",
                "id_range": {"start": 1, "end": "9007199254740993"}
            }
        }"#,
        )
        .unwrap();

        let range = input.nodes[0].id_range.as_ref().unwrap();
        assert_eq!(range.start, 1);
        assert_eq!(range.end, 9_007_199_254_740_993);
    }
}
