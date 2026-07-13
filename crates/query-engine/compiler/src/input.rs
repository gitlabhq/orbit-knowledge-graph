//! Security validation (identifiers, SQL injection) is handled by JSON Schema in lib.rs.

use ontology::constants::{DEFAULT_PRIMARY_KEY, SOURCE_ID_COLUMN, TARGET_ID_COLUMN};
use serde::{Deserialize, Deserializer};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

/// Controls which columns are fetched for dynamically-discovered entities
/// during hydration (PathFinding, Neighbors).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, strum::IntoStaticStr)]
#[strum(serialize_all = "lowercase")]
pub enum DynamicColumnMode {
    #[serde(rename = "*")]
    All,
    #[default]
    #[serde(rename = "default")]
    Default,
}

/// Optional presentation hints that control response shape without affecting query
/// semantics. Only `dynamic_columns` and `include_debug_sql` are recognized.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct QueryOptions {
    #[serde(default)]
    pub dynamic_columns: DynamicColumnMode,
    /// On SaaS: honored for GitLab team members. On self-managed/Dedicated:
    /// honored for instance admins only.
    #[serde(default)]
    pub include_debug_sql: bool,
}

/// Authorization config for an entity type, derived from the ontology and carried
/// through the compilation pipeline so the server never re-consults the ontology at
/// request time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntityAuthConfig {
    /// Rails resource type sent to the authorization service (e.g. "projects").
    pub resource_type: String,
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
    pub nodes: Vec<InputNode>,
    #[serde(default)]
    pub relationships: Vec<InputRelationship>,
    #[serde(default)]
    #[serde(flatten)]
    pub aggregation: InputAggregation,
    pub path: Option<InputPath>,
    pub neighbors: Option<InputNeighbors>,
    #[serde(default = "default_limit")]
    pub limit: u32,
    pub cursor: Option<InputCursor>,
    pub order_by: Option<InputOrderBy>,
    #[serde(default)]
    pub options: QueryOptions,
    /// Auth config for every entity type with redaction configured. Populated by
    /// normalization; covers all ontology entities (not just those in this query)
    /// so dynamic nodes (path/neighbors) can be resolved without re-consulting the ontology.
    #[serde(skip)]
    pub entity_auth: HashMap<String, EntityAuthConfig>,
    #[serde(skip)]
    pub compiler: CompilerMetadata,
    /// True when this Input was constructed for the *dynamic* hydration codepath
    /// (Neighbors and PathFinding origin). Hydration over Traversal/Aggregation
    /// uses the static path and leaves this `false`.
    ///
    /// Selects the SQL shape for the `traversal_path` filter in hydration:
    /// - dynamic: `arrayExists(p -> startsWith(tp, p), [paths])` (constant AST depth,
    ///   safe against ClickHouse `max_parser_depth=1000` when the base query
    ///   surfaced hundreds of namespace paths)
    /// - static: left-nested OR of `startsWith(tp, p_i)` (per-leaf PK pushdown,
    ///   only ever a small project-bounded set of paths)
    #[serde(skip)]
    pub hydration_dynamic: bool,
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
    /// (node_kind, property, direction) → relationship kinds whose edge writes
    /// that denorm tag. A filter is only pushed onto a hop whose relationship
    /// is in this set.
    pub denorm_rel_kinds: HashMap<(String, String, String), Vec<String>>,
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
    /// ORDER BY (sort key) columns per table from the ontology. Used by
    /// the lowerer to emit `LIMIT 1 BY` dedup with PK-prefixed ORDER BY
    /// instead of FINAL for single-hop edge aggregations.
    pub table_sort_keys: HashMap<String, Vec<String>>,
    /// Maps relationship kind → valid source entity kinds. Used by
    /// pathfinding to add intermediate kind filters on frontier hops.
    pub edge_source_kinds: HashMap<String, Vec<String>>,
    /// Maps relationship kind → valid target entity kinds.
    pub edge_target_kinds: HashMap<String, Vec<String>>,
    /// Namespace entity (Group/Project) → (tp-dict table, key column) for pinning a neighbors anchor arm to its centers' exact traversal_paths.
    pub tp_id_lookup: HashMap<String, (String, String)>,
    /// FNV-1a of the canonicalized query JSON minus `cursor`; binds `after` tokens to their query.
    pub query_hash: u64,
    /// Number of `_gkg_cursor_N` readback columns the cursor pass appended.
    pub cursor_key_count: usize,
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
            denorm_rel_kinds: HashMap::new(),
            lowerer_nf_ctes: HashSet::new(),
            text_indexes: HashMap::new(),
            table_columns: HashMap::new(),
            table_sort_keys: HashMap::new(),
            edge_source_kinds: HashMap::new(),
            edge_target_kinds: HashMap::new(),
            tp_id_lookup: HashMap::new(),
            query_hash: 0,
            cursor_key_count: 0,
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
        if crate::passes::normalize::is_wildcard(types) {
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

    /// Rows to fetch from ClickHouse: the page window plus one probe row that
    /// proves `has_more` without being returned.
    pub fn fetch_limit(&self) -> u32 {
        self.cursor.as_ref().map_or(self.limit, |c| c.page_size) + 1
    }
}

impl Default for Input {
    fn default() -> Self {
        Self {
            query_type: QueryType::Traversal,
            nodes: vec![],
            relationships: vec![],
            aggregation: InputAggregation::default(),
            path: None,
            neighbors: None,
            limit: default_limit(),
            cursor: None,
            order_by: None,
            options: QueryOptions::default(),
            entity_auth: HashMap::new(),
            compiler: CompilerMetadata::default(),
            hydration_dynamic: false,
        }
    }
}

fn default_limit() -> u32 {
    30
}

/// Keyset pagination cursor: `after` is an opaque token from the previous
/// page's `pagination.next_cursor`, anchored on the last scanned SQL row so
/// redaction can only shorten a page, never strand it.
#[derive(Debug, Clone, Deserialize)]
pub struct InputCursor {
    pub page_size: u32,
    pub after: Option<String>,
    #[serde(skip)]
    pub seek: Option<Vec<Option<String>>>,
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
    #[serde(default)]
    pub entity: Option<String>,
    /// Resolved table name (e.g., "gl_user"). Populated during normalization.
    #[serde(skip)]
    pub table: Option<String>,
    /// If not specified, only mandatory columns (id, type) are returned.
    #[serde(default, deserialize_with = "deserialize_columns")]
    pub columns: Option<ColumnSelection>,
    #[serde(default, deserialize_with = "deserialize_filters")]
    pub filters: HashMap<String, Vec<InputFilter>>,
    #[serde(default, deserialize_with = "deserialize_id_vec")]
    pub node_ids: Vec<i64>,
    pub id_range: Option<InputIdRange>,
    pub id_property: String,
    /// Which DB column to select as the auth ID for this node. Populated unconditionally
    /// during normalization ("id" for most entities, e.g. "project_id" for Definition).
    /// Always set before enforce.rs runs; do not add fallbacks in downstream code.
    #[serde(skip)]
    pub redaction_id_column: String,
    #[serde(skip)]
    pub virtual_columns: Vec<crate::passes::hydrate::VirtualColumnRequest>,
    /// Filters on virtual columns, separated by normalize so they don't flow
    /// into SQL. Applied in-memory after hydration resolves the column values.
    #[serde(skip)]
    pub virtual_filters: Vec<(String, InputFilter)>,
    #[serde(skip)]
    pub has_traversal_path: bool,
    /// Whether the entity is declared `global: true` in the ontology.
    #[serde(skip)]
    pub is_global: bool,
    /// Narrowed traversal paths extracted from base query results. Used by the
    /// hydration pipeline to inject `startsWith(traversal_path, tp)` into hydration
    /// queries, pruning granules through the primary key.
    #[serde(skip)]
    pub traversal_paths: Vec<String>,
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
            virtual_filters: Vec::new(),
            has_traversal_path: false,
            is_global: false,
            traversal_paths: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnSelection {
    All,
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

#[derive(Debug, Clone, Default, PartialEq)]
pub struct InputFilter {
    pub op: Option<FilterOp>,
    pub value: Option<Value>,
    /// Populated by the validate pass; lets the lowerer bind temporal columns
    /// with their typed CH param.
    pub data_type: Option<ontology::DataType>,
    /// Populated by the validate pass from the ontology field definition.
    /// Used by the planner to decide whether a filter justifies a narrowing CTE.
    pub selectivity: ontology::FieldSelectivity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, strum::AsRefStr, strum::VariantNames)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
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

fn deserialize_filters<'de, D>(
    deserializer: D,
) -> Result<HashMap<String, Vec<InputFilter>>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw: HashMap<String, Value> = HashMap::deserialize(deserializer)?;
    Ok(raw
        .into_iter()
        .map(|(k, v)| (k, parse_filter_entry(v)))
        .collect())
}

/// Parse a filter entry that may be a single filter or an array of
/// PropertyFilter objects (AND-combined, for expressing ranges).
fn parse_filter_entry(value: Value) -> Vec<InputFilter> {
    if let Value::Array(ref arr) = value
        && !arr.is_empty()
        && arr.iter().all(|v| v.is_object() && v.get("op").is_some())
    {
        return arr.iter().cloned().map(parse_single_filter).collect();
    }
    vec![parse_single_filter(value)]
}

fn parse_single_filter(value: Value) -> InputFilter {
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
    pub filters: HashMap<String, Vec<InputFilter>>,
    /// FK column on a node table that encodes this relationship. Set during normalization.
    /// The compiler resolves which node has the column from the edge variant's entity types.
    #[serde(skip)]
    pub fk_column: Option<String>,
    /// Tight `traversal_path` prefix this edge's scan may be confined to. Set by
    /// `restrict` when both endpoints resolve to the same project/group scope, so
    /// the edge scan inherits the PK prefix instead of the broad org-wide one.
    /// Lossless because an edge row's `traversal_path` is its source entity's.
    #[serde(skip)]
    pub scope_prefix: Option<String>,
    /// Whether every resolved variant of this relationship keeps both endpoints
    /// in the same namespace. Set by `restrict`. Only scope-preserving FK edges
    /// link a node to an intrinsic child whose lifecycle is coupled to the
    /// parent; the FK-chain lowering relies on this to be result-equivalent to
    /// the edge scan (an independent entity like a runner can outlive its edge).
    #[serde(skip)]
    pub scope_preserving: bool,
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
    pub fn edge_columns(self) -> (&'static str, &'static str) {
        match self {
            Direction::Outgoing | Direction::Both => (SOURCE_ID_COLUMN, TARGET_ID_COLUMN),
            Direction::Incoming => (TARGET_ID_COLUMN, SOURCE_ID_COLUMN),
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct InputAggregation {
    #[serde(rename = "aggregations")]
    pub metrics: Vec<InputAggregationMetric>,
    #[serde(rename = "group_by")]
    pub group_by: Vec<InputGroupByKey>,
    #[serde(rename = "aggregation_sort")]
    pub sort: Option<InputAggSort>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputAggregationMetric {
    pub function: AggFunction,
    #[serde(default)]
    pub target: Option<String>,
    #[serde(default)]
    pub property: Option<String>,
    #[serde(default)]
    pub alias: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum InputGroupByKey {
    Node {
        node: String,
        #[serde(default)]
        alias: Option<String>,
    },
    Property {
        node: String,
        property: String,
        #[serde(default)]
        alias: Option<String>,
        #[serde(default)]
        transform: Option<PropertyTransform>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PropertyTransform {
    /// Truncate a Date or DateTime property to the start of `unit`.
    Truncate { unit: TruncateUnit },
}

impl PropertyTransform {
    pub fn output_suffix(&self) -> String {
        match self {
            Self::Truncate { unit } => unit.name().to_string(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TruncateUnit {
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Quarter,
    Year,
}

impl TruncateUnit {
    pub fn ch_function(self) -> &'static str {
        match self {
            Self::Minute => "toStartOfMinute",
            Self::Hour => "toStartOfHour",
            Self::Day => "toStartOfDay",
            Self::Week => "toStartOfWeek",
            Self::Month => "toStartOfMonth",
            Self::Quarter => "toStartOfQuarter",
            Self::Year => "toStartOfYear",
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Minute => "minute",
            Self::Hour => "hour",
            Self::Day => "day",
            Self::Week => "week",
            Self::Month => "month",
            Self::Quarter => "quarter",
            Self::Year => "year",
        }
    }

    /// Granularities whose bucket cardinality is too high to allow without
    /// the caller scoping the query to a bounded set.
    pub fn requires_selectivity_guard(self) -> bool {
        matches!(self, Self::Minute | Self::Hour)
    }
}

impl InputGroupByKey {
    pub fn node(&self) -> &str {
        match self {
            Self::Node { node, .. } | Self::Property { node, .. } => node,
        }
    }

    pub fn property(&self) -> Option<&str> {
        match self {
            Self::Node { .. } => None,
            Self::Property { property, .. } => Some(property),
        }
    }

    pub fn transform(&self) -> Option<&PropertyTransform> {
        match self {
            Self::Property { transform, .. } => transform.as_ref(),
            Self::Node { .. } => None,
        }
    }

    pub fn truncate(&self) -> Option<TruncateUnit> {
        self.transform()
            .map(|PropertyTransform::Truncate { unit }| *unit)
    }

    pub fn output_name(&self, is_unique_property: bool) -> String {
        match self {
            Self::Node { node, alias } => alias.clone().unwrap_or_else(|| node.clone()),
            Self::Property {
                node,
                property,
                alias,
                transform,
            } => alias.clone().unwrap_or_else(|| {
                let base = if is_unique_property {
                    property.clone()
                } else {
                    format!("{}_{}", node, property)
                };
                match transform {
                    Some(t) => format!("{}_{}", base, t.output_suffix()),
                    None => base,
                }
            }),
        }
    }
}

pub fn group_by_output_names(groups: &[InputGroupByKey]) -> Vec<String> {
    let mut property_counts: HashMap<&str, usize> = HashMap::new();
    for group in groups {
        if let Some(property) = group.property() {
            *property_counts.entry(property).or_default() += 1;
        }
    }

    groups
        .iter()
        .map(|group| {
            let is_unique_property = group
                .property()
                .map(|property| property_counts[property] == 1)
                .unwrap_or(false);
            group.output_name(is_unique_property)
        })
        .collect()
}

pub fn node_group_ids(groups: &[InputGroupByKey]) -> impl Iterator<Item = &str> {
    groups.iter().filter_map(|group| match group {
        InputGroupByKey::Node { node, .. } => Some(node.as_str()),
        InputGroupByKey::Property { .. } => None,
    })
}

pub fn property_groups(
    groups: &[InputGroupByKey],
) -> impl Iterator<Item = (&str, &str, Option<&str>)> {
    groups.iter().filter_map(|group| match group {
        InputGroupByKey::Property {
            node,
            property,
            alias,
            ..
        } => Some((node.as_str(), property.as_str(), alias.as_deref())),
        InputGroupByKey::Node { .. } => None,
    })
}

pub fn group_by_kind(group: &InputGroupByKey) -> &'static str {
    match group {
        InputGroupByKey::Node { .. } => "node",
        InputGroupByKey::Property { .. } => "property",
    }
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

    /// ClickHouse `-If` combinator name (e.g. `countIf`, `sumIf`).
    pub fn as_sql_if(&self) -> &'static str {
        match self {
            Self::Count => "countIf",
            Self::Sum => "sumIf",
            Self::Avg => "avgIf",
            Self::Min => "minIf",
            Self::Max => "maxIf",
            Self::Collect => "groupArrayIf",
        }
    }
}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, strum::VariantNames)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum PathType {
    Shortest,
}

#[derive(Debug, Clone, Deserialize)]
pub struct InputNeighbors {
    #[serde(default)]
    pub direction: Direction,
    #[serde(default)]
    pub rel_types: Vec<String>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Ordering
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputOrderBy {
    pub node: String,
    pub property: String,
    pub direction: OrderDirection,
}

impl<'de> Deserialize<'de> for InputOrderBy {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let spec = String::deserialize(deserializer)?;
        let caps = crate::passes::validate::order_by_regex()
            .captures(&spec)
            .ok_or_else(|| {
                serde::de::Error::custom(format!(
                    "order_by {spec:?} must be \"[-]node.property\" (leading \"-\" = descending)"
                ))
            })?;
        let direction = if caps.name("descending").is_some() {
            OrderDirection::Desc
        } else {
            OrderDirection::Asc
        };
        Ok(InputOrderBy {
            node: caps["node"].to_owned(),
            property: caps["property"].to_owned(),
            direction,
        })
    }
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
    pub column: String,
    #[serde(default)]
    pub direction: OrderDirection,
}

#[must_use = "the parsed input should be used"]
pub fn parse_input(json: &str) -> Result<Input, serde_json::Error> {
    serde_json::from_str(json)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn order_by(spec: &str) -> InputOrderBy {
        serde_json::from_str(&format!("\"{spec}\"")).expect("order_by parses")
    }

    #[test]
    fn order_by_descending_uses_leading_dash() {
        assert_eq!(
            order_by("-mr.merged_at"),
            InputOrderBy {
                node: "mr".into(),
                property: "merged_at".into(),
                direction: OrderDirection::Desc,
            }
        );
    }

    #[test]
    fn order_by_without_dash_is_ascending() {
        assert_eq!(order_by("u.username").direction, OrderDirection::Asc);
    }

    #[test]
    fn order_by_requires_node_and_property() {
        for malformed in [
            "merged_at",
            "-mr.",
            ".merged_at",
            "-",
            "a.b.c",
            "1mr.merged_at",
            "mr.1merged_at",
        ] {
            assert!(
                serde_json::from_str::<InputOrderBy>(&format!("\"{malformed}\"")).is_err(),
                "{malformed:?} should not parse"
            );
        }

        let over_length = "a".repeat(65);
        assert!(
            serde_json::from_str::<InputOrderBy>(&format!("\"{over_length}.prop\"")).is_err(),
            "identifier longer than 64 chars should not parse"
        );
    }

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
        assert_eq!(
            filters.get("created_at").unwrap()[0].op,
            Some(FilterOp::Gte)
        );
        assert_eq!(filters.get("state").unwrap()[0].op, Some(FilterOp::In));
    }

    #[test]
    fn multi_filter_range() {
        let input = parse_input(
            r#"{
            "query_type": "traversal",
            "nodes": [{
                "id": "mr", "entity": "MergeRequest",
                "filters": {
                    "created_at": [
                        {"op": "gte", "value": "2026-04-01T00:00:00Z"},
                        {"op": "lt", "value": "2026-05-01T00:00:00Z"}
                    ],
                    "state": "merged"
                }
            }]
        }"#,
        )
        .unwrap();

        let filters = &input.nodes[0].filters;
        let created_at = filters.get("created_at").unwrap();
        assert_eq!(created_at.len(), 2);
        assert_eq!(created_at[0].op, Some(FilterOp::Gte));
        assert_eq!(
            created_at[0].value,
            Some(serde_json::json!("2026-04-01T00:00:00Z"))
        );
        assert_eq!(created_at[1].op, Some(FilterOp::Lt));
        assert_eq!(
            created_at[1].value,
            Some(serde_json::json!("2026-05-01T00:00:00Z"))
        );

        let state = filters.get("state").unwrap();
        assert_eq!(state.len(), 1);
        assert_eq!(state[0].op, None);
        assert_eq!(state[0].value, Some(serde_json::json!("merged")));
    }

    #[test]
    fn multi_filter_bare_array_is_equality() {
        let input = parse_input(
            r#"{
            "query_type": "traversal",
            "nodes": [{
                "id": "u", "entity": "User",
                "filters": {"state": [1, 2, 3]}
            }]
        }"#,
        )
        .unwrap();

        let state = input.nodes[0].filters.get("state").unwrap();
        assert_eq!(state.len(), 1);
        assert_eq!(state[0].op, None);
        assert_eq!(state[0].value, Some(serde_json::json!([1, 2, 3])));
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
        let input = parse_input(
            r#"{
            "query_type": "aggregation",
            "nodes": [{"id": "n"}, {"id": "u"}],
            "group_by": [{"kind": "node", "node": "u"}],
            "aggregations": [{"function": "count", "target": "n", "alias": "note_count"}],
            "aggregation_sort": {"column": "note_count", "direction": "DESC"}
        }"#,
        )
        .unwrap();

        assert_eq!(input.query_type, QueryType::Aggregation);
        assert_eq!(input.aggregation.metrics[0].function, AggFunction::Count);
        assert!(input.aggregation.sort.is_some());
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
        assert!(input.aggregation.metrics.is_empty());
    }

    #[test]
    fn traversal_with_single_node() {
        let input = parse_input(
            r#"{
            "query_type": "traversal",
            "nodes": [{"id": "u", "entity": "User", "filters": {"username": "admin"}}]
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
            "nodes": [{"id": "u", "entity": "User", "columns": "*"}]
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
            "nodes": [{"id": "u", "entity": "User", "columns": ["username", "email", "created_at"]}]
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
            "nodes": [{"id": "u", "entity": "User"}]
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
            "nodes": [{"id": "u", "entity": "User", "node_ids": [100]}],
            "neighbors": {"node": "u", "direction": "both"}
        }"#,
        )
        .unwrap();

        assert_eq!(input.query_type, QueryType::Neighbors);
        let neighbors = input.neighbors.unwrap();
        assert_eq!(neighbors.direction, Direction::Both);
    }

    #[test]
    fn options_default_when_omitted() {
        let input =
            parse_input(r#"{"query_type": "traversal", "nodes": [{"id": "u", "entity": "User"}]}"#)
                .unwrap();

        assert_eq!(input.options.dynamic_columns, DynamicColumnMode::Default);
    }

    #[test]
    fn options_dynamic_columns_all() {
        let input = parse_input(
            r#"{
            "query_type": "neighbors",
            "nodes": [{"id": "u", "entity": "User", "node_ids": [1]}],
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
            "nodes": [{"id": "u", "entity": "User", "node_ids": [1]}],
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
            "nodes": [{"id": "u", "entity": "User"}],
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
            "nodes": [{"id": "u", "entity": "User"}],
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
            "nodes": [{"id": "u", "entity": "User"}],
            "options": {"dynamic_columns": "*"}
        }"#,
        )
        .unwrap();

        assert!(!input.options.include_debug_sql);
    }

    #[test]
    fn node_ids_accepts_integers_and_strings() {
        let input = parse_input(
            r#"{
            "query_type": "traversal",
            "nodes": [{
                "id": "u",
                "entity": "User",
                "node_ids": [1, "9007199254740993", -42]
            }]
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
            "nodes": [{
                "id": "u",
                "entity": "User",
                "id_range": {"start": 1, "end": "9007199254740993"}
            }]
        }"#,
        )
        .unwrap();

        let range = input.nodes[0].id_range.as_ref().unwrap();
        assert_eq!(range.start, 1);
        assert_eq!(range.end, 9_007_199_254_740_993);
    }
}
