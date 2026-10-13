//! The compiler's language-neutral query input. Frontends lower into it; the
//! validate pass checks identifiers, ontology membership, and limits.

use ontology::constants::{
    DEFAULT_PRIMARY_KEY, SOURCE_ID_COLUMN, TARGET_ID_COLUMN, TRAVERSAL_PATH_COLUMN,
};
use orbit_utils::traversal_path::TraversalPath;
use serde_json::Value;
use std::collections::HashMap;

mod predicates;
pub use predicates::{BooleanExpression, Condition, PredicateTarget};

/// Controls which columns are fetched for dynamically-discovered entities
/// during hydration (PathFinding, Neighbors).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, strum::IntoStaticStr)]
#[strum(serialize_all = "lowercase")]
pub enum DynamicColumnMode {
    All,
    #[default]
    Default,
}

/// Optional presentation hints that control response shape without affecting query
/// semantics. Only `dynamic_columns` and `include_debug_sql` are recognized.
#[derive(Debug, Clone, Default)]
pub struct QueryOptions {
    pub dynamic_columns: DynamicColumnMode,
    /// On SaaS: honored for GitLab team members. On self-managed/Dedicated:
    /// honored for instance admins only.
    pub include_debug_sql: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RelationshipReturn {
    pub columns: Vec<RelationshipColumn>,
    pub column_order: Vec<String>,
    pub sort: Option<RelationshipOrder>,
}

impl RelationshipReturn {
    pub fn uses(&self, relationship: usize) -> bool {
        self.columns
            .iter()
            .any(|column| column.relationship == relationship)
            || self
                .sort
                .as_ref()
                .is_some_and(|sort| sort.relationship == relationship)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationshipColumn {
    pub name: String,
    pub relationship: usize,
    pub kind: RelationshipColumnKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationshipColumnKind {
    Type,
    Relationship,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationshipOrder {
    pub relationship: usize,
    pub direction: OrderDirection,
}

#[derive(Debug, Clone)]
pub struct Input {
    pub query_type: QueryType,
    pub nodes: Vec<InputNode>,
    pub relationships: Vec<InputRelationship>,
    pub aggregation: InputAggregation,
    pub path: Option<InputPath>,
    pub neighbors: Option<InputNeighbors>,
    pub limit: u32,
    pub cursor: Option<InputCursor>,
    pub order_by: Option<InputOrderBy>,
    pub options: QueryOptions,
    pub predicates: Vec<BooleanExpression<Condition>>,
    pub relationship_return: RelationshipReturn,
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
            predicates: Vec::new(),
            relationship_return: RelationshipReturn::default(),
        }
    }
}

fn default_limit() -> u32 {
    30
}

/// Keyset pagination cursor: `after` is an opaque token from the previous
/// page's `pagination.next_cursor`, anchored on the last scanned SQL row so
/// redaction can only shorten a page, never strand it.
#[derive(Debug, Clone)]
pub struct InputCursor {
    pub page_size: u32,
    pub after: Option<String>,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, strum::Display, strum::IntoStaticStr, strum::VariantNames,
)]
#[strum(serialize_all = "snake_case")]
pub enum QueryType {
    Traversal,
    Aggregation,
    PathFinding,
    Neighbors,
    /// Internal-only: consolidated hydration for multiple entity types.
    /// Generates a UNION ALL of search-like arms, one per node. Skips
    /// security context injection (IDs are pre-authorized by the pipeline).
    Hydration,
}

// ─────────────────────────────────────────────────────────────────────────────
// Nodes
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct InputNode {
    pub id: String,
    pub entity: Option<String>,
    /// If not specified, only mandatory columns (id, type) are returned.
    pub columns: Option<ColumnSelection>,
    pub filters: HashMap<String, Vec<InputFilter>>,
    pub node_ids: Vec<i64>,
    pub id_range: Option<InputIdRange>,
    pub id_property: String,
    /// Narrowed traversal paths extracted from base query results. Used by the
    /// hydration pipeline to inject `startsWith(traversal_path, tp)` into hydration
    /// queries, pruning granules through the primary key.
    pub traversal_paths: Vec<TraversalPath>,
}

impl Default for InputNode {
    fn default() -> Self {
        Self {
            id: String::new(),
            entity: None,
            columns: None,
            filters: HashMap::new(),
            node_ids: Vec::new(),
            id_range: None,
            id_property: DEFAULT_PRIMARY_KEY.to_string(),
            traversal_paths: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnSelection {
    All,
    List(Vec<String>),
}

#[derive(Debug, Clone)]
pub struct InputIdRange {
    pub start: i64,
    pub end: i64,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct InputFilter {
    pub op: Option<FilterOp>,
    pub value: Option<Value>,
    /// When set, compare against another node's column instead of a literal.
    /// Format: `(node_alias, property_name)`. Mutually exclusive with `value`.
    pub rhs_column: Option<(String, String)>,
}

impl InputFilter {
    pub fn value_str(&self) -> Option<&str> {
        self.value.as_ref().and_then(Value::as_str)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::AsRefStr)]
#[strum(serialize_all = "snake_case")]
pub enum FilterOp {
    Eq,
    Ne,
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

#[derive(Debug, Clone)]
pub struct InputRelationship {
    pub types: Vec<String>,
    pub from: String,
    pub to: String,
    pub hops: HopRange,
    pub direction: Direction,
    pub filters: HashMap<String, Vec<InputFilter>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HopRange {
    pub min: u32,
    pub max: u32,
}

impl Default for HopRange {
    fn default() -> Self {
        Self { min: 1, max: 1 }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
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

#[derive(Debug, Clone, Default)]
pub struct InputAggregation {
    pub metrics: Vec<InputAggregationMetric>,
    pub group_by: Vec<InputGroupByKey>,
    pub sort: Option<InputAggSort>,
}

#[derive(Debug, Clone)]
pub struct InputAggregationMetric {
    pub expr: AggExpr,
    pub alias: Option<String>,
}

impl InputAggregationMetric {
    pub fn output_name(&self) -> String {
        self.alias
            .clone()
            .unwrap_or_else(|| self.expr.derived_name())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, strum::EnumDiscriminants)]
#[strum_discriminants(
    name(AggFunction),
    vis(pub),
    derive(strum::Display),
    strum(serialize_all = "lowercase")
)]
pub enum AggExpr {
    Count(TargetRef),
    Sum(PropertyRef),
    Avg(PropertyRef),
    Min(PropertyRef),
    Max(PropertyRef),
    Collect(PropertyRef),
}

impl AggExpr {
    pub fn function(&self) -> AggFunction {
        self.into()
    }

    pub fn node(&self) -> &str {
        match self {
            Self::Count(target) => &target.node,
            Self::Sum(prop)
            | Self::Avg(prop)
            | Self::Min(prop)
            | Self::Max(prop)
            | Self::Collect(prop) => &prop.node,
        }
    }

    pub fn property(&self) -> Option<&str> {
        match self {
            Self::Count(target) => target.property.as_deref(),
            Self::Sum(prop)
            | Self::Avg(prop)
            | Self::Min(prop)
            | Self::Max(prop)
            | Self::Collect(prop) => Some(&prop.property),
        }
    }

    pub fn derived_name(&self) -> String {
        let function = self.function();
        let node = self.node();
        match self.property() {
            Some(property) => format!("{function}_{node}_{property}"),
            None => format!("{function}_{node}"),
        }
    }
}

#[cfg(test)]
impl AggExpr {
    pub(crate) fn from_parts(function: AggFunction, node: &str, property: Option<&str>) -> Self {
        let prop_ref = |property: &str| PropertyRef {
            node: node.to_owned(),
            property: property.to_owned(),
        };
        match function {
            AggFunction::Count => Self::Count(TargetRef {
                node: node.to_owned(),
                property: property.map(str::to_owned),
            }),
            AggFunction::Sum => Self::Sum(prop_ref(property.expect("sum needs a property"))),
            AggFunction::Avg => Self::Avg(prop_ref(property.expect("avg needs a property"))),
            AggFunction::Min => Self::Min(prop_ref(property.expect("min needs a property"))),
            AggFunction::Max => Self::Max(prop_ref(property.expect("max needs a property"))),
            AggFunction::Collect => {
                Self::Collect(prop_ref(property.expect("collect needs a property")))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetRef {
    pub node: String,
    pub property: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertyRef {
    pub node: String,
    pub property: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InputGroupByKey {
    Node {
        node: String,
        alias: Option<String>,
    },
    Property {
        node: String,
        property: String,
        truncate: Option<TruncateUnit>,
        alias: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

    pub fn truncate(&self) -> Option<TruncateUnit> {
        match self {
            Self::Property { truncate, .. } => *truncate,
            Self::Node { .. } => None,
        }
    }

    pub fn output_name(&self) -> String {
        match self {
            Self::Node { node, alias } => alias.clone().unwrap_or_else(|| node.clone()),
            Self::Property {
                node,
                property,
                truncate,
                alias,
            } => alias.clone().unwrap_or_else(|| match truncate {
                Some(unit) => format!("{}_{}_{}", node, property, unit.name()),
                None => format!("{}_{}", node, property),
            }),
        }
    }
}

pub fn group_by_output_names(groups: &[InputGroupByKey]) -> Vec<String> {
    groups.iter().map(InputGroupByKey::output_name).collect()
}

pub fn node_group_ids(groups: &[InputGroupByKey]) -> impl Iterator<Item = &str> {
    groups.iter().filter_map(|group| match group {
        InputGroupByKey::Node { node, .. } => Some(node.as_str()),
        InputGroupByKey::Property { .. } => None,
    })
}

pub fn property_groups(groups: &[InputGroupByKey]) -> impl Iterator<Item = (&str, &str)> {
    groups.iter().filter_map(|group| match group {
        InputGroupByKey::Property { node, property, .. } => {
            Some((node.as_str(), property.as_str()))
        }
        InputGroupByKey::Node { .. } => None,
    })
}

pub fn group_by_kind(group: &InputGroupByKey) -> &'static str {
    match group {
        InputGroupByKey::Node { .. } => "node",
        InputGroupByKey::Property { .. } => "property",
    }
}

#[derive(Debug, Clone)]
pub struct InputPath {
    pub path_type: PathType,
    pub from: String,
    pub to: String,
    pub max_depth: u32,
    pub rel_types: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathType {
    Shortest,
}

#[derive(Debug, Clone)]
pub struct InputNeighbors {
    pub direction: Direction,
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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum OrderDirection {
    #[default]
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputAggSort {
    pub column: String,
    pub direction: OrderDirection,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn group_by_output_names_derive_and_alias() {
        let input = crate::gql::parse(
            "MATCH (u:User)-[:AUTHORED]->(mr:MergeRequest) \
             RETURN u, mr.state, date_trunc('month', mr.created_at), mr.target_branch AS branch, count(mr) AS count",
        )
        .unwrap();
        assert_eq!(
            group_by_output_names(&input.aggregation.group_by),
            vec!["u", "mr_state", "mr_created_at_month", "branch"]
        );
    }

    #[test]
    fn aggregation_derives_output_name_without_alias() {
        let input = crate::gql::parse(
            "MATCH (mr:MergeRequest) RETURN count(mr), avg(mr.merge_duration), max(mr.additions) AS biggest",
        )
        .unwrap();
        let names: Vec<String> = input
            .aggregation
            .metrics
            .iter()
            .map(InputAggregationMetric::output_name)
            .collect();
        assert_eq!(names, ["count_mr", "avg_mr_merge_duration", "biggest"]);
    }
}
