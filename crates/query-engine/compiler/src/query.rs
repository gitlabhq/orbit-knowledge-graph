use std::collections::{BTreeMap, HashMap};
use std::ops::Deref;

use ontology::constants::{SOURCE_ID_COLUMN, TARGET_ID_COLUMN};
use ontology::{DataType, EnumType, FieldSelectivity, VirtualSource};
use orbit_utils::traversal_path::TraversalPath;
use query_data_model::{EntityId, PropertyId, RelationshipId};
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, strum::IntoStaticStr)]
#[strum(serialize_all = "lowercase")]
pub enum DynamicColumnMode {
    #[serde(rename = "*")]
    All,
    #[default]
    #[serde(rename = "default")]
    Default,
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
    #[serde(skip)]
    Hydration,
}

#[derive(Debug, Clone)]
pub struct Cursor {
    pub page_size: u32,
    pub after: Option<String>,
}

#[derive(Debug, Clone)]
pub struct IdRange {
    pub start: i64,
    pub end: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, strum::AsRefStr, strum::VariantNames)]
#[serde(rename_all = "snake_case")]
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
    TokenMatch,
    AllTokens,
    AnyTokens,
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
            Self::Outgoing | Self::Both => (SOURCE_ID_COLUMN, TARGET_ID_COLUMN),
            Self::Incoming => (TARGET_ID_COLUMN, SOURCE_ID_COLUMN),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
#[strum(serialize_all = "lowercase")]
pub enum AggFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    Collect,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, strum::VariantNames)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
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

    pub fn requires_selectivity_guard(self) -> bool {
        matches!(self, Self::Minute | Self::Hour)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum OrderDirection {
    #[default]
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId(pub(crate) usize);

impl NodeId {
    pub fn index(self) -> usize {
        self.0
    }
}

impl Default for NodeId {
    fn default() -> Self {
        Self(usize::MAX)
    }
}

#[derive(Debug, Clone)]
pub enum PropertySource {
    Stored(String),
    Virtual(VirtualSource),
}

#[derive(Debug, Clone)]
pub struct Property {
    pub id: PropertyId,
    pub name: String,
    pub data_type: DataType,
    pub enum_values: Option<BTreeMap<i64, String>>,
    pub enum_type: EnumType,
    pub source: PropertySource,
    pub filterable: bool,
    pub admin_only: bool,
    pub hidden: bool,
    pub allows_like: bool,
    pub has_text_index: bool,
    pub selectivity: FieldSelectivity,
}

impl Property {
    pub fn column(&self) -> Option<&str> {
        match &self.source {
            PropertySource::Stored(column) => Some(column),
            PropertySource::Virtual(_) => None,
        }
    }

    pub fn virtual_source(&self) -> Option<&VirtualSource> {
        match &self.source {
            PropertySource::Stored(_) => None,
            PropertySource::Virtual(source) => Some(source),
        }
    }

    pub fn is_stored(&self) -> bool {
        self.column().is_some()
    }

    pub fn is_virtual(&self) -> bool {
        self.virtual_source().is_some()
    }
}

#[derive(Debug, Clone)]
pub struct StoredProperty {
    property: Property,
    column: String,
}

impl StoredProperty {
    pub(crate) fn new(property: Property, column: String) -> Self {
        Self { property, column }
    }

    pub fn column(&self) -> &str {
        &self.column
    }
}

impl Deref for StoredProperty {
    type Target = Property;

    fn deref(&self) -> &Self::Target {
        &self.property
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Predicate {
    pub op: FilterOp,
    pub value: Option<Value>,
}

impl Predicate {
    pub fn value_str(&self) -> Option<&str> {
        self.value.as_ref().and_then(Value::as_str)
    }
}

#[derive(Debug, Clone)]
pub struct Filter {
    pub name: String,
    pub property: Option<PropertyId>,
    pub data_type: DataType,
    pub stored: bool,
    pub selectivity: FieldSelectivity,
    pub in_sort_key: bool,
    pub predicate: Predicate,
}

#[derive(Debug, Clone)]
pub struct PropertyRef {
    pub node: NodeId,
    pub property: StoredProperty,
}

#[derive(Debug, Clone)]
pub struct Node {
    pub id: NodeId,
    pub alias: String,
    pub entity: EntityId,
    pub entity_name: String,
    pub table: String,
    pub columns: Vec<Property>,
    pub filters: Vec<Filter>,
    pub node_ids: Vec<i64>,
    pub id_range: Option<IdRange>,
    pub id_property: StoredProperty,
    pub traversal_paths: Vec<TraversalPath>,
    pub has_traversal_path: bool,
    pub is_global: bool,
    pub redaction_id_column: String,
}

impl Node {
    pub fn filters(&self, name: &str) -> impl Iterator<Item = &Filter> {
        self.filters
            .iter()
            .filter(move |filter| filter.name == name)
    }

    pub fn has_filter(&self, name: &str) -> bool {
        self.filters.iter().any(|filter| filter.name == name)
    }

    pub fn bounds_scan(&self) -> bool {
        !self.node_ids.is_empty()
            || self.id_range.is_some()
            || self.filters.iter().any(|filter| {
                filter.selectivity == FieldSelectivity::High
                    && matches!(
                        filter.predicate.op,
                        FilterOp::Eq | FilterOp::In | FilterOp::StartsWith | FilterOp::TokenMatch
                    )
            })
    }
}

#[derive(Debug, Clone)]
pub struct RelationshipRoute {
    pub id: RelationshipId,
    pub name: String,
    pub table: String,
}

#[derive(Debug, Clone)]
pub struct Relationship {
    pub routes: Vec<RelationshipRoute>,
    pub from: NodeId,
    pub to: NodeId,
    pub hops: HopRange,
    pub direction: Direction,
    pub filters: Vec<Filter>,
}

impl Relationship {
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.routes.iter().map(|route| route.name.as_str())
    }

    pub fn names_vec(&self) -> Vec<String> {
        self.names().map(str::to_string).collect()
    }
}

#[derive(Debug, Clone)]
pub struct Metric {
    pub function: AggFunction,
    pub target: NodeId,
    pub property: Option<StoredProperty>,
    pub alias: String,
}

#[derive(Debug, Clone)]
pub enum Group {
    Node {
        node: NodeId,
        alias: String,
    },
    Property {
        node: NodeId,
        property: Box<StoredProperty>,
        truncate: Option<TruncateUnit>,
        alias: String,
    },
}

impl Group {
    pub fn node(&self) -> NodeId {
        match self {
            Self::Node { node, .. } | Self::Property { node, .. } => *node,
        }
    }

    pub fn property(&self) -> Option<&StoredProperty> {
        match self {
            Self::Node { .. } => None,
            Self::Property { property, .. } => Some(property),
        }
    }

    pub fn output_name(&self) -> &str {
        match self {
            Self::Node { alias, .. } | Self::Property { alias, .. } => alias,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Aggregation {
    pub metrics: Vec<Metric>,
    pub groups: Vec<Group>,
    pub sort: Option<AggregationSort>,
}

#[derive(Debug, Clone)]
pub struct AggregationSort {
    pub column: String,
    pub direction: OrderDirection,
}

#[derive(Debug, Clone)]
pub struct Path {
    pub from: NodeId,
    pub to: NodeId,
    pub max_depth: u32,
    pub routes: Vec<RelationshipRoute>,
    pub wildcard: bool,
}

#[derive(Debug, Clone)]
pub struct Neighbors {
    pub direction: Direction,
    pub routes: Vec<RelationshipRoute>,
}

#[derive(Debug, Clone)]
pub struct Order {
    pub node: NodeId,
    pub property: StoredProperty,
    pub direction: OrderDirection,
}

#[derive(Debug, Clone)]
pub struct Comparison {
    pub left: PropertyRef,
    pub op: FilterOp,
    pub right: PropertyRef,
}

#[derive(Debug, Clone)]
pub enum QueryBody {
    Traversal {
        relationships: Vec<Relationship>,
        order: Option<Order>,
        comparisons: Vec<Comparison>,
    },
    Aggregation {
        relationships: Vec<Relationship>,
        aggregation: Aggregation,
    },
    PathFinding(Path),
    Neighbors(Neighbors),
    Hydration,
}

#[derive(Debug, Clone)]
pub struct Query {
    pub nodes: Vec<Node>,
    pub limit: u32,
    pub cursor: Option<Cursor>,
    pub dynamic_columns: DynamicColumnMode,
    pub include_debug_sql: bool,
    body: QueryBody,
    aliases: HashMap<String, NodeId>,
}

impl Query {
    pub(crate) fn new(
        mut nodes: Vec<Node>,
        body: QueryBody,
        limit: u32,
        cursor: Option<Cursor>,
        dynamic_columns: DynamicColumnMode,
        include_debug_sql: bool,
    ) -> Self {
        let mut aliases = HashMap::new();
        for (index, node) in nodes.iter_mut().enumerate() {
            node.id = NodeId(index);
            aliases.insert(node.alias.clone(), node.id);
        }
        Self {
            nodes,
            body,
            limit,
            cursor,
            dynamic_columns,
            include_debug_sql,
            aliases,
        }
    }

    pub fn empty(query_type: QueryType) -> Self {
        let body = match query_type {
            QueryType::Traversal => QueryBody::Traversal {
                relationships: Vec::new(),
                order: None,
                comparisons: Vec::new(),
            },
            QueryType::Aggregation => QueryBody::Aggregation {
                relationships: Vec::new(),
                aggregation: Aggregation {
                    metrics: Vec::new(),
                    groups: Vec::new(),
                    sort: None,
                },
            },
            QueryType::Neighbors => QueryBody::Neighbors(Neighbors {
                direction: Direction::default(),
                routes: Vec::new(),
            }),
            QueryType::Hydration => QueryBody::Hydration,
            QueryType::PathFinding => panic!("path_finding query requires endpoints"),
        };
        Self::new(Vec::new(), body, 30, None, Default::default(), false)
    }

    pub fn query_type(&self) -> QueryType {
        match self.body {
            QueryBody::Traversal { .. } => QueryType::Traversal,
            QueryBody::Aggregation { .. } => QueryType::Aggregation,
            QueryBody::PathFinding(_) => QueryType::PathFinding,
            QueryBody::Neighbors(_) => QueryType::Neighbors,
            QueryBody::Hydration => QueryType::Hydration,
        }
    }

    pub fn relationships(&self) -> &[Relationship] {
        match &self.body {
            QueryBody::Traversal { relationships, .. }
            | QueryBody::Aggregation { relationships, .. } => relationships,
            _ => &[],
        }
    }

    pub fn aggregation(&self) -> Option<&Aggregation> {
        match &self.body {
            QueryBody::Aggregation { aggregation, .. } => Some(aggregation),
            _ => None,
        }
    }

    pub fn path(&self) -> Option<&Path> {
        match &self.body {
            QueryBody::PathFinding(path) => Some(path),
            _ => None,
        }
    }

    pub fn neighbors(&self) -> Option<&Neighbors> {
        match &self.body {
            QueryBody::Neighbors(neighbors) => Some(neighbors),
            _ => None,
        }
    }

    pub fn order(&self) -> Option<&Order> {
        match &self.body {
            QueryBody::Traversal { order, .. } => order.as_ref(),
            _ => None,
        }
    }

    pub fn comparisons(&self) -> &[Comparison] {
        match &self.body {
            QueryBody::Traversal { comparisons, .. } => comparisons,
            _ => &[],
        }
    }

    pub fn node(&self, id: NodeId) -> &Node {
        &self.nodes[id.index()]
    }

    pub fn node_mut(&mut self, id: NodeId) -> &mut Node {
        &mut self.nodes[id.index()]
    }

    pub fn node_named(&self, alias: &str) -> Option<&Node> {
        self.aliases.get(alias).map(|id| self.node(*id))
    }

    pub fn node_id(&self, alias: &str) -> Option<NodeId> {
        self.aliases.get(alias).copied()
    }

    pub fn remove_node(&mut self, id: NodeId) {
        self.nodes.remove(id.index());
        let remap = |node: &mut NodeId| {
            if node.index() > id.index() {
                node.0 -= 1;
            }
        };
        match &mut self.body {
            QueryBody::Traversal {
                relationships,
                order,
                comparisons,
            } => {
                relationships.retain_mut(|relationship| {
                    if relationship.from == id || relationship.to == id {
                        return false;
                    }
                    remap(&mut relationship.from);
                    remap(&mut relationship.to);
                    true
                });
                if order.as_ref().is_some_and(|order| order.node == id) {
                    *order = None;
                } else if let Some(order) = order {
                    remap(&mut order.node);
                }
                comparisons.retain_mut(|comparison| {
                    if comparison.left.node == id || comparison.right.node == id {
                        return false;
                    }
                    remap(&mut comparison.left.node);
                    remap(&mut comparison.right.node);
                    true
                });
            }
            QueryBody::Aggregation {
                relationships,
                aggregation,
            } => {
                relationships.retain_mut(|relationship| {
                    if relationship.from == id || relationship.to == id {
                        return false;
                    }
                    remap(&mut relationship.from);
                    remap(&mut relationship.to);
                    true
                });
                aggregation.metrics.retain_mut(|metric| {
                    if metric.target == id {
                        return false;
                    }
                    remap(&mut metric.target);
                    true
                });
                aggregation.groups.retain_mut(|group| {
                    let node = match group {
                        Group::Node { node, .. } | Group::Property { node, .. } => node,
                    };
                    if *node == id {
                        return false;
                    }
                    remap(node);
                    true
                });
            }
            QueryBody::PathFinding(path) => {
                remap(&mut path.from);
                remap(&mut path.to);
            }
            QueryBody::Neighbors(_) | QueryBody::Hydration => {}
        }
        self.reindex();
    }

    pub fn remove_relationship(&mut self, index: usize) {
        match &mut self.body {
            QueryBody::Traversal { relationships, .. }
            | QueryBody::Aggregation { relationships, .. } => {
                relationships.remove(index);
            }
            _ => {}
        }
    }

    pub fn reindex(&mut self) {
        self.aliases.clear();
        for (index, node) in self.nodes.iter_mut().enumerate() {
            node.id = NodeId(index);
            self.aliases.insert(node.alias.clone(), node.id);
        }
    }

    pub fn is_search(&self) -> bool {
        self.query_type() == QueryType::Traversal
            && self.nodes.len() == 1
            && self.relationships().is_empty()
    }

    pub fn fetch_limit(&self) -> u32 {
        self.cursor
            .as_ref()
            .map_or(self.limit, |cursor| cursor.page_size)
            + 1
    }
}

pub(crate) fn is_wildcard(types: &[String]) -> bool {
    types.is_empty() || types == ["*"]
}
