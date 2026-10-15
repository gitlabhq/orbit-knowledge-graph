//! Query validation.
//!
//! [`Validator`] checks in three layers:
//! 1. **Schema validation** — JSON-only: the raw text against the base and
//!    ontology-derived JSON Schemas, before deserialization.
//! 2. **Shape validation** — every [`Input`] regardless of frontend: identifiers,
//!    ontology membership, limits, and filter value shapes.
//! 3. **Cross-reference validation** — node ID references, complexity caps, and
//!    filter typing that neither layer above can express.

use std::collections::HashSet;
use std::sync::OnceLock;

use crate::constants::internal_column_prefix;
use crate::error::{QueryError, Result};
use crate::input::{
    AggFunction, ColumnSelection, Condition, FilterOp, Input, InputFilter, InputGroupByKey,
    InputNode, PredicateTarget, QueryType, group_by_output_names,
};
use crate::query_limits::{
    MAX_DEPTH_CAP, MAX_FILTER_ENTRIES_PER_PROPERTY, MAX_FILTER_STRING_LEN, MAX_HOPS_CAP,
    MAX_IDENTIFIER_LEN, MAX_IN_VALUES, MAX_LIMIT, MAX_PREDICATE_DEPTH, MAX_PREDICATE_LEAVES,
};
use crate::types::SecurityContext;
#[cfg(test)]
use ontology::Ontology;
use ontology::{DataType, TRAVERSAL_PATH_COLUMN};
use orbit_utils::traversal_path::TraversalPath;
#[cfg(test)]
use query_data_model::ClickHouseDataModel;
use query_data_model::PropertyId;

use super::errors::format_schema_error;

mod conditions;
use conditions::{check_condition_column, check_condition_counts};

pub(crate) const BASE_SCHEMA_JSON: &str =
    include_str!(concat!(env!("SCHEMA_DIR"), "/graph_query.schema.json"));

static BASE_SCHEMA_VALIDATOR: OnceLock<jsonschema::Validator> = OnceLock::new();

pub(crate) fn base_validator() -> &'static jsonschema::Validator {
    BASE_SCHEMA_VALIDATOR.get_or_init(|| {
        let schema: serde_json::Value =
            serde_json::from_str(BASE_SCHEMA_JSON).expect("schema.json must be valid JSON");
        jsonschema::validator_for(&schema).expect("schema.json must be a valid JSON Schema")
    })
}

pub(crate) fn order_by_regex() -> &'static regex::Regex {
    static ORDER_BY_REGEX: OnceLock<regex::Regex> = OnceLock::new();
    ORDER_BY_REGEX.get_or_init(|| {
        let schema: serde_json::Value =
            serde_json::from_str(BASE_SCHEMA_JSON).expect("schema.json must be valid JSON");
        let pattern = schema["properties"]["order_by"]["pattern"]
            .as_str()
            .expect("schema.json must define an order_by pattern");
        regex::Regex::new(pattern).expect("order_by pattern must be a valid regex")
    })
}

pub(crate) fn aggregation_sort_regex() -> &'static regex::Regex {
    static AGGREGATION_SORT_REGEX: OnceLock<regex::Regex> = OnceLock::new();
    AGGREGATION_SORT_REGEX.get_or_init(|| {
        let schema: serde_json::Value =
            serde_json::from_str(BASE_SCHEMA_JSON).expect("schema.json must be valid JSON");
        let pattern = schema["properties"]["aggregation_sort"]["pattern"]
            .as_str()
            .expect("schema.json must define an aggregation_sort pattern");
        regex::Regex::new(pattern).expect("aggregation_sort pattern must be a valid regex")
    })
}

pub(crate) fn node_ref_regex() -> &'static regex::Regex {
    static NODE_REF_REGEX: OnceLock<regex::Regex> = OnceLock::new();
    NODE_REF_REGEX.get_or_init(|| {
        let schema: serde_json::Value =
            serde_json::from_str(BASE_SCHEMA_JSON).expect("schema.json must be valid JSON");
        let pattern = schema["$defs"]["NodeOrPropertyRef"]["pattern"]
            .as_str()
            .expect("schema.json must define a NodeOrPropertyRef pattern");
        regex::Regex::new(pattern).expect("NodeOrPropertyRef pattern must be a valid regex")
    })
}

pub(crate) fn collect_schema_errors(
    validator: &jsonschema::Validator,
    value: &serde_json::Value,
) -> Result<()> {
    let errors: Vec<_> = validator
        .iter_errors(value)
        .map(|e| format_schema_error(&e))
        .collect();

    if errors.is_empty() {
        Ok(())
    } else {
        Err(QueryError::Validation(errors.join("; ")))
    }
}

/// Check whether a JSON value is compatible with an ontology `DataType`.
///
/// Returns `None` if compatible, or `Some(reason)` describing the mismatch.
fn check_value_type(value: &serde_json::Value, expected: DataType) -> Option<String> {
    match expected {
        DataType::String | DataType::Date | DataType::DateTime | DataType::Uuid => {
            if !value.is_string() {
                return Some(format!("is {}, not a string", json_type_name(value)));
            }
        }
        // Int-based enums are coerced to string labels by normalization before lowering.
        DataType::Enum => {
            if !value.is_string() && !value.is_i64() && !value.is_u64() {
                return Some(format!(
                    "is {}, not a string or integer",
                    json_type_name(value)
                ));
            }
        }
        DataType::Int => match value {
            serde_json::Value::Number(n) if n.is_i64() || n.is_u64() => {}
            serde_json::Value::Number(_) => {
                return Some("is a float, not an integer".to_string());
            }
            _ => {
                return Some(format!("is {}, not an integer", json_type_name(value)));
            }
        },
        DataType::Float => {
            if !value.is_number() {
                return Some(format!("is {}, not a number", json_type_name(value)));
            }
        }
        DataType::Bool => {
            if !value.is_boolean() {
                return Some(format!("is {}, not a boolean", json_type_name(value)));
            }
        }
    }
    None
}

fn json_type_name(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "a boolean",
        serde_json::Value::Number(_) => "a number",
        serde_json::Value::String(_) => "a string",
        serde_json::Value::Array(_) => "an array",
        serde_json::Value::Object(_) => "an object",
    }
}

/// Maximum span for id_range before it stops counting as selective.
/// Ranges wider than this are effectively unfiltered full-table scans.
const MAX_ID_RANGE_SPAN: i64 = 100_000;

/// Tighter id_range cap for path_finding endpoints. The frontier BFS is
/// O(anchors × E^depth), so we bound anchor cardinality to match
/// MAX_NODE_IDS (500).
const MAX_PATH_ANCHOR_RANGE: i64 = 500;

/// Maximum number of rows the lowerer will resolve from a filtered
/// path_finding endpoint CTE. Matches the node_ids cap (500).
pub(crate) const MAX_PATH_ANCHOR_LIMIT: i64 = 500;

const CONDITION_COLUMN_RULE: &str = "every condition must compare a property of a matched node or relationship with a value or another property";

/// Whether a path_finding endpoint has bounded selectivity.
/// Uses the tighter MAX_PATH_ANCHOR_RANGE cap for id_range because
/// the BFS frontier cost is O(anchors * E^depth).
fn path_endpoint_has_selectivity(node: &InputNode) -> bool {
    if !node.node_ids.is_empty() || !node.filters.is_empty() {
        return true;
    }
    if let Some(ref range) = node.id_range {
        return range.end.saturating_sub(range.start) <= MAX_PATH_ANCHOR_RANGE;
    }
    false
}

/// Whether a node has explicit selectivity (node_ids, filters, or a narrow id_range).
/// Queries where no node is selective tend to produce full-table scans.
fn node_has_selectivity(node: &InputNode) -> bool {
    if !node.node_ids.is_empty() {
        return true;
    }
    let has_literal_filter = node
        .filters
        .values()
        .flatten()
        .any(|f| f.rhs_column.is_none());
    if has_literal_filter {
        return true;
    }
    if let Some(ref range) = node.id_range {
        return range.end.saturating_sub(range.start) <= MAX_ID_RANGE_SPAN;
    }
    false
}

fn has_wide_id_range(node: &InputNode) -> bool {
    node.id_range
        .as_ref()
        .is_some_and(|range| range.end.saturating_sub(range.start) > MAX_ID_RANGE_SPAN)
}

fn wide_id_range_hint(wide: bool) -> String {
    if wide {
        format!(" (an id_range counts only with a span of at most {MAX_ID_RANGE_SPAN})")
    } else {
        String::new()
    }
}

#[derive(Default)]
pub struct Skip {
    pub selectivity: bool,
}

pub struct Validator<'a, M: query_data_model::OrbitQueryModel> {
    model: ValidationModel<'a, M>,
    skip: Skip,
}

enum ValidationModel<'a, M> {
    Borrowed(&'a M),
    #[cfg(test)]
    Owned(M),
}

impl<M> ValidationModel<'_, M> {
    fn get(&self) -> &M {
        match self {
            Self::Borrowed(model) => model,
            #[cfg(test)]
            Self::Owned(model) => model,
        }
    }
}

impl<'a, M: query_data_model::OrbitQueryModel> Validator<'a, M> {
    pub fn new(model: &'a M) -> Self {
        Self {
            model: ValidationModel::Borrowed(model),
            skip: Skip::default(),
        }
    }

    #[must_use]
    pub fn with_skip(mut self, skip: Skip) -> Self {
        self.skip = skip;
        self
    }

    fn virtual_source(&self, entity: &str, prop: &str) -> Option<&ontology::VirtualSource> {
        self.model.get().virtual_source(entity, prop)
    }

    fn property_id(&self, entity: &str, property: &str) -> Option<PropertyId> {
        self.model
            .get()
            .property(entity, property)
            .map(|property| property.id)
    }

    fn property(&self, entity: &str, property: &str) -> Option<&query_data_model::Property> {
        self.model.get().property(entity, property)
    }

    fn field_type(&self, entity: &str, property: &str) -> Option<DataType> {
        self.property(entity, property)
            .map(|property| property.data_type)
    }

    /// Validate what the JSON schema used to enforce, natively on `Input`, so
    /// every frontend is held to the same rules. Complexity caps live in
    /// [`Self::check_depth`].
    pub fn check_shape(&self, input: &Input) -> Result<()> {
        if input.nodes.is_empty() || input.query_type == QueryType::Hydration {
            return Err(QueryError::Validation(
                "a public query requires at least one node and cannot request internal hydration"
                    .into(),
            ));
        }
        if input.limit == 0 || input.limit > MAX_LIMIT {
            return Err(QueryError::Validation(format!(
                "limit must be between 1 and {MAX_LIMIT}"
            )));
        }
        if let Some(cursor) = &input.cursor
            && (cursor.page_size == 0 || cursor.page_size > MAX_LIMIT)
        {
            return Err(QueryError::Validation(format!(
                "cursor.page_size must be between 1 and {MAX_LIMIT}"
            )));
        }
        for node in &input.nodes {
            validate_identifier(&node.id)?;
            check_reserved_prefix("node id", &node.id, &format!("{}_", node.id))?;
            let entity = node
                .entity
                .as_deref()
                .ok_or_else(|| QueryError::Validation("each node requires an entity".into()))?;
            if self.model.get().entity(entity).is_none() {
                return Err(QueryError::AllowlistRejected(format!(
                    "unknown entity '{entity}'"
                )));
            }
            if !node.negated_filters.is_empty() {
                return Err(QueryError::PipelineInvariant(format!(
                    "node \"{}\" has not conditions the frontend did not lift",
                    node.id
                )));
            }
            self.check_field(entity, &node.id_property)?;
            if let Some(ColumnSelection::List(columns)) = &node.columns {
                if columns.is_empty() {
                    return Err(QueryError::Validation(
                        "column selection must list at least one column".into(),
                    ));
                }
                for column in columns {
                    self.check_field(entity, column)?;
                }
            }
            for property in node.filters.keys() {
                self.check_field(entity, property)?;
            }
            check_filters(&node.filters)?;
        }
        for edge in &input.relationships {
            if edge.types.is_empty()
                || edge.hops.min == 0
                || edge.hops.min > edge.hops.max
                || edge.hops.max > MAX_HOPS_CAP
            {
                return Err(QueryError::Validation(format!(
                    "relationships require types and ordered hop bounds between 1 and {MAX_HOPS_CAP}"
                )));
            }
            self.check_relationship_types(&edge.types)?;
            check_filters(&edge.filters)?;
        }
        self.check_predicate_shape(input)?;
        if let Some(path) = &input.path {
            if path.max_depth == 0 || path.max_depth > MAX_DEPTH_CAP {
                return Err(QueryError::Validation(format!(
                    "path max_depth must be between 1 and {MAX_DEPTH_CAP}"
                )));
            }
            self.check_relationship_types(&path.rel_types)?;
        }
        if let Some(neighbors) = &input.neighbors {
            self.check_relationship_types(&neighbors.rel_types)?;
        }
        if input.query_type == QueryType::Neighbors && input.nodes.len() != 1 {
            return Err(QueryError::Validation(
                "neighbors requires exactly one center node".into(),
            ));
        }
        if input.query_type == QueryType::Aggregation && input.aggregation.metrics.is_empty() {
            return Err(QueryError::Validation(
                "aggregation requires at least one metric".into(),
            ));
        }
        for metric in &input.aggregation.metrics {
            validate_identifier(metric.expr.node())?;
            if let Some(property) = metric.expr.property() {
                validate_identifier(property)?;
            }
            if let Some(alias) = &metric.alias {
                validate_identifier(alias)?;
            }
        }
        for group in &input.aggregation.group_by {
            validate_identifier(group.node())?;
            if let Some(property) = group.property() {
                validate_identifier(property)?;
            }
            let (InputGroupByKey::Node { alias, .. } | InputGroupByKey::Property { alias, .. }) =
                group;
            if let Some(alias) = alias {
                validate_identifier(alias)?;
            }
        }
        if let Some(order) = &input.order_by {
            validate_identifier(&order.node)?;
            validate_identifier(&order.property)?;
        }
        if let Some(sort) = &input.aggregation.sort {
            validate_identifier(&sort.column)?;
        }
        Ok(())
    }

    fn check_field(&self, entity: &str, property: &str) -> Result<()> {
        validate_identifier(property)?;
        if ontology::constants::NODE_RESERVED_COLUMNS.contains(&property) {
            return Ok(());
        }
        self.property(entity, property).map(|_| ()).ok_or_else(|| {
            let entity_record = self.model.get().entity(entity);
            let properties: Vec<_> = entity_record
                .into_iter()
                .flat_map(|entity| &entity.properties)
                .filter(|property| !self.model.get().property_is_hidden(**property))
                .map(|property| self.model.get().graph().property(*property).name.as_str())
                .collect();
            QueryError::AllowlistRejected(format!(
                "field \"{property}\" does not exist on node type \"{entity}\"{}",
                ontology::errors::format_candidate_list("fields", &properties)
            ))
        })
    }

    fn check_relationship_types(&self, types: &[String]) -> Result<()> {
        for kind in types {
            if kind != "*" && !self.model.get().relationship_exists(kind) {
                return Err(QueryError::AllowlistRejected(format!(
                    "unknown relationship type {kind:?}"
                )));
            }
        }
        Ok(())
    }

    /// Validate cross-node references that JSON Schema cannot express.
    pub fn check_references(&self, input: &Input) -> Result<()> {
        self.check_duplicate_node_ids(input)?;
        self.check_relationships(input)?;
        self.check_aggregations(input)?;
        self.check_order_by(input)?;
        self.check_path(input)?;
        self.check_neighbors(input)?;
        self.check_depth(input)?;
        self.check_selectivity(input)?;
        self.check_predicates(input)?;
        self.check_filter_types(input)?;
        // Run after individual reference checks so "undefined node X" errors
        // take priority over "node Y is unreferenced".
        self.check_unreferenced_nodes(input)?;
        Ok(())
    }

    /// Enforce query limits for every frontend, including counts across input fields.
    pub fn check_depth(&self, input: &Input) -> Result<()> {
        use crate::query_limits::{
            MAX_COLUMNS, MAX_DEPTH_CAP, MAX_HOPS_CAP, MAX_IN_VALUES, MAX_NODE_IDS, MAX_NODES_CAP,
            MAX_PREDICATES, MAX_REL_TYPES, MAX_RELS_CAP,
        };
        const MAX_AGGS_CAP: usize = 10;
        const MAX_GROUP_BY_KEYS: usize = 4;

        let predicate_count = input.conditions().count()
            + input
                .nodes
                .iter()
                .map(|node| {
                    node.filters.values().map(Vec::len).sum::<usize>()
                        + usize::from(!node.node_ids.is_empty())
                        + 2 * usize::from(node.id_range.is_some())
                })
                .sum::<usize>()
            + input
                .relationships
                .iter()
                .flat_map(|relationship| relationship.filters.values())
                .map(Vec::len)
                .sum::<usize>();
        if predicate_count > MAX_PREDICATES {
            return Err(QueryError::Validation(format!(
                "Query has {predicate_count} predicates; the maximum is {MAX_PREDICATES} across the entire query. Remove redundant conditions or simplify the query."
            )));
        }
        if input.nodes.len() > MAX_NODES_CAP {
            return Err(QueryError::Validation(format!(
                "nodes count ({}) must not exceed {MAX_NODES_CAP}",
                input.nodes.len()
            )));
        }
        if input.relationships.len() > MAX_RELS_CAP {
            return Err(QueryError::Validation(format!(
                "relationships count ({}) must not exceed {MAX_RELS_CAP}",
                input.relationships.len()
            )));
        }
        // Multi-node traversal requires exactly n-1 relationships (1 node + 0 rels is a search).
        if input.query_type == QueryType::Traversal
            && !input.nodes.is_empty()
            && !(input.nodes.len() == 1 && input.relationships.is_empty())
            && input.relationships.len() != input.nodes.len() - 1
        {
            return Err(QueryError::Validation(format!(
                "traversal requires exactly n-1 relationships for n nodes (got {} nodes, {} relationships)",
                input.nodes.len(),
                input.relationships.len()
            )));
        }
        if input.aggregation.metrics.len() > MAX_AGGS_CAP {
            return Err(QueryError::Validation(format!(
                "aggregations count ({}) must not exceed {MAX_AGGS_CAP}",
                input.aggregation.metrics.len()
            )));
        }
        if input.aggregation.group_by.len() > MAX_GROUP_BY_KEYS {
            return Err(QueryError::Validation(format!(
                "group_by count ({}) must not exceed {MAX_GROUP_BY_KEYS}",
                input.aggregation.group_by.len()
            )));
        }
        for rel in &input.relationships {
            // "both" generates OR joins that defeat CH index usage; schema also rejects it,
            // but this guard covers code paths that bypass schema validation.
            if rel.direction == crate::input::Direction::Both
                && input.query_type == QueryType::Aggregation
            {
                return Err(QueryError::Validation(
                    "aggregation does not support direction: \"both\" on relationships; \
                     use separate queries for outgoing and incoming directions"
                        .into(),
                ));
            }
            if rel.hops.max > MAX_HOPS_CAP {
                return Err(QueryError::Validation(format!(
                    "hops upper bound ({}) must not exceed {MAX_HOPS_CAP}",
                    rel.hops.max
                )));
            }
            if rel.types.len() > MAX_REL_TYPES {
                return Err(QueryError::Validation(format!(
                    "relationship type count ({}) must not exceed {MAX_REL_TYPES}",
                    rel.types.len()
                )));
            }
        }
        if let Some(ref path) = input.path {
            if path.max_depth > MAX_DEPTH_CAP {
                return Err(QueryError::Validation(format!(
                    "max_depth ({}) must not exceed {MAX_DEPTH_CAP}",
                    path.max_depth
                )));
            }
            if path.rel_types.len() > MAX_REL_TYPES {
                return Err(QueryError::Validation(format!(
                    "path rel_types count ({}) must not exceed {MAX_REL_TYPES}",
                    path.rel_types.len()
                )));
            }
        }
        if let Some(neighbors) = &input.neighbors
            && neighbors.rel_types.len() > MAX_REL_TYPES
        {
            return Err(QueryError::Validation(format!(
                "neighbors rel_types count ({}) must not exceed {MAX_REL_TYPES}",
                neighbors.rel_types.len()
            )));
        }
        for node in &input.nodes {
            if node.node_ids.len() > MAX_NODE_IDS {
                return Err(QueryError::Validation(format!(
                    "node_ids count ({}) for node \"{}\" must not exceed {MAX_NODE_IDS}",
                    node.node_ids.len(),
                    node.id
                )));
            }
            if let Some(crate::input::ColumnSelection::List(cols)) = &node.columns
                && cols.len() > MAX_COLUMNS
            {
                return Err(QueryError::Validation(format!(
                    "columns count ({}) for node \"{}\" must not exceed {MAX_COLUMNS}",
                    cols.len(),
                    node.id
                )));
            }
            for (prop, filters) in &node.filters {
                for filter in filters {
                    if let Some(FilterOp::In) = filter.op {
                        let len = filter
                            .value
                            .as_ref()
                            .and_then(|v| v.as_array())
                            .map(|a| a.len())
                            .unwrap_or(0);
                        if len > MAX_IN_VALUES {
                            return Err(QueryError::Validation(format!(
                                "IN filter on \"{prop}\" for node \"{}\" has {len} values, must not exceed {MAX_IN_VALUES}",
                                node.id
                            )));
                        }
                    }
                }
            }
        }
        check_condition_counts(input)
    }

    /// Validate that filter values are compatible with the ontology column type.
    ///
    /// For scalar ops (eq, gt, lt, ...) the JSON value must match the column's
    /// DataType. For `in`, every element of the JSON array must match; the
    /// first mismatched element returns a `QueryError::Validation` and the
    /// query is rejected. Ops that carry no value (`is_null`, `is_not_null`)
    /// are skipped. Filters on unknown entities or fields are skipped —
    /// `check_ontology` catches those.
    ///
    /// Relationship filters are validated against the fixed edge table schema.
    /// Unknown edge columns are rejected (fail closed) since they would
    /// produce broken SQL at runtime.
    pub(crate) fn check_filter_types(&self, input: &Input) -> Result<()> {
        if input.predicates.iter().any(|root| {
            root.fold(
                &|_| false,
                &|children| children.is_empty() || children.contains(&true),
                &|child| child,
            )
        }) {
            return Err(QueryError::Validation(CONDITION_COLUMN_RULE.into()));
        }
        for node in &input.nodes {
            for (prop, filters) in &node.filters {
                self.check_node_filters(input, node, prop, filters)?;
            }
        }
        for (i, rel) in input.relationships.iter().enumerate() {
            for (prop, filters) in &rel.filters {
                self.check_relationship_filters(input, i, prop, filters)?;
            }
        }
        for condition in input.conditions().filter(|c| !c.is_relationship_type()) {
            let filters = std::slice::from_ref(&condition.filter);
            match &condition.target {
                PredicateTarget::Node(alias) => {
                    let node = input.node(alias).ok_or_else(|| {
                        QueryError::ReferenceError(format!(
                            "condition references undefined node \"{alias}\""
                        ))
                    })?;
                    self.check_node_filters(input, node, &condition.property, filters)?;
                }
                PredicateTarget::Relationship(index) => {
                    self.check_relationship_filters(input, *index, &condition.property, filters)?;
                }
            }
        }
        Ok(())
    }

    fn check_node_filters(
        &self,
        input: &Input,
        node: &InputNode,
        prop: &str,
        filters: &[InputFilter],
    ) -> Result<()> {
        for filter in filters {
            check_condition_column((&node.id, prop), filter)?;
        }
        let Some(entity) = node.entity.as_deref() else {
            return Ok(());
        };
        let is_traversal_path_filter =
            prop == TRAVERSAL_PATH_COLUMN && self.model.get().entity_has_traversal_path(entity);
        if !is_traversal_path_filter && !self.model.get().property_is_filterable(entity, prop) {
            return Err(QueryError::Validation(format!(
                "filter on \"{prop}\" for {entity}: field is not filterable"
            )));
        }
        if let Some(vs) = self.virtual_source(entity, prop) {
            if matches!(
                input.query_type,
                QueryType::Aggregation | QueryType::Neighbors | QueryType::PathFinding
            ) {
                let qt: &str = input.query_type.into();
                return Err(QueryError::Validation(format!(
                    "filter on \"{prop}\" for {entity}: virtual columns cannot be \
                     filtered in {qt} queries"
                )));
            }
            if vs.disabled {
                return Err(QueryError::Validation(format!(
                    "filter on \"{prop}\" for {entity}: virtual column is not yet \
                     resolvable"
                )));
            }
            if node.node_ids.is_empty() {
                return Err(QueryError::Validation(format!(
                    "filter on \"{prop}\" for {entity}: virtual columns can only be \
                     filtered on nodes with explicit node_ids; searching by virtual \
                     column content is not yet supported"
                )));
            }
            let allowed: Vec<&str> = vs.allowed_ops.iter().map(|s| s.as_str()).collect();
            for filter in filters {
                let op = filter.op.unwrap_or(FilterOp::Eq);
                if !allowed.contains(&op.as_ref()) {
                    return Err(QueryError::Validation(format!(
                        "filter on \"{prop}\" for {entity}: operator \"{}\" is not \
                         supported on this virtual column (allowed: {allowed:?})",
                        op.as_ref()
                    )));
                }
            }
        }
        let Some(data_type) = self.field_type(entity, prop) else {
            return Ok(());
        };
        for filter in filters {
            if is_traversal_path_filter {
                Self::check_traversal_path_filter(
                    &format!("filter on \"{TRAVERSAL_PATH_COLUMN}\" for {entity}"),
                    filter,
                )?;
            }
            if let Some((rhs_node, rhs_prop)) = &filter.rhs_column {
                self.check_property_comparison(input, (&node.id, prop), (rhs_node, rhs_prop))?;
                continue;
            }
            self.check_one_filter(entity, prop, filter, data_type)?;
        }
        Ok(())
    }

    fn check_relationship_filters(
        &self,
        input: &Input,
        index: usize,
        prop: &str,
        filters: &[InputFilter],
    ) -> Result<()> {
        let model = self.model.get();
        let edge_table = input.relationships[index]
            .types
            .first()
            .and_then(|kind| model.relationship_table(kind))
            .unwrap_or_else(|| model.default_edge_table());
        let data_type = model.table_column_type(edge_table, prop).ok_or_else(|| {
            QueryError::Validation(format!(
                "relationship[{index}] filter on unknown edge column \"{prop}\" \
                 (table \"{edge_table}\" does not have this column)"
            ))
        })?;
        for filter in filters {
            check_condition_column(("", prop), filter)?;
            if filter.rhs_column.is_some() {
                return Err(QueryError::Validation(
                    "property comparisons involving relationships are unsupported".into(),
                ));
            }
            if prop == TRAVERSAL_PATH_COLUMN {
                Self::check_traversal_path_filter(
                    &format!("relationship[{index}] filter on \"{TRAVERSAL_PATH_COLUMN}\""),
                    filter,
                )?;
            }
            self.check_one_filter(&format!("relationship[{index}]"), prop, filter, data_type)?;
        }
        Ok(())
    }

    const MIN_LIKE_PATTERN_LEN: usize = 3;

    fn check_traversal_path_filter(label: &str, filter: &InputFilter) -> Result<()> {
        match filter.op.unwrap_or(FilterOp::Eq) {
            FilterOp::Eq | FilterOp::StartsWith => {
                let Some(path) = filter.value_str() else {
                    return Err(QueryError::Validation(format!(
                        "{label}: value must be a traversal_path string"
                    )));
                };
                Self::check_traversal_path_value(label, path)
            }
            FilterOp::In => {
                let Some(paths) = filter.value.as_ref().and_then(|v| v.as_array()) else {
                    return Err(QueryError::Validation(format!(
                        "{label}: \"in\" requires an array of traversal_path strings"
                    )));
                };
                for path in paths {
                    let Some(path) = path.as_str() else {
                        return Err(QueryError::Validation(format!(
                            "{label}: \"in\" values must be traversal_path strings"
                        )));
                    };
                    Self::check_traversal_path_value(label, path)?;
                }
                Ok(())
            }
            _ => Err(QueryError::Validation(format!(
                "{label}: only eq, in, and starts_with are supported"
            ))),
        }
    }

    fn check_traversal_path_value(label: &str, path: &str) -> Result<()> {
        SecurityContext::validate_traversal_path(&TraversalPath::new_unchecked(path)).map_err(
            |err| match err {
                QueryError::Security(msg) => QueryError::Validation(format!("{label}: {msg}")),
                other => other,
            },
        )
    }

    fn check_one_filter(
        &self,
        entity: &str,
        prop: &str,
        filter: &InputFilter,
        data_type: DataType,
    ) -> Result<()> {
        let op = filter.op.unwrap_or(FilterOp::Eq);

        if matches!(op, FilterOp::IsNull | FilterOp::IsNotNull) {
            return Ok(());
        }

        let is_like_op = matches!(
            op,
            FilterOp::Contains | FilterOp::StartsWith | FilterOp::EndsWith
        );

        let is_token_op = matches!(
            op,
            FilterOp::TokenMatch | FilterOp::AllTokens | FilterOp::AnyTokens
        );

        if is_like_op && !self.model.get().property_allows_like(entity, prop) {
            return Err(QueryError::Validation(format!(
                "filter on \"{prop}\" for {entity}: \
                 string operators (contains/starts_with/ends_with) are not allowed on this field"
            )));
        }

        // ClickHouse rejects lower()/startsWith on non-string columns at
        // execution, which would surface as an opaque 500.
        if is_like_op
            && !matches!(
                data_type,
                DataType::String | DataType::Enum | DataType::Uuid
            )
        {
            return Err(QueryError::Validation(format!(
                "filter on \"{prop}\" for {entity}: \
                 string operators (contains/starts_with/ends_with) require a text field, got {data_type}"
            )));
        }

        if is_token_op
            && self
                .property_id(entity, prop)
                .is_none_or(|property| !self.model.get().has_text_index(property))
        {
            return Err(QueryError::Validation(format!(
                "filter on \"{prop}\" for {entity}: \
                 token operators (token_match/all_tokens/any_tokens) require a text index on the field"
            )));
        }

        let Some(value) = filter.value.as_ref() else {
            return Ok(());
        };

        // traversal_path is exempt: short paths like "1/" are valid hierarchical scopes.
        if (is_like_op || is_token_op) && prop != TRAVERSAL_PATH_COLUMN {
            let len = value.as_str().map_or(0, |s| s.chars().count());
            if len < Self::MIN_LIKE_PATTERN_LEN {
                return Err(QueryError::Validation(format!(
                    "filter on \"{prop}\" for {entity}: \
                     search pattern must be at least {} characters, got {len}",
                    Self::MIN_LIKE_PATTERN_LEN
                )));
            }
        }

        if op == FilterOp::TokenMatch
            && value.as_str().is_some_and(|s| {
                s.chars()
                    .any(|c| c.is_ascii() && !c.is_ascii_alphanumeric())
            })
        {
            return Err(QueryError::Validation(format!(
                "filter on \"{prop}\" for {entity}: \
                 token_match value must be one word of letters and digits; use all_tokens for several words"
            )));
        }

        match op {
            FilterOp::In => {
                let Some(arr) = value.as_array() else {
                    return Err(QueryError::Validation(format!(
                        "filter on \"{prop}\" for {entity}: \"in\" requires an array value"
                    )));
                };
                for (i, elem) in arr.iter().enumerate() {
                    if let Some(reason) = check_value_type(elem, data_type) {
                        return Err(QueryError::Validation(format!(
                            "filter on \"{prop}\" for {entity}: element [{i}] {reason}, \
                             expected {data_type}"
                        )));
                    }
                }
            }
            _ => {
                if let Some(reason) = check_value_type(value, data_type) {
                    return Err(QueryError::Validation(format!(
                        "filter on \"{prop}\" for {entity}: value {reason}, expected {data_type}"
                    )));
                }
            }
        }
        Ok(())
    }

    /// Reject queries that would produce unbounded scans or Cartesian explosions.
    ///
    /// The checks are intentionally conservative: they reject shapes that are
    /// structurally guaranteed to be expensive regardless of data volume.
    fn check_selectivity(&self, input: &Input) -> Result<()> {
        if self.skip.selectivity {
            return Ok(());
        }
        match input.query_type {
            // Path-finding endpoints seed BFS frontiers, so each endpoint
            // must have bounded selectivity: node_ids (already capped at 500
            // by check_depth), filters (lowerer caps CTE at MAX_PATH_ANCHOR_LIMIT),
            // or id_range with span ≤ MAX_PATH_ANCHOR_RANGE.
            QueryType::PathFinding => {
                if let Some(ref path) = input.path {
                    for endpoint in [&path.from, &path.to] {
                        let node = input.nodes.iter().find(|n| n.id == *endpoint);
                        if node.is_none_or(|n| !path_endpoint_has_selectivity(n)) {
                            return Err(QueryError::Validation(format!(
                                "path_finding requires node_ids, filters, or id_range \
                                 (max span {MAX_PATH_ANCHOR_RANGE}) on endpoint \"{endpoint}\""
                            )));
                        }
                    }
                }
            }
            QueryType::Neighbors
                if input.nodes.first().is_none_or(|n| !node_has_selectivity(n)) =>
            {
                let hint = wide_id_range_hint(input.nodes.first().is_some_and(has_wide_id_range));
                return Err(QueryError::Validation(format!(
                    "neighbors requires filters or node_ids on the center node \
                     to avoid scanning all edges{hint}"
                )));
            }
            QueryType::Traversal | QueryType::Aggregation
                if !input.nodes.iter().any(node_has_selectivity) =>
            {
                let hint = wide_id_range_hint(input.nodes.iter().any(has_wide_id_range));
                let negation_hint = if input.has_negation() {
                    " (NOT conditions do not count)"
                } else {
                    ""
                };
                return Err(QueryError::Validation(format!(
                    "add filters or node_ids to at least one node{hint}{negation_hint}"
                )));
            }
            _ => {}
        }
        Ok(())
    }

    fn check_relationships(&self, input: &Input) -> Result<()> {
        let node_ids: Vec<&str> = input.nodes.iter().map(|n| n.id.as_str()).collect();

        for (i, rel) in input.relationships.iter().enumerate() {
            if rel.from == rel.to {
                return Err(QueryError::Validation(
                    "relationship endpoints must use different node aliases".into(),
                ));
            }
            if !node_ids.contains(&rel.from.as_str()) {
                return Err(QueryError::ReferenceError(format!(
                    "relationship[{}] references undefined node \"{}\" in 'from'",
                    i, rel.from
                )));
            }

            if !node_ids.contains(&rel.to.as_str()) {
                return Err(QueryError::ReferenceError(format!(
                    "relationship[{}] references undefined node \"{}\" in 'to'",
                    i, rel.to
                )));
            }
        }

        Ok(())
    }

    fn check_aggregations(&self, input: &Input) -> Result<()> {
        if input.query_type != QueryType::Aggregation {
            if !input.aggregation.group_by.is_empty() {
                return Err(QueryError::Validation(
                    "group_by is only supported for aggregation queries".into(),
                ));
            }
            return Ok(());
        }

        if input.path.is_some() {
            return Err(QueryError::Validation(
                "aggregation over shortest paths is not supported".into(),
            ));
        }

        let node_ids: Vec<&str> = input.nodes.iter().map(|n| n.id.as_str()).collect();

        if input.aggregation.group_by.is_empty()
            && input.nodes.len() > 1
            && input.relationships.is_empty()
        {
            return Err(QueryError::Validation(
                "multi-node aggregation without group_by requires relationships to constrain node joins"
                    .into(),
            ));
        }

        let group_output_names = group_by_output_names(&input.aggregation.group_by);
        let mut seen_group_output_names = HashSet::new();
        for name in &group_output_names {
            check_reserved_prefix("output column", name, name)?;
            if !seen_group_output_names.insert(name.clone()) {
                return Err(QueryError::Validation(format!(
                    "duplicate group_by output column \"{name}\""
                )));
            }
        }

        let mut seen_output_names = seen_group_output_names.clone();
        for agg in &input.aggregation.metrics {
            let alias = agg.output_name();
            check_reserved_prefix("output column", &alias, &alias)?;
            if !seen_output_names.insert(alias.clone()) {
                return Err(QueryError::Validation(format!(
                    "aggregation \"{alias}\" conflicts with another output column"
                )));
            }

            let function = agg.expr.function();
            if function == AggFunction::Collect {
                return Err(QueryError::Validation(format!(
                    "aggregation \"{alias}\": \"collect\" is not supported"
                )));
            }

            let target = agg.expr.node();
            if !node_ids.contains(&target) {
                return Err(QueryError::ReferenceError(format!(
                    "aggregation \"{alias}\" references undefined node \"{target}\""
                )));
            }

            if let Some(prop) = agg.expr.property()
                && let Some(node) = input.nodes.iter().find(|n| n.id == target)
            {
                let entity = node
                    .entity
                    .as_ref()
                    .ok_or_else(|| QueryError::ReferenceError("missing entity".into()))?;
                self.check_field(entity, prop).map_err(|e| {
                    QueryError::AllowlistRejected(format!(
                        "invalid property in aggregation \"{alias}\": {e}"
                    ))
                })?;

                if self.virtual_source(entity, prop).is_some() {
                    return Err(QueryError::Validation(format!(
                        "aggregation \"{alias}\": virtual property {entity}.{prop} cannot be read in SQL"
                    )));
                }

                if matches!(function, AggFunction::Sum | AggFunction::Avg) {
                    let data_type = self.field_type(entity, prop).ok_or_else(|| {
                        QueryError::AllowlistRejected(format!(
                            "invalid property in aggregation \"{alias}\": {entity}.{prop}"
                        ))
                    })?;

                    if !matches!(data_type, DataType::Int | DataType::Float) {
                        return Err(QueryError::Validation(format!(
                            "aggregation \"{alias}\": \"{}\" requires a numeric property, got {}.{} ({data_type})",
                            function.to_string().to_uppercase(),
                            entity,
                            prop
                        )));
                    }
                }
            }
        }

        for (i, group) in input.aggregation.group_by.iter().enumerate() {
            let group_node = group.node();
            let node = input
                .nodes
                .iter()
                .find(|n| n.id == group_node)
                .ok_or_else(|| {
                    QueryError::ReferenceError(format!(
                        "group_by[{i}] references undefined node \"{}\"",
                        group_node
                    ))
                })?;
            let entity = node
                .entity
                .as_ref()
                .ok_or_else(|| QueryError::ReferenceError("missing entity".into()))?;

            let Some(property) = group.property() else {
                continue;
            };

            self.check_field(entity, property).map_err(|e| {
                QueryError::AllowlistRejected(format!("invalid property in group_by[{i}]: {e}"))
            })?;

            if !self.model.get().property_is_filterable(entity, property) {
                return Err(QueryError::Validation(format!(
                    "group_by[{i}] on \"{}\" for {entity}: field is not filterable",
                    property
                )));
            }

            if self.virtual_source(entity, property).is_some() {
                return Err(QueryError::Validation(format!(
                    "group_by[{i}] on \"{}\" for {entity}: field is virtual and cannot be grouped in SQL",
                    property
                )));
            }

            if let Some(unit) = group.truncate() {
                let data_type = self.field_type(entity, property);
                if !matches!(
                    data_type,
                    Some(ontology::DataType::Date) | Some(ontology::DataType::DateTime)
                ) {
                    return Err(QueryError::Validation(format!(
                        "group_by[{i}]: truncate \"{}\" requires a Date or DateTime property; \"{}\" on {entity} is {}",
                        unit.name(),
                        property,
                        data_type
                            .map(|t| t.to_string())
                            .unwrap_or_else(|| "unknown".into())
                    )));
                }

                if unit.requires_selectivity_guard()
                    && node.node_ids.is_empty()
                    && !node.filters.contains_key(property)
                {
                    return Err(QueryError::Validation(format!(
                        "group_by[{i}]: truncate \"{}\" on \"{}\" requires either node_ids on \"{}\" \
                         or at least one filter on \"{}\" to bound bucket cardinality",
                        unit.name(),
                        property,
                        group_node,
                        property,
                    )));
                }
            }
        }

        if let Some(sort) = &input.aggregation.sort {
            let mut output_names: HashSet<String> = group_output_names.into_iter().collect();
            output_names.extend(
                input
                    .aggregation
                    .metrics
                    .iter()
                    .map(crate::input::InputAggregationMetric::output_name),
            );
            if !output_names.contains(&sort.column) {
                return Err(QueryError::ReferenceError(format!(
                    "aggregation_sort references unknown output column \"{}\"",
                    sort.column
                )));
            }
        }

        Ok(())
    }

    fn check_order_by(&self, input: &Input) -> Result<()> {
        let Some(order_by) = &input.order_by else {
            return Ok(());
        };

        let node = input
            .nodes
            .iter()
            .find(|n| n.id == order_by.node)
            .ok_or_else(|| {
                QueryError::ReferenceError(format!(
                    "order_by references undefined node \"{}\"",
                    order_by.node
                ))
            })?;

        let entity = node
            .entity
            .as_ref()
            .ok_or_else(|| QueryError::ReferenceError("missing entity".into()))?;
        self.check_field(entity, &order_by.property).map_err(|e| {
            QueryError::AllowlistRejected(format!("invalid order_by property: {}", e))
        })?;
        if self.virtual_source(entity, &order_by.property).is_some() {
            return Err(QueryError::Validation(format!(
                "order_by property {}.{} is virtual and cannot be read in SQL",
                order_by.node, order_by.property
            )));
        }

        Ok(())
    }

    fn check_path(&self, input: &Input) -> Result<()> {
        if input.query_type != QueryType::PathFinding {
            return Ok(());
        }

        let path = input.path.as_ref().ok_or_else(|| {
            QueryError::ReferenceError("path_finding query requires a 'path' configuration".into())
        })?;

        let node_ids: Vec<&str> = input.nodes.iter().map(|n| n.id.as_str()).collect();

        if !node_ids.contains(&path.from.as_str()) {
            return Err(QueryError::ReferenceError(format!(
                "path 'from' references undefined node \"{}\"",
                path.from
            )));
        }

        if !node_ids.contains(&path.to.as_str()) {
            return Err(QueryError::ReferenceError(format!(
                "path 'to' references undefined node \"{}\"",
                path.to
            )));
        }

        // rel_types is always required for pathfinding. Without it, each
        // seed fans out through every edge type from high-degree nodes,
        // producing unbounded fan-out that no runtime limit can cheaply cap.
        // Even pinned endpoints can hit hub nodes (e.g. a project with
        // millions of IN_PROJECT edges).
        if path.rel_types.is_empty() {
            return Err(QueryError::Validation(
                "path_finding requires rel_types to bound fan-out; \
                 without it the frontier expands over every edge type"
                    .into(),
            ));
        }

        Ok(())
    }

    fn check_neighbors(&self, input: &Input) -> Result<()> {
        if input.query_type != QueryType::Neighbors {
            return Ok(());
        }

        input.neighbors.as_ref().ok_or_else(|| {
            QueryError::ReferenceError(
                "neighbors query requires a 'neighbors' configuration".into(),
            )
        })?;

        Ok(())
    }

    /// Every declared node must be referenced by at least one structural field
    /// (relationship, path, neighbors). Unreferenced nodes would be absent from
    /// the FROM clause, producing broken SQL or silently dropped columns.
    fn check_unreferenced_nodes(&self, input: &Input) -> Result<()> {
        let referenced: std::collections::HashSet<&str> = match input.query_type {
            // Single-node query types where all declared nodes are used directly.
            QueryType::Neighbors | QueryType::Hydration => return Ok(()),
            // Search-shaped traversal (1 node, 0 rels) has no references to check.
            QueryType::Traversal if input.is_search() => return Ok(()),
            QueryType::Traversal | QueryType::Aggregation => {
                let mut set: std::collections::HashSet<&str> = input
                    .relationships
                    .iter()
                    .flat_map(|r| [r.from.as_str(), r.to.as_str()])
                    .collect();
                for agg in &input.aggregation.metrics {
                    set.insert(agg.expr.node());
                }
                for group in &input.aggregation.group_by {
                    set.insert(group.node());
                }
                set
            }
            QueryType::PathFinding => {
                let mut set = std::collections::HashSet::new();
                if let Some(ref path) = input.path {
                    set.insert(path.from.as_str());
                    set.insert(path.to.as_str());
                }
                set
            }
        };

        for node in &input.nodes {
            if !referenced.contains(node.id.as_str()) {
                return Err(QueryError::ReferenceError(format!(
                    "node \"{}\" is not referenced by any relationship or path",
                    node.id
                )));
            }
        }
        Ok(())
    }

    fn check_duplicate_node_ids(&self, input: &Input) -> Result<()> {
        let mut seen = std::collections::HashSet::new();
        for node in &input.nodes {
            if !seen.insert(&node.id) {
                return Err(QueryError::ReferenceError(format!(
                    "duplicate node id \"{}\"",
                    node.id
                )));
            }
        }
        Ok(())
    }
}

pub(crate) fn validate_identifier(identifier: &str) -> Result<()> {
    let mut chars = identifier.chars();
    let valid = identifier.len() <= MAX_IDENTIFIER_LEN
        && chars
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_');
    if valid {
        Ok(())
    } else {
        Err(QueryError::Validation(format!(
            "invalid identifier {identifier:?}"
        )))
    }
}

fn check_reserved_prefix(kind: &str, name: &str, column: &str) -> Result<()> {
    let prefix = internal_column_prefix();
    if column.starts_with(prefix) {
        return Err(QueryError::Validation(format!(
            "{kind} {name:?} conflicts with reserved prefix {prefix:?}"
        )));
    }
    Ok(())
}

fn is_valid_filter_value(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Number(_) | serde_json::Value::Bool(_) => true,
        serde_json::Value::String(text) => text.chars().count() <= MAX_FILTER_STRING_LEN,
        serde_json::Value::Array(values) => {
            values.len() <= MAX_IN_VALUES && values.iter().all(is_valid_filter_value)
        }
        serde_json::Value::Null | serde_json::Value::Object(_) => false,
    }
}

fn check_filters(filters: &std::collections::HashMap<String, Vec<InputFilter>>) -> Result<()> {
    for (property, predicates) in filters {
        check_filter_entries(property, predicates)?;
    }
    Ok(())
}

fn check_filter_entries(property: &str, predicates: &[InputFilter]) -> Result<()> {
    validate_identifier(property)?;
    if predicates.is_empty() || predicates.len() > MAX_FILTER_ENTRIES_PER_PROPERTY {
        return Err(QueryError::Validation(format!(
            "filter on {property:?} needs between 1 and {MAX_FILTER_ENTRIES_PER_PROPERTY} predicates"
        )));
    }
    for filter in predicates {
        let op = filter.op.unwrap_or(FilterOp::Eq);
        if matches!(op, FilterOp::IsNull | FilterOp::IsNotNull) {
            if filter.value.is_some() {
                return Err(QueryError::Validation(
                    "null checks cannot have a value".into(),
                ));
            }
            continue;
        }
        if filter.rhs_column.is_some() {
            continue;
        }
        let value = filter
            .value
            .as_ref()
            .ok_or_else(|| QueryError::Validation("predicate requires a value".into()))?;
        if !is_valid_filter_value(value) {
            return Err(QueryError::Validation(format!(
                "invalid filter value for {property:?}; values must be numbers, booleans, strings up to {MAX_FILTER_STRING_LEN} characters, or lists of up to {MAX_IN_VALUES} such values"
            )));
        }
        if op == FilterOp::In
            && !value
                .as_array()
                .is_some_and(|values| (1..=MAX_IN_VALUES).contains(&values.len()))
        {
            return Err(QueryError::Validation(format!(
                "IN requires 1-{MAX_IN_VALUES} values"
            )));
        }
        if matches!(
            op,
            FilterOp::Contains
                | FilterOp::StartsWith
                | FilterOp::EndsWith
                | FilterOp::TokenMatch
                | FilterOp::AllTokens
                | FilterOp::AnyTokens
        ) && !value.is_string()
        {
            return Err(QueryError::Validation(
                "text predicates require a string".into(),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::parse_input;

    fn validator(ontology: &Ontology) -> Validator<'_, ClickHouseDataModel> {
        Validator {
            model: ValidationModel::Owned(
                ClickHouseDataModel::derive(std::sync::Arc::new(ontology.clone())).unwrap(),
            ),
            skip: Skip::default(),
        }
    }

    fn test_ontology() -> Ontology {
        Ontology::new()
            .with_nodes(["User", "Project", "Note"])
            .with_edges(["AUTHORED", "CONTAINS"])
            .with_fields(
                "User",
                [
                    ("username", DataType::String),
                    ("created_at", DataType::DateTime),
                ],
            )
            .with_fields("Project", [("visibility_level", DataType::String)])
    }

    /// The JSON schema and GQL lowering reject these inputs before validation.
    /// The validator still guards inputs built in code.
    #[test]
    fn rejects_inputs_the_frontends_block_first() {
        let ontology = test_ontology();
        let validator = validator(&ontology);
        for (json, expected) in [
            (
                r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User","node_ids":[1]}],"relationships":[{"type":"AUTHORED","from":"ghost","to":"u"}]}"#,
                "undefined node \"ghost\"",
            ),
            (
                r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User","node_ids":[1]}],"relationships":[{"type":"AUTHORED","from":"u","to":"ghost"}]}"#,
                "undefined node \"ghost\"",
            ),
            (
                r#"{"query_type":"traversal","nodes":[{"id":"p","entity":"Project","node_ids":[1]}],"group_by":["p.visibility_level"]}"#,
                "only supported for aggregation",
            ),
            (
                r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User","node_ids":[1]},{"id":"u","entity":"Project"}]}"#,
                "duplicate node id \"u\"",
            ),
            (
                r#"{"query_type":"aggregation","nodes":[{"id":"u","entity":"User","node_ids":[1]},{"id":"n","entity":"Note"}],"relationships":[{"type":"AUTHORED","from":"u","to":"n","direction":"both"}],"group_by":["u"],"aggregations":[{"count":"n","as":"count"}]}"#,
                "does not support direction",
            ),
            (
                r#"{"query_type":"aggregation","nodes":[{"id":"n","entity":"Note"},{"id":"p","entity":"Project","node_ids":[1]}],"aggregations":[{"count":"n","as":"total"}]}"#,
                "without group_by requires relationships",
            ),
            (
                r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User","node_ids":[1],"filters":{"traversal_path":{"starts_with":"1/"}}}]}"#,
                "not filterable",
            ),
        ] {
            let error = validator
                .check_references(&parse_input(json).unwrap())
                .unwrap_err();
            assert!(error.to_string().contains(expected), "{json}: {error}");
        }
        let entries = [r#"{"gte": "2020-01-01"}"#; 11].join(", ");
        let input = parse_input(&format!(
            r#"{{"query_type":"traversal","nodes":[{{"id":"u","entity":"User","filters":{{"created_at":[{entries}]}}}}]}}"#
        ))
        .unwrap();
        let error = validator.check_depth(&input).unwrap_err();
        assert!(
            error.to_string().contains("filter entry count (11)"),
            "{error}"
        );
    }

    #[test]
    fn native_predicate_trees_are_bounded_before_planning() {
        use crate::input::{BooleanExpression, Condition, PredicateTarget};
        let model =
            ClickHouseDataModel::derive(std::sync::Arc::new(Ontology::load_embedded().unwrap()))
                .unwrap();
        let validator = Validator::new(&model);
        let mut input =
            crate::passes::frontend::gql::parse("MATCH (u:User {id: 1}) RETURN u").unwrap();
        let leaf = |target| {
            BooleanExpression::Leaf(Condition {
                target,
                property: "id".into(),
                filter: InputFilter {
                    op: Some(FilterOp::In),
                    value: Some(serde_json::json!([1])),
                    ..Default::default()
                },
            })
        };
        let mut deep = leaf(PredicateTarget::Node("u".into()));
        for _ in 0..1024 {
            deep = BooleanExpression::Not(Box::new(deep));
        }
        input.predicates = vec![deep];
        let error = validator.check_shape(&input).unwrap_err();
        assert!(
            error.to_string().contains("must not nest deeper than 32"),
            "{error}"
        );
        input.predicates = vec![BooleanExpression::Not(Box::new(leaf(
            PredicateTarget::Relationship(7),
        )))];
        let error = validator.check_shape(&input).unwrap_err();
        assert!(
            error.to_string().contains("undefined relationship"),
            "{error}"
        );
    }

    #[test]
    fn edge_column_type_covers_all_reserved_columns() {
        let ontology = ontology::Ontology::load_embedded().expect("embedded ontology must load");
        for col in ontology::EDGE_RESERVED_COLUMNS {
            assert!(
                ontology.get_edge_column_type(col).is_some(),
                "get_edge_column_type must cover reserved column \"{col}\""
            );
        }
    }
}
