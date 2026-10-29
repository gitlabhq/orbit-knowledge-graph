//! Query validation.
//!
//! [`Validator`] checks in two layers:
//! 1. **Shape validation** — identifiers, ontology membership, limits, and
//!    filter value shapes.
//! 2. **Cross-reference validation** — node ID references, complexity caps, and
//!    filter typing that shape checks cannot express.

use std::collections::HashSet;

use crate::constants::internal_column_prefix;
use crate::error::{QueryError, Result};
use crate::input::{
    AggFunction, ColumnSelection, Condition, FilterOp, Input, InputFilter, InputGroupByKey,
    InputNode, PredicateTarget, QueryType, group_by_output_names,
};
use crate::query_limits::{
    MAX_DEPTH_CAP, MAX_FILTER_ENTRIES_PER_PROPERTY, MAX_FILTER_STRING_LEN, MAX_HOPS_CAP,
    MAX_IDENTIFIER_LEN, MAX_IN_VALUES, MAX_LIMIT, MAX_PREDICATE_DEPTH, MAX_SKIP,
};
#[cfg(test)]
use ontology::Ontology;
use ontology::{DataType, TRAVERSAL_PATH_COLUMN};
#[cfg(test)]
use query_data_model::ClickHouseDataModel;
use query_data_model::PropertyId;

mod conditions;
use conditions::{check_condition_column, check_condition_counts};

/// Check whether a JSON value is compatible with an ontology `DataType`.
///
/// Returns `None` if compatible, or `Some(reason)` describing the mismatch.
fn check_value_type(value: &serde_json::Value, expected: DataType) -> Option<String> {
    match expected {
        DataType::String | DataType::Uuid => {
            if !value.is_string() {
                return Some(format!("is {}, not a string", json_type_name(value)));
            }
        }
        DataType::Date => match value.as_str() {
            Some(text) if is_calendar_date(text) => {}
            Some(_) => return Some("is not a valid date; use YYYY-MM-DD".to_string()),
            None => return Some(format!("is {}, not a string", json_type_name(value))),
        },
        DataType::DateTime => match value.as_str() {
            Some(text) if is_calendar_date(text) || is_timestamp(text) => {}
            Some(_) => {
                return Some(
                    "is not a valid date; use YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS[.ffffff][Z]"
                        .to_string(),
                );
            }
            None => return Some(format!("is {}, not a string", json_type_name(value))),
        },
        // Int-based enums are coerced to string labels by normalization before lowering.
        DataType::Enum => match value {
            serde_json::Value::String(_) => {}
            serde_json::Value::Number(n) if n.is_i64() => {}
            serde_json::Value::Number(n) if n.is_u64() => {
                return Some(OUT_OF_INT64_RANGE.to_string());
            }
            _ => {
                return Some(format!(
                    "is {}, not a string or integer",
                    json_type_name(value)
                ));
            }
        },
        DataType::Int => match value {
            serde_json::Value::Number(n) if n.is_i64() => {}
            serde_json::Value::Number(n) if n.is_u64() => {
                return Some(OUT_OF_INT64_RANGE.to_string());
            }
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

const OUT_OF_INT64_RANGE: &str = "is out of range for a signed 64-bit integer";

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
pub(crate) const MAX_ID_RANGE_SPAN: i64 = 100_000;

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

fn calendar_prefix(value: &str) -> Option<&str> {
    value.get(..10).filter(|date| {
        date.bytes().enumerate().all(|(i, byte)| {
            if i == 4 || i == 7 {
                byte == b'-'
            } else {
                byte.is_ascii_digit()
            }
        })
    })
}

fn like_matches(op: FilterOp, text: &str, pattern: &str) -> bool {
    match op {
        FilterOp::Contains => text.contains(pattern),
        FilterOp::StartsWith => text.starts_with(pattern),
        FilterOp::EndsWith => text.ends_with(pattern),
        _ => true,
    }
}

fn filter_values(value: &serde_json::Value) -> &[serde_json::Value] {
    value
        .as_array()
        .map_or(std::slice::from_ref(value), Vec::as_slice)
}

fn at_or_before_epoch(value: &str) -> bool {
    calendar_prefix(value).is_some_and(|date| date <= "1970-01-01")
}

fn is_calendar_date(value: &str) -> bool {
    let value = value.strip_suffix('Z').unwrap_or(value);
    value.len() == 10
        && calendar_prefix(value).is_some()
        && chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d").is_ok()
}

fn is_timestamp(value: &str) -> bool {
    let value = value.strip_suffix('Z').unwrap_or(value);
    calendar_prefix(value).is_some()
        && ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M:%S%.f"]
            .iter()
            .any(|format| chrono::NaiveDateTime::parse_from_str(value, format).is_ok())
}

fn is_aggregate_scalar(data_type: DataType) -> bool {
    matches!(
        data_type,
        DataType::Int | DataType::Float | DataType::Bool | DataType::Date | DataType::DateTime
    )
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

    fn check_aggregation_text_filters(&self, input: &Input) -> Result<()> {
        let model = self.model.get();
        let row_nodes = input.aggregation_row_nodes(model);
        let node_filters = input.nodes.iter().flat_map(|node| {
            node.filters.iter().flat_map(move |(property, filters)| {
                filters
                    .iter()
                    .map(move |filter| (node.id.as_str(), property.as_str(), filter.op))
            })
        });
        let conditions = input.conditions().flat_map(|condition| {
            condition
                .node_properties()
                .map(move |(alias, property)| (alias, property, condition.filter.op))
        });
        for (alias, property, op) in node_filters.chain(conditions) {
            if !op.is_some_and(|op| op.is_like() || op.is_token() || op.is_ordering()) {
                continue;
            }
            let Some(entity) = input.entity_of(alias) else {
                continue;
            };
            if row_nodes.contains(alias)
                || model.entity_authorized_by_namespace(entity)
                || !model.property_is_free_text(entity, property)
            {
                continue;
            }
            return Err(QueryError::Validation(format!(
                "aggregation filter on {entity}.{property}: text matching and ordering require grouping by \"{alias}\""
            )));
        }
        Ok(())
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
        if input.skip.is_some_and(|skip| skip > MAX_SKIP) {
            return Err(QueryError::Validation(format!(
                "skip must be at most {MAX_SKIP}; to page deeper, ORDER BY a unique key such as id and filter past the last value from the previous page"
            )));
        }
        for node in &input.nodes {
            validate_identifier(&node.id)?;
            let entity = node
                .entity
                .as_deref()
                .ok_or_else(|| QueryError::Validation("each node requires an entity".into()))?;
            if self.model.get().entity(entity).is_none() {
                return Err(QueryError::AllowlistRejected(format!(
                    "unknown entity '{entity}'"
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
        let model = self.model.get();
        if ontology::constants::NODE_RESERVED_COLUMNS.contains(&property) {
            return Ok(());
        }
        if let Some(field) = self.property(entity, property)
            && !model.property_is_hidden(field.id)
        {
            if model.property_realization(field.id).is_none()
                && model.entity_table(entity).is_some()
            {
                return Err(QueryError::AllowlistRejected(format!(
                    "field \"{property}\" on {entity} is not available in this data model"
                )));
            }
            return Ok(());
        }
        let properties: Vec<_> = model
            .entity(entity)
            .into_iter()
            .flat_map(|entity| &entity.properties)
            .filter(|property| !model.property_is_hidden(**property))
            .map(|property| model.graph().property(*property).name.as_str())
            .collect();
        Err(QueryError::AllowlistRejected(format!(
            "field \"{property}\" does not exist on node type \"{entity}\"{}",
            ontology::errors::format_candidate_list("fields", &properties)
        )))
    }

    fn check_relationship_types(&self, types: &[String]) -> Result<()> {
        if types.len() > 1 && types.iter().any(|kind| kind == "*") {
            return Err(QueryError::Validation(
                "the \"*\" relationship type cannot be combined with named types; list only the types to follow".into(),
            ));
        }
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
        self.check_node_ids(input)?;
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
            MAX_AGGREGATIONS, MAX_COLUMNS, MAX_DEPTH_CAP, MAX_GROUP_BY_KEYS, MAX_HOPS_CAP,
            MAX_IN_VALUES, MAX_NODE_IDS, MAX_NODES_CAP, MAX_PREDICATES, MAX_REL_TYPES,
            MAX_RELS_CAP,
        };

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
        if input.aggregation.metrics.len() > MAX_AGGREGATIONS {
            return Err(QueryError::Validation(format!(
                "aggregations count ({}) must not exceed {MAX_AGGREGATIONS}",
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
        if !self.model.get().property_is_filterable(entity, prop) {
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
        let Some(property) = self.property(entity, prop) else {
            return Ok(());
        };
        let data_type = property.data_type;
        for filter in filters {
            if let Some((rhs_node, rhs_prop)) = &filter.rhs_column {
                self.check_property_comparison(
                    input,
                    (&node.id, prop),
                    filter.op.unwrap_or(FilterOp::Eq),
                    (rhs_node, rhs_prop),
                )?;
                continue;
            }
            self.check_one_filter(entity, prop, filter, data_type)?;
            self.check_enum_filter(entity, prop, filter, property)?;
        }
        Ok(())
    }

    fn check_enum_filter(
        &self,
        entity: &str,
        prop: &str,
        filter: &InputFilter,
        property: &query_data_model::Property,
    ) -> Result<()> {
        let op = filter.op.unwrap_or(FilterOp::Eq);
        if property.data_type != DataType::Enum || op.is_token() {
            return Ok(());
        }
        let Some(value) = filter.value.as_ref() else {
            return Ok(());
        };
        let declared = property.enum_values.iter().flatten();
        let integer_keys = property.enum_type == ontology::EnumType::Int;
        let is_declared = |value: &serde_json::Value| match value {
            serde_json::Value::String(text) if op.is_like() => declared
                .clone()
                .any(|(_, label)| like_matches(op, &label.to_lowercase(), &text.to_lowercase())),
            serde_json::Value::String(text) => declared.clone().any(|(_, label)| label == text),
            serde_json::Value::Number(number) => {
                integer_keys
                    && number
                        .as_i64()
                        .is_some_and(|key| declared.clone().any(|(known, _)| *known == key))
            }
            _ => false,
        };
        let Some(invalid) = filter_values(value)
            .iter()
            .find(|value| !is_declared(value))
        else {
            return Ok(());
        };
        let problem = if op.is_like() {
            format!("no enum value {} {invalid}", op.as_ref().replace('_', " "))
        } else {
            format!("invalid enum value {invalid}")
        };
        let valid: Vec<String> = declared
            .map(|(key, label)| {
                if integer_keys {
                    format!("{key} ({label})")
                } else {
                    label.clone()
                }
            })
            .collect();
        Err(QueryError::Validation(format!(
            "filter on \"{prop}\" for {entity}: {problem}{}",
            ontology::errors::format_candidate_list("values", &valid)
        )))
    }

    fn check_edge_kind_filter(
        &self,
        input: &Input,
        index: usize,
        prop: &str,
        filter: &InputFilter,
    ) -> Result<()> {
        let op = filter.op.unwrap_or(FilterOp::Eq);
        if !matches!(op, FilterOp::Eq | FilterOp::Ne | FilterOp::In) {
            return Ok(());
        }
        let Some(value) = filter.value.as_ref() else {
            return Ok(());
        };
        let allowed = self.edge_kind_values(input, index, prop);
        if allowed.is_empty() {
            return Ok(());
        }
        if op == FilterOp::Ne && allowed.len() == 1 && value.as_str() == Some(&allowed[0]) {
            return Err(QueryError::Validation(format!(
                "relationship[{index}] filter on \"{prop}\": <> {value} excludes the only value this pattern allows; remove the filter"
            )));
        }
        let Some(invalid) = filter_values(value).iter().find(|value| {
            !value
                .as_str()
                .is_some_and(|name| allowed.iter().any(|kind| kind == name))
        }) else {
            return Ok(());
        };
        Err(QueryError::Validation(format!(
            "relationship[{index}] filter on \"{prop}\": {invalid} cannot match this pattern{}",
            ontology::errors::format_candidate_list("values", &allowed)
        )))
    }

    fn edge_kind_values(&self, input: &Input, index: usize, prop: &str) -> Vec<String> {
        use crate::input::Direction;
        use ontology::constants::{
            RELATIONSHIP_KIND_COLUMN, SOURCE_KIND_COLUMN, TARGET_KIND_COLUMN,
        };
        let relationship = &input.relationships[index];
        let graph = self.model.get().graph();
        let every_entity = || graph.entities().map(|entity| entity.name.clone()).collect();
        let untyped =
            relationship.types.is_empty() || relationship.types.iter().any(|kind| kind == "*");
        match prop {
            RELATIONSHIP_KIND_COLUMN if untyped => graph.relationship_names(None, None),
            RELATIONSHIP_KIND_COLUMN => relationship.types.clone(),
            SOURCE_KIND_COLUMN | TARGET_KIND_COLUMN if relationship.hops.max > 1 => every_entity(),
            SOURCE_KIND_COLUMN | TARGET_KIND_COLUMN => {
                let (from, to) = (
                    input.entity_of(&relationship.from),
                    input.entity_of(&relationship.to),
                );
                let (near, far) = if prop == SOURCE_KIND_COLUMN {
                    (from, to)
                } else {
                    (to, from)
                };
                let endpoints = match relationship.direction {
                    Direction::Outgoing => vec![near],
                    Direction::Incoming => vec![far],
                    Direction::Both => vec![near, far],
                };
                if endpoints.contains(&None) {
                    return every_entity();
                }
                let mut kinds: Vec<String> = endpoints
                    .into_iter()
                    .flatten()
                    .map(str::to_string)
                    .collect();
                kinds.dedup();
                kinds
            }
            _ => Vec::new(),
        }
    }

    fn check_relationship_filters(
        &self,
        input: &Input,
        index: usize,
        prop: &str,
        filters: &[InputFilter],
    ) -> Result<()> {
        let model = self.model.get();
        if prop.starts_with('_') || prop == TRAVERSAL_PATH_COLUMN {
            return Err(QueryError::Validation(format!(
                "relationship[{index}] filter on private edge column \"{prop}\""
            )));
        }
        let relationship = &input.relationships[index];
        let from = input
            .node(&relationship.from)
            .and_then(|node| node.entity.as_deref());
        let to = input
            .node(&relationship.to)
            .and_then(|node| node.entity.as_deref());
        let mut edge_tables = Vec::new();
        if relationship.hops.max == 1
            && let Some((from, to)) = from.zip(to)
        {
            if relationship.direction != crate::input::Direction::Incoming {
                edge_tables.extend(model.relationship_tables_between(
                    &relationship.types,
                    from,
                    to,
                ));
            }
            if relationship.direction != crate::input::Direction::Outgoing {
                edge_tables.extend(model.relationship_tables_between(
                    &relationship.types,
                    to,
                    from,
                ));
            }
        }
        if edge_tables.is_empty() {
            edge_tables = model.relationship_tables(&relationship.types);
        }
        let mut types = edge_tables
            .iter()
            .map(|table| model.table_column_type(table, prop));
        let data_type = types.next().flatten();
        let Some(data_type) =
            data_type.filter(|first| types.all(|data_type| data_type == Some(*first)))
        else {
            return Err(QueryError::Validation(format!(
                "relationship[{index}] filter on unknown edge column \"{prop}\" or incompatible column types across tables {edge_tables:?}"
            )));
        };
        for filter in filters {
            check_condition_column(("", prop), filter)?;
            if filter.rhs_column.is_some() {
                return Err(QueryError::Validation(
                    "property comparisons involving relationships are unsupported".into(),
                ));
            }
            self.check_one_filter(&format!("relationship[{index}]"), prop, filter, data_type)?;
            self.check_edge_kind_filter(input, index, prop, filter)?;
        }
        Ok(())
    }

    const MIN_LIKE_PATTERN_LEN: usize = 3;

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

        if filter.from_temporal_literal && !matches!(data_type, DataType::Date | DataType::DateTime)
        {
            return Err(QueryError::Validation(format!(
                "filter on \"{prop}\" for {entity}: datetime() and date() compare only with date fields, but this field is {data_type}"
            )));
        }

        if data_type == DataType::Enum && op.is_ordering() {
            return Err(QueryError::Validation(format!(
                "filter on \"{prop}\" for {entity}: enum values have no order; use = or IN instead of <, >, <=, or >="
            )));
        }

        let is_like_op = op.is_like();
        let is_token_op = op.is_token();

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

        if self
            .property_id(entity, prop)
            .is_some_and(|property| self.model.get().null_as_epoch(property))
            && filter_values(value)
                .iter()
                .filter_map(serde_json::Value::as_str)
                .any(at_or_before_epoch)
        {
            return Err(QueryError::Validation(format!(
                "filter on \"{prop}\" for {entity}: \
                 values on or before 1970-01-01 stand for null; use is_null or is_not_null"
            )));
        }

        if is_like_op || is_token_op {
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
                    "add filters or node_ids to at least one node, such as \
                     (p:Project {{full_path: 'group/project'}}) or WHERE mr.project_id = \
                     42{hint}{negation_hint}"
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
            if !seen_group_output_names.insert(name.clone()) {
                return Err(QueryError::Validation(format!(
                    "duplicate group_by output column \"{name}\""
                )));
            }
        }

        let mut seen_output_names = seen_group_output_names.clone();
        for agg in &input.aggregation.metrics {
            let alias = agg.output_name();
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

            if agg.distinct && function != AggFunction::Count {
                return Err(QueryError::Validation(format!(
                    "aggregation \"{alias}\": \"distinct\" works only with \"count\""
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

                if matches!(
                    function,
                    AggFunction::Sum | AggFunction::Avg | AggFunction::Min | AggFunction::Max
                ) {
                    let data_type = self.field_type(entity, prop).ok_or_else(|| {
                        QueryError::AllowlistRejected(format!(
                            "invalid property in aggregation \"{alias}\": {entity}.{prop}"
                        ))
                    })?;

                    if matches!(function, AggFunction::Sum | AggFunction::Avg)
                        && !matches!(data_type, DataType::Int | DataType::Float)
                    {
                        return Err(QueryError::Validation(format!(
                            "aggregation \"{alias}\": \"{}\" requires a numeric property, got {}.{} ({data_type})",
                            function.to_string().to_uppercase(),
                            entity,
                            prop
                        )));
                    }

                    if data_type == DataType::Enum {
                        return Err(QueryError::Validation(format!(
                            "aggregation \"{alias}\": \"{}\" does not apply to enum {entity}.{prop}, because enum values have no order; group by it instead",
                            function.to_string().to_uppercase(),
                        )));
                    }

                    if !is_aggregate_scalar(data_type) {
                        return Err(QueryError::Validation(format!(
                            "aggregation \"{alias}\": aggregates are restricted to scalars, got {entity}.{prop} ({data_type})"
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

        self.check_aggregation_text_filters(input)?;

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

        let named_by_type_condition = input.predicates.iter().any(|root| {
            root.relationship_type()
                .is_some_and(|(_, negated)| !negated)
        });
        if crate::passes::normalize::is_wildcard(&path.rel_types) && !named_by_type_condition {
            return Err(QueryError::Validation(
                "shortest paths require named relationship types, such as \
                 -[:CONTAINS*1..3]-> or [\"CONTAINS\"]; a wildcard expands over every edge type"
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

    /// Output columns are `{node id}_{property}`, so two ids overlap when one is the
    /// other's prefix followed by `_`.
    fn check_node_ids(&self, input: &Input) -> Result<()> {
        for (index, node) in input.nodes.iter().enumerate() {
            for other in &input.nodes[index + 1..] {
                if node.id == other.id {
                    return Err(QueryError::ReferenceError(format!(
                        "duplicate node id \"{}\"",
                        node.id
                    )));
                }
                let (short, long) = if node.id.len() < other.id.len() {
                    (&node.id, &other.id)
                } else {
                    (&other.id, &node.id)
                };
                if long.starts_with(&format!("{short}_")) {
                    return Err(QueryError::Validation(format!(
                        "node ids \"{short}\" and \"{long}\" overlap; rename one so neither starts with the other followed by \"_\""
                    )));
                }
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
    if !valid {
        return Err(QueryError::Validation(format!(
            "invalid identifier {identifier:?}"
        )));
    }
    let prefix = internal_column_prefix();
    if format!("{identifier}_").starts_with(prefix) {
        return Err(QueryError::Validation(format!(
            "identifier {identifier:?} conflicts with reserved prefix {prefix:?}"
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
        if op.is_text() && !value.is_string() {
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

    fn validator(ontology: &Ontology) -> Validator<'_, ClickHouseDataModel> {
        Validator {
            model: ValidationModel::Owned(
                ClickHouseDataModel::derive(std::sync::Arc::new(ontology.clone())).unwrap(),
            ),
            skip: Skip::default(),
        }
    }
    use crate::gql::parse;
    use ontology::{DataType, FieldSource, VirtualSource};
    use std::collections::BTreeMap;

    fn test_ontology() -> Ontology {
        Ontology::new()
            .with_nodes(["User", "Project", "Note"])
            .with_edges(["AUTHORED", "CONTAINS"])
            .with_fields(
                "User",
                [
                    ("username", DataType::String),
                    ("created_at", DataType::DateTime),
                    ("user_type", DataType::Enum),
                ],
            )
            .with_fields(
                "Note",
                [
                    ("confidential", DataType::Bool),
                    ("noteable_id", DataType::Int),
                ],
            )
            .with_fields(
                "Project",
                [
                    ("name", DataType::String),
                    ("visibility_level", DataType::Enum),
                ],
            )
            .with_edge_columns([
                ("traversal_path", DataType::String),
                ("relationship_kind", DataType::String),
                ("source_id", DataType::Int),
                ("source_kind", DataType::String),
                ("target_id", DataType::Int),
                ("target_kind", DataType::String),
            ])
            .modify_field("User", "user_type", |field| {
                field.enum_values = Some(BTreeMap::from([
                    (0, "human".into()),
                    (6, "project_bot".into()),
                ]));
            })
            .unwrap()
            .modify_field("Project", "visibility_level", |field| {
                field.enum_values = Some(BTreeMap::from([
                    (0, "private".into()),
                    (20, "public".into()),
                ]));
            })
            .unwrap()
    }

    fn assert_ok(query: &str) {
        let input = parse(query).unwrap();
        let ontology = test_ontology();
        validator(&ontology).check_references(&input).unwrap();
    }

    fn assert_rejects(query: &str, expected: &str) {
        assert_input_rejects(parse(query).unwrap(), expected);
    }

    fn assert_input_rejects(input: Input, expected: &str) {
        let ontology = test_ontology();
        let err = validator(&ontology).check_references(&input).unwrap_err();
        assert!(
            err.to_string().contains(expected),
            "expected error containing \"{expected}\", got: {err}"
        );
    }

    fn mutated(query: &str, mutate: impl FnOnce(&mut Input)) -> Input {
        let mut input = parse(query).unwrap();
        mutate(&mut input);
        input
    }

    #[test]
    fn epoch_boundary_rejects_dates_on_or_before_1970() {
        for value in ["1970-01-01", "1970-01-01T00:00:00Z", "1969-12-31T23:59:59Z"] {
            assert!(at_or_before_epoch(value), "{value}");
        }
        for value in ["1970-01-02", "2024-06-10T00:00:00Z", "now", "0"] {
            assert!(!at_or_before_epoch(value), "{value}");
        }
    }

    #[test]
    fn selectivity_error_mentions_id_range_cap_only_for_wide_ranges() {
        let ontology = test_ontology();
        let error_for = |query: &str| {
            let input = parse(query).unwrap();
            validator(&ontology)
                .check_references(&input)
                .unwrap_err()
                .to_string()
        };

        let plain = error_for(r#"MATCH (u:User) RETURN u"#);
        assert!(plain.ends_with("or WHERE mr.project_id = 42"), "{plain}");

        let neighbors = error_for(
            r#"MATCH (u:User)--(neighbor)
                RETURN u, neighbor"#,
        );
        assert!(
            neighbors.ends_with("on the center node to avoid scanning all edges"),
            "{neighbors}"
        );

        let wide_neighbors = error_for(
            r#"MATCH (u:User)--(neighbor)
                WHERE u.id >= 1 AND u.id <= 999999999
                RETURN u, neighbor"#,
        );
        assert!(
            wide_neighbors.ends_with(
                "avoid scanning all edges (an id_range counts only with a span of at most 100000)"
            ),
            "{wide_neighbors}"
        );
    }

    #[test]
    fn node_filter_cap_counts_property_keys_not_entries() {
        let ontology = test_ontology();
        let validator = validator(&ontology);

        let two_props_many_entries = parse(
            r#"MATCH (u:User)
                WHERE u.created_at >= '2020-01-01' AND u.created_at < '2020-02-01' AND u.created_at >= '2020-03-01' AND u.created_at < '2020-04-01' AND u.created_at >= '2020-05-01' AND u.created_at < '2020-06-01' AND u.username CONTAINS 'a' AND u.username CONTAINS 'b' AND u.username CONTAINS 'c' AND u.username CONTAINS 'd' AND u.username CONTAINS 'e' AND u.username CONTAINS 'f'
                RETURN u"#,
        )
        .unwrap();
        assert!(validator.check_depth(&two_props_many_entries).is_ok());

        let eleven_props: String = (0..=10)
            .map(|i| format!("p{i}: true"))
            .collect::<Vec<_>>()
            .join(", ");
        let over_key_cap = parse(&format!("MATCH (u:User {{{eleven_props}}}) RETURN u")).unwrap();
        let err = validator.check_depth(&over_key_cap).unwrap_err();
        assert!(
            err.to_string().contains("filter property count (11)"),
            "got: {err}"
        );
    }

    #[test]
    fn per_property_filter_entry_cap_rejects_overlong_and_chains() {
        let ontology = test_ontology();
        let validator = validator(&ontology);

        let eleven_conditions: String = (0..11)
            .map(|_| "u.created_at >= '2020-01-01'")
            .collect::<Vec<_>>()
            .join(" AND ");
        let over_cap = parse(&format!(
            "MATCH (u:User) WHERE {eleven_conditions} RETURN u"
        ))
        .unwrap();
        let err = validator.check_depth(&over_cap).unwrap_err();
        assert!(
            err.to_string().contains("filter entry count (11)"),
            "got: {err}"
        );
    }

    #[test]
    fn cross_reference_validation() {
        assert_ok(
            r#"MATCH (u:User {id: 1})-[:AUTHORED]->(n:Note)
                RETURN u, n"#,
        );

        assert_ok(
            r#"MATCH (u:User {id: 1})-[:AUTHORED]->(n:Note)
                RETURN u{.username}, count(n) AS note_count"#,
        );

        assert_ok(
            r#"MATCH (u:User {id: 1})
                RETURN u{.username}
                ORDER BY u.username"#,
        );

        assert_ok(
            r#"MATCH p = ANY SHORTEST (a:Project {id: 1})-[:CONTAINS*1..2]->(b:Project {id: 2})
                RETURN p"#,
        );

        assert_ok(
            r#"MATCH (u:User {id: 1})--(neighbor)
                RETURN u, neighbor"#,
        );

        assert_input_rejects(
            mutated(
                "MATCH (u:User {id: 1})-[:AUTHORED]->(n:Note) RETURN u, n",
                |input| input.relationships[0].from = "ghost".into(),
            ),
            "undefined node \"ghost\"",
        );

        assert_input_rejects(
            mutated(
                "MATCH (u:User {id: 1})-[:AUTHORED]->(n:Note) RETURN u, n",
                |input| input.relationships[0].to = "ghost".into(),
            ),
            "undefined node \"ghost\"",
        );

        assert_rejects(
            r#"MATCH (u:User {id: 1})
                RETURN count(missing) AS c"#,
            "undefined node \"missing\"",
        );

        assert_input_rejects(
            mutated("MATCH (u:User {id: 1}) RETURN u, count(u) AS c", |input| {
                input.aggregation.group_by[0] = InputGroupByKey::Node {
                    node: "missing".into(),
                    alias: None,
                }
            }),
            "undefined node \"missing\"",
        );

        assert_rejects(
            r#"MATCH (u:User {id: 1})
                RETURN sum(u.nonexistent) AS total"#,
            "invalid property",
        );

        assert_ok(
            r#"MATCH (p:Project {id: 1})
                RETURN p.visibility_level, count(p) AS project_count"#,
        );

        assert_rejects(
            r#"MATCH (p:Project {id: 1})
                RETURN missing.visibility_level, count(p) AS project_count"#,
            "undefined node \"missing\"",
        );

        assert_rejects(
            r#"MATCH (p:Project {id: 1})
                RETURN p.not_real, count(p) AS project_count"#,
            "invalid property",
        );

        assert_input_rejects(
            mutated("MATCH (p:Project {id: 1}) RETURN p", |input| {
                input.aggregation.group_by.push(InputGroupByKey::Property {
                    node: "p".into(),
                    property: "visibility_level".into(),
                    truncate: None,
                    alias: None,
                })
            }),
            "only supported for aggregation",
        );

        assert_rejects(
            r#"MATCH (p:Project {id: 1})
                RETURN p.visibility_level, p.visibility_level, count(p) AS project_count"#,
            "duplicate group_by output column",
        );

        assert_rejects(
            r#"MATCH (p:Project {id: 1})
                RETURN p.visibility_level, count(p) AS p_visibility_level"#,
            "conflicts with another output column",
        );

        assert_rejects(
            r#"MATCH (u:User {id: 1})
                RETURN u
                ORDER BY missing.username"#,
            "undefined node \"missing\"",
        );

        assert_rejects(
            r#"MATCH (u:User {id: 1})
                RETURN u
                ORDER BY u.nonexistent"#,
            "does not exist",
        );

        assert_input_rejects(
            mutated(
                "MATCH p = ANY SHORTEST (a:Project {id: 1})-[:CONTAINS*1..2]->(b:Project {id: 2}) RETURN p",
                |input| input.path.as_mut().unwrap().from = "ghost".into(),
            ),
            "undefined node \"ghost\"",
        );

        assert_input_rejects(
            mutated(
                "MATCH p = ANY SHORTEST (a:Project {id: 1})-[:CONTAINS*1..2]->(b:Project {id: 2}) RETURN p",
                |input| input.path.as_mut().unwrap().to = "ghost".into(),
            ),
            "undefined node \"ghost\"",
        );
    }

    #[test]
    fn rejects_duplicate_node_ids() {
        assert_input_rejects(
            mutated(
                "MATCH (u:User {id: 1})-[:AUTHORED]->(n:Note) RETURN u, n",
                |input| input.nodes[1].id = "u".into(),
            ),
            "duplicate node id \"u\"",
        );
    }

    #[test]
    fn accepts_unique_node_ids() {
        assert_ok(
            r#"MATCH (u:User {id: 1})<-[:CONTAINS]-(p:Project)
                RETURN u, p"#,
        );
    }

    #[test]
    fn rejects_virtual_group_by_property() {
        let input = parse(
            r#"MATCH (p:Project {id: 1})
                RETURN p.name, count(p) AS project_count"#,
        )
        .unwrap();
        let ontology = test_ontology()
            .modify_field("Project", "name", |field| {
                field.source = FieldSource::Virtual(VirtualSource {
                    service: "example".into(),
                    lookup: "project_name".into(),
                    disabled: false,
                    depends_on: vec![],
                    allowed_ops: VirtualSource::DEFAULT_ALLOWED_OPS
                        .iter()
                        .map(|s| s.to_string())
                        .collect(),
                });
            })
            .unwrap();
        let err = validator(&ontology).check_references(&input).unwrap_err();
        assert!(
            err.to_string().contains("field is virtual"),
            "expected virtual field rejection, got: {err}"
        );
    }

    #[test]
    fn rejects_unreferenced_traversal_node() {
        // 3 nodes but 1 relationship: n-1 check fires before the orphan check
        assert_input_rejects(
            mutated(
                "MATCH (u:User {id: 1})<-[:CONTAINS]-(p:Project) RETURN u, p",
                |input| {
                    input.nodes.push(InputNode {
                        id: "orphan".into(),
                        entity: Some("Note".into()),
                        ..Default::default()
                    })
                },
            ),
            "n-1 relationships",
        );
    }

    #[test]
    fn accepts_aggregation_node_referenced_only_by_target() {
        assert_ok(
            r#"MATCH (p:Project {id: 1})
                RETURN count(p) AS total"#,
        );
    }

    #[test]
    fn rejects_collect_aggregation() {
        assert_input_rejects(
            mutated(
                "MATCH (u:User {id: 1}) RETURN count(u) AS usernames",
                |input| {
                    input.aggregation.metrics[0].expr =
                        crate::input::AggExpr::Collect(crate::input::PropertyRef {
                            node: "u".into(),
                            property: "username".into(),
                        })
                },
            ),
            "\"collect\" is not supported",
        );
    }

    #[test]
    fn accepts_distinct_count_and_rejects_it_on_other_functions() {
        assert_ok("MATCH (u:User {id: 1}) RETURN count(DISTINCT u.username) AS n");
        assert_input_rejects(
            mutated("MATCH (u:User {id: 1}) RETURN sum(u.id) AS n", |input| {
                input.aggregation.metrics[0].distinct = true
            }),
            "\"distinct\" works only with \"count\"",
        );
    }

    #[test]
    fn rejects_property_less_sum_avg_min_max() {
        for func in ["sum", "avg", "min", "max"] {
            let query = format!("MATCH (u:User {{id: 1}}) RETURN {func}(u) AS result");
            let err = parse(&query).unwrap_err().to_string();
            assert!(err.contains("node.property"), "{func}: got {err}");
        }
        assert_ok(
            r#"MATCH (u:User {id: 1})
                RETURN count(u) AS total"#,
        );
    }

    #[test]
    fn rejects_sum_on_string_property() {
        assert_rejects(
            r#"MATCH (u:User {id: 1})
                RETURN sum(u.username) AS username_sum"#,
            "requires a numeric property",
        );
    }

    #[test]
    fn rejects_avg_on_string_property() {
        assert_rejects(
            r#"MATCH (u:User {id: 1})
                RETURN avg(u.username) AS username_avg"#,
            "requires a numeric property",
        );
    }

    #[test]
    fn accepts_top_level_group_by_for_all_aggregations() {
        assert_ok(
            r#"MATCH (u:User {id: 1})-[:MEMBER_OF]->(g:Group)
                RETURN u, count(u) AS total, count(g) AS group_count"#,
        );
    }

    #[test]
    fn rejects_disconnected_multi_node_aggregation_without_group_by() {
        assert_input_rejects(
            mutated(
                "MATCH (mr:MergeRequest)-[:IN_PROJECT]->(p:Project {id: 278964}) RETURN count(mr) AS total",
                |input| input.relationships.clear(),
            ),
            "without group_by requires relationships",
        );
    }

    #[test]
    fn accepts_single_node_aggregation_without_group_by() {
        assert_ok(
            r#"MATCH (u:User {id: 1})
                RETURN count(u) AS total"#,
        );
    }

    #[test]
    fn accepts_string_filter_on_string_column() {
        assert_ok(
            r#"MATCH (u:User {username: 'alice'})
                RETURN u"#,
        );
    }

    #[test]
    fn rejects_aggregation_direction_both() {
        assert_input_rejects(
            mutated(
                "MATCH (u:User {id: 1})-[:AUTHORED]->(mr:MergeRequest) RETURN u, count(mr) AS count",
                |input| input.relationships[0].direction = crate::input::Direction::Both,
            ),
            "does not support direction",
        );
    }

    #[test]
    fn accepts_int_filter_on_int_column() {
        assert_ok(
            r#"MATCH (n:Note {noteable_id: 42})
                RETURN n"#,
        );
    }

    #[test]
    fn accepts_bool_filter_on_bool_column() {
        assert_ok(
            r#"MATCH (n:Note {confidential: true})
                RETURN n"#,
        );
    }

    #[test]
    fn accepts_string_in_filter() {
        assert_ok(
            r#"MATCH (u:User)
                WHERE u.username IN ['alice', 'bob']
                RETURN u"#,
        );
    }

    #[test]
    fn accepts_int_in_filter() {
        assert_ok(
            r#"MATCH (n:Note)
                WHERE n.noteable_id IN [1, 2, 3]
                RETURN n"#,
        );
    }

    #[test]
    fn accepts_is_null_without_value_check() {
        assert_ok(
            r#"MATCH (u:User)
                WHERE u.username IS NULL
                RETURN u"#,
        );
    }

    #[test]
    fn rejects_int_on_string_column() {
        assert_rejects(
            r#"MATCH (u:User {username: 42})
                RETURN u"#,
            "expected String",
        );
    }

    #[test]
    fn rejects_string_on_int_column() {
        assert_rejects(
            r#"MATCH (n:Note {noteable_id: 'abc'})
                RETURN n"#,
            "expected Int",
        );
    }

    #[test]
    fn rejects_string_on_bool_column() {
        assert_rejects(
            r#"MATCH (n:Note {confidential: 'yes'})
                RETURN n"#,
            "expected Bool",
        );
    }

    #[test]
    fn rejects_mixed_type_in_array() {
        assert_rejects(
            r#"MATCH (n:Note)
                WHERE n.noteable_id IN [1, 'two', 3]
                RETURN n"#,
            "element [1]",
        );
    }

    #[test]
    fn rejects_float_on_int_column() {
        assert_rejects(
            r#"MATCH (n:Note {noteable_id: 3.14})
                RETURN n"#,
            "is a float, not an integer",
        );
    }

    #[test]
    fn accepts_int_on_float_column() {
        // Integers are valid for float columns (widening).
        let ontology = Ontology::new()
            .with_nodes(["Metric"])
            .with_fields("Metric", [("score", DataType::Float)]);

        let input = parse(
            r#"MATCH (m:Metric {score: 42})
                RETURN m"#,
        )
        .unwrap();
        validator(&ontology).check_references(&input).unwrap();
    }

    #[test]
    fn accepts_string_on_enum_column() {
        assert_ok(
            r#"MATCH (u:User {user_type: 'human'})
                RETURN u"#,
        );
    }

    #[test]
    fn accepts_int_on_enum_column() {
        // Int-based enums pass validation; normalization coerces to string later.
        assert_ok(
            r#"MATCH (u:User {user_type: 0})
                RETURN u"#,
        );
    }

    #[test]
    fn rejects_bool_on_enum_column() {
        assert_rejects(
            r#"MATCH (u:User {user_type: true})
                RETURN u"#,
            "not a string or integer",
        );
    }

    #[test]
    fn filter_type_mismatch_fails_closed() {
        let ontology = test_ontology();
        let validator = validator(&ontology);

        let input = parse(
            r#"MATCH (n:Note {noteable_id: 'bad'})
                RETURN n"#,
        )
        .unwrap();
        assert!(
            validator.check_references(&input).is_err(),
            "scalar type mismatch must reject the query"
        );

        let input = parse(
            r#"MATCH (n:Note)
                WHERE n.noteable_id IN [1, 'bad', 3]
                RETURN n"#,
        )
        .unwrap();
        assert!(
            validator.check_references(&input).is_err(),
            "IN filter with a mismatched element must reject the query"
        );
    }

    #[test]
    fn accepts_int_filter_on_edge_source_id() {
        assert_ok(
            r#"MATCH (u:User {id: 1})-[r:AUTHORED {source_id: 42}]->(n:Note)
                RETURN u, n"#,
        );
    }

    #[test]
    fn accepts_string_filter_on_edge_relationship_kind() {
        assert_ok(
            r#"MATCH (u:User {id: 1})-[r:AUTHORED {source_kind: 'User'}]->(n:Note)
                RETURN u, n"#,
        );
    }

    #[test]
    fn accepts_is_null_on_edge_column() {
        assert_ok(
            r#"MATCH (u:User {id: 1})-[r:AUTHORED]->(n:Note)
                WHERE r.target_kind IS NULL
                RETURN u, n"#,
        );
    }

    #[test]
    fn accepts_in_filter_on_edge_int_column() {
        assert_ok(
            r#"MATCH (u:User {id: 1})-[r:AUTHORED]->(n:Note)
                WHERE r.target_id IN [1, 2, 3]
                RETURN u, n"#,
        );
    }

    #[test]
    fn rejects_string_on_edge_int_column() {
        assert_rejects(
            r#"MATCH (u:User {id: 1})-[r:AUTHORED {source_id: 'not-a-number'}]->(n:Note)
                RETURN u, n"#,
            "expected Int",
        );
    }

    #[test]
    fn rejects_int_on_edge_string_column() {
        assert_rejects(
            r#"MATCH (u:User {id: 1})-[r:AUTHORED {target_kind: 123}]->(n:Note)
                RETURN u, n"#,
            "expected String",
        );
    }

    #[test]
    fn rejects_unknown_edge_column_filter() {
        assert_rejects(
            r#"MATCH (u:User {id: 1})-[r:AUTHORED {bogus_column: 42}]->(n:Note)
                RETURN u, n"#,
            "unknown edge column \"bogus_column\"",
        );
    }

    #[test]
    fn rejects_mixed_types_in_edge_in_filter() {
        assert_rejects(
            r#"MATCH (u:User {id: 1})-[r:AUTHORED]->(n:Note)
                WHERE r.source_id IN [1, 'bad', 3]
                RETURN u, n"#,
            "element [1]",
        );
    }

    #[test]
    fn edge_filter_type_comes_from_ontology_not_hardcoded() {
        // Build an ontology where source_id is String instead of Int.
        // If types were hardcoded in Rust, this test would FAIL (the hardcoded
        // type is Int, so integer filters would pass). If types are YAML-driven,
        // the Validator reads String and correctly rejects the integer.
        let ontology = ontology::Ontology::new()
            .with_nodes(["User", "Note"])
            .with_edges(["AUTHORED"])
            .with_edge_columns([
                ("traversal_path", DataType::String),
                ("relationship_kind", DataType::String),
                ("source_id", DataType::String), // deliberately wrong: String instead of Int
                ("source_kind", DataType::String),
                ("target_id", DataType::Int),
                ("target_kind", DataType::String),
            ]);
        let validator = validator(&ontology);

        let input = parse(
            r#"MATCH (u:User {id: 1})-[r:AUTHORED {source_id: 42}]->(n:Note)
            RETURN u, n"#,
        )
        .unwrap();

        let result = validator.check_references(&input);
        assert!(
            result.is_err(),
            "integer filter on String source_id should fail"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("source_id"),
            "error should mention source_id, got: {err}"
        );
    }

    #[test]
    fn edge_filter_passes_with_correct_ontology_types() {
        let ontology = ontology::Ontology::new()
            .with_nodes(["User", "Note"])
            .with_edges(["AUTHORED"])
            .with_edge_columns([
                ("traversal_path", DataType::String),
                ("relationship_kind", DataType::String),
                ("source_id", DataType::Int),
                ("source_kind", DataType::String),
                ("target_id", DataType::Int),
                ("target_kind", DataType::String),
            ]);
        let validator = validator(&ontology);

        let input = parse(
            r#"MATCH (u:User {id: 1})-[r:AUTHORED {source_id: 42}]->(n:Note)
            RETURN u, n"#,
        )
        .unwrap();

        let result = validator.check_references(&input);
        assert!(
            result.is_ok(),
            "integer filter on Int source_id should pass, got: {:?}",
            result
        );
    }

    #[test]
    fn identifiers_reject_non_ascii() {
        let ontology = Ontology::load_embedded().unwrap();
        let v = validator(&ontology);
        for name in ["usеr", "ᴜser", "üser", "用户"] {
            assert!(
                parse(&format!("MATCH ({name}:User {{id: 1}}) RETURN {name}")).is_err(),
                "{name}"
            );
            let escaped = parse(&format!("MATCH (`{name}`:User {{id: 1}}) RETURN `{name}`"));
            assert!(
                escaped.is_err() || v.check_shape(&escaped.unwrap()).is_err(),
                "{name}"
            );
        }
    }

    #[test]
    fn selectivity_guards() {
        assert_ok(
            r#"MATCH p = ANY SHORTEST (a:Project {id: 1})-[:CONTAINS*1..3]->(b:Project {id: 2})
                RETURN p"#,
        );
        assert_ok(
            r#"MATCH p = ANY SHORTEST (a:User {username: 'alice'})-[:CONTAINS*1..2]->(b:Project {id: 2})
                RETURN p"#,
        );
        assert_ok(
            r#"MATCH p = ANY SHORTEST (a:User)-[:CONTAINS*1..2]->(b:Project {id: 2})
                WHERE a.id >= 1 AND a.id <= 100
                RETURN p"#,
        );
        assert_rejects(
            r#"MATCH p = ANY SHORTEST (a:User)-[:CONTAINS*1..2]->(b:Project {id: 2})
                WHERE a.id >= 1 AND a.id <= 10000
                RETURN p"#,
            "endpoint \"a\"",
        );
        assert_rejects(
            r#"MATCH p = ANY SHORTEST (a:User)-[:CONTAINS*1..2]->(b:Project {id: 2})
                RETURN p"#,
            "endpoint \"a\"",
        );
        assert_rejects(
            r#"MATCH p = ANY SHORTEST (a:Project {id: 1})-[:CONTAINS*1..2]->(b:User)
                RETURN p"#,
            "endpoint \"b\"",
        );
        for query in [
            "MATCH p = ANY SHORTEST (a:User {username: 'alice'})-[:CONTAINS*1..2]->(b:Project {id: 2}) RETURN p",
            "MATCH p = ANY SHORTEST (a:Project {id: 1})-[:CONTAINS*1..2]->(b:Project {id: 2}) RETURN p",
        ] {
            assert_input_rejects(
                mutated(query, |input| {
                    input.path.as_mut().unwrap().rel_types.clear()
                }),
                "require named relationship types",
            );
        }

        assert_ok(
            r#"MATCH (u:User {id: 1})--(neighbor)
                RETURN u, neighbor"#,
        );
        assert_ok(
            r#"MATCH (u:User {username: 'root'})--(neighbor)
                RETURN u, neighbor"#,
        );
        assert_rejects(
            r#"MATCH (u:User)--(neighbor)
                RETURN u, neighbor"#,
            "center node",
        );

        assert_ok(
            r#"MATCH (u:User {username: 'root'})
                RETURN u"#,
        );
        assert_ok(
            r#"MATCH (u:User {id: 1})
                RETURN u"#,
        );
        assert_rejects(
            r#"MATCH (u:User)
                RETURN u"#,
            "filters or node_ids to at least one node",
        );

        assert_ok(
            r#"MATCH (u:User {id: 1})<-[:CONTAINS]-(p:Project)
                RETURN u, p"#,
        );
        assert_ok(
            r#"MATCH (u:User)<-[:CONTAINS]-(p:Project {name: 'gitlab'})
                RETURN u, p"#,
        );
        assert_rejects(
            r#"MATCH (u:User)<-[:CONTAINS]-(p:Project)
                RETURN u, p"#,
            "filters or node_ids to at least one node",
        );
        assert_ok(
            r#"MATCH (u:User {id: 1})<-[:CONTAINS*1..2]-(p:Project)
                RETURN u, p"#,
        );
        assert_rejects(
            r#"MATCH (u:User)<-[:CONTAINS*1..2]-(p:Project)
                RETURN u, p"#,
            "filters or node_ids to at least one node",
        );

        assert_ok(
            r#"MATCH (u:User {username: 'root'})<-[:CONTAINS]-(p:Project)
                RETURN p, count(u) AS c"#,
        );
        assert_rejects(
            r#"MATCH (u:User)<-[:CONTAINS]-(p:Project)
                RETURN p, count(u) AS c"#,
            "filters or node_ids to at least one node",
        );

        assert_ok(
            r#"MATCH (u:User)
                WHERE u.id >= 1 AND u.id <= 100
                RETURN u"#,
        );
        assert_rejects(
            r#"MATCH (u:User)
                WHERE u.id >= 1 AND u.id <= 999999999
                RETURN u"#,
            "add filters or node_ids to at least one node, such as \
             (p:Project {full_path: 'group/project'}) or WHERE mr.project_id = 42 \
             (an id_range counts only with a span of at most 100000)",
        );
        assert_ok(
            r#"MATCH (u:User)<-[:CONTAINS]-(p:Project)
                WHERE u.id >= 1 AND u.id <= 100
                RETURN u, p"#,
        );
        assert_ok(
            r#"MATCH (u:User)<-[:CONTAINS]-(p:Project)
                WHERE u.id >= 1 AND u.id <= 100
                RETURN p, count(u) AS c"#,
        );
        assert_ok(
            r#"MATCH p = ANY SHORTEST (a:Project)-[:CONTAINS*1..2]->(b:Project {id: 2})
                WHERE a.id >= 1 AND a.id <= 100
                RETURN p"#,
        );
        assert_rejects(
            r#"MATCH p = ANY SHORTEST (a:Project)-[:CONTAINS*1..2]->(b:Project {id: 2})
                WHERE a.id >= 1 AND a.id <= 1000
                RETURN p"#,
            "endpoint \"a\"",
        );
    }

    fn ontology_with_sensitive_field() -> Ontology {
        Ontology::new()
            .with_nodes(["User"])
            .with_edges(["AUTHORED"])
            .with_fields(
                "User",
                [("username", DataType::String), ("email", DataType::String)],
            )
            .modify_field("User", "email", |f| f.like_allowed = false)
            .unwrap()
    }

    fn ontology_with_unfilterable_field() -> Ontology {
        Ontology::new()
            .with_nodes(["Group"])
            .with_edges(["CONTAINS"])
            .with_fields(
                "Group",
                [
                    ("name", DataType::String),
                    ("private_note", DataType::String),
                    ("traversal_path", DataType::String),
                ],
            )
            .modify_field("Group", "private_note", |f| f.filterable = false)
            .unwrap()
            .modify_field("Group", "traversal_path", |f| f.filterable = false)
            .unwrap()
    }

    #[test]
    fn rejects_short_like_pattern() {
        let ont = test_ontology();
        let validator = validator(&ont);
        let input = parse(
            r#"MATCH (u:User)
                WHERE u.username CONTAINS 'ab'
                RETURN u
                LIMIT 10"#,
        )
        .unwrap();

        let err = validator.check_references(&input).unwrap_err();
        assert!(
            err.to_string()
                .contains("search pattern must be at least 3 characters"),
            "expected min length error, got: {err}"
        );
    }

    #[test]
    fn rejects_like_on_disallowed_field() {
        let ont = ontology_with_sensitive_field();
        let validator = validator(&ont);
        let input = parse(
            r#"MATCH (u:User)
                WHERE u.email CONTAINS 'example'
                RETURN u
                LIMIT 10"#,
        )
        .unwrap();

        let err = validator.check_references(&input).unwrap_err();
        assert!(
            err.to_string().contains("string operators"),
            "expected like_allowed rejection, got: {err}"
        );
    }

    #[test]
    fn rejects_like_on_non_text_field() {
        let ont = Ontology::new()
            .with_nodes(["User"])
            .with_edges(["AUTHORED"])
            .with_fields(
                "User",
                [
                    ("created_at", DataType::DateTime),
                    ("state", DataType::Enum),
                ],
            )
            .modify_field("User", "state", |field| {
                field.enum_values = Some(BTreeMap::from([(0, "active".into())]));
            })
            .unwrap();
        let validator = validator(&ont);
        let input = parse(
            r#"MATCH (u:User)
                WHERE u.created_at CONTAINS '2024'
                RETURN u
                LIMIT 10"#,
        )
        .unwrap();

        let err = validator.check_references(&input).unwrap_err();
        assert!(
            err.to_string().contains("require a text field"),
            "expected non-text rejection, got: {err}"
        );

        let input = parse(
            r#"MATCH (u:User)
                WHERE u.state CONTAINS 'act'
                RETURN u
                LIMIT 10"#,
        )
        .unwrap();
        assert!(validator.check_references(&input).is_ok());
    }

    #[test]
    fn accepts_equality_on_like_disallowed_field() {
        let ont = ontology_with_sensitive_field();
        let validator = validator(&ont);
        let input = parse(
            r#"MATCH (u:User {email: 'test@example.com'})
                RETURN u
                LIMIT 10"#,
        )
        .unwrap();

        assert!(
            validator.check_references(&input).is_ok(),
            "equality filter on like_allowed:false field should pass"
        );
    }

    #[test]
    fn rejects_filter_on_unfilterable_field() {
        let ont = ontology_with_unfilterable_field();
        let validator = validator(&ont);
        let input = parse(
            r#"MATCH (g:Group {private_note: 'secret'})
                RETURN g
                LIMIT 10"#,
        )
        .unwrap();

        let err = validator.check_references(&input).unwrap_err();
        assert!(
            err.to_string().contains("not filterable"),
            "expected filterable rejection, got: {err}"
        );
    }

    #[test]
    fn rejects_group_by_property_on_unfilterable_field() {
        let ont = ontology_with_unfilterable_field();
        let validator = validator(&ont);
        let input = parse(
            r#"MATCH (g:Group {id: 1})
                RETURN g.private_note, count(g) AS group_count
                LIMIT 10"#,
        )
        .unwrap();

        let err = validator.check_references(&input).unwrap_err();
        assert!(
            err.to_string().contains("not filterable"),
            "expected group_by filterable rejection, got: {err}"
        );
    }

    #[test]
    fn accepts_filter_on_filterable_field() {
        let ont = ontology_with_unfilterable_field();
        let validator = validator(&ont);
        let input = parse(
            r#"MATCH (g:Group {name: 'Public Group'})
                RETURN g
                LIMIT 10"#,
        )
        .unwrap();

        assert!(
            validator.check_references(&input).is_ok(),
            "filter on filterable:true field should pass"
        );
    }

    #[test]
    fn accepts_multi_filter_range() {
        assert_ok(
            r#"MATCH (u:User {id: 1})
                WHERE u.created_at >= '2026-04-01T00:00:00Z' AND u.created_at < '2026-05-01T00:00:00Z'
                RETURN u"#,
        );
    }

    #[test]
    fn rejects_multi_filter_type_mismatch() {
        assert_rejects(
            r#"MATCH (u:User {id: 1})
                WHERE u.created_at >= '2026-04-01T00:00:00Z' AND u.created_at < 12345
                RETURN u"#,
            "not a string",
        );
    }
}
