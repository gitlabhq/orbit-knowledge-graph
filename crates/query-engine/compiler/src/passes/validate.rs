//! Query validation.
//!
//! Two-phase validation via [`Validator`]:
//! 1. **Schema validation** — structural correctness via JSON Schema (base + ontology-derived).
//!    Entity types, columns, filters, relationship types, and hop ranges are all enforced here.
//! 2. **Cross-reference validation** — node ID references that JSON Schema cannot express
//!    (e.g. relationship from/to must reference a declared node ID).

use std::collections::HashSet;
use std::sync::OnceLock;

use crate::error::{QueryError, Result};
use crate::input::{
    AggFunction, FilterOp, Input, InputFilter, InputNode, QueryType, group_by_output_names,
};
use crate::types::SecurityContext;
use ontology::{DataType, Ontology, TRAVERSAL_PATH_COLUMN};

use super::errors::format_schema_error;

pub(crate) const BASE_SCHEMA_JSON: &str =
    include_str!(concat!(env!("SCHEMA_DIR"), "/graph_query.schema.json"));

static BASE_SCHEMA_VALIDATOR: OnceLock<jsonschema::Validator> = OnceLock::new();

fn base_validator() -> &'static jsonschema::Validator {
    BASE_SCHEMA_VALIDATOR.get_or_init(|| {
        let schema: serde_json::Value =
            serde_json::from_str(BASE_SCHEMA_JSON).expect("schema.json must be valid JSON");
        jsonschema::validator_for(&schema).expect("schema.json must be a valid JSON Schema")
    })
}

fn collect_schema_errors(
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
    if !node.node_ids.is_empty() || !node.filters.is_empty() {
        return true;
    }
    if let Some(ref range) = node.id_range {
        return range.end.saturating_sub(range.start) <= MAX_ID_RANGE_SPAN;
    }
    false
}

pub struct Validator<'a> {
    ontology: &'a Ontology,
}

impl<'a> Validator<'a> {
    pub fn new(ontology: &'a Ontology) -> Self {
        Self { ontology }
    }

    /// If the field is virtual, returns the allowed filter operators from the
    /// ontology. Returns `None` for non-virtual fields.
    fn virtual_allowed_ops(&self, entity: &str, prop: &str) -> Option<Vec<&str>> {
        let node = self.ontology.get_node(entity)?;
        let field = node.fields.iter().find(|f| f.name == prop)?;
        if let ontology::FieldSource::Virtual(vs) = &field.source {
            Some(vs.allowed_ops.iter().map(|s| s.as_str()).collect())
        } else {
            None
        }
    }

    /// Parse JSON and validate against the base schema (structure, identifiers, security).
    pub fn check_json(&self, json: &str) -> Result<serde_json::Value> {
        let value: serde_json::Value = serde_json::from_str(json)?;
        collect_schema_errors(base_validator(), &value)?;
        Ok(value)
    }

    /// Validate against the ontology-derived schema (entity types, columns, relationship types).
    pub fn check_ontology(&self, value: &serde_json::Value) -> Result<()> {
        let schema = self
            .ontology
            .derive_json_schema(BASE_SCHEMA_JSON)
            .map_err(|e| QueryError::Validation(format!("failed to derive schema: {e}")))?;

        let validator = jsonschema::validator_for(&schema)
            .map_err(|e| QueryError::Validation(format!("invalid derived schema: {e}")))?;

        collect_schema_errors(&validator, value).map_err(|e| match e {
            QueryError::Validation(msg) => QueryError::AllowlistRejected(msg),
            other => other,
        })
    }

    /// Validate cross-node references that JSON Schema cannot express.
    pub fn check_references(&self, input: &Input) -> Result<()> {
        self.check_duplicate_node_ids(input)?;
        self.check_pagination(input)?;
        self.check_relationships(input)?;
        self.check_reachability(input)?;
        self.check_aggregations(input)?;
        self.check_order_by(input)?;
        self.check_path(input)?;
        self.check_neighbors(input)?;
        self.check_depth(input)?;
        self.check_selectivity(input)?;
        self.check_filter_types(input)?;
        // Run after individual reference checks so "undefined node X" errors
        // take priority over "node Y is unreferenced".
        self.check_unreferenced_nodes(input)?;
        Ok(())
    }

    /// Defense-in-depth: reject queries that exceed hard caps on complexity.
    /// The JSON schema already enforces these limits via maxItems / maximum /
    /// maxProperties, so this only fires if schema validation was somehow bypassed.
    pub fn check_depth(&self, input: &Input) -> Result<()> {
        const MAX_HOPS_CAP: u32 = 3;
        const MAX_DEPTH_CAP: u32 = 3;
        const MAX_NODES_CAP: usize = 5;
        const MAX_RELS_CAP: usize = 5;
        const MAX_AGGS_CAP: usize = 10;
        const MAX_NODE_IDS: usize = 500;
        const MAX_IN_VALUES: usize = 100;
        const MAX_FILTERS_PER_NODE: usize = 20;
        const MAX_FILTERS_PER_REL: usize = 10;
        const MAX_COLUMNS: usize = 50;
        const MAX_REL_TYPES: usize = 10;
        const MAX_GROUP_BY_KEYS: usize = 4;

        if input.nodes.len() > MAX_NODES_CAP {
            return Err(QueryError::DepthExceeded(format!(
                "nodes count ({}) must not exceed {MAX_NODES_CAP}",
                input.nodes.len()
            )));
        }
        if input.relationships.len() > MAX_RELS_CAP {
            return Err(QueryError::DepthExceeded(format!(
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
            return Err(QueryError::LimitExceeded(format!(
                "aggregations count ({}) must not exceed {MAX_AGGS_CAP}",
                input.aggregation.metrics.len()
            )));
        }
        if input.aggregation.group_by.len() > MAX_GROUP_BY_KEYS {
            return Err(QueryError::LimitExceeded(format!(
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
            if rel.max_hops > MAX_HOPS_CAP {
                return Err(QueryError::DepthExceeded(format!(
                    "max_hops ({}) must not exceed {MAX_HOPS_CAP}",
                    rel.max_hops
                )));
            }
            if rel.types.len() > MAX_REL_TYPES {
                return Err(QueryError::LimitExceeded(format!(
                    "relationship type count ({}) must not exceed {MAX_REL_TYPES}",
                    rel.types.len()
                )));
            }
            let rel_filter_count: usize = rel.filters.values().map(|v| v.len()).sum();
            if rel_filter_count > MAX_FILTERS_PER_REL {
                return Err(QueryError::LimitExceeded(format!(
                    "relationship filter count ({rel_filter_count}) must not exceed {MAX_FILTERS_PER_REL}",
                )));
            }
        }
        if let Some(ref path) = input.path {
            if path.max_depth > MAX_DEPTH_CAP {
                return Err(QueryError::DepthExceeded(format!(
                    "max_depth ({}) must not exceed {MAX_DEPTH_CAP}",
                    path.max_depth
                )));
            }
            if path.rel_types.len() > MAX_REL_TYPES {
                return Err(QueryError::LimitExceeded(format!(
                    "path rel_types count ({}) must not exceed {MAX_REL_TYPES}",
                    path.rel_types.len()
                )));
            }
        }
        for node in &input.nodes {
            if node.node_ids.len() > MAX_NODE_IDS {
                return Err(QueryError::LimitExceeded(format!(
                    "node_ids count ({}) for node \"{}\" must not exceed {MAX_NODE_IDS}",
                    node.node_ids.len(),
                    node.id
                )));
            }
            let node_filter_count: usize = node.filters.values().map(|v| v.len()).sum();
            if node_filter_count > MAX_FILTERS_PER_NODE {
                return Err(QueryError::LimitExceeded(format!(
                    "filter count ({node_filter_count}) for node \"{}\" must not exceed {MAX_FILTERS_PER_NODE}",
                    node.id
                )));
            }
            if let Some(crate::input::ColumnSelection::List(cols)) = &node.columns
                && cols.len() > MAX_COLUMNS
            {
                return Err(QueryError::LimitExceeded(format!(
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
                            return Err(QueryError::LimitExceeded(format!(
                                "IN filter on \"{prop}\" for node \"{}\" has {len} values, must not exceed {MAX_IN_VALUES}",
                                node.id
                            )));
                        }
                    }
                }
            }
        }
        Ok(())
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
    fn check_filter_types(&self, input: &Input) -> Result<()> {
        for node in &input.nodes {
            let Some(entity) = node.entity.as_deref() else {
                continue;
            };
            for (prop, filters) in &node.filters {
                let is_traversal_path_filter = prop == TRAVERSAL_PATH_COLUMN
                    && self
                        .ontology
                        .get_node(entity)
                        .is_some_and(|n| n.has_traversal_path);
                if !is_traversal_path_filter
                    && !self
                        .ontology
                        .check_field_flag(entity, prop, |f| f.filterable)
                {
                    return Err(QueryError::Validation(format!(
                        "filter on \"{prop}\" for {entity}: field is not filterable"
                    )));
                }
                if let Some(allowed) = self.virtual_allowed_ops(entity, prop) {
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
                let Some(data_type) = self.ontology.get_field_type(entity, prop) else {
                    continue;
                };
                for filter in filters {
                    if is_traversal_path_filter {
                        Self::check_traversal_path_filter(
                            &format!("filter on \"{TRAVERSAL_PATH_COLUMN}\" for {entity}"),
                            filter,
                        )?;
                    }
                    self.check_one_filter(entity, prop, filter, data_type)?;
                }
            }
        }

        for (i, rel) in input.relationships.iter().enumerate() {
            let edge_table = rel
                .types
                .first()
                .map(|t| self.ontology.edge_table_for_relationship(t))
                .unwrap_or(self.ontology.edge_table());
            for (prop, filters) in &rel.filters {
                let Some(data_type) = self.ontology.get_edge_table_column_type(edge_table, prop)
                else {
                    return Err(QueryError::Validation(format!(
                        "relationship[{i}] filter on unknown edge column \"{prop}\" \
                         (table \"{edge_table}\" does not have this column)"
                    )));
                };
                for filter in filters {
                    if prop == TRAVERSAL_PATH_COLUMN {
                        Self::check_traversal_path_filter(
                            &format!("relationship[{i}] filter on \"{TRAVERSAL_PATH_COLUMN}\""),
                            filter,
                        )?;
                    }
                    self.check_one_filter(&format!("relationship[{i}]"), prop, filter, data_type)?;
                }
            }
        }

        Ok(())
    }

    /// Annotate every filter with its resolved column [`DataType`].
    /// Runs after `check_filter_types`, so unknown columns fall through
    /// with `data_type = None` and the lowerer infers from the JSON value.
    pub fn annotate_filter_types(&self, input: &mut Input) {
        for node in &mut input.nodes {
            let Some(entity) = node.entity.clone() else {
                continue;
            };
            for (prop, filters) in node.filters.iter_mut() {
                let dt = self.ontology.get_field_type(&entity, prop);
                let selectivity = self
                    .ontology
                    .get_node(&entity)
                    .and_then(|n| n.fields.iter().find(|f| f.name == *prop))
                    .map(|f| f.selectivity)
                    .unwrap_or_default();
                for filter in filters {
                    filter.data_type = dt;
                    filter.selectivity = selectivity;
                }
            }
        }
        for rel in &mut input.relationships {
            for (prop, filters) in rel.filters.iter_mut() {
                let dt = self.ontology.get_edge_column_type(prop);
                for filter in filters {
                    filter.data_type = dt;
                }
            }
        }
    }

    const MIN_LIKE_PATTERN_LEN: usize = 3;

    fn check_traversal_path_filter(label: &str, filter: &InputFilter) -> Result<()> {
        match filter.op.unwrap_or(FilterOp::Eq) {
            FilterOp::Eq | FilterOp::StartsWith => {
                let Some(path) = filter.value.as_ref().and_then(|v| v.as_str()) else {
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
        SecurityContext::validate_traversal_path(path).map_err(|err| match err {
            QueryError::Security(msg) => QueryError::Validation(format!("{label}: {msg}")),
            other => other,
        })
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

        if is_like_op
            && !self
                .ontology
                .check_field_flag(entity, prop, |f| f.like_allowed)
        {
            return Err(QueryError::Validation(format!(
                "filter on \"{prop}\" for {entity}: \
                 LIKE operators (contains/starts_with/ends_with) are not allowed on this field"
            )));
        }

        if is_token_op && self.ontology.text_index_tokenizer(entity, prop).is_none() {
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
            QueryType::Neighbors => {
                if let Some(ref nb) = input.neighbors {
                    let node = input.nodes.iter().find(|n| n.id == nb.node);
                    if node.is_none_or(|n| !node_has_selectivity(n)) {
                        return Err(QueryError::Validation(
                            "neighbors requires node_ids or filters on the center node \
                             to avoid scanning all edges"
                                .into(),
                        ));
                    }
                }
            }
            QueryType::Traversal | QueryType::Aggregation
                if !input.nodes.iter().any(node_has_selectivity) =>
            {
                return Err(QueryError::Validation(
                    "traversal and aggregation queries require node_ids or filters on \
                     at least one node to avoid full edge table scans"
                        .into(),
                ));
            }
            _ => {}
        }
        Ok(())
    }

    fn check_pagination(&self, input: &Input) -> Result<()> {
        if let Some(ref cursor) = input.cursor
            && cursor.offset.saturating_add(cursor.page_size) > input.limit
        {
            return Err(QueryError::PaginationError(format!(
                "cursor.offset ({}) + cursor.page_size ({}) must not exceed limit ({})",
                cursor.offset, cursor.page_size, input.limit
            )));
        }
        Ok(())
    }

    fn check_relationships(&self, input: &Input) -> Result<()> {
        let node_ids: Vec<&str> = input.nodes.iter().map(|n| n.id.as_str()).collect();

        for (i, rel) in input.relationships.iter().enumerate() {
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

    /// Reject a relationship whose declared endpoint entities are not connected
    /// by any of its declared types within the hop budget. The flat schema only
    /// checks that each edge *name* exists, so `Project -[AUTHORED]-> User`
    /// (no such triple) passes schema validation and fails later with an opaque
    /// empty result. This surfaces it pre-execution and names the kinds that do
    /// connect the two endpoints.
    fn check_reachability(&self, input: &Input) -> Result<()> {
        if !matches!(
            input.query_type,
            QueryType::Traversal | QueryType::Aggregation
        ) || input.is_search()
        {
            return Ok(());
        }

        let entity_of: std::collections::HashMap<&str, &str> = input
            .nodes
            .iter()
            .filter_map(|n| n.entity.as_deref().map(|e| (n.id.as_str(), e)))
            .collect();

        let graph = self.ontology.graph();

        for (i, rel) in input.relationships.iter().enumerate() {
            if rel.types.iter().any(|t| t == "*") {
                continue;
            }
            let (Some(&from), Some(&to)) = (
                entity_of.get(rel.from.as_str()),
                entity_of.get(rel.to.as_str()),
            ) else {
                continue;
            };

            if graph.fk_reaches(from, to) || graph.fk_reaches(to, from) {
                continue;
            }

            let connecting = self.kinds_connecting(&graph, from, to);
            if rel.max_hops > 1 {
                if from == to
                    || graph
                        .reachable_within(from, rel.max_hops as usize)
                        .contains(to)
                    || graph
                        .reachable_within(to, rel.max_hops as usize)
                        .contains(from)
                {
                    continue;
                }
            } else if rel.types.iter().any(|t| connecting.contains(t.as_str())) {
                continue;
            }

            let hint = if connecting.is_empty() {
                let mut kinds: Vec<&str> = graph
                    .neighbors(from, ontology::EdgeDirection::Outgoing)
                    .iter()
                    .chain(graph.neighbors(from, ontology::EdgeDirection::Incoming))
                    .map(|a| a.neighbor_kind.as_str())
                    .collect();
                kinds.sort();
                kinds.dedup();
                if kinds.is_empty() {
                    format!("\"{from}\" has no relationships in the ontology")
                } else {
                    format!("\"{from}\" connects only to: [{}]", kinds.join(", "))
                }
            } else {
                format!(
                    "relationship types connecting \"{from}\" and \"{to}\": [{}]",
                    connecting.iter().copied().collect::<Vec<_>>().join(", ")
                )
            };

            return Err(QueryError::ReferenceError(format!(
                "relationship[{i}] type {:?} does not connect \"{from}\" and \"{to}\"; {hint}",
                rel.types
            )));
        }

        Ok(())
    }

    /// Relationship kinds connecting two entity kinds in either orientation.
    /// Direction is deliberately ignored: the ontology stores each triple in one
    /// canonical orientation and the lowerer matches it regardless of the
    /// query's declared direction, so a stricter check would false-positive.
    fn kinds_connecting<'g>(
        &self,
        graph: &'g ontology::OntologyGraph,
        a: &str,
        b: &str,
    ) -> std::collections::BTreeSet<&'g str> {
        graph
            .neighbors(a, ontology::EdgeDirection::Outgoing)
            .iter()
            .chain(graph.neighbors(a, ontology::EdgeDirection::Incoming))
            .filter(|adj| adj.neighbor_kind == b)
            .map(|adj| adj.relationship_kind.as_str())
            .collect()
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
                    "duplicate group_by output alias \"{name}\""
                )));
            }
        }

        let mut seen_output_names = seen_group_output_names.clone();
        for (i, agg) in input.aggregation.metrics.iter().enumerate() {
            let agg_alias = agg
                .alias
                .clone()
                .unwrap_or_else(|| agg.function.to_string());
            if !seen_output_names.insert(agg_alias.clone()) {
                return Err(QueryError::Validation(format!(
                    "aggregation[{i}] output alias \"{agg_alias}\" conflicts with another output column"
                )));
            }

            if agg.function == AggFunction::Collect {
                return Err(QueryError::Validation(format!(
                    "aggregation[{i}] function \"collect\" is not supported"
                )));
            }

            // sum/avg/min/max without a property silently aggregate the edge
            // ID column after edge-only optimization (e.g. SUM(e0.source_id)),
            // which is meaningless. Require an explicit property.
            if matches!(
                agg.function,
                AggFunction::Sum | AggFunction::Avg | AggFunction::Min | AggFunction::Max
            ) && agg.property.is_none()
            {
                return Err(QueryError::Validation(format!(
                    "aggregation[{i}] function \"{}\" requires a 'property' field",
                    agg.function.as_sql()
                )));
            }

            if let Some(target) = &agg.target
                && !node_ids.contains(&target.as_str())
            {
                return Err(QueryError::ReferenceError(format!(
                    "aggregation[{}] references undefined node \"{}\" in 'target'",
                    i, target
                )));
            }

            if let (Some(prop), Some(target)) = (&agg.property, &agg.target)
                && let Some(node) = input.nodes.iter().find(|n| n.id == *target)
            {
                let entity = node
                    .entity
                    .as_ref()
                    .ok_or_else(|| QueryError::ReferenceError("missing entity".into()))?;
                self.ontology.validate_field(entity, prop).map_err(|e| {
                    QueryError::AllowlistRejected(format!(
                        "invalid property in aggregation[{}]: {}",
                        i, e
                    ))
                })?;

                if matches!(agg.function, AggFunction::Sum | AggFunction::Avg) {
                    let data_type =
                        self.ontology.get_field_type(entity, prop).ok_or_else(|| {
                            QueryError::AllowlistRejected(format!(
                                "invalid property in aggregation[{}]: {}.{}",
                                i, entity, prop
                            ))
                        })?;

                    if !matches!(data_type, DataType::Int | DataType::Float) {
                        return Err(QueryError::Validation(format!(
                            "aggregation[{i}] function \"{}\" requires a numeric property, got {}.{} ({data_type})",
                            agg.function.as_sql(),
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

            self.ontology
                .validate_field(entity, property)
                .map_err(|e| {
                    QueryError::AllowlistRejected(format!("invalid property in group_by[{i}]: {e}"))
                })?;

            if !self
                .ontology
                .check_field_flag(entity, property, |f| f.filterable)
            {
                return Err(QueryError::Validation(format!(
                    "group_by[{i}] on \"{}\" for {entity}: field is not filterable",
                    property
                )));
            }

            if let Some(field) = self
                .ontology
                .get_node(entity)
                .and_then(|n| n.fields.iter().find(|f| f.name == property))
                && field.is_virtual()
            {
                return Err(QueryError::Validation(format!(
                    "group_by[{i}] on \"{}\" for {entity}: field is virtual and cannot be grouped in SQL",
                    property
                )));
            }

            if let Some(unit) = group.truncate() {
                let field = self
                    .ontology
                    .get_node(entity)
                    .and_then(|n| n.fields.iter().find(|f| f.name == property));
                let data_type = field.map(|f| f.data_type);
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
            output_names.extend(input.aggregation.metrics.iter().map(|agg| {
                agg.alias
                    .clone()
                    .unwrap_or_else(|| agg.function.to_string())
            }));
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
        self.ontology
            .validate_field(entity, &order_by.property)
            .map_err(|e| {
                QueryError::AllowlistRejected(format!("invalid order_by property: {}", e))
            })?;

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

        let neighbors = input.neighbors.as_ref().ok_or_else(|| {
            QueryError::ReferenceError(
                "neighbors query requires a 'neighbors' configuration".into(),
            )
        })?;

        let node_ids: Vec<&str> = input.nodes.iter().map(|n| n.id.as_str()).collect();

        if !node_ids.contains(&neighbors.node.as_str()) {
            return Err(QueryError::ReferenceError(format!(
                "neighbors 'node' references undefined node \"{}\"",
                neighbors.node
            )));
        }

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
                    if let Some(ref t) = agg.target {
                        set.insert(t.as_str());
                    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::parse_input;
    use ontology::{DataType, FieldSource, VirtualSource};

    fn variant(kind: &str, source: &str, target: &str) -> ontology::EdgeEntity {
        ontology::EdgeEntity {
            relationship_kind: kind.to_string(),
            source: source.to_string(),
            source_kind: source.to_string(),
            target: target.to_string(),
            target_kind: target.to_string(),
            destination_table: ontology::EDGE_TABLE.to_string(),
            fk_column: None,
            scope: None,
        }
    }

    fn test_ontology() -> Ontology {
        Ontology::new()
            .with_nodes(["User", "Project", "Note", "Group", "MergeRequest"])
            .with_edges(["AUTHORED", "CONTAINS", "MEMBER_OF"])
            .with_edge_variant(variant("AUTHORED", "User", "Note"))
            .with_edge_variant(variant("AUTHORED", "User", "MergeRequest"))
            .with_edge_variant(variant("CONTAINS", "Project", "User"))
            .with_edge_variant(variant("CONTAINS", "Project", "Project"))
            .with_edge_variant(variant("MEMBER_OF", "User", "Group"))
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
                    ("visibility_level", DataType::String),
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
    }

    fn assert_ok(json: &str) {
        let input = parse_input(json).unwrap();
        let ontology = test_ontology();
        Validator::new(&ontology).check_references(&input).unwrap();
    }

    fn assert_rejects(json: &str, expected: &str) {
        let input = parse_input(json).unwrap();
        let ontology = test_ontology();
        let err = Validator::new(&ontology)
            .check_references(&input)
            .unwrap_err();
        assert!(
            err.to_string().contains(expected),
            "expected error containing \"{expected}\", got: {err}"
        );
    }

    #[test]
    fn cross_reference_validation() {
        assert_ok(
            r#"{
                "query_type": "traversal",
                "nodes": [
                    {"id": "u", "entity": "User", "node_ids": [1]},
                    {"id": "n", "entity": "Note"}
                ],
                "relationships": [{"type": "AUTHORED", "from": "u", "to": "n"}]
            }"#,
        );

        assert_ok(
            r#"{
                "query_type": "aggregation",
                "nodes": [
                    {"id": "u", "entity": "User", "node_ids": [1], "columns": ["username"]},
                    {"id": "n", "entity": "Note"}
                ],
                "relationships": [{"type": "AUTHORED", "from": "u", "to": "n"}],
                "group_by": [{"kind": "node", "node": "u"}],
                "aggregations": [{
                    "function": "count",
                    "target": "n",
                    "alias": "note_count"
                }]
            }"#,
        );

        assert_ok(
            r#"{
                "query_type": "traversal",
                "node": {"id": "u", "entity": "User", "node_ids": [1], "columns": ["username"]},
                "order_by": {"node": "u", "property": "username", "direction": "ASC"}
            }"#,
        );

        assert_ok(
            r#"{
                "query_type": "path_finding",
                "nodes": [
                    {"id": "a", "entity": "Project", "node_ids": [1]},
                    {"id": "b", "entity": "Project", "node_ids": [2]}
                ],
                "path": {"type": "shortest", "from": "a", "to": "b", "max_depth": 2,
                         "rel_types": ["CONTAINS"]}
            }"#,
        );

        assert_ok(
            r#"{
                "query_type": "neighbors",
                "node": {"id": "u", "entity": "User", "node_ids": [1]},
                "neighbors": {"node": "u", "direction": "both"}
            }"#,
        );

        assert_rejects(
            r#"{
                "query_type": "traversal",
                "nodes": [{"id": "u", "entity": "User", "node_ids": [1]}],
                "relationships": [{"type": "AUTHORED", "from": "ghost", "to": "u"}]
            }"#,
            "undefined node \"ghost\"",
        );

        assert_rejects(
            r#"{
                "query_type": "traversal",
                "nodes": [{"id": "u", "entity": "User", "node_ids": [1]}],
                "relationships": [{"type": "AUTHORED", "from": "u", "to": "ghost"}]
            }"#,
            "undefined node \"ghost\"",
        );

        assert_rejects(
            r#"{
                "query_type": "aggregation",
                "nodes": [{"id": "u", "entity": "User", "node_ids": [1]}],
                "aggregations": [{
                    "function": "count",
                    "target": "missing",
                    "alias": "c"
                }]
            }"#,
            "undefined node \"missing\"",
        );

        assert_rejects(
            r#"{
                "query_type": "aggregation",
                "nodes": [{"id": "u", "entity": "User", "node_ids": [1]}],
                "group_by": [{"kind": "node", "node": "missing"}],
                "aggregations": [{
                    "function": "count",
                    "target": "u",
                    "alias": "c"
                }]
            }"#,
            "undefined node \"missing\"",
        );

        assert_rejects(
            r#"{
                "query_type": "aggregation",
                "nodes": [{"id": "u", "entity": "User", "node_ids": [1]}],
                "aggregations": [{
                    "function": "sum",
                    "target": "u",
                    "property": "nonexistent",
                    "alias": "total"
                }]
            }"#,
            "invalid property",
        );

        assert_ok(
            r#"{
                "query_type": "aggregation",
                "nodes": [{"id": "p", "entity": "Project", "node_ids": [1]}],
                "group_by": [{"kind": "property", "node": "p", "property": "visibility_level"}],
                "aggregations": [{"function": "count", "target": "p", "alias": "project_count"}]
            }"#,
        );

        assert_rejects(
            r#"{
                "query_type": "aggregation",
                "nodes": [{"id": "p", "entity": "Project", "node_ids": [1]}],
                "group_by": [{"kind": "property", "node": "missing", "property": "visibility_level"}],
                "aggregations": [{"function": "count", "target": "p", "alias": "project_count"}]
            }"#,
            "undefined node \"missing\"",
        );

        assert_rejects(
            r#"{
                "query_type": "aggregation",
                "nodes": [{"id": "p", "entity": "Project", "node_ids": [1]}],
                "group_by": [{"kind": "property", "node": "p", "property": "not_real"}],
                "aggregations": [{"function": "count", "target": "p", "alias": "project_count"}]
            }"#,
            "invalid property",
        );

        assert_rejects(
            r#"{
                "query_type": "traversal",
                "node": {"id": "p", "entity": "Project", "node_ids": [1]},
                "group_by": [{"kind": "property", "node": "p", "property": "visibility_level"}]
            }"#,
            "only supported for aggregation",
        );

        assert_rejects(
            r#"{
                "query_type": "aggregation",
                "nodes": [{"id": "p", "entity": "Project", "node_ids": [1]}],
                "group_by": [
                    {"kind": "property", "node": "p", "property": "visibility_level", "alias": "bucket"},
                    {"kind": "node", "node": "p", "alias": "bucket"}
                ],
                "aggregations": [{
                    "function": "count",
                    "target": "p",
                    "alias": "project_count"
                }]
            }"#,
            "duplicate group_by output alias",
        );

        assert_rejects(
            r#"{
                "query_type": "aggregation",
                "nodes": [{"id": "p", "entity": "Project", "node_ids": [1]}],
                "group_by": [{"kind": "property", "node": "p", "property": "visibility_level", "alias": "count"}],
                "aggregations": [{"function": "count", "target": "p"}]
            }"#,
            "conflicts with another output column",
        );

        assert_rejects(
            r#"{
                "query_type": "traversal",
                "node": {"id": "u", "entity": "User", "node_ids": [1]},
                "order_by": {"node": "missing", "property": "username", "direction": "ASC"}
            }"#,
            "undefined node \"missing\"",
        );

        assert_rejects(
            r#"{
                "query_type": "traversal",
                "node": {"id": "u", "entity": "User", "node_ids": [1]},
                "order_by": {"node": "u", "property": "nonexistent", "direction": "ASC"}
            }"#,
            "does not exist",
        );

        assert_rejects(
            r#"{
                "query_type": "path_finding",
                "nodes": [
                    {"id": "a", "entity": "Project", "node_ids": [1]},
                    {"id": "b", "entity": "Project", "node_ids": [2]}
                ],
                "path": {"type": "shortest", "from": "ghost", "to": "b", "max_depth": 2,
                         "rel_types": ["CONTAINS"]}
            }"#,
            "undefined node \"ghost\"",
        );

        assert_rejects(
            r#"{
                "query_type": "path_finding",
                "nodes": [
                    {"id": "a", "entity": "Project", "node_ids": [1]},
                    {"id": "b", "entity": "Project", "node_ids": [2]}
                ],
                "path": {"type": "shortest", "from": "a", "to": "ghost", "max_depth": 2,
                         "rel_types": ["CONTAINS"]}
            }"#,
            "undefined node \"ghost\"",
        );

        assert_rejects(
            r#"{
                "query_type": "neighbors",
                "node": {"id": "u", "entity": "User", "node_ids": [1]},
                "neighbors": {"node": "ghost", "direction": "both"}
            }"#,
            "undefined node \"ghost\"",
        );
    }

    #[test]
    fn rejects_duplicate_node_ids() {
        assert_rejects(
            r#"{
                "query_type": "traversal",
                "nodes": [
                    {"id": "u", "entity": "User", "node_ids": [1]},
                    {"id": "u", "entity": "Project"}
                ]
            }"#,
            "duplicate node id \"u\"",
        );
    }

    #[test]
    fn rejects_relationship_type_that_does_not_connect_endpoints() {
        assert_rejects(
            r#"{
                "query_type": "traversal",
                "nodes": [
                    {"id": "u", "entity": "User", "node_ids": [1]},
                    {"id": "g", "entity": "Group"}
                ],
                "relationships": [{"type": "AUTHORED", "from": "u", "to": "g"}]
            }"#,
            "does not connect \"User\" and \"Group\"",
        );
    }

    #[test]
    fn reachability_hint_names_the_connecting_types() {
        assert_rejects(
            r#"{
                "query_type": "traversal",
                "nodes": [
                    {"id": "u", "entity": "User", "node_ids": [1]},
                    {"id": "g", "entity": "Group"}
                ],
                "relationships": [{"type": "AUTHORED", "from": "u", "to": "g"}]
            }"#,
            "MEMBER_OF",
        );
    }

    #[test]
    fn wildcard_relationship_type_skips_reachability() {
        assert_ok(
            r#"{
                "query_type": "traversal",
                "nodes": [
                    {"id": "u", "entity": "User", "node_ids": [1]},
                    {"id": "g", "entity": "Group"}
                ],
                "relationships": [{"type": "*", "from": "u", "to": "g"}]
            }"#,
        );
    }

    #[test]
    fn accepts_unique_node_ids() {
        assert_ok(
            r#"{
                "query_type": "traversal",
                "nodes": [
                    {"id": "u", "entity": "User", "node_ids": [1]},
                    {"id": "p", "entity": "Project"}
                ],
                "relationships": [{"type": "CONTAINS", "from": "p", "to": "u"}]
            }"#,
        );
    }

    #[test]
    fn rejects_virtual_group_by_property() {
        let input = parse_input(
            r#"{
                "query_type": "aggregation",
                "nodes": [{"id": "p", "entity": "Project", "node_ids": [1]}],
                "group_by": [{"kind": "property", "node": "p", "property": "name"}],
                "aggregations": [{"function": "count", "target": "p", "alias": "project_count"}]
            }"#,
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
        let err = Validator::new(&ontology)
            .check_references(&input)
            .unwrap_err();
        assert!(
            err.to_string().contains("field is virtual"),
            "expected virtual field rejection, got: {err}"
        );
    }

    #[test]
    fn rejects_unreferenced_traversal_node() {
        // 3 nodes but 1 relationship: n-1 check fires before the orphan check
        assert_rejects(
            r#"{
                "query_type": "traversal",
                "nodes": [
                    {"id": "u", "entity": "User", "node_ids": [1]},
                    {"id": "p", "entity": "Project"},
                    {"id": "orphan", "entity": "Note"}
                ],
                "relationships": [{"type": "CONTAINS", "from": "p", "to": "u"}]
            }"#,
            "n-1 relationships",
        );
    }

    #[test]
    fn accepts_aggregation_node_referenced_only_by_target() {
        assert_ok(
            r#"{
                "query_type": "aggregation",
                "nodes": [{"id": "p", "entity": "Project", "node_ids": [1]}],
                "aggregations": [{"function": "count", "target": "p", "alias": "total"}]
            }"#,
        );
    }

    #[test]
    fn rejects_collect_aggregation() {
        assert_rejects(
            r#"{
                "query_type": "aggregation",
                "nodes": [{"id": "u", "entity": "User", "node_ids": [1]}],
                "aggregations": [{
                    "function": "collect",
                    "target": "u",
                    "property": "username",
                    "alias": "usernames"
                }]
            }"#,
            "function \"collect\" is not supported",
        );
    }

    #[test]
    fn rejects_property_less_sum_avg_min_max() {
        for func in ["sum", "avg", "min", "max"] {
            let json = format!(
                r#"{{
                    "query_type": "aggregation",
                    "nodes": [{{"id": "u", "entity": "User", "node_ids": [1]}}],
                    "aggregations": [{{
                        "function": "{func}",
                        "target": "u",
                        "alias": "result"
                    }}]
                }}"#
            );
            assert_rejects(&json, "requires a 'property' field");
        }
        assert_ok(
            r#"{
                "query_type": "aggregation",
                "nodes": [{"id": "u", "entity": "User", "node_ids": [1]}],
                "aggregations": [{"function": "count", "target": "u", "alias": "total"}]
            }"#,
        );
    }

    #[test]
    fn rejects_sum_on_string_property() {
        assert_rejects(
            r#"{
                "query_type": "aggregation",
                "nodes": [{"id": "u", "entity": "User", "node_ids": [1]}],
                "aggregations": [{
                    "function": "sum",
                    "target": "u",
                    "property": "username",
                    "alias": "username_sum"
                }]
            }"#,
            "requires a numeric property",
        );
    }

    #[test]
    fn rejects_avg_on_string_property() {
        assert_rejects(
            r#"{
                "query_type": "aggregation",
                "nodes": [{"id": "u", "entity": "User", "node_ids": [1]}],
                "aggregations": [{
                    "function": "avg",
                    "target": "u",
                    "property": "username",
                    "alias": "username_avg"
                }]
            }"#,
            "requires a numeric property",
        );
    }

    #[test]
    fn accepts_min_on_string_property() {
        assert_ok(
            r#"{
                "query_type": "aggregation",
                "nodes": [{"id": "u", "entity": "User", "node_ids": [1]}],
                "aggregations": [{
                    "function": "min",
                    "target": "u",
                    "property": "username",
                    "alias": "first_username"
                }]
            }"#,
        );
    }

    #[test]
    fn accepts_top_level_group_by_for_all_aggregations() {
        assert_ok(
            r#"{
                "query_type": "aggregation",
                "nodes": [
                    {"id": "u", "entity": "User", "node_ids": [1]},
                    {"id": "g", "entity": "Group"}
                ],
                "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
                "group_by": [{"kind": "node", "node": "u"}],
                "aggregations": [
                    {"function": "count", "target": "u", "alias": "total"},
                    {"function": "count", "target": "g", "alias": "group_count"}
                ]
            }"#,
        );
    }

    #[test]
    fn rejects_disconnected_multi_node_aggregation_without_group_by() {
        assert_rejects(
            r#"{
                "query_type": "aggregation",
                "nodes": [
                    {"id": "mr", "entity": "MergeRequest"},
                    {"id": "p", "entity": "Project", "node_ids": [278964]}
                ],
                "aggregations": [
                    {"function": "count", "target": "mr", "alias": "total"}
                ]
            }"#,
            "without group_by requires relationships",
        );
    }

    #[test]
    fn accepts_single_node_aggregation_without_group_by() {
        assert_ok(
            r#"{
                "query_type": "aggregation",
                "node": {"id": "u", "entity": "User", "node_ids": [1]},
                "aggregations": [
                    {"function": "count", "target": "u", "alias": "total"}
                ]
            }"#,
        );
    }

    #[test]
    fn accepts_string_filter_on_string_column() {
        assert_ok(
            r#"{
                "query_type": "traversal",
                "node": {"id": "u", "entity": "User", "filters": {"username": "alice"}}
            }"#,
        );
    }

    #[test]
    fn rejects_aggregation_direction_both() {
        assert_rejects(
            r#"{
                "query_type": "aggregation",
                "nodes": [
                    {"id": "u", "entity": "User", "node_ids": [1]},
                    {"id": "mr", "entity": "MergeRequest"}
                ],
                "relationships": [{"type": "AUTHORED", "from": "u", "to": "mr", "direction": "both"}],
                "group_by": [{"kind": "node", "node": "u"}],
                "aggregations": [{"function": "count", "target": "mr"}]
            }"#,
            "does not support direction",
        );
    }

    #[test]
    fn accepts_int_filter_on_int_column() {
        assert_ok(
            r#"{
                "query_type": "traversal",
                "node": {"id": "n", "entity": "Note", "filters": {"noteable_id": 42}}
            }"#,
        );
    }

    #[test]
    fn accepts_large_u64_on_int_column() {
        // u64 values beyond i64::MAX are still valid integers
        assert_ok(
            r#"{
                "query_type": "traversal",
                "node": {"id": "n", "entity": "Note", "filters": {"noteable_id": 9223372036854775808}}
            }"#,
        );
    }

    #[test]
    fn accepts_bool_filter_on_bool_column() {
        assert_ok(
            r#"{
                "query_type": "traversal",
                "node": {"id": "n", "entity": "Note", "filters": {"confidential": true}}
            }"#,
        );
    }

    #[test]
    fn accepts_string_in_filter() {
        assert_ok(
            r#"{
                "query_type": "traversal",
                "node": {
                    "id": "u", "entity": "User",
                    "filters": {"username": {"op": "in", "value": ["alice", "bob"]}}
                }
            }"#,
        );
    }

    #[test]
    fn accepts_int_in_filter() {
        assert_ok(
            r#"{
                "query_type": "traversal",
                "node": {
                    "id": "n", "entity": "Note",
                    "filters": {"noteable_id": {"op": "in", "value": [1, 2, 3]}}
                }
            }"#,
        );
    }

    #[test]
    fn accepts_is_null_without_value_check() {
        assert_ok(
            r#"{
                "query_type": "traversal",
                "node": {
                    "id": "u", "entity": "User",
                    "filters": {"username": {"op": "is_null"}}
                }
            }"#,
        );
    }

    #[test]
    fn rejects_int_on_string_column() {
        assert_rejects(
            r#"{
                "query_type": "traversal",
                "node": {"id": "u", "entity": "User", "filters": {"username": 42}}
            }"#,
            "expected String",
        );
    }

    #[test]
    fn rejects_string_on_int_column() {
        assert_rejects(
            r#"{
                "query_type": "traversal",
                "node": {"id": "n", "entity": "Note", "filters": {"noteable_id": "abc"}}
            }"#,
            "expected Int",
        );
    }

    #[test]
    fn rejects_string_on_bool_column() {
        assert_rejects(
            r#"{
                "query_type": "traversal",
                "node": {"id": "n", "entity": "Note", "filters": {"confidential": "yes"}}
            }"#,
            "expected Bool",
        );
    }

    #[test]
    fn rejects_mixed_type_in_array() {
        assert_rejects(
            r#"{
                "query_type": "traversal",
                "node": {
                    "id": "n", "entity": "Note",
                    "filters": {"noteable_id": {"op": "in", "value": [1, "two", 3]}}
                }
            }"#,
            "element [1]",
        );
    }

    #[test]
    fn rejects_float_on_int_column() {
        assert_rejects(
            r#"{
                "query_type": "traversal",
                "node": {"id": "n", "entity": "Note", "filters": {"noteable_id": 3.14}}
            }"#,
            "is a float, not an integer",
        );
    }

    #[test]
    fn accepts_int_on_float_column() {
        // Integers are valid for float columns (widening).
        let ontology = Ontology::new()
            .with_nodes(["Metric"])
            .with_fields("Metric", [("score", DataType::Float)]);

        let input = parse_input(
            r#"{
                "query_type": "traversal",
                "node": {"id": "m", "entity": "Metric", "filters": {"score": 42}}
            }"#,
        )
        .unwrap();
        Validator::new(&ontology).check_references(&input).unwrap();
    }

    #[test]
    fn accepts_string_on_enum_column() {
        assert_ok(
            r#"{
                "query_type": "traversal",
                "node": {"id": "u", "entity": "User", "filters": {"user_type": "human"}}
            }"#,
        );
    }

    #[test]
    fn accepts_int_on_enum_column() {
        // Int-based enums pass validation; normalization coerces to string later.
        assert_ok(
            r#"{
                "query_type": "traversal",
                "node": {"id": "u", "entity": "User", "filters": {"user_type": 0}}
            }"#,
        );
    }

    #[test]
    fn rejects_bool_on_enum_column() {
        assert_rejects(
            r#"{
                "query_type": "traversal",
                "node": {"id": "u", "entity": "User", "filters": {"user_type": true}}
            }"#,
            "not a string or integer",
        );
    }

    #[test]
    fn filter_type_mismatch_fails_closed() {
        let ontology = test_ontology();
        let validator = Validator::new(&ontology);

        let input = parse_input(
            r#"{
                "query_type": "traversal",
                "node": {"id": "n", "entity": "Note", "filters": {"noteable_id": "bad"}}
            }"#,
        )
        .unwrap();
        assert!(
            validator.check_references(&input).is_err(),
            "scalar type mismatch must reject the query"
        );

        let input = parse_input(
            r#"{
                "query_type": "traversal",
                "node": {
                    "id": "n", "entity": "Note",
                    "filters": {"noteable_id": {"op": "in", "value": [1, "bad", 3]}}
                }
            }"#,
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
            r#"{
                "query_type": "traversal",
                "nodes": [
                    {"id": "u", "entity": "User", "node_ids": [1]},
                    {"id": "n", "entity": "Note"}
                ],
                "relationships": [{
                    "type": "AUTHORED", "from": "u", "to": "n",
                    "filters": {"source_id": 42}
                }]
            }"#,
        );
    }

    #[test]
    fn accepts_string_filter_on_edge_relationship_kind() {
        assert_ok(
            r#"{
                "query_type": "traversal",
                "nodes": [
                    {"id": "u", "entity": "User", "node_ids": [1]},
                    {"id": "n", "entity": "Note"}
                ],
                "relationships": [{
                    "type": "AUTHORED", "from": "u", "to": "n",
                    "filters": {"source_kind": "User"}
                }]
            }"#,
        );
    }

    #[test]
    fn accepts_is_null_on_edge_column() {
        assert_ok(
            r#"{
                "query_type": "traversal",
                "nodes": [
                    {"id": "u", "entity": "User", "node_ids": [1]},
                    {"id": "n", "entity": "Note"}
                ],
                "relationships": [{
                    "type": "AUTHORED", "from": "u", "to": "n",
                    "filters": {"target_kind": {"op": "is_null"}}
                }]
            }"#,
        );
    }

    #[test]
    fn accepts_in_filter_on_edge_int_column() {
        assert_ok(
            r#"{
                "query_type": "traversal",
                "nodes": [
                    {"id": "u", "entity": "User", "node_ids": [1]},
                    {"id": "n", "entity": "Note"}
                ],
                "relationships": [{
                    "type": "AUTHORED", "from": "u", "to": "n",
                    "filters": {"target_id": {"op": "in", "value": [1, 2, 3]}}
                }]
            }"#,
        );
    }

    #[test]
    fn rejects_string_on_edge_int_column() {
        assert_rejects(
            r#"{
                "query_type": "traversal",
                "nodes": [
                    {"id": "u", "entity": "User", "node_ids": [1]},
                    {"id": "n", "entity": "Note"}
                ],
                "relationships": [{
                    "type": "AUTHORED", "from": "u", "to": "n",
                    "filters": {"source_id": "not-a-number"}
                }]
            }"#,
            "expected Int",
        );
    }

    #[test]
    fn rejects_int_on_edge_string_column() {
        assert_rejects(
            r#"{
                "query_type": "traversal",
                "nodes": [
                    {"id": "u", "entity": "User", "node_ids": [1]},
                    {"id": "n", "entity": "Note"}
                ],
                "relationships": [{
                    "type": "AUTHORED", "from": "u", "to": "n",
                    "filters": {"target_kind": 123}
                }]
            }"#,
            "expected String",
        );
    }

    #[test]
    fn rejects_unknown_edge_column_filter() {
        assert_rejects(
            r#"{
                "query_type": "traversal",
                "nodes": [
                    {"id": "u", "entity": "User", "node_ids": [1]},
                    {"id": "n", "entity": "Note"}
                ],
                "relationships": [{
                    "type": "AUTHORED", "from": "u", "to": "n",
                    "filters": {"bogus_column": 42}
                }]
            }"#,
            "unknown edge column \"bogus_column\"",
        );
    }

    #[test]
    fn rejects_mixed_types_in_edge_in_filter() {
        assert_rejects(
            r#"{
                "query_type": "traversal",
                "nodes": [
                    {"id": "u", "entity": "User", "node_ids": [1]},
                    {"id": "n", "entity": "Note"}
                ],
                "relationships": [{
                    "type": "AUTHORED", "from": "u", "to": "n",
                    "filters": {"source_id": {"op": "in", "value": [1, "bad", 3]}}
                }]
            }"#,
            "element [1]",
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

    #[test]
    fn edge_filter_type_comes_from_ontology_not_hardcoded() {
        // Build an ontology where source_id is String instead of Int.
        // If types were hardcoded in Rust, this test would FAIL (the hardcoded
        // type is Int, so integer filters would pass). If types are YAML-driven,
        // the Validator reads String and correctly rejects the integer.
        let ontology = ontology::Ontology::new()
            .with_nodes(["User", "Note"])
            .with_edges(["AUTHORED"])
            .with_edge_variant(variant("AUTHORED", "User", "Note"))
            .with_edge_columns([
                ("traversal_path", DataType::String),
                ("relationship_kind", DataType::String),
                ("source_id", DataType::String), // deliberately wrong: String instead of Int
                ("source_kind", DataType::String),
                ("target_id", DataType::Int),
                ("target_kind", DataType::String),
            ]);
        let validator = Validator::new(&ontology);

        let input = parse_input(r#"{
            "query_type": "traversal",
            "nodes": [{"id": "u", "entity": "User", "node_ids": [1]}, {"id": "n", "entity": "Note"}],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "n", "filters": {"source_id": 42}}]
        }"#)
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
            .with_edge_variant(variant("AUTHORED", "User", "Note"))
            .with_edge_columns([
                ("traversal_path", DataType::String),
                ("relationship_kind", DataType::String),
                ("source_id", DataType::Int),
                ("source_kind", DataType::String),
                ("target_id", DataType::Int),
                ("target_kind", DataType::String),
            ]);
        let validator = Validator::new(&ontology);

        let input = parse_input(r#"{
            "query_type": "traversal",
            "nodes": [{"id": "u", "entity": "User", "node_ids": [1]}, {"id": "n", "entity": "Note"}],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "n", "filters": {"source_id": 42}}]
        }"#)
        .unwrap();

        let result = validator.check_references(&input);
        assert!(
            result.is_ok(),
            "integer filter on Int source_id should pass, got: {:?}",
            result
        );
    }

    /// Verify the Identifier regex in graph_query.schema.json only matches ASCII.
    /// This is a defense against homoglyph attacks — the regex engine treats
    /// [a-zA-Z] as ASCII-only, but this test makes the assumption explicit.
    /// The pattern is loaded from the schema to prevent staleness.
    #[test]
    fn identifier_regex_rejects_non_ascii() {
        let schema: serde_json::Value = serde_json::from_str(BASE_SCHEMA_JSON).unwrap();
        let pattern = schema["$defs"]["Identifier"]["pattern"]
            .as_str()
            .expect("Identifier pattern missing from schema");
        let re = regex::Regex::new(pattern).unwrap();

        assert!(re.is_match("user"));
        assert!(re.is_match("_foo"));
        assert!(re.is_match("User123"));

        assert!(!re.is_match("usеr")); // Cyrillic е (U+0435)
        assert!(!re.is_match("ᴜser")); // Latin small capital U (U+1D1C)
        assert!(!re.is_match("üser")); // Latin u with diaeresis
        assert!(!re.is_match("用户")); // CJK
    }

    #[test]
    fn selectivity_guards() {
        assert_ok(
            r#"{
                "query_type": "path_finding",
                "nodes": [
                    {"id": "a", "entity": "Project", "node_ids": [1]},
                    {"id": "b", "entity": "Project", "node_ids": [2]}
                ],
                "path": {"type": "shortest", "from": "a", "to": "b", "max_depth": 3,
                         "rel_types": ["CONTAINS"]}
            }"#,
        );
        assert_ok(
            r#"{
                "query_type": "path_finding",
                "nodes": [
                    {"id": "a", "entity": "User", "filters": {"username": "alice"}},
                    {"id": "b", "entity": "Project", "node_ids": [2]}
                ],
                "path": {"type": "shortest", "from": "a", "to": "b", "max_depth": 2,
                         "rel_types": ["CONTAINS"]}
            }"#,
        );
        assert_ok(
            r#"{
                "query_type": "path_finding",
                "nodes": [
                    {"id": "a", "entity": "User", "id_range": {"start": 1, "end": 100}},
                    {"id": "b", "entity": "Project", "node_ids": [2]}
                ],
                "path": {"type": "shortest", "from": "a", "to": "b", "max_depth": 2,
                         "rel_types": ["CONTAINS"]}
            }"#,
        );
        assert_rejects(
            r#"{
                "query_type": "path_finding",
                "nodes": [
                    {"id": "a", "entity": "User", "id_range": {"start": 1, "end": 10000}},
                    {"id": "b", "entity": "Project", "node_ids": [2]}
                ],
                "path": {"type": "shortest", "from": "a", "to": "b", "max_depth": 2,
                         "rel_types": ["CONTAINS"]}
            }"#,
            "endpoint \"a\"",
        );
        assert_rejects(
            r#"{
                "query_type": "path_finding",
                "nodes": [
                    {"id": "a", "entity": "User"},
                    {"id": "b", "entity": "Project", "node_ids": [2]}
                ],
                "path": {"type": "shortest", "from": "a", "to": "b", "max_depth": 2,
                         "rel_types": ["CONTAINS"]}
            }"#,
            "endpoint \"a\"",
        );
        assert_rejects(
            r#"{
                "query_type": "path_finding",
                "nodes": [
                    {"id": "a", "entity": "Project", "node_ids": [1]},
                    {"id": "b", "entity": "User"}
                ],
                "path": {"type": "shortest", "from": "a", "to": "b", "max_depth": 2,
                         "rel_types": ["CONTAINS"]}
            }"#,
            "endpoint \"b\"",
        );
        assert_rejects(
            r#"{
                "query_type": "path_finding",
                "nodes": [
                    {"id": "a", "entity": "User", "filters": {"username": "alice"}},
                    {"id": "b", "entity": "Project", "node_ids": [2]}
                ],
                "path": {"type": "shortest", "from": "a", "to": "b", "max_depth": 2}
            }"#,
            "requires rel_types",
        );
        assert_rejects(
            r#"{
                "query_type": "path_finding",
                "nodes": [
                    {"id": "a", "entity": "Project", "node_ids": [1]},
                    {"id": "b", "entity": "Project", "node_ids": [2]}
                ],
                "path": {"type": "shortest", "from": "a", "to": "b", "max_depth": 2}
            }"#,
            "requires rel_types",
        );

        assert_ok(
            r#"{
                "query_type": "neighbors",
                "node": {"id": "u", "entity": "User", "node_ids": [1]},
                "neighbors": {"node": "u", "direction": "both"}
            }"#,
        );
        assert_ok(
            r#"{
                "query_type": "neighbors",
                "node": {"id": "u", "entity": "User", "filters": {"username": "root"}},
                "neighbors": {"node": "u", "direction": "both"}
            }"#,
        );
        assert_rejects(
            r#"{
                "query_type": "neighbors",
                "node": {"id": "u", "entity": "User"},
                "neighbors": {"node": "u", "direction": "both"}
            }"#,
            "center node",
        );

        assert_ok(
            r#"{
                "query_type": "traversal",
                "node": {"id": "u", "entity": "User", "filters": {"username": "root"}}
            }"#,
        );
        assert_ok(
            r#"{
                "query_type": "traversal",
                "node": {"id": "u", "entity": "User", "node_ids": [1]}
            }"#,
        );
        assert_rejects(
            r#"{
                "query_type": "traversal",
                "node": {"id": "u", "entity": "User"}
            }"#,
            "full edge table scans",
        );

        assert_ok(
            r#"{
                "query_type": "traversal",
                "nodes": [
                    {"id": "u", "entity": "User", "node_ids": [1]},
                    {"id": "p", "entity": "Project"}
                ],
                "relationships": [{"type": "CONTAINS", "from": "p", "to": "u"}]
            }"#,
        );
        assert_ok(
            r#"{
                "query_type": "traversal",
                "nodes": [
                    {"id": "u", "entity": "User"},
                    {"id": "p", "entity": "Project", "filters": {"name": "gitlab"}}
                ],
                "relationships": [{"type": "CONTAINS", "from": "p", "to": "u"}]
            }"#,
        );
        assert_rejects(
            r#"{
                "query_type": "traversal",
                "nodes": [
                    {"id": "u", "entity": "User"},
                    {"id": "p", "entity": "Project"}
                ],
                "relationships": [{"type": "CONTAINS", "from": "p", "to": "u"}]
            }"#,
            "full edge table scans",
        );
        assert_ok(
            r#"{
                "query_type": "traversal",
                "nodes": [
                    {"id": "u", "entity": "User", "node_ids": [1]},
                    {"id": "p", "entity": "Project"}
                ],
                "relationships": [{"type": "CONTAINS", "from": "p", "to": "u", "max_hops": 2}]
            }"#,
        );
        assert_rejects(
            r#"{
                "query_type": "traversal",
                "nodes": [
                    {"id": "u", "entity": "User"},
                    {"id": "p", "entity": "Project"}
                ],
                "relationships": [{"type": "CONTAINS", "from": "p", "to": "u", "max_hops": 2}]
            }"#,
            "full edge table scans",
        );

        assert_ok(
            r#"{
                "query_type": "aggregation",
                "nodes": [
                    {"id": "u", "entity": "User", "filters": {"username": "root"}},
                    {"id": "p", "entity": "Project"}
                ],
                "relationships": [{"type": "CONTAINS", "from": "p", "to": "u"}],
                "group_by": [{"kind": "node", "node": "p"}],
                "aggregations": [{"function": "count", "target": "u", "alias": "c"}]
            }"#,
        );
        assert_rejects(
            r#"{
                "query_type": "aggregation",
                "nodes": [
                    {"id": "u", "entity": "User"},
                    {"id": "p", "entity": "Project"}
                ],
                "relationships": [{"type": "CONTAINS", "from": "p", "to": "u"}],
                "group_by": [{"kind": "node", "node": "p"}],
                "aggregations": [{"function": "count", "target": "u", "alias": "c"}]
            }"#,
            "full edge table scans",
        );

        assert_ok(
            r#"{
                "query_type": "traversal",
                "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 100}}
            }"#,
        );
        assert_rejects(
            r#"{
                "query_type": "traversal",
                "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 999999999}}
            }"#,
            "full edge table scans",
        );
        assert_ok(
            r#"{
                "query_type": "traversal",
                "nodes": [
                    {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 100}},
                    {"id": "p", "entity": "Project"}
                ],
                "relationships": [{"type": "CONTAINS", "from": "p", "to": "u"}]
            }"#,
        );
        assert_ok(
            r#"{
                "query_type": "aggregation",
                "nodes": [
                    {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 100}},
                    {"id": "p", "entity": "Project"}
                ],
                "relationships": [{"type": "CONTAINS", "from": "p", "to": "u"}],
                "group_by": [{"kind": "node", "node": "p"}],
                "aggregations": [{"function": "count", "target": "u", "alias": "c"}]
            }"#,
        );
        assert_ok(
            r#"{
                "query_type": "path_finding",
                "nodes": [
                    {"id": "a", "entity": "Project", "id_range": {"start": 1, "end": 100}},
                    {"id": "b", "entity": "Project", "node_ids": [2]}
                ],
                "path": {"type": "shortest", "from": "a", "to": "b", "max_depth": 2,
                         "rel_types": ["CONTAINS"]}
            }"#,
        );
        assert_rejects(
            r#"{
                "query_type": "path_finding",
                "nodes": [
                    {"id": "a", "entity": "Project", "id_range": {"start": 1, "end": 1000}},
                    {"id": "b", "entity": "Project", "node_ids": [2]}
                ],
                "path": {"type": "shortest", "from": "a", "to": "b", "max_depth": 2,
                         "rel_types": ["CONTAINS"]}
            }"#,
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
        let validator = Validator::new(&ont);
        let input = parse_input(
            r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User",
                     "filters": {"username": {"op": "contains", "value": "ab"}}},
            "limit": 10
        }"#,
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
        let validator = Validator::new(&ont);
        let input = parse_input(
            r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User",
                     "filters": {"email": {"op": "contains", "value": "example"}}},
            "limit": 10
        }"#,
        )
        .unwrap();

        let err = validator.check_references(&input).unwrap_err();
        assert!(
            err.to_string().contains("LIKE operators"),
            "expected like_allowed rejection, got: {err}"
        );
    }

    #[test]
    fn accepts_equality_on_like_disallowed_field() {
        let ont = ontology_with_sensitive_field();
        let validator = Validator::new(&ont);
        let input = parse_input(
            r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User",
                     "filters": {"email": "test@example.com"}},
            "limit": 10
        }"#,
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
        let validator = Validator::new(&ont);
        let input = parse_input(
            r#"{
            "query_type": "traversal",
            "node": {"id": "g", "entity": "Group",
                     "filters": {"private_note": "secret"}},
            "limit": 10
        }"#,
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
        let validator = Validator::new(&ont);
        let input = parse_input(
            r#"{
            "query_type": "aggregation",
            "nodes": [{"id": "g", "entity": "Group", "node_ids": [1]}],
            "group_by": [{"kind": "property", "node": "g", "property": "private_note"}],
            "aggregations": [{"function": "count", "target": "g", "alias": "group_count"}],
            "limit": 10
        }"#,
        )
        .unwrap();

        let err = validator.check_references(&input).unwrap_err();
        assert!(
            err.to_string().contains("not filterable"),
            "expected group_by filterable rejection, got: {err}"
        );
    }

    #[test]
    fn accepts_filter_on_traversal_path_even_when_field_is_unfilterable() {
        let ont = ontology_with_unfilterable_field();
        let validator = Validator::new(&ont);
        let input = parse_input(
            r#"{
            "query_type": "traversal",
            "node": {"id": "g", "entity": "Group",
                     "filters": {"traversal_path": {"op": "starts_with", "value": "1/"}}},
            "limit": 10
        }"#,
        )
        .unwrap();

        assert!(
            validator.check_references(&input).is_ok(),
            "traversal_path scope is guarded by RestrictPass, not the ontology filterable flag"
        );
    }

    #[test]
    fn rejects_traversal_path_filter_on_global_entity() {
        let ont = test_ontology();
        let validator = Validator::new(&ont);
        let input = parse_input(
            r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "node_ids": [1],
                     "filters": {"traversal_path": {"op": "starts_with", "value": "1/"}}},
            "limit": 10
        }"#,
        )
        .unwrap();

        let err = validator.check_references(&input).unwrap_err();
        assert!(
            err.to_string().contains("not filterable"),
            "expected global entity traversal_path rejection, got: {err}"
        );
    }

    #[test]
    fn rejects_unsupported_traversal_path_filter_operator() {
        let ont = ontology_with_unfilterable_field();
        let validator = Validator::new(&ont);
        let input = parse_input(
            r#"{
            "query_type": "traversal",
            "node": {"id": "g", "entity": "Group",
                     "filters": {"traversal_path": {"op": "contains", "value": "100"}}},
            "limit": 10
        }"#,
        )
        .unwrap();

        let err = validator.check_references(&input).unwrap_err();
        assert!(
            err.to_string().contains("only eq, in, and starts_with"),
            "expected traversal_path operator rejection, got: {err}"
        );
    }

    #[test]
    fn rejects_invalid_traversal_path_filter_format() {
        let ont = ontology_with_unfilterable_field();
        let validator = Validator::new(&ont);
        let input = parse_input(
            r#"{
            "query_type": "traversal",
            "node": {"id": "g", "entity": "Group",
                     "filters": {"traversal_path": {"op": "starts_with", "value": "1/100"}}},
            "limit": 10
        }"#,
        )
        .unwrap();

        let err = validator.check_references(&input).unwrap_err();
        assert!(
            err.to_string().contains("invalid traversal_path format"),
            "expected traversal_path format rejection, got: {err}"
        );
    }

    #[test]
    fn rejects_unsupported_relationship_traversal_path_filter_operator() {
        let ont = test_ontology();
        let validator = Validator::new(&ont);
        let input = parse_input(
            r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "a", "entity": "Project", "node_ids": [1]},
                {"id": "b", "entity": "Project"}
            ],
            "relationships": [{
                "type": "CONTAINS",
                "from": "a",
                "to": "b",
                "filters": {"traversal_path": {"op": "ends_with", "value": "100/"}}
            }],
            "limit": 10
        }"#,
        )
        .unwrap();

        let err = validator.check_references(&input).unwrap_err();
        assert!(
            err.to_string().contains("only eq, in, and starts_with"),
            "expected relationship traversal_path operator rejection, got: {err}"
        );
    }

    #[test]
    fn accepts_filter_on_filterable_field() {
        let ont = ontology_with_unfilterable_field();
        let validator = Validator::new(&ont);
        let input = parse_input(
            r#"{
            "query_type": "traversal",
            "node": {"id": "g", "entity": "Group",
                     "filters": {"name": "Public Group"}},
            "limit": 10
        }"#,
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
            r#"{
                "query_type": "traversal",
                "node": {"id": "u", "entity": "User",
                         "node_ids": [1],
                         "filters": {"created_at": [
                             {"op": "gte", "value": "2026-04-01T00:00:00Z"},
                             {"op": "lt", "value": "2026-05-01T00:00:00Z"}
                         ]}}
            }"#,
        );
    }

    #[test]
    fn rejects_multi_filter_type_mismatch() {
        assert_rejects(
            r#"{
                "query_type": "traversal",
                "node": {"id": "u", "entity": "User",
                         "node_ids": [1],
                         "filters": {"created_at": [
                             {"op": "gte", "value": "2026-04-01T00:00:00Z"},
                             {"op": "lt", "value": 12345}
                         ]}}
            }"#,
            "not a string",
        );
    }
}
