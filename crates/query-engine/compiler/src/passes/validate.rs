//! Query validation.
//!
//! Two-phase validation via [`Validator`]:
//! 1. **Schema validation** — structural correctness via JSON Schema (base + ontology-derived).
//!    Entity types, columns, filters, relationship types, and hop ranges are all enforced here.
//! 2. **Cross-reference validation** — node ID references that JSON Schema cannot express
//!    (e.g. relationship from/to must reference a declared node ID).

use std::sync::OnceLock;

use crate::error::{QueryError, Result};
use crate::input::{AggFunction, FilterOp, Input, InputFilter, InputNode, QueryType};
use ontology::{DataType, Ontology};

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
        .map(|e| sanitize_schema_error(&format!("{} at {}", e, e.instance_path())))
        .collect();

    if errors.is_empty() {
        Ok(())
    } else {
        Err(QueryError::Validation(errors.join("; ")))
    }
}

/// Strip enumerated valid values from jsonschema enum-rejection messages.
/// Matches `is not one of ["quoted","values",...]` — requires at least one
/// quoted element to avoid false positives on non-enum bracket content.
fn sanitize_schema_error(msg: &str) -> String {
    static RE: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r#"is not one of \["[^"]*".*?\]"#).expect("valid regex")
    });
    RE.replace_all(msg, "is not an allowed value").to_string()
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
        // Enums accept strings (string-based) or integers (int-based, coerced
        // to string labels by normalization before lowering).
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

// ─────────────────────────────────────────────────────────────────────────────
// Validator
// ─────────────────────────────────────────────────────────────────────────────

pub struct Validator<'a> {
    ontology: &'a Ontology,
}

impl<'a> Validator<'a> {
    pub fn new(ontology: &'a Ontology) -> Self {
        Self { ontology }
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
        // Traversal with 1 node + 0 rels is a search (single table scan).
        // Multi-node traversal requires exactly n-1 relationships.
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
        if input.aggregations.len() > MAX_AGGS_CAP {
            return Err(QueryError::LimitExceeded(format!(
                "aggregations count ({}) must not exceed {MAX_AGGS_CAP}",
                input.aggregations.len()
            )));
        }
        for rel in &input.relationships {
            // direction: "both" generates OR join conditions that defeat
            // ClickHouse index and projection usage. The JSON schema also
            // rejects this for aggregation, but this guard covers code paths
            // that bypass schema validation (CLI, internal tests, gRPC).
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
            if rel.filters.len() > MAX_FILTERS_PER_REL {
                return Err(QueryError::LimitExceeded(format!(
                    "relationship filter count ({}) must not exceed {MAX_FILTERS_PER_REL}",
                    rel.filters.len()
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
            if node.filters.len() > MAX_FILTERS_PER_NODE {
                return Err(QueryError::LimitExceeded(format!(
                    "filter count ({}) for node \"{}\" must not exceed {MAX_FILTERS_PER_NODE}",
                    node.filters.len(),
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
            for (prop, filter) in &node.filters {
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
            for (prop, filter) in &node.filters {
                if !self
                    .ontology
                    .check_field_flag(entity, prop, |f| f.filterable)
                {
                    return Err(QueryError::Validation(format!(
                        "filter on \"{prop}\" for {entity}: field is not filterable"
                    )));
                }
                let Some(data_type) = self.ontology.get_field_type(entity, prop) else {
                    continue;
                };
                self.check_one_filter(entity, prop, filter, data_type)?;
            }
        }

        for (i, rel) in input.relationships.iter().enumerate() {
            for (prop, filter) in &rel.filters {
                let Some(data_type) = self.ontology.get_edge_column_type(prop) else {
                    return Err(QueryError::Validation(format!(
                        "relationship[{i}] filter on unknown edge column \"{prop}\""
                    )));
                };
                self.check_one_filter(&format!("relationship[{i}]"), prop, filter, data_type)?;
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
            for (prop, filter) in node.filters.iter_mut() {
                filter.data_type = self.ontology.get_field_type(&entity, prop);
            }
        }
        for rel in &mut input.relationships {
            for (prop, filter) in rel.filters.iter_mut() {
                filter.data_type = self.ontology.get_edge_column_type(prop);
            }
        }
    }

    /// Minimum number of characters required in a LIKE filter value.
    const MIN_LIKE_PATTERN_LEN: usize = 3;

    fn check_one_filter(
        &self,
        entity: &str,
        prop: &str,
        filter: &InputFilter,
        data_type: DataType,
    ) -> Result<()> {
        let op = filter.op.unwrap_or(FilterOp::Eq);

        // Ops without a value — nothing to type-check.
        if matches!(op, FilterOp::IsNull | FilterOp::IsNotNull) {
            return Ok(());
        }

        let is_like_op = matches!(
            op,
            FilterOp::Contains | FilterOp::StartsWith | FilterOp::EndsWith
        );

        // Reject LIKE operators on fields marked like_allowed: false.
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

        let Some(value) = filter.value.as_ref() else {
            return Ok(());
        };

        // Enforce minimum pattern length for LIKE filters.
        if is_like_op {
            let len = value.as_str().map_or(0, |s| s.chars().count());
            if len < Self::MIN_LIKE_PATTERN_LEN {
                return Err(QueryError::Validation(format!(
                    "filter on \"{prop}\" for {entity}: \
                     LIKE pattern must be at least {} characters, got {len}",
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
            // Neighbors without selectivity on the center node scans all edges.
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

            // Traversal/aggregation without selectivity scans entire edge tables.
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

    fn check_aggregations(&self, input: &Input) -> Result<()> {
        if input.query_type != QueryType::Aggregation {
            return Ok(());
        }

        let node_ids: Vec<&str> = input.nodes.iter().map(|n| n.id.as_str()).collect();

        let has_grouped = input.aggregations.iter().any(|a| a.group_by.is_some());
        let has_ungrouped = input.aggregations.iter().any(|a| a.group_by.is_none());
        if has_grouped && has_ungrouped {
            return Err(QueryError::Validation(
                "cannot mix grouped and ungrouped aggregations in the same query".into(),
            ));
        }

        // Multi-node aggregation without group_by produces a full cross-join
        // scan that will timeout on any real dataset. Require group_by when
        // there are 2+ nodes.
        if input.nodes.len() > 1 && has_ungrouped {
            return Err(QueryError::Validation(
                "multi-node aggregation requires 'group_by' on each aggregation".into(),
            ));
        }

        for (i, agg) in input.aggregations.iter().enumerate() {
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

            if let Some(group_by) = &agg.group_by
                && !node_ids.contains(&group_by.as_str())
            {
                return Err(QueryError::ReferenceError(format!(
                    "aggregation[{}] references undefined node \"{}\" in 'group_by'",
                    i, group_by
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

        // When an endpoint uses filters or id_range (not node_ids), the
        // anchor CTE can resolve up to MAX_PATH_ANCHOR_LIMIT (500) nodes.
        // Without rel_types each of those seeds fans out through every edge
        // type across all physical tables — O(500 × |E|^depth). Require
        // rel_types so the frontier only traverses relevant relationships.
        // Pinned endpoints (node_ids) have a known, small anchor set where
        // the hop-frontier optimizer bounds intermediate work adequately.
        if path.rel_types.is_empty() {
            let both_pinned = [&path.from, &path.to].iter().all(|endpoint| {
                input
                    .nodes
                    .iter()
                    .find(|n| n.id == **endpoint)
                    .is_some_and(|n| !n.node_ids.is_empty())
            });
            if !both_pinned {
                return Err(QueryError::Validation(
                    "path_finding requires rel_types when an endpoint uses filters \
                     or id_range; without it the frontier scans all edge tables"
                        .into(),
                ));
            }
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
                for agg in &input.aggregations {
                    if let Some(ref t) = agg.target {
                        set.insert(t.as_str());
                    }
                    if let Some(ref g) = agg.group_by {
                        set.insert(g.as_str());
                    }
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

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::parse_input;
    use ontology::DataType;

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
            .with_fields("Project", [("name", DataType::String)])
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
        // ── Happy paths ─────────────────────────────────────────────

        // Valid traversal with relationship
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

        // Valid aggregation with target and group_by
        assert_ok(
            r#"{
                "query_type": "aggregation",
                "nodes": [
                    {"id": "u", "entity": "User", "node_ids": [1], "columns": ["username"]},
                    {"id": "n", "entity": "Note"}
                ],
                "relationships": [{"type": "AUTHORED", "from": "u", "to": "n"}],
                "aggregations": [{
                    "function": "count",
                    "target": "n",
                    "group_by": "u",
                    "alias": "note_count"
                }]
            }"#,
        );

        // Valid order_by referencing declared node
        assert_ok(
            r#"{
                "query_type": "traversal",
                "node": {"id": "u", "entity": "User", "node_ids": [1], "columns": ["username"]},
                "order_by": {"node": "u", "property": "username", "direction": "ASC"}
            }"#,
        );

        // Valid path_finding
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

        // Valid neighbors
        assert_ok(
            r#"{
                "query_type": "neighbors",
                "node": {"id": "u", "entity": "User", "node_ids": [1]},
                "neighbors": {"node": "u", "direction": "both"}
            }"#,
        );

        // ── Relationship from/to ────────────────────────────────────

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

        // ── Aggregation references ──────────────────────────────────

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
                "aggregations": [{
                    "function": "count",
                    "target": "u",
                    "group_by": "missing",
                    "alias": "c"
                }]
            }"#,
            "undefined node \"missing\"",
        );

        // Aggregation property that doesn't exist on the target entity
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

        // ── Order by references ─────────────────────────────────────

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

        // ── Path from/to ────────────────────────────────────────────

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

        // ── Neighbors node ──────────────────────────────────────────

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
        // count without property is fine
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
    fn rejects_mixed_grouped_and_ungrouped_aggregations() {
        assert_rejects(
            r#"{
                "query_type": "aggregation",
                "nodes": [
                    {"id": "u", "entity": "User", "node_ids": [1]},
                    {"id": "g", "entity": "Group"}
                ],
                "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
                "aggregations": [
                    {"function": "count", "target": "u", "alias": "total"},
                    {"function": "count", "target": "g", "group_by": "u", "alias": "group_count"}
                ]
            }"#,
            "cannot mix grouped and ungrouped aggregations",
        );
    }

    #[test]
    fn rejects_multi_node_aggregation_without_group_by() {
        assert_rejects(
            r#"{
                "query_type": "aggregation",
                "nodes": [
                    {"id": "mr", "entity": "MergeRequest"},
                    {"id": "p", "entity": "Project", "node_ids": [278964]}
                ],
                "relationships": [{"type": "IN_PROJECT", "from": "mr", "to": "p"}],
                "aggregations": [
                    {"function": "count", "target": "mr", "alias": "total"}
                ]
            }"#,
            "multi-node aggregation requires 'group_by'",
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

    // ── Filter type validation ──────────────────────────────────────

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
                "aggregations": [{"function": "count", "target": "mr", "group_by": "u"}]
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

        // Scalar op: wrong type must produce Err, not silently pass.
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

        // IN op: one bad element among valid ones must produce Err.
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

    // ── Relationship filter type validation ─────────────────────────

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
        // Same setup but with correct types — integer filter on source_id passes.
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

        // Valid ASCII identifiers
        assert!(re.is_match("user"));
        assert!(re.is_match("_foo"));
        assert!(re.is_match("User123"));

        // Homoglyph / non-ASCII must be rejected
        assert!(!re.is_match("usеr")); // Cyrillic е (U+0435)
        assert!(!re.is_match("ᴜser")); // Latin small capital U (U+1D1C)
        assert!(!re.is_match("üser")); // Latin u with diaeresis
        assert!(!re.is_match("用户")); // CJK
    }

    // ── Selectivity guards ────────────────────────────────────────────

    #[test]
    fn selectivity_guards() {
        // ── Path finding ────────────────────────────────────────────
        // Both endpoints pinned: OK
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
        // Filters on endpoint: OK (lowerer resolves via capped CTE)
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
        // Narrow id_range on endpoint: OK
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
        // Wide id_range on endpoint exceeds MAX_PATH_ANCHOR_RANGE (500)
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
        // No selectivity on start endpoint
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
        // No selectivity on end endpoint
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
        // Missing rel_types with filtered endpoint: rejected
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
        // Missing rel_types with both endpoints pinned: OK
        assert_ok(
            r#"{
                "query_type": "path_finding",
                "nodes": [
                    {"id": "a", "entity": "Project", "node_ids": [1]},
                    {"id": "b", "entity": "Project", "node_ids": [2]}
                ],
                "path": {"type": "shortest", "from": "a", "to": "b", "max_depth": 2}
            }"#,
        );

        // ── Neighbors ───────────────────────────────────────────────
        // Center node pinned: OK
        assert_ok(
            r#"{
                "query_type": "neighbors",
                "node": {"id": "u", "entity": "User", "node_ids": [1]},
                "neighbors": {"node": "u", "direction": "both"}
            }"#,
        );
        // Center node with filter: OK
        assert_ok(
            r#"{
                "query_type": "neighbors",
                "node": {"id": "u", "entity": "User", "filters": {"username": "root"}},
                "neighbors": {"node": "u", "direction": "both"}
            }"#,
        );
        // Center node without selectivity
        assert_rejects(
            r#"{
                "query_type": "neighbors",
                "node": {"id": "u", "entity": "User"},
                "neighbors": {"node": "u", "direction": "both"}
            }"#,
            "center node",
        );

        // ── Search ──────────────────────────────────────────────────
        // Search with filter: OK
        assert_ok(
            r#"{
                "query_type": "traversal",
                "node": {"id": "u", "entity": "User", "filters": {"username": "root"}}
            }"#,
        );
        // Search with node_ids: OK
        assert_ok(
            r#"{
                "query_type": "traversal",
                "node": {"id": "u", "entity": "User", "node_ids": [1]}
            }"#,
        );
        // Single-node traversal without selectivity
        assert_rejects(
            r#"{
                "query_type": "traversal",
                "node": {"id": "u", "entity": "User"}
            }"#,
            "full edge table scans",
        );

        // ── Traversal ───────────────────────────────────────────────
        // Single-hop with pinned node: OK
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
        // Single-hop with filter on second node: OK
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
        // Single-hop without any selectivity
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
        // Multi-hop with pinned node: OK
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
        // Multi-hop without any selectivity
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

        // ── Aggregation ─────────────────────────────────────────────
        // Aggregation with filter: OK
        assert_ok(
            r#"{
                "query_type": "aggregation",
                "nodes": [
                    {"id": "u", "entity": "User", "filters": {"username": "root"}},
                    {"id": "p", "entity": "Project"}
                ],
                "relationships": [{"type": "CONTAINS", "from": "p", "to": "u"}],
                "aggregations": [{"function": "count", "target": "u", "group_by": "p", "alias": "c"}]
            }"#,
        );
        // Aggregation without any selectivity
        assert_rejects(
            r#"{
                "query_type": "aggregation",
                "nodes": [
                    {"id": "u", "entity": "User"},
                    {"id": "p", "entity": "Project"}
                ],
                "relationships": [{"type": "CONTAINS", "from": "p", "to": "u"}],
                "aggregations": [{"function": "count", "target": "u", "group_by": "p", "alias": "c"}]
            }"#,
            "full edge table scans",
        );

        // ── id_range selectivity ────────────────────────────────────
        // Narrow id_range counts as selective
        assert_ok(
            r#"{
                "query_type": "traversal",
                "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 100}}
            }"#,
        );
        // Wide id_range does NOT count as selective
        assert_rejects(
            r#"{
                "query_type": "traversal",
                "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 999999999}}
            }"#,
            "full edge table scans",
        );
        // Traversal with id_range: OK (lowerer generates CTE with range condition)
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
        // Aggregation with id_range: OK
        assert_ok(
            r#"{
                "query_type": "aggregation",
                "nodes": [
                    {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 100}},
                    {"id": "p", "entity": "Project"}
                ],
                "relationships": [{"type": "CONTAINS", "from": "p", "to": "u"}],
                "aggregations": [{"function": "count", "target": "u", "group_by": "p", "alias": "c"}]
            }"#,
        );
        // path_finding with narrow id_range: OK (within MAX_PATH_ANCHOR_RANGE)
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
        // path_finding with wide id_range: rejected (exceeds MAX_PATH_ANCHOR_RANGE)
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

    // ── LIKE security controls ──────────────────────────────────────

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
                    ("traversal_path", DataType::String),
                ],
            )
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
                .contains("LIKE pattern must be at least 3 characters"),
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
                     "filters": {"traversal_path": {"op": "starts_with", "value": "1/100"}}},
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
}
