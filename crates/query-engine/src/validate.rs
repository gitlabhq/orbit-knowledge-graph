//! Query validation.
//!
//! Two-phase validation via [`Validator`]:
//! 1. **Schema validation** — structural correctness via JSON Schema (base + ontology-derived).
//!    Entity types, columns, filters, relationship types, and hop ranges are all enforced here.
//! 2. **Cross-reference validation** — node ID references that JSON Schema cannot express
//!    (e.g. relationship from/to must reference a declared node ID).

use std::sync::OnceLock;

use crate::error::{QueryError, Result};
use crate::input::{FilterOp, Input, InputFilter, QueryType};
use ontology::{DataType, Ontology};

pub(crate) const BASE_SCHEMA_JSON: &str = include_str!("../../ontology/schema.json");

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
        .map(|e| format!("{} at {}", e, e.instance_path()))
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
        self.check_filter_types(input)?;
        // Run after individual reference checks so "undefined node X" errors
        // take priority over "node Y is unreferenced".
        self.check_unreferenced_nodes(input)?;
        Ok(())
    }

    /// Defense-in-depth: reject queries that exceed hard caps on complexity.
    /// The JSON schema already enforces these limits via maxItems / maximum,
    /// so this only fires if schema validation was somehow bypassed.
    pub fn check_depth(&self, input: &Input) -> Result<()> {
        const MAX_HOPS_CAP: u32 = 3;
        const MAX_DEPTH_CAP: u32 = 3;
        const MAX_NODES_CAP: usize = 5;
        const MAX_RELS_CAP: usize = 5;
        const MAX_NODE_IDS: usize = 500;
        const MAX_IN_VALUES: usize = 100;

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
        for rel in &input.relationships {
            if rel.max_hops > MAX_HOPS_CAP {
                return Err(QueryError::DepthExceeded(format!(
                    "max_hops ({}) must not exceed {MAX_HOPS_CAP}",
                    rel.max_hops
                )));
            }
        }
        if let Some(ref path) = input.path
            && path.max_depth > MAX_DEPTH_CAP
        {
            return Err(QueryError::DepthExceeded(format!(
                "max_depth ({}) must not exceed {MAX_DEPTH_CAP}",
                path.max_depth
            )));
        }
        Ok(())
    }

    fn check_pagination(&self, input: &Input) -> Result<()> {
        if let Some(ref range) = input.range {
            if range.end <= range.start {
                return Err(QueryError::PaginationError(format!(
                    "range.end ({}) must be greater than range.start ({})",
                    range.end, range.start
                )));
            }
            let window = range.end - range.start;
            if window > 1000 {
                return Err(QueryError::PaginationError(format!(
                    "range window size ({window}) must not exceed 1000"
                )));
            }
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

        for (i, agg) in input.aggregations.iter().enumerate() {
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
            // Single-node query types: the one declared node is the query.
            QueryType::Search | QueryType::Neighbors => return Ok(()),
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
                    {"id": "u", "entity": "User"},
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
                    {"id": "u", "entity": "User", "columns": ["username"]},
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
                "query_type": "search",
                "node": {"id": "u", "entity": "User", "columns": ["username"]},
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
                "path": {"type": "shortest", "from": "a", "to": "b", "max_depth": 2}
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
                "nodes": [{"id": "u", "entity": "User"}],
                "relationships": [{"type": "AUTHORED", "from": "ghost", "to": "u"}]
            }"#,
            "undefined node \"ghost\"",
        );

        assert_rejects(
            r#"{
                "query_type": "traversal",
                "nodes": [{"id": "u", "entity": "User"}],
                "relationships": [{"type": "AUTHORED", "from": "u", "to": "ghost"}]
            }"#,
            "undefined node \"ghost\"",
        );

        // ── Aggregation references ──────────────────────────────────

        assert_rejects(
            r#"{
                "query_type": "aggregation",
                "nodes": [{"id": "u", "entity": "User"}],
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
                "nodes": [{"id": "u", "entity": "User"}],
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
                "nodes": [{"id": "u", "entity": "User"}],
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
                "query_type": "search",
                "node": {"id": "u", "entity": "User"},
                "order_by": {"node": "missing", "property": "username", "direction": "ASC"}
            }"#,
            "undefined node \"missing\"",
        );

        assert_rejects(
            r#"{
                "query_type": "search",
                "node": {"id": "u", "entity": "User"},
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
                "path": {"type": "shortest", "from": "ghost", "to": "b", "max_depth": 2}
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
                "path": {"type": "shortest", "from": "a", "to": "ghost", "max_depth": 2}
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
                    {"id": "u", "entity": "User"},
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
                    {"id": "u", "entity": "User"},
                    {"id": "p", "entity": "Project"}
                ],
                "relationships": [{"type": "CONTAINS", "from": "p", "to": "u"}]
            }"#,
        );
    }

    #[test]
    fn rejects_unreferenced_traversal_node() {
        assert_rejects(
            r#"{
                "query_type": "traversal",
                "nodes": [
                    {"id": "u", "entity": "User"},
                    {"id": "p", "entity": "Project"},
                    {"id": "orphan", "entity": "Note"}
                ],
                "relationships": [{"type": "CONTAINS", "from": "p", "to": "u"}]
            }"#,
            "node \"orphan\" is not referenced",
        );
    }

    #[test]
    fn accepts_aggregation_node_referenced_only_by_target() {
        assert_ok(
            r#"{
                "query_type": "aggregation",
                "nodes": [{"id": "p", "entity": "Project"}],
                "aggregations": [{"function": "count", "target": "p", "alias": "total"}]
            }"#,
        );
    }
}
