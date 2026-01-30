//! Input validation against ontology.
//!
//! Validates semantic correctness of parsed input before lowering.
//! After validation, the input is guaranteed to be correct and lowering cannot fail.

use crate::error::{QueryError, Result};
use crate::input::{ColumnSelection, Input, InputNode, QueryType};
use ontology::Ontology;

/// Validate parsed input against the ontology.
///
/// Checks:
/// - At least one node is defined
/// - All nodes have entity types
/// - All entity types exist in the ontology
/// - All relationship node references are valid
/// - All relationship types exist in the ontology
/// - All filter properties exist on their entities
/// - Path config references valid nodes (for path_finding)
/// - Aggregation targets reference valid nodes
/// - Order by references valid nodes and properties
pub fn validate(input: &Input, ontology: &Ontology) -> Result<()> {
    validate_nodes(&input.nodes, ontology)?;
    validate_relationships(input, ontology)?;
    validate_filters(input, ontology)?;
    validate_columns(input, ontology)?;
    validate_aggregations(input, ontology)?;
    validate_order_by(input, ontology)?;
    validate_path(input, ontology)?;
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Node validation
// ─────────────────────────────────────────────────────────────────────────────

fn validate_nodes(nodes: &[InputNode], ontology: &Ontology) -> Result<()> {
    if nodes.is_empty() {
        return Err(err("at least one node is required"));
    }

    for node in nodes {
        let entity = node.entity.as_ref().ok_or_else(|| {
            err(format!(
                "node \"{}\" requires an entity type to determine which table to query",
                node.id
            ))
        })?;

        // Validate entity exists in ontology
        if !ontology.has_node(entity) {
            return Err(err(format!(
                "unknown entity type \"{}\" for node \"{}\"",
                entity, node.id
            )));
        }
    }

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Relationship validation
// ─────────────────────────────────────────────────────────────────────────────

fn validate_relationships(input: &Input, ontology: &Ontology) -> Result<()> {
    let node_ids: Vec<&str> = input.nodes.iter().map(|n| n.id.as_str()).collect();

    for (i, rel) in input.relationships.iter().enumerate() {
        // Validate "from" node reference
        if !node_ids.contains(&rel.from.as_str()) {
            return Err(err(format!(
                "relationship[{}] references undefined node \"{}\" in 'from'",
                i, rel.from
            )));
        }

        // Validate "to" node reference
        if !node_ids.contains(&rel.to.as_str()) {
            return Err(err(format!(
                "relationship[{}] references undefined node \"{}\" in 'to'",
                i, rel.to
            )));
        }

        // Validate relationship types
        for rel_type in &rel.types {
            if rel_type != "*" && !ontology.has_edge(rel_type) {
                return Err(err(format!(
                    "unknown relationship type \"{}\" in relationship[{}]",
                    rel_type, i
                )));
            }
        }

        // Validate hop constraints
        if rel.min_hops > rel.max_hops {
            return Err(err(format!(
                "relationship[{}] has min_hops ({}) > max_hops ({})",
                i, rel.min_hops, rel.max_hops
            )));
        }
    }

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Filter validation
// ─────────────────────────────────────────────────────────────────────────────

fn validate_filters(input: &Input, ontology: &Ontology) -> Result<()> {
    for node in &input.nodes {
        let entity = node.entity.as_ref().ok_or_else(|| err("missing entity"))?;
        for prop in node.filters.keys() {
            ontology
                .validate_field(entity, prop)
                .map_err(|e| err(format!("invalid filter on node \"{}\": {}", node.id, e)))?;
        }
    }

    // Edge filters don't need ontology validation (dynamic properties)
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Column validation
// ─────────────────────────────────────────────────────────────────────────────

fn validate_columns(input: &Input, ontology: &Ontology) -> Result<()> {
    for node in &input.nodes {
        let Some(columns) = &node.columns else {
            continue;
        };

        let entity = node.entity.as_ref().ok_or_else(|| err("missing entity"))?;

        match columns {
            ColumnSelection::All => {
                // Wildcard is always valid if entity exists (already validated)
            }
            ColumnSelection::List(cols) => {
                for col in cols {
                    ontology.validate_field(entity, col).map_err(|e| {
                        err(format!(
                            "invalid column \"{}\" on node \"{}\": {}",
                            col, node.id, e
                        ))
                    })?;
                }
            }
        }
    }

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Aggregation validation
// ─────────────────────────────────────────────────────────────────────────────

fn validate_aggregations(input: &Input, ontology: &Ontology) -> Result<()> {
    if input.query_type != QueryType::Aggregation {
        return Ok(());
    }

    let node_ids: Vec<&str> = input.nodes.iter().map(|n| n.id.as_str()).collect();

    for (i, agg) in input.aggregations.iter().enumerate() {
        // Validate target node reference
        if let Some(target) = &agg.target {
            if !node_ids.contains(&target.as_str()) {
                return Err(err(format!(
                    "aggregation[{}] references undefined node \"{}\" in 'target'",
                    i, target
                )));
            }
        }

        // Validate group_by node reference
        if let Some(group_by) = &agg.group_by {
            if !node_ids.contains(&group_by.as_str()) {
                return Err(err(format!(
                    "aggregation[{}] references undefined node \"{}\" in 'group_by'",
                    i, group_by
                )));
            }
        }

        // Validate property exists on target entity
        if let (Some(prop), Some(target)) = (&agg.property, &agg.target) {
            if let Some(node) = input.nodes.iter().find(|n| n.id == *target) {
                let entity = node.entity.as_ref().ok_or_else(|| err("missing entity"))?;
                ontology
                    .validate_field(entity, prop)
                    .map_err(|e| err(format!("invalid property in aggregation[{}]: {}", i, e)))?;
            }
        }
    }

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Order by validation
// ─────────────────────────────────────────────────────────────────────────────

fn validate_order_by(input: &Input, ontology: &Ontology) -> Result<()> {
    let Some(order_by) = &input.order_by else {
        return Ok(());
    };

    // Validate node reference
    let node = input
        .nodes
        .iter()
        .find(|n| n.id == order_by.node)
        .ok_or_else(|| {
            err(format!(
                "order_by references undefined node \"{}\"",
                order_by.node
            ))
        })?;

    // Validate property exists on entity
    let entity = node.entity.as_ref().ok_or_else(|| err("missing entity"))?;
    ontology
        .validate_field(entity, &order_by.property)
        .map_err(|e| err(format!("invalid order_by property: {}", e)))?;

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Path finding validation
// ─────────────────────────────────────────────────────────────────────────────

fn validate_path(input: &Input, ontology: &Ontology) -> Result<()> {
    if input.query_type != QueryType::PathFinding {
        return Ok(());
    }

    let path = input
        .path
        .as_ref()
        .ok_or_else(|| err("path_finding query requires a 'path' configuration"))?;

    let node_ids: Vec<&str> = input.nodes.iter().map(|n| n.id.as_str()).collect();

    // Validate "from" node reference
    if !node_ids.contains(&path.from.as_str()) {
        return Err(err(format!(
            "path 'from' references undefined node \"{}\"",
            path.from
        )));
    }

    // Validate "to" node reference
    if !node_ids.contains(&path.to.as_str()) {
        return Err(err(format!(
            "path 'to' references undefined node \"{}\"",
            path.to
        )));
    }

    // Validate relationship types in path
    for rel_type in &path.rel_types {
        if rel_type != "*" && !ontology.has_edge(rel_type) {
            return Err(err(format!(
                "unknown relationship type \"{}\" in path configuration",
                rel_type
            )));
        }
    }

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

fn err(msg: impl Into<String>) -> QueryError {
    QueryError::Validation(msg.into())
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
                ],
            )
            .with_fields("Note", [("confidential", DataType::Bool)])
    }

    #[test]
    fn valid_traversal() {
        let input = parse_input(
            r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User"},
                {"id": "n", "entity": "Note"}
            ],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "n"}]
        }"#,
        )
        .unwrap();

        assert!(validate(&input, &test_ontology()).is_ok());
    }

    #[test]
    fn empty_nodes_rejected() {
        let input = parse_input(
            r#"{
            "query_type": "traversal",
            "nodes": []
        }"#,
        )
        .unwrap();

        let err = validate(&input, &test_ontology()).unwrap_err();
        assert!(err.to_string().contains("at least one node"));
    }

    #[test]
    fn missing_entity_rejected() {
        let input = parse_input(
            r#"{
            "query_type": "traversal",
            "nodes": [{"id": "u"}]
        }"#,
        )
        .unwrap();

        let err = validate(&input, &test_ontology()).unwrap_err();
        assert!(err.to_string().contains("requires an entity type"));
    }

    #[test]
    fn unknown_entity_rejected() {
        let input = parse_input(
            r#"{
            "query_type": "traversal",
            "nodes": [{"id": "u", "entity": "NonExistent"}]
        }"#,
        )
        .unwrap();

        let err = validate(&input, &test_ontology()).unwrap_err();
        assert!(err.to_string().contains("unknown entity type"));
    }

    #[test]
    fn undefined_relationship_from_rejected() {
        let input = parse_input(
            r#"{
            "query_type": "traversal",
            "nodes": [{"id": "u", "entity": "User"}],
            "relationships": [{"type": "AUTHORED", "from": "undefined", "to": "u"}]
        }"#,
        )
        .unwrap();

        let err = validate(&input, &test_ontology()).unwrap_err();
        assert!(err.to_string().contains("undefined node"));
    }

    #[test]
    fn unknown_relationship_type_rejected() {
        let input = parse_input(
            r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User"},
                {"id": "n", "entity": "Note"}
            ],
            "relationships": [{"type": "INVALID_TYPE", "from": "u", "to": "n"}]
        }"#,
        )
        .unwrap();

        let err = validate(&input, &test_ontology()).unwrap_err();
        assert!(err.to_string().contains("unknown relationship type"));
    }

    #[test]
    fn invalid_filter_property_rejected() {
        let input = parse_input(
            r#"{
            "query_type": "traversal",
            "nodes": [{"id": "u", "entity": "User", "filters": {"nonexistent": "value"}}]
        }"#,
        )
        .unwrap();

        let err = validate(&input, &test_ontology()).unwrap_err();
        assert!(err.to_string().contains("invalid filter"));
    }

    #[test]
    fn valid_filter_property_accepted() {
        let input = parse_input(
            r#"{
            "query_type": "traversal",
            "nodes": [{"id": "u", "entity": "User", "filters": {"username": "admin"}}]
        }"#,
        )
        .unwrap();

        assert!(validate(&input, &test_ontology()).is_ok());
    }

    #[test]
    fn wildcard_relationship_type_accepted() {
        let input = parse_input(
            r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User"},
                {"id": "n", "entity": "Note"}
            ],
            "relationships": [{"type": "*", "from": "u", "to": "n"}]
        }"#,
        )
        .unwrap();

        assert!(validate(&input, &test_ontology()).is_ok());
    }

    #[test]
    fn invalid_hop_range_rejected() {
        let input = parse_input(
            r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User"},
                {"id": "n", "entity": "Note"}
            ],
            "relationships": [{
                "type": "AUTHORED",
                "from": "u",
                "to": "n",
                "min_hops": 5,
                "max_hops": 2
            }]
        }"#,
        )
        .unwrap();

        let err = validate(&input, &test_ontology()).unwrap_err();
        assert!(err.to_string().contains("min_hops"));
        assert!(err.to_string().contains("max_hops"));
    }

    #[test]
    fn valid_hop_range_accepted() {
        let input = parse_input(
            r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User"},
                {"id": "n", "entity": "Note"}
            ],
            "relationships": [{
                "type": "AUTHORED",
                "from": "u",
                "to": "n",
                "min_hops": 1,
                "max_hops": 3
            }]
        }"#,
        )
        .unwrap();

        assert!(validate(&input, &test_ontology()).is_ok());
    }

    #[test]
    fn valid_columns_accepted() {
        let input = parse_input(
            r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username", "created_at"]}
        }"#,
        )
        .unwrap();

        assert!(validate(&input, &test_ontology()).is_ok());
    }

    #[test]
    fn wildcard_columns_accepted() {
        let input = parse_input(
            r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": "*"}
        }"#,
        )
        .unwrap();

        assert!(validate(&input, &test_ontology()).is_ok());
    }

    #[test]
    fn invalid_column_rejected() {
        let input = parse_input(
            r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username", "nonexistent_column"]}
        }"#,
        )
        .unwrap();

        let err = validate(&input, &test_ontology()).unwrap_err();
        assert!(err.to_string().contains("invalid column"));
        assert!(err.to_string().contains("nonexistent_column"));
    }

    #[test]
    fn id_column_always_valid() {
        let input = parse_input(
            r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["id", "username"]}
        }"#,
        )
        .unwrap();

        assert!(validate(&input, &test_ontology()).is_ok());
    }
}
