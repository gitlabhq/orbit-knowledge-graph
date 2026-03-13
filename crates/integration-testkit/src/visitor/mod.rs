//! Typed helpers for walking formatted query pipeline responses.
//!
//! Built on top of [`GraphResponse`], [`GraphNode`], and [`GraphEdge`] —
//! the same types produced by [`GraphFormatter`] and validated by
//! `query_response.json`.
//!
//! # Usage
//!
//! ```ignore
//! use integration_testkit::visitor::{ResponseView, NodeExt};
//!
//! // With assertion enforcement — parses the query and requires matching assertions:
//! let resp = ResponseView::for_query(query_json, pipeline_response);
//! resp.assert_node_order("User", &[1, 2, 3]); // required because query has order_by
//! // Drop panics if required assertions were not called.
//!
//! // Without enforcement — for unit tests or exploratory checks:
//! let resp = ResponseView::new(pipeline_response);
//! ```

mod enforcement;

use enforcement::AssertionTracker;
pub use enforcement::{QueryRequirements, Requirement};

use std::collections::HashSet;

use gkg_server::query_pipeline::{GraphEdge, GraphNode, GraphResponse};
use query_engine::input::{Input, QueryType};
use serde_json::Value;

// ─────────────────────────────────────────────────────────────────────────────
// MustInspect — drop-enforced result wrapper
// ─────────────────────────────────────────────────────────────────────────────

/// Wrapper that panics on drop if the inner value was never inspected.
///
/// Returned by [`ResponseView`] methods that satisfy enforcement requirements
/// (`node_ids`, `edges_of_type`, `path_ids`). Transparent in normal use —
/// implements [`Deref`], [`PartialEq`], and [`Debug`] so callers can compare,
/// iterate, or call methods without ceremony. Panics on drop only if the
/// value was never accessed at all (the "satisfy and discard" pattern).
///
/// Use [`into_inner`](Self::into_inner) to take ownership when needed
/// (e.g. in enforcement tests that satisfy the tracker without data checks).
pub struct MustInspect<T> {
    value: Option<T>,
    accessed: std::cell::Cell<bool>,
    context: &'static str,
}

impl<T> MustInspect<T> {
    fn new(value: T, context: &'static str) -> Self {
        Self {
            value: Some(value),
            accessed: std::cell::Cell::new(false),
            context,
        }
    }

    /// Extract the inner value, consuming the wrapper.
    pub fn into_inner(mut self) -> T {
        self.accessed.set(true);
        self.value.take().unwrap()
    }
}

impl<T> std::ops::Deref for MustInspect<T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.accessed.set(true);
        self.value.as_ref().unwrap()
    }
}

impl<T: PartialEq> PartialEq<T> for MustInspect<T> {
    fn eq(&self, other: &T) -> bool {
        self.accessed.set(true);
        self.value.as_ref().unwrap() == other
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for MustInspect<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.value.as_ref().unwrap().fmt(f)
    }
}

impl<T> Drop for MustInspect<T> {
    fn drop(&mut self) {
        if !self.accessed.get() && !std::thread::panicking() {
            panic!(
                "{}: return value was discarded without inspection",
                self.context
            );
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ResponseView
// ─────────────────────────────────────────────────────────────────────────────

/// Typed view over a formatted query pipeline response.
///
/// Wraps [`GraphResponse`] and provides ergonomic lookup, iteration, and
/// assertion helpers for integration tests.
///
/// When created via [`for_query`](Self::for_query), the query JSON is parsed
/// to derive assertion requirements. If the test drops the view without
/// satisfying all requirements, the drop impl panics with a list of what
/// was missed.
pub struct ResponseView {
    pub response: GraphResponse,
    tracker: AssertionTracker,
}

impl ResponseView {
    /// Create a view without assertion enforcement.
    ///
    /// Only available in `integration-testkit`'s own unit tests.
    /// External crates must use [`for_query`](Self::for_query).
    #[cfg(test)]
    pub(crate) fn new(response: GraphResponse) -> Self {
        Self {
            response,
            tracker: AssertionTracker::empty(),
        }
    }

    /// Create a view with assertion enforcement derived from the compiled [`Input`].
    ///
    /// Validates structural invariants on construction:
    /// - `response.query_type` matches the input
    /// - Search and aggregation responses have zero edges (the formatter never
    ///   produces edges for these query types)
    pub fn for_query(input: &Input, response: GraphResponse) -> Self {
        let response_type: QueryType = serde_json::from_value(Value::String(
            response.query_type.clone(),
        ))
        .unwrap_or_else(|_| panic!("unknown response query_type '{}'", response.query_type));
        assert_eq!(
            response_type, input.query_type,
            "response query_type '{}' does not match input '{}'",
            response.query_type, input.query_type,
        );

        if matches!(input.query_type, QueryType::Search | QueryType::Aggregation) {
            assert!(
                response.edges.is_empty(),
                "{} response must have zero edges, got {}",
                input.query_type,
                response.edges.len(),
            );
        }

        Self {
            response,
            tracker: AssertionTracker::new(input.requirements()),
        }
    }

    /// Explicitly skip a requirement that doesn't apply to this test case.
    pub fn skip_requirement(&self, req: Requirement) {
        self.tracker.skip(req);
    }

    pub fn query_type(&self) -> &str {
        &self.response.query_type
    }

    pub fn nodes(&self) -> &[GraphNode] {
        &self.response.nodes
    }

    pub fn edges(&self) -> &[GraphEdge] {
        &self.response.edges
    }

    pub fn node_count(&self) -> usize {
        self.response.nodes.len()
    }

    /// Assert exact node count. Satisfies [`Requirement::Range`].
    ///
    /// Does NOT satisfy [`Requirement::NodeIds`] — use [`node_ids`](Self::node_ids)
    /// or [`assert_node_order`](Self::assert_node_order) to verify which IDs were returned.
    pub fn assert_node_count(&self, expected: usize) {
        self.tracker.satisfy(Requirement::Range);
        assert_eq!(
            self.response.nodes.len(),
            expected,
            "expected {expected} nodes, got {}",
            self.response.nodes.len()
        );
    }

    pub fn edge_count(&self) -> usize {
        self.response.edges.len()
    }

    // ── Node lookups ─────────────────────────────────────────────────

    pub fn find_node(&self, entity_type: &str, id: i64) -> Option<&GraphNode> {
        self.response
            .nodes
            .iter()
            .find(|n| n.entity_type == entity_type && n.id == id)
    }

    pub fn nodes_of_type(&self, entity_type: &str) -> Vec<&GraphNode> {
        self.response
            .nodes
            .iter()
            .filter(|n| n.entity_type == entity_type)
            .collect()
    }

    /// Satisfies [`Requirement::NodeIds`].
    pub fn node_ids(&self, entity_type: &str) -> MustInspect<HashSet<i64>> {
        self.tracker.satisfy(Requirement::NodeIds);
        let ids = self
            .response
            .nodes
            .iter()
            .filter(|n| n.entity_type == entity_type)
            .map(|n| n.id)
            .collect();
        MustInspect::new(ids, "node_ids()")
    }

    /// Return IDs of nodes with the given type, preserving response order.
    pub fn node_ids_ordered(&self, entity_type: &str) -> Vec<i64> {
        self.response
            .nodes
            .iter()
            .filter(|n| n.entity_type == entity_type)
            .map(|n| n.id)
            .collect()
    }

    pub fn all_node_ids(&self) -> HashSet<(String, i64)> {
        self.response
            .nodes
            .iter()
            .map(|n| (n.entity_type.clone(), n.id))
            .collect()
    }

    // ── Edge lookups ─────────────────────────────────────────────────

    pub fn find_edge(
        &self,
        from: &str,
        from_id: i64,
        to: &str,
        to_id: i64,
        edge_type: &str,
    ) -> Option<&GraphEdge> {
        self.response.edges.iter().find(|e| {
            e.from == from
                && e.from_id == from_id
                && e.to == to
                && e.to_id == to_id
                && e.edge_type == edge_type
        })
    }

    pub fn edges_from(&self, entity_type: &str, id: i64) -> Vec<&GraphEdge> {
        self.response
            .edges
            .iter()
            .filter(|e| e.from == entity_type && e.from_id == id)
            .collect()
    }

    pub fn edges_to(&self, entity_type: &str, id: i64) -> Vec<&GraphEdge> {
        self.response
            .edges
            .iter()
            .filter(|e| e.to == entity_type && e.to_id == id)
            .collect()
    }

    /// Satisfies [`Requirement::Relationship`] for the given edge type, and [`Requirement::Neighbors`].
    pub fn edges_of_type(&self, edge_type: &str) -> MustInspect<Vec<&GraphEdge>> {
        self.tracker.satisfy(Requirement::Relationship {
            edge_type: edge_type.to_string(),
        });
        self.tracker.satisfy(Requirement::Neighbors);
        let edges = self
            .response
            .edges
            .iter()
            .filter(|e| e.edge_type == edge_type)
            .collect();
        MustInspect::new(edges, "edges_of_type()")
    }

    pub fn edge_tuples(&self) -> HashSet<(String, i64, String, i64, String)> {
        self.response
            .edges
            .iter()
            .map(|e| {
                (
                    e.from.clone(),
                    e.from_id,
                    e.to.clone(),
                    e.to_id,
                    e.edge_type.clone(),
                )
            })
            .collect()
    }

    /// Return the set of distinct path_ids present in edges.
    ///
    /// Tests should use this to discover which paths exist, then call
    /// [`path`] for each one explicitly.
    /// Satisfies [`Requirement::PathFinding`].
    pub fn path_ids(&self) -> MustInspect<HashSet<usize>> {
        self.tracker.satisfy(Requirement::PathFinding);
        let ids = self
            .response
            .edges
            .iter()
            .filter_map(|e| e.path_id)
            .collect();
        MustInspect::new(ids, "path_ids()")
    }

    /// Return edges belonging to a specific `path_id`, sorted by `step`.
    ///
    /// Returns an empty vec if no edges have this path_id. Tests should
    /// call [`path_ids`] first and iterate explicitly rather than relying
    /// on auto-grouping.
    pub fn path(&self, path_id: usize) -> Vec<&GraphEdge> {
        let mut edges: Vec<&GraphEdge> = self
            .response
            .edges
            .iter()
            .filter(|e| e.path_id == Some(path_id))
            .collect();
        edges.sort_by_key(|e| e.step.unwrap_or(0));
        edges
    }

    // ── Assertions ───────────────────────────────────────────────────

    pub fn assert_node_exists(&self, entity_type: &str, id: i64) {
        assert!(
            self.find_node(entity_type, id).is_some(),
            "expected node {entity_type}:{id} in response, found: {:?}",
            self.all_node_ids()
        );
    }

    pub fn assert_node_absent(&self, entity_type: &str, id: i64) {
        assert!(
            self.find_node(entity_type, id).is_none(),
            "node {entity_type}:{id} should not be in response"
        );
    }

    /// Assert a node exists and satisfies a predicate.
    ///
    /// Panics if the predicate also passes for a blank node (same type/id,
    /// empty properties) — this catches trivial predicates like `|_| true`
    /// that don't actually inspect the data.
    ///
    /// Satisfies [`Requirement::Aggregation`] (property value was checked).
    pub fn assert_node(&self, entity_type: &str, id: i64, predicate: impl Fn(&GraphNode) -> bool) {
        self.tracker.satisfy(Requirement::Aggregation);
        assert_predicate_is_nontrivial(entity_type, id, &predicate);
        let node = self
            .find_node(entity_type, id)
            .unwrap_or_else(|| panic!("node {entity_type}:{id} not found"));
        assert!(
            predicate(node),
            "node {entity_type}:{id} did not satisfy predicate. Node: {node:?}",
        );
    }

    /// Satisfies [`Requirement::Relationship`] for the given edge type, and [`Requirement::Neighbors`].
    pub fn assert_edge_exists(
        &self,
        from: &str,
        from_id: i64,
        to: &str,
        to_id: i64,
        edge_type: &str,
    ) {
        self.tracker.satisfy(Requirement::Relationship {
            edge_type: edge_type.to_string(),
        });
        self.tracker.satisfy(Requirement::Neighbors);
        assert!(
            self.find_edge(from, from_id, to, to_id, edge_type)
                .is_some(),
            "expected edge {from}:{from_id} --{edge_type}--> {to}:{to_id}, found: {:?}",
            self.edge_tuples()
        );
    }

    /// Assert that a specific edge does NOT exist.
    ///
    /// Does NOT satisfy [`Requirement::Relationship`] or [`Requirement::Neighbors`] —
    /// a negative assertion proves nothing about what edges exist. Use
    /// [`assert_edge_exists`](Self::assert_edge_exists) or
    /// [`edges_of_type`](Self::edges_of_type) for positive verification.
    pub fn assert_edge_absent(
        &self,
        from: &str,
        from_id: i64,
        to: &str,
        to_id: i64,
        edge_type: &str,
    ) {
        assert!(
            self.find_edge(from, from_id, to, to_id, edge_type)
                .is_none(),
            "edge {from}:{from_id} --{edge_type}--> {to}:{to_id} should not exist"
        );
    }

    /// Assert that all nodes satisfy a predicate (structural check, no enforcement).
    ///
    /// For filter enforcement use [`assert_filter`](Self::assert_filter) instead.
    pub fn assert_all_nodes(&self, predicate: impl Fn(&GraphNode) -> bool, msg: &str) {
        for (i, node) in self.response.nodes.iter().enumerate() {
            assert!(
                predicate(node),
                "node {i} ({}:{}) failed: {msg}",
                node.entity_type,
                node.id,
            );
        }
    }

    /// Assert that a filter on `field` produced correct results for nodes of
    /// `entity_type`. Checks that every node of the given type satisfies the predicate.
    ///
    /// Panics if:
    /// - Zero nodes match `entity_type` (use [`assert_node_count`](Self::assert_node_count)
    ///   to assert empty results instead — `assert_filter` requires at least one node
    ///   because there is nothing to run the predicate against).
    /// - The predicate passes for a blank node with no properties
    ///   (catches trivial predicates like `|_| true`).
    ///
    /// Satisfies [`Requirement::Filter`] for the specific `field`.
    pub fn assert_filter(
        &self,
        entity_type: &str,
        field: &str,
        predicate: impl Fn(&GraphNode) -> bool,
    ) {
        self.tracker.satisfy(Requirement::Filter {
            field: field.to_string(),
        });
        assert_predicate_is_nontrivial(entity_type, 0, &predicate);
        let matching: Vec<&GraphNode> = self
            .response
            .nodes
            .iter()
            .filter(|n| n.entity_type == entity_type)
            .collect();
        assert!(
            !matching.is_empty(),
            "assert_filter('{entity_type}', '{field}'): zero nodes of type '{entity_type}' \
             in response — use assert_node_count(0) to assert empty results",
        );
        for node in matching {
            assert!(
                predicate(node),
                "{}:{} failed filter assertion on '{field}'",
                node.entity_type,
                node.id,
            );
        }
    }

    /// Assert that the IDs of nodes with `entity_type` match `expected` exactly (unordered).
    /// Satisfies [`Requirement::NodeIds`].
    pub fn assert_node_ids(&self, entity_type: &str, expected: &[i64]) {
        self.tracker.satisfy(Requirement::NodeIds);
        let actual: HashSet<i64> = self
            .response
            .nodes
            .iter()
            .filter(|n| n.entity_type == entity_type)
            .map(|n| n.id)
            .collect();
        let expected_set: HashSet<i64> = expected.iter().copied().collect();
        assert_eq!(actual, expected_set, "{entity_type} node IDs mismatch");
    }

    /// Assert the exact set of `(from_id, to_id)` pairs for edges of `edge_type`.
    /// Satisfies [`Requirement::Relationship`] and [`Requirement::Neighbors`].
    pub fn assert_edge_set(&self, edge_type: &str, expected: &[(i64, i64)]) {
        self.tracker.satisfy(Requirement::Relationship {
            edge_type: edge_type.to_string(),
        });
        self.tracker.satisfy(Requirement::Neighbors);
        let actual: HashSet<(i64, i64)> = self
            .response
            .edges
            .iter()
            .filter(|e| e.edge_type == edge_type)
            .map(|e| (e.from_id, e.to_id))
            .collect();
        let expected_set: HashSet<(i64, i64)> = expected.iter().copied().collect();
        assert_eq!(actual, expected_set, "{edge_type} edge set mismatch");
    }

    /// Assert the number of edges with `edge_type`.
    /// Satisfies [`Requirement::Relationship`] and [`Requirement::Neighbors`].
    pub fn assert_edge_count(&self, edge_type: &str, expected: usize) {
        self.tracker.satisfy(Requirement::Relationship {
            edge_type: edge_type.to_string(),
        });
        self.tracker.satisfy(Requirement::Neighbors);
        let actual = self
            .response
            .edges
            .iter()
            .filter(|e| e.edge_type == edge_type)
            .count();
        assert_eq!(
            actual, expected,
            "expected {expected} {edge_type} edges, got {actual}"
        );
    }

    /// Assert that nodes of the given type appear in exactly this ID order.
    /// Satisfies [`Requirement::OrderBy`], [`Requirement::NodeIds`], and
    /// [`Requirement::AggregationSort`].
    pub fn assert_node_order(&self, entity_type: &str, expected_ids: &[i64]) {
        self.tracker.satisfy(Requirement::OrderBy);
        self.tracker.satisfy(Requirement::NodeIds);
        self.tracker.satisfy(Requirement::AggregationSort);
        let actual = self.node_ids_ordered(entity_type);
        assert_eq!(actual, expected_ids, "{entity_type} nodes in wrong order");
    }

    /// Assert that every node referenced by an edge exists in the nodes array.
    pub fn assert_referential_integrity(&self) {
        let all = self.all_node_ids();
        for edge in &self.response.edges {
            let from = (edge.from.clone(), edge.from_id);
            let to = (edge.to.clone(), edge.to_id);
            assert!(
                all.contains(&from),
                "edge references non-existent source node {from:?}"
            );
            assert!(
                all.contains(&to),
                "edge references non-existent target node {to:?}"
            );
        }
    }
}

/// Panic if the predicate returns `true` for a blank node (same type/id, no
/// properties). Catches trivial predicates like `|_| true` or `|n| n.has_prop("x")`
/// that don't actually verify a value.
fn assert_predicate_is_nontrivial(
    entity_type: &str,
    id: i64,
    predicate: &impl Fn(&GraphNode) -> bool,
) {
    let blank = GraphNode {
        entity_type: entity_type.to_string(),
        id,
        properties: serde_json::Map::new(),
    };
    assert!(
        !predicate(&blank),
        "trivial predicate: passes for a blank {entity_type} node with no properties. \
         Check actual property values instead of using |_| true or has_prop().",
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// NodeExt — typed property access for GraphNode
// ─────────────────────────────────────────────────────────────────────────────

/// Extension trait providing property access and assertion helpers on
/// [`GraphNode`]. Properties live in the flattened `properties` map.
pub trait NodeExt {
    fn prop(&self, key: &str) -> Option<&Value>;
    fn prop_str(&self, key: &str) -> Option<&str>;
    fn prop_i64(&self, key: &str) -> Option<i64>;
    fn prop_f64(&self, key: &str) -> Option<f64>;
    fn prop_bool(&self, key: &str) -> Option<bool>;
    fn has_prop(&self, key: &str) -> bool;
    fn assert_prop(&self, key: &str, expected: &Value);
    fn assert_str(&self, key: &str, expected: &str);
    fn assert_i64(&self, key: &str, expected: i64);
}

impl NodeExt for GraphNode {
    fn prop(&self, key: &str) -> Option<&Value> {
        self.properties.get(key).filter(|v| !v.is_null())
    }

    fn prop_str(&self, key: &str) -> Option<&str> {
        self.properties.get(key)?.as_str()
    }

    fn prop_i64(&self, key: &str) -> Option<i64> {
        self.properties.get(key)?.as_i64()
    }

    fn prop_f64(&self, key: &str) -> Option<f64> {
        self.properties.get(key)?.as_f64()
    }

    fn prop_bool(&self, key: &str) -> Option<bool> {
        self.properties.get(key)?.as_bool()
    }

    fn has_prop(&self, key: &str) -> bool {
        self.properties.get(key).is_some_and(|v| !v.is_null())
    }

    fn assert_prop(&self, key: &str, expected: &Value) {
        let actual = self.properties.get(key);
        assert_eq!(
            actual,
            Some(expected),
            "{}:{} property '{key}' mismatch",
            self.entity_type,
            self.id
        );
    }

    fn assert_str(&self, key: &str, expected: &str) {
        assert_eq!(
            self.prop_str(key),
            Some(expected),
            "{}:{} property '{key}' expected \"{expected}\"",
            self.entity_type,
            self.id
        );
    }

    fn assert_i64(&self, key: &str, expected: i64) {
        assert_eq!(
            self.prop_i64(key),
            Some(expected),
            "{}:{} property '{key}' expected {expected}",
            self.entity_type,
            self.id
        );
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use serde_json::json;

    pub(crate) fn make_node(entity_type: &str, id: i64, props: &[(&str, Value)]) -> GraphNode {
        let mut properties = serde_json::Map::new();
        for (k, v) in props {
            properties.insert(k.to_string(), v.clone());
        }
        GraphNode {
            entity_type: entity_type.to_string(),
            id,
            properties,
        }
    }

    pub(crate) fn make_edge(
        from: &str,
        from_id: i64,
        to: &str,
        to_id: i64,
        edge_type: &str,
    ) -> GraphEdge {
        GraphEdge {
            from: from.to_string(),
            from_id,
            to: to.to_string(),
            to_id,
            edge_type: edge_type.to_string(),
            depth: None,
            path_id: None,
            step: None,
        }
    }

    pub(crate) fn make_path_edge(
        from: &str,
        from_id: i64,
        to: &str,
        to_id: i64,
        edge_type: &str,
        path_id: usize,
        step: usize,
    ) -> GraphEdge {
        GraphEdge {
            path_id: Some(path_id),
            step: Some(step),
            ..make_edge(from, from_id, to, to_id, edge_type)
        }
    }

    pub(crate) fn sample_response() -> GraphResponse {
        GraphResponse {
            query_type: "traversal".to_string(),
            nodes: vec![
                make_node("User", 1, &[("username", json!("alice"))]),
                make_node("User", 2, &[("username", json!("bob"))]),
                make_node("Group", 100, &[("name", json!("Public"))]),
                make_node("Group", 101, &[("name", json!("Private"))]),
            ],
            edges: vec![
                make_edge("User", 1, "Group", 100, "MEMBER_OF"),
                make_edge("User", 1, "Group", 101, "MEMBER_OF"),
                make_edge("User", 2, "Group", 100, "MEMBER_OF"),
            ],
        }
    }

    pub(crate) fn sample_search_response() -> GraphResponse {
        GraphResponse {
            query_type: "search".to_string(),
            nodes: vec![
                make_node("User", 1, &[("username", json!("alice"))]),
                make_node("User", 2, &[("username", json!("bob"))]),
            ],
            edges: vec![],
        }
    }

    pub(crate) fn sample_aggregation_response() -> GraphResponse {
        GraphResponse {
            query_type: "aggregation".to_string(),
            nodes: vec![
                make_node("User", 1, &[("username", json!("alice"))]),
                make_node("User", 2, &[("username", json!("bob"))]),
            ],
            edges: vec![],
        }
    }

    pub(crate) fn sample_neighbors_response() -> GraphResponse {
        GraphResponse {
            query_type: "neighbors".to_string(),
            nodes: vec![
                make_node("User", 1, &[("username", json!("alice"))]),
                make_node("Group", 100, &[("name", json!("Public"))]),
                make_node("Group", 101, &[("name", json!("Private"))]),
            ],
            edges: vec![
                make_edge("User", 1, "Group", 100, "MEMBER_OF"),
                make_edge("User", 1, "Group", 101, "MEMBER_OF"),
            ],
        }
    }

    // ── ResponseView: basic accessors ────────────────────────────────

    #[test]
    fn query_type_returns_response_type() {
        let view = ResponseView::new(sample_response());
        assert_eq!(view.query_type(), "traversal");
    }

    #[test]
    fn node_and_edge_counts() {
        let view = ResponseView::new(sample_response());
        assert_eq!(view.node_count(), 4);
        assert_eq!(view.edge_count(), 3);
    }

    // ── Node lookups ─────────────────────────────────────────────────

    #[test]
    fn find_node_returns_matching_node() {
        let view = ResponseView::new(sample_response());
        let alice = view.find_node("User", 1).unwrap();
        assert_eq!(alice.entity_type, "User");
        assert_eq!(alice.id, 1);
    }

    #[test]
    fn find_node_returns_none_for_missing() {
        let view = ResponseView::new(sample_response());
        assert!(view.find_node("User", 999).is_none());
        assert!(view.find_node("Project", 1).is_none());
    }

    #[test]
    fn nodes_of_type_filters_correctly() {
        let view = ResponseView::new(sample_response());
        let users = view.nodes_of_type("User");
        assert_eq!(users.len(), 2);
        assert!(users.iter().all(|n| n.entity_type == "User"));
    }

    #[test]
    fn node_ids_returns_correct_set() {
        let view = ResponseView::new(sample_response());
        assert_eq!(view.node_ids("User"), HashSet::from([1, 2]));
        assert_eq!(view.node_ids("Group"), HashSet::from([100, 101]));
        assert_eq!(view.node_ids("Project"), HashSet::new());
    }

    #[test]
    fn node_ids_ordered_preserves_response_order() {
        let view = ResponseView::new(sample_response());
        assert_eq!(view.node_ids_ordered("User"), vec![1, 2]);
        assert_eq!(view.node_ids_ordered("Group"), vec![100, 101]);
        assert_eq!(view.node_ids_ordered("Project"), Vec::<i64>::new());
    }

    #[test]
    fn all_node_ids_returns_type_id_pairs() {
        let view = ResponseView::new(sample_response());
        let all = view.all_node_ids();
        assert_eq!(all.len(), 4);
        assert!(all.contains(&("User".to_string(), 1)));
        assert!(all.contains(&("Group".to_string(), 101)));
    }

    // ── Edge lookups ─────────────────────────────────────────────────

    #[test]
    fn find_edge_returns_matching_edge() {
        let view = ResponseView::new(sample_response());
        let e = view
            .find_edge("User", 1, "Group", 100, "MEMBER_OF")
            .unwrap();
        assert_eq!(e.from_id, 1);
        assert_eq!(e.to_id, 100);
    }

    #[test]
    fn find_edge_returns_none_for_wrong_type() {
        let view = ResponseView::new(sample_response());
        assert!(
            view.find_edge("User", 1, "Group", 100, "CONTAINS")
                .is_none()
        );
    }

    #[test]
    fn edges_from_filters_by_source() {
        let view = ResponseView::new(sample_response());
        let from_alice = view.edges_from("User", 1);
        assert_eq!(from_alice.len(), 2);
        let from_bob = view.edges_from("User", 2);
        assert_eq!(from_bob.len(), 1);
    }

    #[test]
    fn edges_to_filters_by_target() {
        let view = ResponseView::new(sample_response());
        let to_100 = view.edges_to("Group", 100);
        assert_eq!(to_100.len(), 2);
        let to_101 = view.edges_to("Group", 101);
        assert_eq!(to_101.len(), 1);
    }

    #[test]
    fn edges_of_type_filters_correctly() {
        let view = ResponseView::new(sample_response());
        assert_eq!(view.edges_of_type("MEMBER_OF").len(), 3);
        assert_eq!(view.edges_of_type("CONTAINS").len(), 0);
    }

    #[test]
    fn edge_tuples_returns_all_edges_as_tuples() {
        let view = ResponseView::new(sample_response());
        let tuples = view.edge_tuples();
        assert_eq!(tuples.len(), 3);
        assert!(tuples.contains(&(
            "User".to_string(),
            2,
            "Group".to_string(),
            100,
            "MEMBER_OF".to_string()
        )));
    }

    // ── Path helpers ─────────────────────────────────────────────────
    #[test]
    fn path_ids_returns_empty_when_no_path_edges() {
        let view = ResponseView::new(sample_response());
        assert!(view.path_ids().is_empty());
    }

    #[test]
    fn path_ids_returns_distinct_ids() {
        let resp = GraphResponse {
            query_type: "path_finding".to_string(),
            nodes: vec![
                make_node("User", 1, &[]),
                make_node("Group", 100, &[]),
                make_node("Project", 1000, &[]),
            ],
            edges: vec![
                make_path_edge("User", 1, "Group", 100, "MEMBER_OF", 0, 0),
                make_path_edge("Group", 100, "Project", 1000, "CONTAINS", 0, 1),
                make_path_edge("User", 1, "Group", 100, "MEMBER_OF", 2, 0),
                make_path_edge("Group", 100, "Project", 1000, "CONTAINS", 2, 1),
            ],
        };
        let view = ResponseView::new(resp);
        assert_eq!(view.path_ids(), HashSet::from([0, 2]));
    }

    #[test]
    fn path_returns_edges_sorted_by_step() {
        let resp = GraphResponse {
            query_type: "path_finding".to_string(),
            nodes: vec![
                make_node("User", 1, &[]),
                make_node("Group", 100, &[]),
                make_node("Project", 1000, &[]),
            ],
            edges: vec![
                make_path_edge("Group", 100, "Project", 1000, "CONTAINS", 0, 1),
                make_path_edge("User", 1, "Group", 100, "MEMBER_OF", 0, 0),
            ],
        };
        let view = ResponseView::new(resp);
        let path = view.path(0);
        assert_eq!(path.len(), 2);
        assert_eq!(path[0].step, Some(0));
        assert_eq!(path[0].from, "User");
        assert_eq!(path[1].step, Some(1));
        assert_eq!(path[1].from, "Group");
    }

    #[test]
    fn path_returns_empty_for_missing_id() {
        let view = ResponseView::new(sample_response());
        assert!(view.path(99).is_empty());
    }

    // ── Assertions ───────────────────────────────────────────────────

    #[test]
    fn assert_node_exists_passes_for_present_node() {
        let view = ResponseView::new(sample_response());
        view.assert_node_exists("User", 1);
    }

    #[test]
    #[should_panic(expected = "expected node User:999")]
    fn assert_node_exists_panics_for_missing_node() {
        let view = ResponseView::new(sample_response());
        view.assert_node_exists("User", 999);
    }

    #[test]
    fn assert_node_absent_passes_for_missing_node() {
        let view = ResponseView::new(sample_response());
        view.assert_node_absent("User", 999);
    }

    #[test]
    #[should_panic(expected = "should not be in response")]
    fn assert_node_absent_panics_for_present_node() {
        let view = ResponseView::new(sample_response());
        view.assert_node_absent("User", 1);
    }

    #[test]
    fn assert_node_passes_when_predicate_true() {
        let view = ResponseView::new(sample_response());
        view.assert_node("User", 1, |n| n.prop_str("username") == Some("alice"));
    }

    #[test]
    #[should_panic(expected = "did not satisfy predicate")]
    fn assert_node_panics_when_predicate_false() {
        let view = ResponseView::new(sample_response());
        view.assert_node("User", 1, |n| n.prop_str("username") == Some("wrong"));
    }

    #[test]
    fn assert_edge_exists_passes_for_present_edge() {
        let view = ResponseView::new(sample_response());
        view.assert_edge_exists("User", 1, "Group", 100, "MEMBER_OF");
    }

    #[test]
    #[should_panic(expected = "expected edge")]
    fn assert_edge_exists_panics_for_missing_edge() {
        let view = ResponseView::new(sample_response());
        view.assert_edge_exists("User", 1, "Group", 999, "MEMBER_OF");
    }

    #[test]
    fn assert_edge_absent_passes_for_missing_edge() {
        let view = ResponseView::new(sample_response());
        view.assert_edge_absent("User", 1, "Group", 999, "MEMBER_OF");
    }

    #[test]
    #[should_panic(expected = "should not exist")]
    fn assert_edge_absent_panics_for_present_edge() {
        let view = ResponseView::new(sample_response());
        view.assert_edge_absent("User", 1, "Group", 100, "MEMBER_OF");
    }

    #[test]
    fn assert_all_nodes_passes_when_all_match() {
        let view = ResponseView::new(sample_response());
        view.assert_all_nodes(|n| n.id > 0, "all ids should be positive");
    }

    #[test]
    #[should_panic(expected = "failed: impossible")]
    fn assert_all_nodes_panics_when_one_fails() {
        let view = ResponseView::new(sample_response());
        view.assert_all_nodes(|n| n.entity_type == "User", "impossible");
    }

    #[test]
    fn assert_node_order_passes_for_correct_order() {
        let view = ResponseView::new(sample_response());
        view.assert_node_order("User", &[1, 2]);
        view.assert_node_order("Group", &[100, 101]);
    }

    #[test]
    #[should_panic(expected = "nodes in wrong order")]
    fn assert_node_order_panics_for_wrong_order() {
        let view = ResponseView::new(sample_response());
        view.assert_node_order("User", &[2, 1]);
    }

    #[test]
    fn assert_referential_integrity_passes_for_valid_response() {
        let view = ResponseView::new(sample_response());
        view.assert_referential_integrity();
    }

    #[test]
    #[should_panic(expected = "non-existent target node")]
    fn assert_referential_integrity_panics_for_dangling_edge() {
        let resp = GraphResponse {
            query_type: "traversal".to_string(),
            nodes: vec![make_node("User", 1, &[])],
            edges: vec![make_edge("User", 1, "Group", 999, "MEMBER_OF")],
        };
        ResponseView::new(resp).assert_referential_integrity();
    }

    // ── NodeExt ──────────────────────────────────────────────────────

    #[test]
    fn prop_returns_non_null_value() {
        let node = make_node("User", 1, &[("name", json!("alice")), ("age", json!(30))]);
        assert_eq!(node.prop("name"), Some(&json!("alice")));
        assert_eq!(node.prop("age"), Some(&json!(30)));
    }

    #[test]
    fn prop_returns_none_for_null_and_missing() {
        let node = make_node("User", 1, &[("gone", Value::Null)]);
        assert!(node.prop("gone").is_none());
        assert!(node.prop("missing").is_none());
    }

    #[test]
    fn prop_str_returns_string_values() {
        let node = make_node("User", 1, &[("name", json!("alice")), ("count", json!(5))]);
        assert_eq!(node.prop_str("name"), Some("alice"));
        assert_eq!(node.prop_str("count"), None);
    }

    #[test]
    fn prop_i64_returns_integer_values() {
        let node = make_node("User", 1, &[("count", json!(42)), ("name", json!("x"))]);
        assert_eq!(node.prop_i64("count"), Some(42));
        assert_eq!(node.prop_i64("name"), None);
    }

    #[test]
    fn prop_f64_returns_float_values() {
        let node = make_node("User", 1, &[("score", json!(1.1))]);
        assert_eq!(node.prop_f64("score"), Some(1.1));
        assert!(node.prop_f64("missing").is_none());
    }

    #[test]
    fn prop_bool_returns_boolean_values() {
        let node = make_node("User", 1, &[("active", json!(true)), ("name", json!("x"))]);
        assert_eq!(node.prop_bool("active"), Some(true));
        assert_eq!(node.prop_bool("name"), None);
    }

    #[test]
    fn has_prop_true_for_present_false_for_null_or_missing() {
        let node = make_node("User", 1, &[("name", json!("a")), ("gone", Value::Null)]);
        assert!(node.has_prop("name"));
        assert!(!node.has_prop("gone"));
        assert!(!node.has_prop("missing"));
    }

    #[test]
    fn assert_str_passes_for_correct_value() {
        let node = make_node("User", 1, &[("name", json!("alice"))]);
        node.assert_str("name", "alice");
    }

    #[test]
    #[should_panic(expected = "expected \"wrong\"")]
    fn assert_str_panics_for_wrong_value() {
        let node = make_node("User", 1, &[("name", json!("alice"))]);
        node.assert_str("name", "wrong");
    }

    #[test]
    fn assert_i64_passes_for_correct_value() {
        let node = make_node("User", 1, &[("count", json!(7))]);
        node.assert_i64("count", 7);
    }

    #[test]
    #[should_panic(expected = "expected 99")]
    fn assert_i64_panics_for_wrong_value() {
        let node = make_node("User", 1, &[("count", json!(7))]);
        node.assert_i64("count", 99);
    }

    #[test]
    fn assert_prop_passes_for_exact_match() {
        let node = make_node("User", 1, &[("tags", json!(["a", "b"]))]);
        node.assert_prop("tags", &json!(["a", "b"]));
    }

    // ── Empty response ───────────────────────────────────────────────

    #[test]
    fn empty_response_returns_zero_counts_and_empty_collections() {
        let resp = GraphResponse {
            query_type: "search".to_string(),
            nodes: vec![],
            edges: vec![],
        };
        let view = ResponseView::new(resp);
        assert_eq!(view.node_count(), 0);
        assert_eq!(view.edge_count(), 0);
        assert!(view.node_ids("User").is_empty());
        assert!(view.edges_of_type("MEMBER_OF").is_empty());
        assert!(view.path_ids().is_empty());
        view.assert_referential_integrity();
    }
}
