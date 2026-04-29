use std::cell::RefCell;
use std::collections::HashSet;

use query_engine::compiler::input::{Input, QueryType};

/// Query features that require corresponding assertions in the test.
///
/// Some variants carry data from the AST (field name, edge type) so the
/// enforcement is granular: a query with two filter fields or two
/// relationship types produces one requirement per field/type, and the
/// test must assert each individually.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Requirement {
    /// Query has `order_by` — test must call `assert_node_order`.
    OrderBy,
    /// Query filters on `field` — test must call `assert_filter` for this field.
    Filter { field: String },
    /// Query has `node_ids` — test must verify IDs via `node_ids()` or `assert_node_order`.
    NodeIds,
    /// Query type is `path_finding` — test must call `path_ids` + `path`.
    PathFinding,
    /// Query type is `aggregation` — test must assert a property value on a result node.
    Aggregation,
    /// Traversal query includes edge type — test must assert edges of this type
    /// via `edges_of_type`, `assert_edge_exists`, or `assert_edge_absent`.
    Relationship { edge_type: String },
    /// Query type is `neighbors` — test must verify neighbor edges.
    Neighbors,
    /// Query has `aggregation_sort` — test must call `assert_node_order`.
    AggregationSort,
    /// Query has `cursor` — test must call `assert_node_count` to verify the page.
    Cursor,
    /// Query returns nodes — test must call `assert_node_count`.
    ///
    /// Always derived for `search`, `traversal`, and `neighbors` queries
    /// to ensure no unexpected rows leak into the response.
    NodeCount,
}

impl std::fmt::Display for Requirement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OrderBy => write!(f, "OrderBy (query has order_by — call assert_node_order)"),
            Self::Filter { field } => {
                write!(f, "Filter on '{field}' (call assert_filter for '{field}')")
            }
            Self::NodeIds => write!(f, "NodeIds (query has node_ids — verify ID set)"),
            Self::PathFinding => write!(f, "PathFinding (call path_ids + path)"),
            Self::Aggregation => write!(f, "Aggregation (assert a property value on result)"),
            Self::Relationship { edge_type } => {
                write!(f, "Relationship '{edge_type}' (assert {edge_type} edges)")
            }
            Self::Neighbors => write!(f, "Neighbors (verify neighbor edges)"),
            Self::AggregationSort => {
                write!(
                    f,
                    "AggregationSort (query has aggregation_sort — call assert_node_order)"
                )
            }
            Self::Cursor => {
                write!(
                    f,
                    "Cursor (query has cursor — call assert_node_count to verify page)"
                )
            }
            Self::NodeCount => {
                write!(f, "NodeCount (call assert_node_count to verify total rows)")
            }
        }
    }
}

/// Extension trait that derives assertion [`Requirement`]s from the
/// compiler's validated query AST.
///
/// Implemented on [`Input`] so callers can write `input.requirements()`
/// instead of a free function.
pub trait QueryRequirements {
    fn requirements(&self) -> HashSet<Requirement>;
}

impl QueryRequirements for Input {
    fn requirements(&self) -> HashSet<Requirement> {
        let mut reqs = HashSet::new();

        if self.order_by.is_some() {
            reqs.insert(Requirement::OrderBy);
        }

        for node in &self.nodes {
            for field in node.filters.keys() {
                reqs.insert(Requirement::Filter {
                    field: field.clone(),
                });
            }
        }

        // node_ids in path_finding defines endpoints, not a result filter.
        if self.query_type != QueryType::PathFinding
            && self.nodes.iter().any(|n| !n.node_ids.is_empty())
        {
            reqs.insert(Requirement::NodeIds);
        }

        match self.query_type {
            QueryType::PathFinding => {
                reqs.insert(Requirement::PathFinding);
            }
            QueryType::Aggregation => {
                reqs.insert(Requirement::Aggregation);
            }
            QueryType::Neighbors => {
                reqs.insert(Requirement::Neighbors);
                reqs.insert(Requirement::NodeCount);
            }
            QueryType::Traversal => {
                reqs.insert(Requirement::NodeCount);
            }
            QueryType::Hydration => {}
        }

        // Traversal queries with joins produce edges the test must verify per type.
        // Aggregation also uses relationships for joins but doesn't produce edges.
        if self.query_type == QueryType::Traversal {
            for rel in &self.relationships {
                for edge_type in &rel.types {
                    reqs.insert(Requirement::Relationship {
                        edge_type: edge_type.clone(),
                    });
                }
            }
        }

        if self.aggregation_sort.is_some() {
            reqs.insert(Requirement::AggregationSort);
        }

        if self.cursor.is_some() {
            reqs.insert(Requirement::Cursor);
        }

        reqs
    }
}

/// Tracks which query-derived requirements have been satisfied by test assertions.
///
/// When created via [`super::ResponseView::for_query`], requirements are derived
/// from the compiled [`Input`]. Each assertion method on `ResponseView` marks the
/// relevant requirement as satisfied. On drop, panics if any requirement was not met.
pub(super) struct AssertionTracker {
    required: HashSet<Requirement>,
    satisfied: RefCell<HashSet<Requirement>>,
}

impl AssertionTracker {
    pub(super) fn new(required: HashSet<Requirement>) -> Self {
        Self {
            required,
            satisfied: RefCell::new(HashSet::new()),
        }
    }

    #[cfg(test)]
    pub(super) fn empty() -> Self {
        Self::new(HashSet::new())
    }

    pub(super) fn satisfy(&self, req: Requirement) {
        self.satisfied.borrow_mut().insert(req);
    }

    pub(super) fn skip(&self, req: Requirement) {
        if self.required.contains(&req) {
            self.satisfied.borrow_mut().insert(req);
        }
    }

    /// Mark every `Requirement::Filter { .. }` in the required set as
    /// satisfied. Used by empty-result assertions where filter checks
    /// are vacuously true.
    pub(super) fn satisfy_all_filters(&self) {
        let filters: Vec<Requirement> = self
            .required
            .iter()
            .filter(|r| matches!(r, Requirement::Filter { .. }))
            .cloned()
            .collect();
        let mut satisfied = self.satisfied.borrow_mut();
        for f in filters {
            satisfied.insert(f);
        }
    }

    fn unsatisfied(&self) -> HashSet<Requirement> {
        self.required
            .difference(&self.satisfied.borrow())
            .cloned()
            .collect()
    }
}

impl Drop for AssertionTracker {
    fn drop(&mut self) {
        if std::thread::panicking() {
            return;
        }
        let missing = self.unsatisfied();
        if !missing.is_empty() {
            let mut list: Vec<String> = missing.iter().map(|r| format!("  - {r}")).collect();
            list.sort();
            panic!(
                "ResponseView dropped with unsatisfied assertion requirements:\n{}",
                list.join("\n")
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use query_engine::compiler::input::parse_input;

    use crate::visitor::tests::{
        make_node, make_path_edge, sample_aggregation_response, sample_neighbors_response,
        sample_response, sample_search_response,
    };
    use crate::visitor::{NodeExt, ResponseView};
    use query_engine::formatters::GraphResponse;

    fn parse_test_input(json: &str) -> Input {
        parse_input(json).expect("test query JSON should parse into Input")
    }

    // ── Requirement derivation ───────────────────────────────────────

    #[test]
    fn requirements_from_search_with_order_by() {
        let input = parse_test_input(
            r#"{"query_type": "traversal", "node": {"id": "u", "entity": "User"},
                "order_by": {"node": "u", "property": "id"}, "limit": 10}"#,
        );
        let reqs = input.requirements();
        assert!(reqs.contains(&Requirement::OrderBy));
        assert!(reqs.contains(&Requirement::NodeCount));
        assert_eq!(reqs.len(), 2);
    }

    #[test]
    fn requirements_from_search_with_filter() {
        let input = parse_test_input(
            r#"{"query_type": "traversal",
                "node": {"id": "u", "entity": "User", "filters": {"state": "active"}},
                "limit": 10}"#,
        );
        let reqs = input.requirements();
        assert!(reqs.contains(&Requirement::Filter {
            field: "state".into()
        }));
        assert!(reqs.contains(&Requirement::NodeCount));
        assert_eq!(reqs.len(), 2);
    }

    #[test]
    fn requirements_from_search_with_multiple_filters() {
        let input = parse_test_input(
            r#"{"query_type": "traversal",
                "node": {"id": "u", "entity": "User",
                         "filters": {"state": "active", "user_type": "human"}},
                "limit": 10}"#,
        );
        let reqs = input.requirements();
        assert!(reqs.contains(&Requirement::Filter {
            field: "state".into()
        }));
        assert!(reqs.contains(&Requirement::Filter {
            field: "user_type".into()
        }));
        assert!(reqs.contains(&Requirement::NodeCount));
        assert_eq!(reqs.len(), 3);
    }

    #[test]
    fn requirements_from_search_with_node_ids() {
        let input = parse_test_input(
            r#"{"query_type": "traversal",
                "node": {"id": "u", "entity": "User", "node_ids": [1, 2]},
                "limit": 10}"#,
        );
        let reqs = input.requirements();
        assert!(reqs.contains(&Requirement::NodeIds));
        assert!(reqs.contains(&Requirement::NodeCount));
        assert_eq!(reqs.len(), 2);
    }

    #[test]
    fn requirements_from_path_finding_excludes_node_ids() {
        let input = parse_test_input(
            r#"{"query_type": "path_finding",
                "nodes": [
                    {"id": "s", "entity": "User", "node_ids": [1]},
                    {"id": "e", "entity": "Project", "node_ids": [1000]}
                ],
                "path": {"type": "shortest", "from": "s", "to": "e", "max_depth": 3}}"#,
        );
        let reqs = input.requirements();
        assert!(reqs.contains(&Requirement::PathFinding));
        assert!(
            !reqs.contains(&Requirement::NodeIds),
            "path_finding node_ids are endpoints, not result filters"
        );
        assert_eq!(reqs.len(), 1);
    }

    #[test]
    fn requirements_from_aggregation() {
        let input = parse_test_input(
            r#"{"query_type": "aggregation",
                "nodes": [{"id": "u", "entity": "User"}],
                "aggregations": [{"function": "count", "target": "u", "alias": "c"}],
                "limit": 10}"#,
        );
        let reqs = input.requirements();
        assert!(reqs.contains(&Requirement::Aggregation));
        assert_eq!(reqs.len(), 1);
    }

    #[test]
    fn requirements_from_plain_search_has_node_count() {
        let input = parse_test_input(
            r#"{"query_type": "traversal",
                "node": {"id": "u", "entity": "User"},
                "limit": 10}"#,
        );
        let reqs = input.requirements();
        assert_eq!(reqs, HashSet::from([Requirement::NodeCount]));
    }

    #[test]
    fn requirements_from_default_input_has_node_count() {
        // Input::default() has query_type Search, which always requires NodeCount.
        assert_eq!(
            Input::default().requirements(),
            HashSet::from([Requirement::NodeCount])
        );
    }

    #[test]
    fn requirements_from_traversal_with_single_relationship() {
        let input = parse_test_input(
            r#"{"query_type": "traversal",
                "nodes": [
                    {"id": "u", "entity": "User"},
                    {"id": "g", "entity": "Group"}
                ],
                "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
                "limit": 10}"#,
        );
        let reqs = input.requirements();
        assert!(reqs.contains(&Requirement::Relationship {
            edge_type: "MEMBER_OF".into()
        }));
        assert!(reqs.contains(&Requirement::NodeCount));
        assert_eq!(reqs.len(), 2);
    }

    #[test]
    fn requirements_from_traversal_with_multiple_relationships() {
        let input = parse_test_input(
            r#"{"query_type": "traversal",
                "nodes": [
                    {"id": "u", "entity": "User"},
                    {"id": "g", "entity": "Group"},
                    {"id": "p", "entity": "Project"}
                ],
                "relationships": [
                    {"type": "MEMBER_OF", "from": "u", "to": "g"},
                    {"type": "CONTAINS", "from": "g", "to": "p"}
                ],
                "limit": 10}"#,
        );
        let reqs = input.requirements();
        assert!(reqs.contains(&Requirement::Relationship {
            edge_type: "MEMBER_OF".into()
        }));
        assert!(reqs.contains(&Requirement::Relationship {
            edge_type: "CONTAINS".into()
        }));
        assert!(reqs.contains(&Requirement::NodeCount));
        assert_eq!(reqs.len(), 3);
    }

    #[test]
    fn requirements_from_aggregation_excludes_relationships() {
        let input = parse_test_input(
            r#"{"query_type": "aggregation",
                "nodes": [
                    {"id": "u", "entity": "User"},
                    {"id": "mr", "entity": "MergeRequest"}
                ],
                "relationships": [{"type": "AUTHORED", "from": "u", "to": "mr"}],
                "aggregations": [{"function": "count", "target": "mr", "group_by": "u", "alias": "c"}],
                "limit": 10}"#,
        );
        let reqs = input.requirements();
        assert!(
            !reqs.contains(&Requirement::Relationship {
                edge_type: "AUTHORED".into()
            }),
            "aggregation uses relationships for joins but produces no edges"
        );
        assert!(reqs.contains(&Requirement::Aggregation));
    }

    #[test]
    fn requirements_from_neighbors() {
        let input = parse_test_input(
            r#"{"query_type": "neighbors",
                "node": {"id": "u", "entity": "User", "node_ids": [1]},
                "neighbors": {"node": "u", "direction": "outgoing"}}"#,
        );
        let reqs = input.requirements();
        assert!(reqs.contains(&Requirement::Neighbors));
        assert!(reqs.contains(&Requirement::NodeIds));
        assert!(reqs.contains(&Requirement::NodeCount));
        assert_eq!(reqs.len(), 3);
    }

    #[test]
    fn requirements_from_aggregation_sort() {
        let input = parse_test_input(
            r#"{"query_type": "aggregation",
                "nodes": [{"id": "u", "entity": "User"}],
                "aggregations": [{"function": "count", "target": "u", "alias": "c"}],
                "aggregation_sort": {"agg_index": 0, "direction": "DESC"},
                "limit": 10}"#,
        );
        let reqs = input.requirements();
        assert!(reqs.contains(&Requirement::AggregationSort));
        assert!(reqs.contains(&Requirement::Aggregation));
        assert_eq!(reqs.len(), 2);
    }

    // ── Assertion enforcement ────────────────────────────────────────

    #[test]
    fn for_query_plain_search_requires_node_count() {
        let input = parse_test_input(
            r#"{"query_type": "traversal", "node": {"id": "u", "entity": "User"}, "limit": 10}"#,
        );
        let view = ResponseView::for_query(&input, sample_search_response());
        view.assert_node_count(2);
    }

    #[test]
    fn for_query_order_satisfied_by_assert_node_order() {
        let input = parse_test_input(
            r#"{"query_type": "traversal", "node": {"id": "u", "entity": "User"},
                "order_by": {"node": "u", "property": "id"}, "limit": 10}"#,
        );
        let view = ResponseView::for_query(&input, sample_search_response());
        view.assert_node_count(2);
        view.assert_node_order("User", &[1, 2]);
    }

    #[test]
    fn for_query_filter_satisfied_by_assert_filter() {
        let input = parse_test_input(
            r#"{"query_type": "traversal",
                "node": {"id": "u", "entity": "User", "filters": {"username": "alice"}},
                "limit": 10}"#,
        );
        let view = ResponseView::for_query(&input, sample_search_response());
        view.assert_node_count(2);
        view.assert_filter("User", "username", |n| n.prop_str("username").is_some());
    }

    #[test]
    fn for_query_multi_filter_requires_all_fields() {
        let input = parse_test_input(
            r#"{"query_type": "traversal",
                "node": {"id": "u", "entity": "User",
                         "filters": {"username": "alice", "state": "active"}},
                "limit": 10}"#,
        );
        let view = ResponseView::for_query(&input, sample_search_response());
        view.assert_node_count(2);
        view.assert_filter("User", "username", |n| n.prop_str("username").is_some());
        view.assert_filter("User", "state", |n| {
            matches!(n.prop_str("username"), Some("alice" | "bob"))
        });
    }

    #[test]
    #[should_panic(expected = "unsatisfied assertion requirements")]
    fn for_query_multi_filter_panics_on_partial_satisfaction() {
        let input = parse_test_input(
            r#"{"query_type": "traversal",
                "node": {"id": "u", "entity": "User",
                         "filters": {"username": "alice", "state": "active"}},
                "limit": 10}"#,
        );
        let view = ResponseView::for_query(&input, sample_search_response());
        view.assert_filter("User", "username", |n| n.prop_str("username").is_some());
        drop(view);
    }

    #[test]
    fn for_query_node_ids_satisfied_by_node_ids() {
        let input = parse_test_input(
            r#"{"query_type": "traversal",
                "node": {"id": "u", "entity": "User", "node_ids": [1, 2]},
                "limit": 10}"#,
        );
        let view = ResponseView::for_query(&input, sample_search_response());
        view.assert_node_count(2);
        view.assert_node_ids("User", &[1, 2]);
    }

    #[test]
    fn for_query_aggregation_satisfied_by_assert_node() {
        let input = parse_test_input(
            r#"{"query_type": "aggregation",
                "nodes": [{"id": "u", "entity": "User"}],
                "aggregations": [{"function": "count", "target": "u", "alias": "c"}],
                "limit": 10}"#,
        );
        let view = ResponseView::for_query(&input, sample_aggregation_response());
        view.assert_node("User", 1, |n| n.prop_str("username") == Some("alice"));
    }

    #[test]
    fn for_query_path_finding_satisfied_by_path_ids() {
        let input = parse_test_input(
            r#"{"query_type": "path_finding",
                "nodes": [{"id": "s", "entity": "User", "node_ids": [1]},
                           {"id": "e", "entity": "Project", "node_ids": [1000]}],
                "path": {"type": "shortest", "from": "s", "to": "e", "max_depth": 3}}"#,
        );
        let resp = GraphResponse {
            format_version: query_engine::formatters::RAW_OUTPUT_FORMAT_VERSION.to_string(),
            query_type: "path_finding".to_string(),
            nodes: vec![make_node("User", 1, &[]), make_node("Project", 1000, &[])],
            edges: vec![make_path_edge("User", 1, "Project", 1000, "CONTAINS", 0, 0)],
            columns: None,
            pagination: None,
        };
        let view = ResponseView::for_query(&input, resp);
        assert_eq!(view.path_ids().len(), 1);
    }

    #[test]
    fn for_query_relationship_satisfied_by_edges_of_type() {
        let input = parse_test_input(
            r#"{"query_type": "traversal",
                "nodes": [
                    {"id": "u", "entity": "User"},
                    {"id": "g", "entity": "Group"}
                ],
                "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
                "limit": 10}"#,
        );
        let view = ResponseView::for_query(&input, sample_response());
        view.assert_node_count(4);
        view.assert_edge_set("MEMBER_OF", &[(1, 100), (1, 101), (2, 100)]);
    }

    #[test]
    fn for_query_relationship_satisfied_by_assert_edge_exists() {
        let input = parse_test_input(
            r#"{"query_type": "traversal",
                "nodes": [
                    {"id": "u", "entity": "User"},
                    {"id": "g", "entity": "Group"}
                ],
                "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
                "limit": 10}"#,
        );
        let view = ResponseView::for_query(&input, sample_response());
        view.assert_node_count(4);
        view.assert_edge_exists("User", 1, "Group", 100, "MEMBER_OF");
    }

    #[test]
    #[should_panic(expected = "unsatisfied assertion requirements")]
    fn for_query_relationship_not_satisfied_by_assert_edge_absent() {
        let input = parse_test_input(
            r#"{"query_type": "traversal",
                "nodes": [
                    {"id": "u", "entity": "User"},
                    {"id": "g", "entity": "Group"}
                ],
                "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
                "limit": 10}"#,
        );
        let view = ResponseView::for_query(&input, sample_response());
        view.assert_edge_absent("User", 1, "Group", 999, "MEMBER_OF");
    }

    #[test]
    fn for_query_multi_relationship_requires_all_types() {
        let input = parse_test_input(
            r#"{"query_type": "traversal",
                "nodes": [
                    {"id": "u", "entity": "User"},
                    {"id": "g", "entity": "Group"},
                    {"id": "p", "entity": "Project"}
                ],
                "relationships": [
                    {"type": "MEMBER_OF", "from": "u", "to": "g"},
                    {"type": "CONTAINS", "from": "g", "to": "p"}
                ],
                "limit": 10}"#,
        );
        let view = ResponseView::for_query(&input, sample_response());
        view.assert_node_count(4);
        view.assert_edge_set("MEMBER_OF", &[(1, 100), (1, 101), (2, 100)]);
        view.assert_edge_count("CONTAINS", 0);
    }

    #[test]
    #[should_panic(expected = "unsatisfied assertion requirements")]
    fn for_query_multi_relationship_panics_on_partial() {
        let input = parse_test_input(
            r#"{"query_type": "traversal",
                "nodes": [
                    {"id": "u", "entity": "User"},
                    {"id": "g", "entity": "Group"},
                    {"id": "p", "entity": "Project"}
                ],
                "relationships": [
                    {"type": "MEMBER_OF", "from": "u", "to": "g"},
                    {"type": "CONTAINS", "from": "g", "to": "p"}
                ],
                "limit": 10}"#,
        );
        let view = ResponseView::for_query(&input, sample_response());
        view.assert_edge_set("MEMBER_OF", &[(1, 100), (1, 101), (2, 100)]);
        drop(view);
    }

    #[test]
    fn for_query_neighbors_satisfied_by_assert_edge_exists() {
        let input = parse_test_input(
            r#"{"query_type": "neighbors",
                "node": {"id": "u", "entity": "User", "node_ids": [1]},
                "neighbors": {"node": "u", "direction": "outgoing"}}"#,
        );
        let view = ResponseView::for_query(&input, sample_neighbors_response());
        view.assert_node_count(3);
        view.assert_edge_exists("User", 1, "Group", 100, "MEMBER_OF");
        view.assert_node_ids("User", &[1]);
    }

    #[test]
    fn for_query_neighbors_satisfied_by_edges_of_type() {
        let input = parse_test_input(
            r#"{"query_type": "neighbors",
                "node": {"id": "u", "entity": "User", "node_ids": [1]},
                "neighbors": {"node": "u", "direction": "outgoing"}}"#,
        );
        let view = ResponseView::for_query(&input, sample_neighbors_response());
        view.assert_node_count(3);
        view.assert_edge_set("MEMBER_OF", &[(1, 100), (1, 101)]);
        view.assert_node_ids("User", &[1]);
    }

    #[test]
    fn for_query_aggregation_sort_satisfied_by_assert_node_order() {
        let input = parse_test_input(
            r#"{"query_type": "aggregation",
                "nodes": [{"id": "u", "entity": "User"}],
                "aggregations": [{"function": "count", "target": "u", "alias": "c"}],
                "aggregation_sort": {"agg_index": 0, "direction": "DESC"},
                "limit": 10}"#,
        );
        let view = ResponseView::for_query(&input, sample_aggregation_response());
        // aggregation queries don't require NodeCount
        view.assert_node_order("User", &[1, 2]);
        view.assert_node("User", 1, |n| n.prop_str("username") == Some("alice"));
    }

    #[test]
    fn requirements_from_cursor() {
        let input = parse_test_input(
            r#"{"query_type": "traversal",
                "node": {"id": "u", "entity": "User"},
                "cursor": {"offset": 0, "page_size": 5},
                "limit": 10}"#,
        );
        let reqs = input.requirements();
        assert!(reqs.contains(&Requirement::Cursor));
        assert!(reqs.contains(&Requirement::NodeCount));
        assert_eq!(reqs.len(), 2);
    }

    #[test]
    fn for_query_cursor_satisfied_by_assert_node_count() {
        let input = parse_test_input(
            r#"{"query_type": "traversal",
                "node": {"id": "u", "entity": "User"},
                "cursor": {"offset": 0, "page_size": 5},
                "limit": 10}"#,
        );
        let view = ResponseView::for_query(&input, sample_search_response());
        view.assert_node_count(2);
    }

    #[test]
    fn for_query_node_ids_satisfied_by_assert_node_ids() {
        let input = parse_test_input(
            r#"{"query_type": "traversal",
                "node": {"id": "u", "entity": "User", "node_ids": [1, 2]},
                "limit": 10}"#,
        );
        let view = ResponseView::for_query(&input, sample_search_response());
        view.assert_node_count(2);
        view.assert_node_ids("User", &[1, 2]);
    }

    #[test]
    fn for_query_neighbors_satisfied_by_assert_edge_set() {
        let input = parse_test_input(
            r#"{"query_type": "neighbors",
                "node": {"id": "u", "entity": "User", "node_ids": [1]},
                "neighbors": {"node": "u", "direction": "outgoing"}}"#,
        );
        let view = ResponseView::for_query(&input, sample_neighbors_response());
        view.assert_node_count(3);
        view.assert_edge_set("MEMBER_OF", &[(1, 100), (1, 101)]);
        view.assert_node_ids("User", &[1]);
    }

    #[test]
    fn for_query_neighbors_satisfied_by_assert_edge_count() {
        let input = parse_test_input(
            r#"{"query_type": "neighbors",
                "node": {"id": "u", "entity": "User", "node_ids": [1]},
                "neighbors": {"node": "u", "direction": "outgoing"}}"#,
        );
        let view = ResponseView::for_query(&input, sample_neighbors_response());
        view.assert_node_count(3);
        view.assert_edge_count("MEMBER_OF", 2);
        view.assert_node_ids("User", &[1]);
    }

    #[test]
    #[should_panic(expected = "unsatisfied assertion requirements")]
    fn for_query_node_ids_not_satisfied_by_assert_node_count() {
        let input = parse_test_input(
            r#"{"query_type": "traversal",
                "node": {"id": "u", "entity": "User", "node_ids": [1, 2]},
                "limit": 10}"#,
        );
        let view = ResponseView::for_query(&input, sample_search_response());
        view.assert_node_count(2);
        drop(view);
    }

    // ── Panic on unsatisfied ─────────────────────────────────────────

    #[test]
    #[should_panic(expected = "NodeCount")]
    fn for_query_panics_on_unsatisfied_node_count() {
        let input = parse_test_input(
            r#"{"query_type": "traversal",
                "node": {"id": "u", "entity": "User"},
                "limit": 10}"#,
        );
        let view = ResponseView::for_query(&input, sample_search_response());
        drop(view);
    }

    #[test]
    #[should_panic(expected = "unsatisfied assertion requirements")]
    fn for_query_panics_on_unsatisfied_order() {
        let input = parse_test_input(
            r#"{"query_type": "traversal", "node": {"id": "u", "entity": "User"},
                "order_by": {"node": "u", "property": "id"}, "limit": 10}"#,
        );
        let view = ResponseView::for_query(&input, sample_search_response());
        drop(view);
    }

    #[test]
    #[should_panic(expected = "Filter on 'state'")]
    fn for_query_panics_on_unsatisfied_filter_shows_field() {
        let input = parse_test_input(
            r#"{"query_type": "traversal",
                "node": {"id": "u", "entity": "User", "filters": {"state": "active"}},
                "limit": 10}"#,
        );
        let view = ResponseView::for_query(&input, sample_search_response());
        drop(view);
    }

    #[test]
    #[should_panic(expected = "unsatisfied assertion requirements")]
    fn for_query_panics_on_unsatisfied_aggregation() {
        let input = parse_test_input(
            r#"{"query_type": "aggregation",
                "nodes": [{"id": "u", "entity": "User"}],
                "aggregations": [{"function": "count", "target": "u", "alias": "c"}],
                "limit": 10}"#,
        );
        let view = ResponseView::for_query(&input, sample_aggregation_response());
        drop(view);
    }

    #[test]
    #[should_panic(expected = "Relationship 'MEMBER_OF'")]
    fn for_query_panics_on_unsatisfied_relationship_shows_type() {
        let input = parse_test_input(
            r#"{"query_type": "traversal",
                "nodes": [
                    {"id": "u", "entity": "User"},
                    {"id": "g", "entity": "Group"}
                ],
                "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
                "limit": 10}"#,
        );
        let view = ResponseView::for_query(&input, sample_response());
        drop(view);
    }

    #[test]
    #[should_panic(expected = "unsatisfied assertion requirements")]
    fn for_query_panics_on_unsatisfied_neighbors() {
        let input = parse_test_input(
            r#"{"query_type": "neighbors",
                "node": {"id": "u", "entity": "User", "node_ids": [1]},
                "neighbors": {"node": "u", "direction": "outgoing"}}"#,
        );
        let view = ResponseView::for_query(&input, sample_neighbors_response());
        view.assert_node_ids("User", &[1]);
    }

    #[test]
    #[should_panic(expected = "Cursor")]
    fn for_query_panics_on_unsatisfied_cursor() {
        let input = parse_test_input(
            r#"{"query_type": "traversal",
                "node": {"id": "u", "entity": "User"},
                "cursor": {"offset": 0, "page_size": 5},
                "limit": 10}"#,
        );
        let view = ResponseView::for_query(&input, sample_search_response());
        drop(view);
    }

    #[test]
    #[should_panic(expected = "unsatisfied assertion requirements")]
    fn for_query_panics_on_unsatisfied_path_finding() {
        let input = parse_test_input(
            r#"{"query_type": "path_finding",
                "nodes": [{"id": "s", "entity": "User", "node_ids": [1]},
                           {"id": "e", "entity": "Project", "node_ids": [1000]}],
                "path": {"type": "shortest", "from": "s", "to": "e", "max_depth": 3}}"#,
        );
        let resp = GraphResponse {
            format_version: query_engine::formatters::RAW_OUTPUT_FORMAT_VERSION.to_string(),
            query_type: "path_finding".to_string(),
            nodes: vec![make_node("User", 1, &[]), make_node("Project", 1000, &[])],
            edges: vec![make_path_edge("User", 1, "Project", 1000, "CONTAINS", 0, 0)],
            columns: None,
            pagination: None,
        };
        let view = ResponseView::for_query(&input, resp);
        drop(view);
    }

    #[test]
    #[should_panic(expected = "unsatisfied assertion requirements")]
    fn for_query_panics_on_unsatisfied_aggregation_sort() {
        let input = parse_test_input(
            r#"{"query_type": "aggregation",
                "nodes": [{"id": "u", "entity": "User"}],
                "aggregations": [{"function": "count", "target": "u", "alias": "c"}],
                "aggregation_sort": {"agg_index": 0, "direction": "DESC"},
                "limit": 10}"#,
        );
        let view = ResponseView::for_query(&input, sample_aggregation_response());
        view.assert_node("User", 1, |n| n.prop_str("username") == Some("alice"));
        drop(view);
    }

    #[test]
    #[should_panic(expected = "unsatisfied assertion requirements")]
    fn for_query_panics_on_unsatisfied_node_ids() {
        let input = parse_test_input(
            r#"{"query_type": "traversal",
                "node": {"id": "u", "entity": "User", "node_ids": [1, 2]},
                "limit": 10}"#,
        );
        let view = ResponseView::for_query(&input, sample_search_response());
        drop(view);
    }

    #[test]
    fn for_query_combined_features_requires_all() {
        let input = parse_test_input(
            r#"{"query_type": "traversal",
                "node": {"id": "u", "entity": "User", "node_ids": [1, 2],
                         "filters": {"username": {"op": "in", "value": ["alice", "bob"]}}},
                "order_by": {"node": "u", "property": "id"},
                "cursor": {"offset": 0, "page_size": 5},
                "limit": 10}"#,
        );
        let view = ResponseView::for_query(&input, sample_search_response());
        view.assert_node_order("User", &[1, 2]);
        view.assert_node_count(2);
        view.assert_filter("User", "username", |n| {
            matches!(n.prop_str("username"), Some("alice" | "bob"))
        });
    }

    // ── Skip + new() ─────────────────────────────────────────────────

    #[test]
    fn skip_requirement_prevents_panic() {
        let input = parse_test_input(
            r#"{"query_type": "traversal", "node": {"id": "u", "entity": "User"},
                "order_by": {"node": "u", "property": "id"}, "limit": 10}"#,
        );
        let view = ResponseView::for_query(&input, sample_search_response());
        view.assert_node_count(2);
        view.skip_requirement(Requirement::OrderBy);
    }

    #[test]
    fn skip_requirement_works_for_granular_variants() {
        let input = parse_test_input(
            r#"{"query_type": "traversal",
                "nodes": [
                    {"id": "u", "entity": "User"},
                    {"id": "g", "entity": "Group"}
                ],
                "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
                "limit": 10}"#,
        );
        let view = ResponseView::for_query(&input, sample_response());
        view.assert_node_count(4);
        view.skip_requirement(Requirement::Relationship {
            edge_type: "MEMBER_OF".into(),
        });
    }

    #[test]
    fn new_has_no_enforcement() {
        let view = ResponseView::new(sample_response());
        drop(view);
    }

    #[test]
    #[should_panic(expected = "trivial predicate")]
    fn assert_node_rejects_trivial_predicate() {
        let view = ResponseView::new(sample_response());
        view.assert_node("User", 1, |_| true);
    }

    #[test]
    #[should_panic(expected = "trivial predicate")]
    fn assert_filter_rejects_trivial_predicate() {
        let view = ResponseView::new(sample_search_response());
        view.assert_filter("User", "username", |_| true);
    }
}
