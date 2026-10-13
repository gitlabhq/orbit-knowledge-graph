use std::collections::{BTreeMap, BTreeSet};

use query_engine::compiler::Frontend;
use query_engine::formatters::{GraphEdge, GraphResponse};

use super::format::AllPagesExpect;

type EdgeIdentity = (String, i64, String, i64, String, Option<i64>);

fn edge_identity(edge: &GraphEdge) -> EdgeIdentity {
    (
        edge.from.clone(),
        edge.from_id,
        edge.to.clone(),
        edge.to_id,
        edge.edge_type.clone(),
        edge.depth,
    )
}

fn canonical_json(value: &impl serde::Serialize) -> String {
    let mut value = serde_json::to_value(value).unwrap();
    value.sort_all_objects();
    value.to_string()
}

pub(super) fn with_after(frontend: Frontend, base_query: &str, token: &str) -> String {
    match frontend {
        Frontend::Gql => format!("{} AFTER '{token}'", base_query.trim_end()),
    }
}

pub(super) fn without_cursor(frontend: Frontend, base_query: &str) -> String {
    match frontend {
        Frontend::Gql => {
            let query = strip_after(base_query.trim_end());
            let (prefix, size) = query
                .rsplit_once(char::is_whitespace)
                .expect("paginated GQL must end with PAGE N");
            assert!(
                size.parse::<u32>().is_ok(),
                "paginated GQL must end with PAGE N"
            );
            let (query, page) = prefix
                .trim_end()
                .rsplit_once(char::is_whitespace)
                .expect("paginated GQL must end with PAGE N");
            assert!(
                page.eq_ignore_ascii_case("PAGE"),
                "paginated GQL must end with PAGE N"
            );
            format!("{} LIMIT 1000", query.trim_end())
        }
    }
}

fn strip_after(query: &str) -> &str {
    let Some(body) = query.strip_suffix('\'') else {
        return query;
    };
    let Some((prefix, _)) = body.rsplit_once('\'') else {
        return query;
    };
    let prefix = prefix.trim_end();
    match prefix.len().checked_sub("AFTER".len()) {
        Some(at) if prefix.is_char_boundary(at) && prefix[at..].eq_ignore_ascii_case("AFTER") => {
            prefix[..at].trim_end()
        }
        _ => query,
    }
}

#[derive(Default)]
pub(super) struct PageCollection {
    node_ids: BTreeMap<String, Vec<i64>>,
    group_ids: BTreeMap<String, Vec<i64>>,
    nodes: BTreeSet<(String, i64)>,
    edges: Vec<EdgeIdentity>,
    rows: Vec<String>,
    paths: Vec<Vec<(Option<usize>, EdgeIdentity)>>,
    page_count: usize,
}

impl PageCollection {
    pub(super) fn push(&mut self, response: &GraphResponse) {
        self.page_count += 1;
        for node in &response.nodes {
            self.node_ids
                .entry(node.entity_type.clone())
                .or_default()
                .push(node.id);
            self.nodes.insert((node.entity_type.clone(), node.id));
        }
        let mut paths: BTreeMap<usize, Vec<_>> = BTreeMap::new();
        for edge in &response.edges {
            let identity = edge_identity(edge);
            self.edges.push(identity.clone());
            if let Some(path_id) = edge.path_id {
                paths
                    .entry(path_id)
                    .or_default()
                    .push((edge.step, identity));
            }
        }
        for mut path in paths.into_values() {
            path.sort();
            self.paths.push(path);
        }
        for row in response.rows.iter().flatten() {
            self.rows.push(canonical_json(row));
            for (key, value) in row {
                if let Some(node) = value.as_object() {
                    let entity = node["type"].as_str().expect("group node type");
                    let id = node["id"]
                        .as_str()
                        .expect("group node id")
                        .parse()
                        .expect("integer group node id");
                    self.group_ids
                        .entry(format!("{key}:{entity}"))
                        .or_default()
                        .push(id);
                }
            }
        }
    }

    pub(super) fn assert_expect(&self, expect: &AllPagesExpect, label: &str) {
        if let Some(count) = expect.page_count {
            assert_eq!(self.page_count, count, "{label}: page count mismatch");
        }
        if expect.no_duplicate_ids {
            for (entity, ids) in self.node_ids.iter().chain(&self.group_ids) {
                let unique: BTreeSet<_> = ids.iter().collect();
                assert_eq!(
                    unique.len(),
                    ids.len(),
                    "{label}: duplicate {entity} IDs across pages: {ids:?}"
                );
            }
        }
        for (entity, expected) in &expect.node_order {
            let actual = self.node_ids.get(entity).map_or(&[][..], Vec::as_slice);
            assert_eq!(
                actual, expected,
                "{label}: collected {entity} order mismatch"
            );
        }
        for (actual_ids, expected_ids) in [
            (&self.node_ids, &expect.node_ids),
            (&self.group_ids, &expect.group_node_ids),
        ] {
            for (entity, expected) in expected_ids {
                let mut actual = actual_ids.get(entity).cloned().unwrap_or_default();
                actual.sort();
                actual.dedup();
                assert_eq!(
                    actual, *expected,
                    "{label}: collected {entity} IDs mismatch"
                );
            }
        }
        let edges: BTreeSet<_> = self.edges.iter().collect();
        if expect.no_duplicate_edges {
            assert_eq!(
                edges.len(),
                self.edges.len(),
                "{label}: duplicate typed edges across pages"
            );
        }
        if let Some(count) = expect.edge_count {
            assert_eq!(edges.len(), count, "{label}: collected edge count mismatch");
        }
    }

    pub(super) fn assert_matches(&self, response: &GraphResponse, label: &str) {
        let pagination = response
            .pagination
            .as_ref()
            .expect("pagination should be present");
        assert!(
            !pagination.has_more && !pagination.truncated,
            "{label}: unpaginated comparison exceeds limit 1000"
        );
        let mut full = Self::default();
        full.push(response);
        assert_eq!(
            self.nodes, full.nodes,
            "{label}: paginated node set differs from unpaginated result"
        );
        assert_eq!(
            self.edges.iter().collect::<BTreeSet<_>>(),
            full.edges.iter().collect::<BTreeSet<_>>(),
            "{label}: paginated typed edges differ from unpaginated result"
        );
        let mut rows = self.rows.clone();
        rows.sort();
        full.rows.sort();
        assert_eq!(
            rows, full.rows,
            "{label}: paginated grouped rows differ from unpaginated result"
        );
        let mut paths = self.paths.clone();
        paths.sort();
        full.paths.sort();
        assert_eq!(
            paths, full.paths,
            "{label}: paginated paths differ from unpaginated result"
        );
    }
}

pub(super) fn next_page(
    response: &GraphResponse,
    index: usize,
    count: usize,
    label: &str,
) -> Option<String> {
    let pagination = response
        .pagination
        .as_ref()
        .expect("pagination should be present");
    assert_eq!(
        pagination.has_more,
        pagination.next_cursor.is_some(),
        "{label}: has_more and next_cursor disagree"
    );
    assert_eq!(
        pagination.has_more,
        index + 1 < count,
        "{label}: page sequence must end at exhaustion"
    );
    pagination.next_cursor.clone()
}
