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

/// Splits a paginated GQL statement ending in `SKIP 0 LIMIT n` into the
/// statement prefix and the page size.
fn window(frontend: Frontend, base_query: &str) -> (&str, u32) {
    assert_eq!(frontend, Frontend::Gql, "pages require a GQL query");
    let message = "paginated GQL must end with SKIP 0 LIMIT N";
    let mut tokens = base_query.trim_end().rsplitn(5, char::is_whitespace);
    let size = tokens
        .next()
        .and_then(|size| size.parse().ok())
        .expect(message);
    assert!(
        tokens
            .next()
            .is_some_and(|word| word.eq_ignore_ascii_case("LIMIT")),
        "{message}"
    );
    assert_eq!(tokens.next(), Some("0"), "{message}");
    assert!(
        tokens
            .next()
            .is_some_and(|word| word.eq_ignore_ascii_case("SKIP")),
        "{message}"
    );
    (tokens.next().expect(message).trim_end(), size)
}

pub(super) fn page_query(frontend: Frontend, base_query: &str, index: usize) -> String {
    let (prefix, size) = window(frontend, base_query);
    format!("{prefix} SKIP {} LIMIT {size}", index as u32 * size)
}

pub(super) fn unpaginated(frontend: Frontend, base_query: &str) -> String {
    let (prefix, _) = window(frontend, base_query);
    format!("{prefix} LIMIT 1000")
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

pub(super) fn assert_page_bounds(
    response: &GraphResponse,
    index: usize,
    count: usize,
    label: &str,
) {
    let pagination = response
        .pagination
        .as_ref()
        .expect("pagination should be present");
    assert_eq!(
        pagination.has_more,
        index + 1 < count,
        "{label}: page sequence must end at exhaustion"
    );
}
