use std::collections::{HashMap, HashSet};

use crate::ppr::{KindRates, rank_neighborhood};
use crate::types::{Edge, Graph};

pub const EDGE_LIMIT: usize = 40;

pub struct NodeLabel {
    pub label: String,
    pub loc: String,
}

pub struct ExpandedNeighborhood {
    pub edges: Vec<Edge>,
    pub hidden_by_kind: Vec<(String, usize)>,
    pub surfaced: Vec<(i64, f64)>,
}

pub const SURFACED_POOL: usize = 10;
pub const SURFACED_MIN_SCORE: f64 = 1e-3;
pub const SURFACED_MAX_DEGREE: u64 = 200;

/// Hands over whatever graph it considers relevant for the seeds — the whole
/// thing, an authorized neighborhood, anything between. Ranking cannot tell
/// the difference. Sources that return a partial graph must supply true
/// degrees; `None` means the edge list is complete and degrees derive from it.
/// Returned edges must already be rankable (scoped/local symbols excluded).
pub trait GraphSource {
    type Error;

    fn graph(&self, seeds: &[i64]) -> Result<Graph, Self::Error>;
    fn labels(&self, ids: &[i64]) -> Result<HashMap<i64, NodeLabel>, Self::Error>;
}

pub fn expand_neighborhood<S: GraphSource>(
    source: &S,
    term_seeds: &[Vec<(i64, f64)>],
    kind_rates: &HashMap<String, KindRates>,
    focus: Option<&str>,
) -> Result<ExpandedNeighborhood, S::Error> {
    let mut seed_ids: Vec<i64> = Vec::new();
    let mut seen_seed: HashSet<i64> = HashSet::new();
    for &(id, _) in term_seeds.iter().flatten() {
        if seen_seed.insert(id) {
            seed_ids.push(id);
        }
    }
    let graph = source.graph(&seed_ids)?;
    let ranked = rank_neighborhood(&graph, term_seeds, kind_rates, focus, EDGE_LIMIT);

    let mut label_ids: Vec<i64> = Vec::new();
    let mut seen: HashSet<i64> = HashSet::new();
    for &i in &ranked.selected {
        for id in [graph.edges[i].source, graph.edges[i].target] {
            if seen.insert(id) {
                label_ids.push(id);
            }
        }
    }
    let labels = source.labels(&label_ids)?;
    let display = |id: i64| -> Option<(String, String)> {
        labels
            .get(&id)
            .filter(|l| !l.label.trim().is_empty())
            .map(|l| (l.label.clone(), l.loc.clone()))
    };
    let edges = ranked
        .selected
        .into_iter()
        .filter_map(|i| {
            let e = &graph.edges[i];
            let (source_label, source_loc) = display(e.source)?;
            let (target_label, target_loc) = display(e.target)?;
            Some(Edge {
                kind: graph.kinds[e.kind as usize].clone(),
                source: source_label,
                source_loc,
                target: target_label,
                target_loc,
            })
        })
        .collect();
    let surfaced = ranked
        .node_scores
        .into_iter()
        .filter(|n| {
            n.score >= SURFACED_MIN_SCORE
                && !seen_seed.contains(&n.id)
                && n.degree <= SURFACED_MAX_DEGREE
        })
        .take(SURFACED_POOL)
        .map(|n| (n.id, n.score))
        .collect();
    Ok(ExpandedNeighborhood {
        edges,
        hidden_by_kind: ranked.hidden_by_kind,
        surfaced,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::GraphEdge;

    struct FakeSource {
        kinds: Vec<String>,
        edges: Vec<(u16, i64, i64)>,
        unlabeled: bool,
    }

    impl FakeSource {
        fn new(edges: Vec<(&str, i64, i64)>) -> Self {
            let mut kinds: Vec<String> = Vec::new();
            let edges = edges
                .into_iter()
                .map(|(kind, source, target)| {
                    let idx = kinds.iter().position(|k| k == kind).unwrap_or_else(|| {
                        kinds.push(kind.to_string());
                        kinds.len() - 1
                    });
                    (idx as u16, source, target)
                })
                .collect();
            Self {
                kinds,
                edges,
                unlabeled: false,
            }
        }
    }

    impl GraphSource for FakeSource {
        type Error = std::convert::Infallible;

        fn graph(&self, _seeds: &[i64]) -> Result<Graph, Self::Error> {
            Ok(Graph {
                kinds: self.kinds.clone(),
                edges: self
                    .edges
                    .iter()
                    .map(|&(kind, source, target)| GraphEdge {
                        kind,
                        source,
                        target,
                    })
                    .collect(),
                degrees: None,
            })
        }

        fn labels(&self, ids: &[i64]) -> Result<HashMap<i64, NodeLabel>, Self::Error> {
            if self.unlabeled {
                return Ok(HashMap::new());
            }
            Ok(ids
                .iter()
                .map(|&id| {
                    (
                        id,
                        NodeLabel {
                            label: format!("label_{id}"),
                            loc: String::new(),
                        },
                    )
                })
                .collect())
        }
    }

    fn seeds(ids: &[i64]) -> Vec<Vec<(i64, f64)>> {
        vec![ids.iter().map(|&id| (id, 1.0)).collect()]
    }

    fn weights() -> HashMap<String, KindRates> {
        HashMap::from([("CALLS".to_string(), KindRates::new(1.0))])
    }

    #[test]
    fn expansion_ranks_whatever_graph_the_source_hands_over() {
        let source = FakeSource::new(vec![("CALLS", 1, 2), ("CALLS", 2, 3)]);
        let expanded = expand_neighborhood(&source, &seeds(&[1]), &weights(), None).unwrap();
        let mut rendered: Vec<String> = expanded
            .edges
            .iter()
            .map(|e| format!("{} -> {}", e.source, e.target))
            .collect();
        rendered.sort();
        assert_eq!(rendered, vec!["label_1 -> label_2", "label_2 -> label_3"]);

        let mut source = FakeSource::new(vec![("CALLS", 1, 42)]);
        source.unlabeled = true;
        let expanded = expand_neighborhood(&source, &seeds(&[1]), &weights(), None).unwrap();
        assert_eq!(
            expanded.edges[0].target, "42",
            "unlabeled nodes fall back to ids"
        );
    }

    #[test]
    fn surfaced_nodes_come_from_consensus_not_from_seeds_or_hubs() {
        let mut edges = vec![("CALLS", 1, 2)];
        for i in 0..(SURFACED_MAX_DEGREE + 10) {
            edges.push(("CALLS", 1000 + i as i64, 3));
        }
        edges.push(("CALLS", 2, 3));
        let source = FakeSource::new(edges);
        let expanded = expand_neighborhood(&source, &seeds(&[1]), &weights(), None).unwrap();
        assert!(
            expanded.surfaced.iter().any(|&(id, _)| id == 2),
            "reachable non-seed must surface"
        );
        assert!(
            expanded.surfaced.iter().all(|&(id, _)| id != 1),
            "seeds never surface"
        );
        assert!(
            expanded.surfaced.iter().all(|&(id, _)| id != 3),
            "hubs above the degree ceiling never surface"
        );
    }
}
