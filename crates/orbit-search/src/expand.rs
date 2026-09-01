use std::collections::HashMap;

use crate::ppr::{KindRates, rank_neighborhood};
use crate::types::{Edge, Graph, TermSeeds, dedupe_ids};

pub const EDGE_LIMIT: usize = 20;

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

pub trait GraphSource {
    type Error;

    fn graph(&self, seeds: &[i64]) -> Result<Graph, Self::Error>;
    fn labels(&self, ids: &[i64]) -> Result<HashMap<i64, NodeLabel>, Self::Error>;
}

pub fn expand_neighborhood<S: GraphSource>(
    source: &S,
    term_seeds: &[TermSeeds],
    kind_rates: &HashMap<String, KindRates>,
    focus: Option<&str>,
) -> Result<ExpandedNeighborhood, S::Error> {
    let seed_ids = dedupe_ids(
        term_seeds
            .iter()
            .flat_map(|t| t.seeds.iter().map(|&(id, _)| id)),
    );
    let graph = source.graph(&seed_ids)?;
    let ranked = rank_neighborhood(&graph, term_seeds, kind_rates, focus, EDGE_LIMIT);

    let label_ids = dedupe_ids(
        ranked
            .selected
            .iter()
            .flat_map(|&i| [graph.edges[i].source, graph.edges[i].target]),
    );
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
        .filter(|n| !seed_ids.contains(&n.id))
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

    fn seeds(ids: &[i64]) -> Vec<TermSeeds> {
        vec![TermSeeds {
            seeds: ids.iter().map(|&id| (id, 1.0)).collect(),
            weight: 1.0,
        }]
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
        assert!(
            expanded.edges.is_empty(),
            "edges with unresolvable labels are dropped"
        );
    }

    #[test]
    fn surfaced_nodes_exclude_seeds() {
        let source = FakeSource::new(vec![("CALLS", 1, 2), ("CALLS", 2, 3)]);
        let expanded = expand_neighborhood(&source, &seeds(&[1]), &weights(), None).unwrap();
        assert!(
            expanded.surfaced.iter().any(|&(id, _)| id == 2),
            "reachable non-seed must surface"
        );
        assert!(
            expanded.surfaced.iter().all(|&(id, _)| id != 1),
            "seeds never surface"
        );
    }
}
