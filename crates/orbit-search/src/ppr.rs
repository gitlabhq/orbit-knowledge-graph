use std::collections::HashMap;

pub const PPR_DAMPING: f64 = 0.85;
pub const DEFAULT_EDGE_WEIGHT: f64 = 0.5;
pub const REVERSE_EDGE_FACTOR: f64 = 0.5;
pub const FOCUS_BOOST: f64 = 2.0;

const TOLERANCE: f64 = 1e-6;
const MAX_ITERATIONS: usize = 100;

pub struct SubGraph {
    out: Vec<Vec<(usize, f64)>>,
}

impl SubGraph {
    pub fn new(node_count: usize) -> Self {
        Self {
            out: vec![Vec::new(); node_count],
        }
    }

    pub fn node_count(&self) -> usize {
        self.out.len()
    }

    pub fn add_edge(&mut self, source: usize, target: usize, weight: f64) {
        self.out[source].push((target, weight));
    }
}

pub fn edge_weight(kind_weight: f64, target_degree: u64) -> f64 {
    kind_weight / (1.0 + (1.0 + target_degree as f64).ln())
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct NeighborhoodEdge {
    pub kind: String,
    pub source: String,
    pub target: String,
}

pub struct RankedNeighborhood {
    pub selected: Vec<usize>,
    pub hidden_by_kind: Vec<(String, usize)>,
    pub node_scores: Vec<(String, f64)>,
}

const CONSENSUS_EPSILON: f64 = 1e-9;
pub const CONSENSUS_QUORUM: f64 = 0.6;

pub fn rank_neighborhood(
    edges: &[NeighborhoodEdge],
    degrees: &HashMap<String, u64>,
    term_seeds: &[Vec<(String, f64)>],
    kind_weights: &HashMap<String, f64>,
    focus: Option<&str>,
    cap: usize,
) -> RankedNeighborhood {
    let mut index: HashMap<&str, usize> = HashMap::new();
    let mut node_count = 0usize;
    for (id, _) in term_seeds.iter().flatten() {
        index.entry(id.as_str()).or_insert_with(|| {
            node_count += 1;
            node_count - 1
        });
    }
    for e in edges {
        for id in [e.source.as_str(), e.target.as_str()] {
            index.entry(id).or_insert_with(|| {
                node_count += 1;
                node_count - 1
            });
        }
    }

    let kind_weight = |kind: &str| -> f64 {
        if focus == Some(kind) {
            return FOCUS_BOOST;
        }
        kind_weights
            .get(kind)
            .copied()
            .unwrap_or(DEFAULT_EDGE_WEIGHT)
    };
    let degree = |id: &str| degrees.get(id).copied().unwrap_or(0);

    let mut graph = SubGraph::new(node_count);
    for e in edges {
        let s = index[e.source.as_str()];
        let t = index[e.target.as_str()];
        let kw = kind_weight(&e.kind);
        graph.add_edge(s, t, edge_weight(kw, degree(&e.target)));
        graph.add_edge(
            t,
            s,
            REVERSE_EDGE_FACTOR * edge_weight(kw, degree(&e.source)),
        );
    }

    let per_term_scores: Vec<Vec<f64>> = term_seeds
        .iter()
        .filter(|set| set.iter().any(|&(_, w)| w > 0.0))
        .map(|set| {
            let seed_indices: Vec<(usize, f64)> =
                set.iter().map(|(id, w)| (index[id.as_str()], *w)).collect();
            personalized_pagerank(&graph, &seed_indices, PPR_DAMPING)
        })
        .collect();
    if per_term_scores.is_empty() {
        return RankedNeighborhood {
            selected: Vec::new(),
            hidden_by_kind: Vec::new(),
            node_scores: Vec::new(),
        };
    }
    let term_count = per_term_scores.len();
    let quorum = ((term_count as f64 * CONSENSUS_QUORUM).ceil() as usize).clamp(1, term_count);
    let scores: Vec<f64> = (0..node_count)
        .map(|v| {
            let mut logs: Vec<f64> = per_term_scores
                .iter()
                .map(|r| (r[v] + CONSENSUS_EPSILON).ln())
                .collect();
            logs.sort_by(|a, b| b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal));
            let quorum_sum: f64 = logs[..quorum].iter().sum();
            (quorum_sum / quorum as f64).exp()
        })
        .collect();

    let mut order: Vec<(usize, f64)> = edges
        .iter()
        .enumerate()
        .map(|(i, e)| {
            (
                i,
                scores[index[e.source.as_str()]] + scores[index[e.target.as_str()]],
            )
        })
        .collect();
    order.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

    let mut selected: Vec<(usize, f64)> = order.into_iter().take(cap).collect();
    selected.sort_by(|&(a, sa), &(b, sb)| {
        let ka = &edges[a].kind;
        let kb = &edges[b].kind;
        let fa = focus != Some(ka.as_str());
        let fb = focus != Some(kb.as_str());
        fa.cmp(&fb)
            .then_with(|| ka.cmp(kb))
            .then_with(|| sb.partial_cmp(&sa).unwrap_or(std::cmp::Ordering::Equal))
    });

    let mut shown: HashMap<&str, usize> = HashMap::new();
    for &(i, _) in &selected {
        *shown.entry(edges[i].kind.as_str()).or_insert(0) += 1;
    }
    let mut totals: HashMap<&str, usize> = HashMap::new();
    for e in edges {
        *totals.entry(e.kind.as_str()).or_insert(0) += 1;
    }
    let mut hidden_by_kind: Vec<(String, usize)> = totals
        .into_iter()
        .filter_map(|(kind, total)| {
            let hidden = total - shown.get(kind).copied().unwrap_or(0);
            (hidden > 0).then(|| (kind.to_string(), hidden))
        })
        .collect();
    hidden_by_kind.sort();

    let mut node_scores: Vec<(String, f64)> = index
        .into_iter()
        .map(|(id, i)| (id.to_string(), scores[i]))
        .filter(|&(_, s)| s > CONSENSUS_EPSILON * 1.5)
        .collect();
    node_scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

    RankedNeighborhood {
        selected: selected.into_iter().map(|(i, _)| i).collect(),
        hidden_by_kind,
        node_scores,
    }
}

pub fn personalized_pagerank(graph: &SubGraph, seeds: &[(usize, f64)], damping: f64) -> Vec<f64> {
    let n = graph.node_count();
    let mut teleport = vec![0.0; n];
    let seed_total: f64 = seeds.iter().map(|&(_, w)| w.max(0.0)).sum();
    if n == 0 || seed_total <= 0.0 {
        return teleport;
    }
    for &(i, w) in seeds {
        if w > 0.0 {
            teleport[i] += w / seed_total;
        }
    }
    let out_totals: Vec<f64> = graph
        .out
        .iter()
        .map(|edges| edges.iter().map(|&(_, w)| w).sum())
        .collect();
    let mut score = teleport.clone();
    for _ in 0..MAX_ITERATIONS {
        let mut next = vec![0.0; n];
        let mut dangling = 0.0;
        for (i, edges) in graph.out.iter().enumerate() {
            if out_totals[i] <= 0.0 {
                dangling += score[i];
                continue;
            }
            for &(j, w) in edges {
                next[j] += score[i] * w / out_totals[i];
            }
        }
        let mut delta = 0.0;
        for i in 0..n {
            let updated =
                (1.0 - damping) * teleport[i] + damping * (next[i] + dangling * teleport[i]);
            delta += (updated - score[i]).abs();
            score[i] = updated;
        }
        if delta < TOLERANCE {
            break;
        }
    }
    score
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cycle(n: usize) -> SubGraph {
        let mut g = SubGraph::new(n);
        for i in 0..n {
            g.add_edge(i, (i + 1) % n, 1.0);
        }
        g
    }

    #[test]
    fn seed_weights_bias_scores_and_uniform_seeds_stay_uniform() {
        let g = cycle(4);
        let scores = personalized_pagerank(&g, &[(1, 1.0)], PPR_DAMPING);
        for i in [0, 2, 3] {
            assert!(scores[1] > scores[i], "scores were {scores:?}");
        }

        let scores = personalized_pagerank(&g, &[(0, 3.0), (2, 1.0)], PPR_DAMPING);
        assert!(scores[0] > scores[2], "scores were {scores:?}");

        let uniform: Vec<(usize, f64)> = (0..4).map(|i| (i, 1.0)).collect();
        let scores = personalized_pagerank(&g, &uniform, PPR_DAMPING);
        assert!((scores.iter().sum::<f64>() - 1.0).abs() < 1e-6);
        for s in &scores {
            assert!((s - 0.25).abs() < 1e-6, "scores were {scores:?}");
        }
    }

    #[test]
    fn dangling_mass_returns_to_the_seed_instead_of_leaking() {
        let mut g = SubGraph::new(2);
        g.add_edge(0, 1, 1.0);
        let scores = personalized_pagerank(&g, &[(0, 1.0)], PPR_DAMPING);
        assert!((scores.iter().sum::<f64>() - 1.0).abs() < 1e-6);
        assert!(scores[0] > scores[1], "scores were {scores:?}");
    }

    #[test]
    fn edge_weight_steers_rank_toward_heavy_edges_and_quiet_targets() {
        assert!(edge_weight(1.0, 0) > edge_weight(1.0, 10));
        assert!(edge_weight(1.0, 10) > edge_weight(1.0, 1000));
        assert!(edge_weight(1.0, 5) > edge_weight(0.4, 5));

        let mut g = SubGraph::new(3);
        g.add_edge(0, 1, edge_weight(1.0, 1000));
        g.add_edge(0, 2, edge_weight(0.9, 2));
        let scores = personalized_pagerank(&g, &[(0, 1.0)], PPR_DAMPING);
        assert!(scores[2] > scores[1], "scores were {scores:?}");
    }

    #[test]
    fn degenerate_inputs_return_zeros_or_empty() {
        let g = cycle(3);
        assert_eq!(personalized_pagerank(&g, &[], PPR_DAMPING), vec![0.0; 3]);
        assert_eq!(
            personalized_pagerank(&g, &[(0, 0.0), (1, -1.0)], PPR_DAMPING),
            vec![0.0; 3]
        );
        assert!(personalized_pagerank(&SubGraph::new(0), &[(0, 1.0)], PPR_DAMPING).is_empty());
    }

    fn edge(kind: &str, source: &str, target: &str) -> NeighborhoodEdge {
        NeighborhoodEdge {
            kind: kind.to_string(),
            source: source.to_string(),
            target: target.to_string(),
        }
    }

    fn ranked_top(
        edges: &[NeighborhoodEdge],
        degrees: &HashMap<String, u64>,
        focus: Option<&str>,
        cap: usize,
    ) -> RankedNeighborhood {
        let weights = HashMap::from([("CALLS".to_string(), 1.0), ("CONTAINS".to_string(), 0.4)]);
        rank_neighborhood(
            edges,
            degrees,
            &[vec![("seed".to_string(), 1.0)]],
            &weights,
            focus,
            cap,
        )
    }

    #[test]
    fn rank_neighborhood_ranks_by_proximity_kind_weight_and_target_degree() {
        let near_vs_far = [
            edge("CALLS", "seed", "near"),
            edge("CALLS", "far_a", "far_b"),
        ];
        let ranked = ranked_top(&near_vs_far, &HashMap::new(), None, 1);
        assert_eq!(ranked.selected, vec![0]);
        assert_eq!(ranked.hidden_by_kind, vec![("CALLS".to_string(), 1)]);

        let call_vs_contain = [
            edge("CONTAINS", "seed", "parent"),
            edge("CALLS", "seed", "callee"),
        ];
        assert_eq!(
            ranked_top(&call_vs_contain, &HashMap::new(), None, 1).selected,
            vec![1]
        );

        let hub_vs_quiet = [edge("CALLS", "seed", "hub"), edge("CALLS", "seed", "quiet")];
        let degrees = HashMap::from([("hub".to_string(), 5000), ("quiet".to_string(), 3)]);
        assert_eq!(
            ranked_top(&hub_vs_quiet, &degrees, None, 1).selected,
            vec![1]
        );
    }

    #[test]
    fn focus_outweighs_declared_kind_weights_and_leads_the_grouping() {
        let edges = [
            edge("CALLS", "seed", "callee"),
            edge("CONTAINS", "seed", "parent"),
        ];
        assert_eq!(
            ranked_top(&edges, &HashMap::new(), Some("CONTAINS"), 1).selected,
            vec![1]
        );

        let edges = [
            edge("CONTAINS", "seed", "a"),
            edge("CALLS", "seed", "b"),
            edge("CONTAINS", "seed", "c"),
        ];
        let ranked = ranked_top(&edges, &HashMap::new(), Some("CONTAINS"), 3);
        let kinds: Vec<&str> = ranked
            .selected
            .iter()
            .map(|&i| edges[i].kind.as_str())
            .collect();
        assert_eq!(kinds, vec!["CONTAINS", "CONTAINS", "CALLS"]);
        assert!(ranked.hidden_by_kind.is_empty());
    }

    #[test]
    fn triangulation_surfaces_the_node_connected_to_multiple_weak_seeds() {
        let edges = [
            edge("CALLS", "seed_a", "answer"),
            edge("CALLS", "seed_b", "answer"),
            edge("CALLS", "seed_a", "satellite_a"),
            edge("CALLS", "seed_b", "satellite_b"),
        ];
        let weights = HashMap::from([("CALLS".to_string(), 1.0)]);
        let ranked = rank_neighborhood(
            &edges,
            &HashMap::new(),
            &[
                vec![("seed_a".to_string(), 1.0)],
                vec![("seed_b".to_string(), 1.0)],
            ],
            &weights,
            None,
            4,
        );
        let non_seed_top = ranked
            .node_scores
            .iter()
            .find(|(id, _)| !id.starts_with("seed"))
            .unwrap();
        assert_eq!(
            non_seed_top.0, "answer",
            "scores were {:?}",
            ranked.node_scores
        );
    }

    #[test]
    fn quorum_consensus_forgives_filler_terms_but_still_requires_agreement() {
        let edges = [
            edge("CALLS", "seed_a", "answer"),
            edge("CALLS", "seed_b", "answer"),
            edge("CALLS", "seed_a", "satellite"),
        ];
        let weights = HashMap::from([("CALLS".to_string(), 1.0)]);
        let ranked = rank_neighborhood(
            &edges,
            &HashMap::new(),
            &[
                vec![("seed_a".to_string(), 1.0)],
                vec![("seed_b".to_string(), 1.0)],
                vec![("filler".to_string(), 1.0)],
            ],
            &weights,
            None,
            4,
        );
        let score_of = |id: &str| {
            ranked
                .node_scores
                .iter()
                .find(|(n, _)| n == id)
                .map(|&(_, s)| s)
                .unwrap_or(0.0)
        };
        assert!(
            score_of("answer") > score_of("satellite"),
            "two-term consensus must beat a single-term satellite despite the dead filler term; scores were {:?}",
            ranked.node_scores
        );
    }

    #[test]
    fn rank_neighborhood_reaches_two_hops_out() {
        let edges = [
            edge("CALLS", "seed", "mid"),
            edge("CALLS", "mid", "far"),
            edge("CALLS", "stray_a", "stray_b"),
        ];
        assert_eq!(
            ranked_top(&edges, &HashMap::new(), None, 2).selected,
            vec![0, 1]
        );
    }
}
