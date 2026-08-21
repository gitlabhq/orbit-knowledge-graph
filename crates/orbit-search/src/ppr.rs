use std::collections::HashMap;

pub const PPR_DAMPING: f64 = 0.85;
pub const DEFAULT_EDGE_WEIGHT: f64 = 0.5;
pub const REVERSE_EDGE_FACTOR: f64 = 0.5;
pub const FOCUS_WEIGHT: f64 = 1.0;
pub const MAX_KIND_WEIGHT: f64 = 1.0;

const TOLERANCE: f64 = 1e-6;
const MAX_ITERATIONS: usize = 100;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct KindRates {
    pub forward: f64,
    pub reverse: f64,
}

impl KindRates {
    pub fn new(forward: f64) -> Self {
        Self {
            forward,
            reverse: REVERSE_EDGE_FACTOR * forward,
        }
    }
}

impl Default for KindRates {
    fn default() -> Self {
        Self::new(DEFAULT_EDGE_WEIGHT)
    }
}

pub struct SubGraph {
    out: Vec<Vec<(usize, f64)>>,
    out_totals: Vec<f64>,
}

impl SubGraph {
    pub fn new(node_count: usize) -> Self {
        Self {
            out: vec![Vec::new(); node_count],
            out_totals: vec![0.0; node_count],
        }
    }

    pub fn node_count(&self) -> usize {
        self.out.len()
    }

    pub fn add_edge(&mut self, source: usize, target: usize, weight: f64) {
        self.out[source].push((target, weight));
        self.out_totals[source] += weight;
    }
}

fn hub_damping(degree: u64) -> f64 {
    1.0 / (1.0 + (1.0 + degree as f64).ln())
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
    kind_rates: &HashMap<String, KindRates>,
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

    let rates = |kind: &str| -> KindRates {
        let mut r = kind_rates.get(kind).copied().unwrap_or_default();
        r.forward = r.forward.clamp(0.0, MAX_KIND_WEIGHT);
        r.reverse = r.reverse.clamp(0.0, MAX_KIND_WEIGHT);
        if focus == Some(kind) {
            r.forward = FOCUS_WEIGHT;
        }
        r
    };
    let degree = |id: &str| degrees.get(id).copied().unwrap_or(0);

    let mut fwd_pools: HashMap<(usize, &str), f64> = HashMap::new();
    let mut rev_pools: HashMap<(usize, &str), f64> = HashMap::new();
    for e in edges {
        let s = index[e.source.as_str()];
        let t = index[e.target.as_str()];
        *fwd_pools.entry((s, e.kind.as_str())).or_insert(0.0) += hub_damping(degree(&e.target));
        *rev_pools.entry((t, e.kind.as_str())).or_insert(0.0) += hub_damping(degree(&e.source));
    }
    // ObjectRank Eq. 4: each (node, kind, direction) group is an independent
    // authority budget. The split within a group is proportional to hub
    // damping instead of the paper's even 1/OutDeg split.
    let placed: Vec<(usize, usize, f64, f64)> = edges
        .iter()
        .map(|e| {
            let s = index[e.source.as_str()];
            let t = index[e.target.as_str()];
            let r = rates(&e.kind);
            let fwd = r.forward * hub_damping(degree(&e.target)) / fwd_pools[&(s, e.kind.as_str())];
            let rev = r.reverse * hub_damping(degree(&e.source)) / rev_pools[&(t, e.kind.as_str())];
            (s, t, fwd, rev)
        })
        .collect();
    let mut graph = SubGraph::new(node_count);
    for &(s, t, fwd, rev) in &placed {
        graph.add_edge(s, t, fwd);
        graph.add_edge(t, s, rev);
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

    let flux = |from: usize, weight: f64| {
        let total = graph.out_totals[from];
        if total > 0.0 {
            scores[from] * weight / total.max(1.0)
        } else {
            0.0
        }
    };
    let mut order: Vec<(usize, f64)> = placed
        .iter()
        .enumerate()
        .map(|(i, &(s, t, fwd, rev))| (i, flux(s, fwd) + flux(t, rev)))
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
    node_scores.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });

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
    let mut score = teleport.clone();
    for _ in 0..MAX_ITERATIONS {
        let mut next = vec![0.0; n];
        let mut dangling = 0.0;
        for (i, edges) in graph.out.iter().enumerate() {
            let total = graph.out_totals[i];
            if total <= 0.0 {
                dangling += score[i];
                continue;
            }
            let norm = total.max(1.0);
            for &(j, w) in edges {
                next[j] += score[i] * w / norm;
            }
            dangling += score[i] * (1.0 - total / norm);
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

    fn seed() -> Vec<Vec<(String, f64)>> {
        vec![vec![("seed".to_string(), 1.0)]]
    }

    fn edge(kind: &str, source: &str, target: &str) -> NeighborhoodEdge {
        NeighborhoodEdge {
            kind: kind.to_string(),
            source: source.to_string(),
            target: target.to_string(),
        }
    }

    #[test]
    fn low_transfer_rates_leak_mass_back_to_seeds() {
        let mut full = SubGraph::new(2);
        full.add_edge(0, 1, 1.0);
        let mut leaky = SubGraph::new(2);
        leaky.add_edge(0, 1, 0.2);
        let full_scores = personalized_pagerank(&full, &[(0, 1.0)], PPR_DAMPING);
        let leaky_scores = personalized_pagerank(&leaky, &[(0, 1.0)], PPR_DAMPING);
        assert!(leaky_scores[1] < full_scores[1]);
        assert!(leaky_scores[0] > full_scores[0]);
        assert!((leaky_scores.iter().sum::<f64>() - 1.0).abs() < 1e-6);
    }

    #[test]
    fn kind_rates_are_independent_budgets() {
        let mut edges = vec![edge("CALLS", "seed", "a")];
        for i in 0..10 {
            edges.push(edge("CONTAINS", "seed", &format!("b{i}")));
        }
        let rates = HashMap::from([
            ("CALLS".to_string(), KindRates::new(0.6)),
            ("CONTAINS".to_string(), KindRates::new(0.2)),
        ]);
        let ranked = rank_neighborhood(&edges, &HashMap::new(), &seed(), &rates, None, 20);
        let score = |id: &str| {
            ranked
                .node_scores
                .iter()
                .find(|(n, _)| n == id)
                .map(|&(_, s)| s)
                .unwrap()
        };
        assert!(
            score("a") > 20.0 * score("b0"),
            "CONTAINS fan-out must not dilute the CALLS budget"
        );
    }

    #[test]
    fn zero_reverse_rate_blocks_backward_authority_flow() {
        let edges = vec![edge("CITES", "a", "seed")];
        let one_way = HashMap::from([(
            "CITES".to_string(),
            KindRates {
                forward: 0.7,
                reverse: 0.0,
            },
        )]);
        let ranked = rank_neighborhood(&edges, &HashMap::new(), &seed(), &one_way, None, 5);
        assert!(ranked.node_scores.iter().all(|(id, _)| id != "a"));

        let both_ways = HashMap::from([("CITES".to_string(), KindRates::new(0.7))]);
        let ranked = rank_neighborhood(&edges, &HashMap::new(), &seed(), &both_ways, None, 5);
        assert!(ranked.node_scores.iter().any(|(id, _)| id == "a"));
    }

    #[test]
    fn flux_selection_prefers_focused_kind_edges() {
        let edges = vec![edge("CONTAINS", "seed", "b"), edge("CALLS", "seed", "a")];
        let rates = HashMap::from([
            ("CALLS".to_string(), KindRates::new(0.9)),
            ("CONTAINS".to_string(), KindRates::new(0.9)),
        ]);
        let ranked = rank_neighborhood(&edges, &HashMap::new(), &seed(), &rates, Some("CALLS"), 1);
        assert_eq!(ranked.selected, vec![1]);
    }

    #[test]
    fn pagerank_normalizes_and_handles_degenerate_inputs() {
        let scores = personalized_pagerank(&cycle(4), &[(1, 1.0)], PPR_DAMPING);
        assert!((scores.iter().sum::<f64>() - 1.0).abs() < 1e-6);

        let g = cycle(3);
        assert_eq!(personalized_pagerank(&g, &[], PPR_DAMPING), vec![0.0; 3]);
        assert_eq!(
            personalized_pagerank(&g, &[(0, 0.0), (1, -1.0)], PPR_DAMPING),
            vec![0.0; 3]
        );
        assert!(personalized_pagerank(&SubGraph::new(0), &[(0, 1.0)], PPR_DAMPING).is_empty());
    }

    #[test]
    fn node_scores_break_ties_by_id() {
        let edges: Vec<NeighborhoodEdge> = ["b", "a", "d", "c"]
            .iter()
            .map(|t| edge("CALLS", "seed", t))
            .collect();
        let rates = HashMap::from([("CALLS".to_string(), KindRates::new(1.0))]);
        let ranked = rank_neighborhood(&edges, &HashMap::new(), &seed(), &rates, None, 10);
        let pos = |id: &str| {
            ranked
                .node_scores
                .iter()
                .position(|(n, _)| n == id)
                .unwrap()
        };
        assert!(pos("a") < pos("b"));
        assert!(pos("b") < pos("c"));
        assert!(pos("c") < pos("d"));
    }

    #[test]
    fn rank_neighborhood_respects_the_cap_and_reports_hidden_counts() {
        let edges: Vec<NeighborhoodEdge> = (0..5)
            .map(|i| edge("CALLS", "seed", &format!("t{i}")))
            .collect();
        let rates = HashMap::from([("CALLS".to_string(), KindRates::new(1.0))]);
        let ranked = rank_neighborhood(&edges, &HashMap::new(), &seed(), &rates, None, 2);
        assert_eq!(ranked.selected.len(), 2);
        assert_eq!(ranked.hidden_by_kind, vec![("CALLS".to_string(), 3)]);
        assert!(ranked.node_scores.iter().all(|&(_, s)| s > 0.0));

        let empty = rank_neighborhood(&edges, &HashMap::new(), &[], &rates, None, 2);
        assert!(empty.selected.is_empty() && empty.node_scores.is_empty());
    }
}
