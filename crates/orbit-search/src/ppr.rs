use std::collections::HashMap;

use crate::types::Graph;

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

pub struct Transitions {
    out: Vec<Vec<(usize, f64)>>,
    out_totals: Vec<f64>,
}

impl Transitions {
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
        if weight > 0.0 {
            self.out[source].push((target, weight));
            self.out_totals[source] += weight;
        }
    }
}

fn hub_damping(degree: u64) -> f64 {
    1.0 / (1.0 + (1.0 + degree as f64).ln())
}

pub struct RankedNeighborhood {
    pub selected: Vec<usize>,
    pub hidden_by_kind: Vec<(String, usize)>,
    pub node_scores: Vec<(i64, f64)>,
}

const CONSENSUS_EPSILON: f64 = 1e-9;
pub const CONSENSUS_QUORUM: f64 = 0.6;

pub fn rank_neighborhood(
    graph: &Graph,
    degrees: &HashMap<i64, u64>,
    term_seeds: &[Vec<(i64, f64)>],
    kind_rates: &HashMap<String, KindRates>,
    focus: Option<&str>,
    cap: usize,
) -> RankedNeighborhood {
    let mut index: HashMap<i64, usize> = HashMap::new();
    let mut ids: Vec<i64> = Vec::new();
    let mut intern = |id: i64, ids: &mut Vec<i64>, index: &mut HashMap<i64, usize>| -> usize {
        *index.entry(id).or_insert_with(|| {
            ids.push(id);
            ids.len() - 1
        })
    };
    for &(id, _) in term_seeds.iter().flatten() {
        intern(id, &mut ids, &mut index);
    }
    for e in &graph.edges {
        intern(e.source, &mut ids, &mut index);
        intern(e.target, &mut ids, &mut index);
    }
    let node_count = ids.len();

    let rates_by_kind: Vec<KindRates> = graph
        .kinds
        .iter()
        .map(|kind| {
            let mut r = kind_rates.get(kind).copied().unwrap_or_default();
            r.forward = r.forward.clamp(0.0, MAX_KIND_WEIGHT);
            r.reverse = r.reverse.clamp(0.0, MAX_KIND_WEIGHT);
            if focus == Some(kind.as_str()) {
                r.forward = FOCUS_WEIGHT;
            }
            r
        })
        .collect();
    let degree = |id: i64| degrees.get(&id).copied().unwrap_or(0);

    let mut fwd_pools: HashMap<(usize, u16), f64> = HashMap::new();
    let mut rev_pools: HashMap<(usize, u16), f64> = HashMap::new();
    for e in &graph.edges {
        let s = index[&e.source];
        let t = index[&e.target];
        *fwd_pools.entry((s, e.kind)).or_insert(0.0) += hub_damping(degree(e.target));
        *rev_pools.entry((t, e.kind)).or_insert(0.0) += hub_damping(degree(e.source));
    }
    // ObjectRank Eq. 4: each (node, kind, direction) group is an independent
    // authority budget. The split within a group is proportional to hub
    // damping instead of the paper's even 1/OutDeg split.
    let placed: Vec<(usize, usize, f64, f64)> = graph
        .edges
        .iter()
        .map(|e| {
            let s = index[&e.source];
            let t = index[&e.target];
            let r = rates_by_kind[e.kind as usize];
            let fwd = r.forward * hub_damping(degree(e.target)) / fwd_pools[&(s, e.kind)];
            let rev = r.reverse * hub_damping(degree(e.source)) / rev_pools[&(t, e.kind)];
            (s, t, fwd, rev)
        })
        .collect();
    let mut transitions = Transitions::new(node_count);
    for &(s, t, fwd, rev) in &placed {
        transitions.add_edge(s, t, fwd);
        transitions.add_edge(t, s, rev);
    }

    let per_term_scores: Vec<Vec<f64>> = term_seeds
        .iter()
        .filter(|set| set.iter().any(|&(_, w)| w > 0.0))
        .map(|set| {
            let seed_indices: Vec<(usize, f64)> =
                set.iter().map(|&(id, w)| (index[&id], w)).collect();
            personalized_pagerank(&transitions, &seed_indices, PPR_DAMPING)
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
        let total = transitions.out_totals[from];
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

    let focus_kind: Option<u16> =
        focus.and_then(|f| graph.kinds.iter().position(|k| k == f).map(|i| i as u16));
    let mut selected: Vec<(usize, f64)> = order.into_iter().take(cap).collect();
    selected.sort_by(|&(a, sa), &(b, sb)| {
        let ka = graph.edges[a].kind;
        let kb = graph.edges[b].kind;
        let fa = focus_kind != Some(ka);
        let fb = focus_kind != Some(kb);
        fa.cmp(&fb)
            .then_with(|| graph.kinds[ka as usize].cmp(&graph.kinds[kb as usize]))
            .then_with(|| sb.partial_cmp(&sa).unwrap_or(std::cmp::Ordering::Equal))
    });

    let mut shown: HashMap<u16, usize> = HashMap::new();
    for &(i, _) in &selected {
        *shown.entry(graph.edges[i].kind).or_insert(0) += 1;
    }
    let mut totals: HashMap<u16, usize> = HashMap::new();
    for e in &graph.edges {
        *totals.entry(e.kind).or_insert(0) += 1;
    }
    let mut hidden_by_kind: Vec<(String, usize)> = totals
        .into_iter()
        .filter_map(|(kind, total)| {
            let hidden = total - shown.get(&kind).copied().unwrap_or(0);
            (hidden > 0).then(|| (graph.kinds[kind as usize].clone(), hidden))
        })
        .collect();
    hidden_by_kind.sort();

    let mut node_scores: Vec<(i64, f64)> = ids
        .iter()
        .enumerate()
        .map(|(i, &id)| (id, scores[i]))
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

pub fn personalized_pagerank(
    transitions: &Transitions,
    seeds: &[(usize, f64)],
    damping: f64,
) -> Vec<f64> {
    let n = transitions.node_count();
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
        for (i, edges) in transitions.out.iter().enumerate() {
            let total = transitions.out_totals[i];
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
    use crate::types::GraphEdge;

    fn cycle(n: usize) -> Transitions {
        let mut g = Transitions::new(n);
        for i in 0..n {
            g.add_edge(i, (i + 1) % n, 1.0);
        }
        g
    }

    fn seed() -> Vec<Vec<(i64, f64)>> {
        vec![vec![(0, 1.0)]]
    }

    fn graph(kinds: &[&str], edges: &[(&str, i64, i64)]) -> Graph {
        Graph {
            kinds: kinds.iter().map(|k| (*k).to_string()).collect(),
            edges: edges
                .iter()
                .map(|&(kind, source, target)| GraphEdge {
                    kind: kinds.iter().position(|k| *k == kind).unwrap() as u16,
                    source,
                    target,
                })
                .collect(),
            degrees: None,
        }
    }

    #[test]
    fn low_transfer_rates_leak_mass_back_to_seeds() {
        let mut full = Transitions::new(2);
        full.add_edge(0, 1, 1.0);
        let mut leaky = Transitions::new(2);
        leaky.add_edge(0, 1, 0.2);
        let full_scores = personalized_pagerank(&full, &[(0, 1.0)], PPR_DAMPING);
        let leaky_scores = personalized_pagerank(&leaky, &[(0, 1.0)], PPR_DAMPING);
        assert!(leaky_scores[1] < full_scores[1]);
        assert!(leaky_scores[0] > full_scores[0]);
        assert!((leaky_scores.iter().sum::<f64>() - 1.0).abs() < 1e-6);
    }

    #[test]
    fn kind_rates_are_independent_budgets() {
        let mut edges = vec![("CALLS", 0i64, 1i64)];
        let mut containments: Vec<(String, i64, i64)> = Vec::new();
        for i in 0..10 {
            containments.push(("CONTAINS".to_string(), 0, 10 + i));
        }
        let all: Vec<(&str, i64, i64)> = edges
            .drain(..)
            .chain(containments.iter().map(|(k, s, t)| (k.as_str(), *s, *t)))
            .collect();
        let g = graph(&["CALLS", "CONTAINS"], &all);
        let rates = HashMap::from([
            ("CALLS".to_string(), KindRates::new(0.6)),
            ("CONTAINS".to_string(), KindRates::new(0.2)),
        ]);
        let ranked = rank_neighborhood(&g, &HashMap::new(), &seed(), &rates, None, 20);
        let score = |id: i64| {
            ranked
                .node_scores
                .iter()
                .find(|&&(n, _)| n == id)
                .map(|&(_, s)| s)
                .unwrap()
        };
        assert!(
            score(1) > 20.0 * score(10),
            "CONTAINS fan-out must not dilute the CALLS budget"
        );
    }

    #[test]
    fn zero_reverse_rate_blocks_backward_authority_flow() {
        let g = graph(&["CITES"], &[("CITES", 1, 0)]);
        let one_way = HashMap::from([(
            "CITES".to_string(),
            KindRates {
                forward: 0.7,
                reverse: 0.0,
            },
        )]);
        let ranked = rank_neighborhood(&g, &HashMap::new(), &seed(), &one_way, None, 5);
        assert!(ranked.node_scores.iter().all(|&(id, _)| id != 1));

        let both_ways = HashMap::from([("CITES".to_string(), KindRates::new(0.7))]);
        let ranked = rank_neighborhood(&g, &HashMap::new(), &seed(), &both_ways, None, 5);
        assert!(ranked.node_scores.iter().any(|&(id, _)| id == 1));
    }

    #[test]
    fn flux_selection_prefers_focused_kind_edges() {
        let g = graph(
            &["CONTAINS", "CALLS"],
            &[("CONTAINS", 0, 2), ("CALLS", 0, 1)],
        );
        let rates = HashMap::from([
            ("CALLS".to_string(), KindRates::new(0.9)),
            ("CONTAINS".to_string(), KindRates::new(0.9)),
        ]);
        let ranked = rank_neighborhood(&g, &HashMap::new(), &seed(), &rates, Some("CALLS"), 1);
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
        assert!(personalized_pagerank(&Transitions::new(0), &[(0, 1.0)], PPR_DAMPING).is_empty());
    }

    #[test]
    fn node_scores_break_ties_by_id() {
        let g = graph(
            &["CALLS"],
            &[
                ("CALLS", 0, 2),
                ("CALLS", 0, 1),
                ("CALLS", 0, 4),
                ("CALLS", 0, 3),
            ],
        );
        let rates = HashMap::from([("CALLS".to_string(), KindRates::new(1.0))]);
        let ranked = rank_neighborhood(&g, &HashMap::new(), &seed(), &rates, None, 10);
        let pos = |id: i64| {
            ranked
                .node_scores
                .iter()
                .position(|&(n, _)| n == id)
                .unwrap()
        };
        assert!(pos(1) < pos(2));
        assert!(pos(2) < pos(3));
        assert!(pos(3) < pos(4));
    }

    #[test]
    fn rank_neighborhood_respects_the_cap_and_reports_hidden_counts() {
        let edges: Vec<(&str, i64, i64)> = (0..5).map(|i| ("CALLS", 0i64, 10 + i)).collect();
        let g = graph(&["CALLS"], &edges);
        let rates = HashMap::from([("CALLS".to_string(), KindRates::new(1.0))]);
        let ranked = rank_neighborhood(&g, &HashMap::new(), &seed(), &rates, None, 2);
        assert_eq!(ranked.selected.len(), 2);
        assert_eq!(ranked.hidden_by_kind, vec![("CALLS".to_string(), 3)]);
        assert!(ranked.node_scores.iter().all(|&(_, s)| s > 0.0));

        let empty = rank_neighborhood(&g, &HashMap::new(), &[], &rates, None, 2);
        assert!(empty.selected.is_empty() && empty.node_scores.is_empty());
    }
}
