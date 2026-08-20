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
    fn rank_neighborhood_respects_the_cap_and_reports_hidden_counts() {
        let edges: Vec<NeighborhoodEdge> = (0..5)
            .map(|i| NeighborhoodEdge {
                kind: "CALLS".to_string(),
                source: "seed".to_string(),
                target: format!("t{i}"),
            })
            .collect();
        let weights = HashMap::from([("CALLS".to_string(), 1.0)]);
        let ranked = rank_neighborhood(
            &edges,
            &HashMap::new(),
            &[vec![("seed".to_string(), 1.0)]],
            &weights,
            None,
            2,
        );
        assert_eq!(ranked.selected.len(), 2);
        assert_eq!(ranked.hidden_by_kind, vec![("CALLS".to_string(), 3)]);
        assert!(ranked.node_scores.iter().all(|&(_, s)| s > 0.0));

        let empty = rank_neighborhood(&edges, &HashMap::new(), &[], &weights, None, 2);
        assert!(empty.selected.is_empty() && empty.node_scores.is_empty());
    }
}
