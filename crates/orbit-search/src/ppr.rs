pub const PPR_DAMPING: f64 = 0.85;
pub const DEFAULT_EDGE_WEIGHT: f64 = 0.5;
pub const REVERSE_EDGE_FACTOR: f64 = 0.5;

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

/// Power iteration with seed-biased teleportation. Dangling mass teleports
/// back to the seeds rather than uniformly, so rank cannot leak out of the
/// question's neighborhood. Returns scores summing to ~1, or all zeros when
/// the seeds carry no positive weight.
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
    fn seeded_node_outranks_symmetric_peers() {
        let g = cycle(4);
        let scores = personalized_pagerank(&g, &[(1, 1.0)], PPR_DAMPING);
        for i in [0, 2, 3] {
            assert!(scores[1] > scores[i], "scores were {scores:?}");
        }
    }

    #[test]
    fn uniform_seeds_on_a_cycle_give_uniform_scores_summing_to_one() {
        let g = cycle(4);
        let seeds: Vec<(usize, f64)> = (0..4).map(|i| (i, 1.0)).collect();
        let scores = personalized_pagerank(&g, &seeds, PPR_DAMPING);
        let sum: f64 = scores.iter().sum();
        assert!((sum - 1.0).abs() < 1e-6, "sum was {sum}");
        for s in &scores {
            assert!((s - 0.25).abs() < 1e-6, "scores were {scores:?}");
        }
    }

    #[test]
    fn dangling_mass_returns_to_the_seed_instead_of_leaking() {
        let mut g = SubGraph::new(2);
        g.add_edge(0, 1, 1.0);
        let scores = personalized_pagerank(&g, &[(0, 1.0)], PPR_DAMPING);
        let sum: f64 = scores.iter().sum();
        assert!((sum - 1.0).abs() < 1e-6, "sum was {sum}");
        assert!(scores[0] > scores[1], "scores were {scores:?}");
    }

    #[test]
    fn heavier_edges_receive_more_rank() {
        let mut g = SubGraph::new(3);
        g.add_edge(0, 1, 1.0);
        g.add_edge(0, 2, 0.4);
        let scores = personalized_pagerank(&g, &[(0, 1.0)], PPR_DAMPING);
        assert!(scores[1] > scores[2], "scores were {scores:?}");
    }

    #[test]
    fn hub_damping_prefers_the_quiet_target() {
        let mut g = SubGraph::new(3);
        g.add_edge(0, 1, edge_weight(1.0, 1000));
        g.add_edge(0, 2, edge_weight(1.0, 2));
        let scores = personalized_pagerank(&g, &[(0, 1.0)], PPR_DAMPING);
        assert!(scores[2] > scores[1], "scores were {scores:?}");
    }

    #[test]
    fn seed_weights_bias_proportionally() {
        let g = cycle(4);
        let scores = personalized_pagerank(&g, &[(0, 3.0), (2, 1.0)], PPR_DAMPING);
        assert!(scores[0] > scores[2], "scores were {scores:?}");
    }

    #[test]
    fn no_positive_seed_weight_returns_zeros() {
        let g = cycle(3);
        assert_eq!(personalized_pagerank(&g, &[], PPR_DAMPING), vec![0.0; 3]);
        assert_eq!(
            personalized_pagerank(&g, &[(0, 0.0), (1, -1.0)], PPR_DAMPING),
            vec![0.0; 3]
        );
    }

    #[test]
    fn empty_graph_returns_empty() {
        let g = SubGraph::new(0);
        assert!(personalized_pagerank(&g, &[(0, 1.0)], PPR_DAMPING).is_empty());
    }

    #[test]
    fn edge_weight_decays_with_target_degree_and_scales_with_kind() {
        assert!(edge_weight(1.0, 0) > edge_weight(1.0, 10));
        assert!(edge_weight(1.0, 10) > edge_weight(1.0, 1000));
        assert!(edge_weight(1.0, 5) > edge_weight(0.4, 5));
    }
}
