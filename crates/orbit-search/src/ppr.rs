use std::collections::{HashMap, HashSet};

use crate::types::{Graph, TermSeeds};

pub const PPR_DAMPING: f64 = 0.85;
pub const DEFAULT_EDGE_WEIGHT: f64 = 0.5;
pub const REVERSE_EDGE_FACTOR: f64 = 0.5;
pub const FOCUS_WEIGHT: f64 = 1.0;
pub const MAX_KIND_WEIGHT: f64 = 1.0;

const LAMBDA: f64 = 1e-4;
const EPOCHS: u32 = 8;
const MAX_SCANS: usize = 200;

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
    offsets: Vec<u32>,
    targets: Vec<u32>,
    weights: Vec<f32>,
    out_totals: Vec<f64>,
}

impl Transitions {
    pub fn new(node_count: usize, entries: &[(usize, usize, f64)]) -> Self {
        let mut counts = vec![0u32; node_count + 1];
        for &(s, _, w) in entries {
            if w > 0.0 {
                counts[s + 1] += 1;
            }
        }
        for i in 0..node_count {
            counts[i + 1] += counts[i];
        }
        let offsets = counts;
        let total_edges = offsets[node_count] as usize;
        let mut targets = vec![0u32; total_edges];
        let mut weights = vec![0f32; total_edges];
        let mut cursor = offsets.clone();
        let mut out_totals = vec![0.0; node_count];
        for &(s, t, w) in entries {
            if w > 0.0 {
                let slot = cursor[s] as usize;
                targets[slot] = t as u32;
                weights[slot] = w as f32;
                cursor[s] += 1;
                out_totals[s] += w;
            }
        }
        Self {
            offsets,
            targets,
            weights,
            out_totals,
        }
    }

    pub fn node_count(&self) -> usize {
        self.out_totals.len()
    }

    fn edge_count(&self) -> usize {
        self.targets.len()
    }

    fn out(&self, v: usize) -> (&[u32], &[f32]) {
        let range = self.offsets[v] as usize..self.offsets[v + 1] as usize;
        (&self.targets[range.clone()], &self.weights[range])
    }

    fn out_len(&self, v: usize) -> usize {
        (self.offsets[v + 1] - self.offsets[v]) as usize
    }
}

pub struct ScoredNode {
    pub id: i64,
    pub score: f64,
}

pub struct RankedNeighborhood {
    pub selected: Vec<usize>,
    pub hidden_by_kind: Vec<(String, usize)>,
    pub node_scores: Vec<ScoredNode>,
}

const CONSENSUS_EPSILON: f64 = 1e-9;
pub const CONSENSUS_QUORUM: f64 = 0.6;
pub const SPECIFICITY_EXPONENT: f64 = 1.0;
pub const SCORED_NODE_POOL: usize = 1024;

pub fn rank_neighborhood(
    graph: &Graph,
    term_seeds: &[TermSeeds],
    kind_rates: &HashMap<String, KindRates>,
    focus: Option<&str>,
    cap: usize,
) -> RankedNeighborhood {
    let mut index: rustc_hash::FxHashMap<i64, u32> = rustc_hash::FxHashMap::default();
    let mut ids: Vec<i64> = Vec::new();
    let intern =
        |id: i64, ids: &mut Vec<i64>, index: &mut rustc_hash::FxHashMap<i64, u32>| -> u32 {
            *index.entry(id).or_insert_with(|| {
                ids.push(id);
                (ids.len() - 1) as u32
            })
        };
    for &(id, _) in term_seeds.iter().flat_map(|t| t.seeds.iter()) {
        intern(id, &mut ids, &mut index);
    }
    let endpoints: Vec<(u32, u32)> = graph
        .edges
        .iter()
        .map(|e| {
            (
                intern(e.source, &mut ids, &mut index),
                intern(e.target, &mut ids, &mut index),
            )
        })
        .collect();
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
                r.reverse = FOCUS_WEIGHT;
            }
            r
        })
        .collect();
    let kind_count = graph.kinds.len().max(1);
    let mut fwd_pools = vec![0.0f64; node_count * kind_count];
    let mut rev_pools = vec![0.0f64; node_count * kind_count];
    let mut degrees = vec![0u32; node_count];
    for (e, &(s, t)) in graph.edges.iter().zip(&endpoints) {
        degrees[s as usize] += 1;
        degrees[t as usize] += 1;
        fwd_pools[s as usize * kind_count + e.kind as usize] += 1.0;
        rev_pools[t as usize * kind_count + e.kind as usize] += 1.0;
    }
    // ObjectRank Eq. 4: each (node, kind, direction) group is an independent
    // authority budget, split evenly across the group's edges.
    let placed: Vec<(usize, usize, f64, f64)> = graph
        .edges
        .iter()
        .zip(&endpoints)
        .map(|(e, &(s, t))| {
            let (s, t) = (s as usize, t as usize);
            let r = rates_by_kind[e.kind as usize];
            let kind = e.kind as usize;
            let fwd = r.forward / fwd_pools[s * kind_count + kind];
            let rev = r.reverse / rev_pools[t * kind_count + kind];
            (s, t, fwd, rev)
        })
        .collect();
    let mut entries: Vec<(usize, usize, f64)> = Vec::with_capacity(placed.len() * 2);
    for &(s, t, fwd, rev) in &placed {
        entries.push((s, t, fwd));
        entries.push((t, s, rev));
    }
    let transitions = Transitions::new(node_count, &entries);
    let inverse_entries: Vec<(usize, usize, f64)> =
        entries.iter().map(|&(s, t, w)| (t, s, w)).collect();
    let inverted = Transitions::new(node_count, &inverse_entries);
    drop(inverse_entries);
    drop(entries);

    let seed_sets: Vec<(Vec<(usize, f64)>, f64)> = term_seeds
        .iter()
        .filter(|t| t.seeds.iter().any(|&(_, w)| w > 0.0))
        .map(|t| {
            let set = t
                .seeds
                .iter()
                .map(|&(id, w)| (index[&id] as usize, w))
                .collect();
            (set, t.weight.max(f64::MIN_POSITIVE))
        })
        .collect();
    let per_term_logs: Vec<Vec<f64>> = {
        use rayon::prelude::*;
        seed_sets
            .par_iter()
            .map(|(seeds, _)| {
                let relevance = personalized_pagerank(&transitions, seeds, PPR_DAMPING);
                let specificity = personalized_pagerank(&inverted, seeds, PPR_DAMPING);
                relevance
                    .iter()
                    .zip(&specificity)
                    .map(|(&r, &p)| {
                        (r + CONSENSUS_EPSILON).ln()
                            + SPECIFICITY_EXPONENT * (p + CONSENSUS_EPSILON).ln()
                    })
                    .collect()
            })
            .collect()
    };
    if per_term_logs.is_empty() {
        return RankedNeighborhood {
            selected: Vec::new(),
            hidden_by_kind: Vec::new(),
            node_scores: Vec::new(),
        };
    }
    let term_count = per_term_logs.len();
    let quorum = ((term_count as f64 * CONSENSUS_QUORUM).ceil() as usize).clamp(1, term_count);
    let scores: Vec<f64> = (0..node_count)
        .map(|v| {
            let mut logs: Vec<(f64, f64)> = per_term_logs
                .iter()
                .zip(&seed_sets)
                .map(|(term, (_, weight))| (term[v], *weight))
                .collect();
            logs.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
            let (log_sum, weight_sum) = logs[..quorum]
                .iter()
                .fold((0.0, 0.0), |(ls, ws), &(l, w)| (ls + l * w, ws + w));
            (log_sum / weight_sum).exp()
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
    let by_flux = |a: &(usize, f64), b: &(usize, f64)| {
        b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal)
    };
    if order.len() > cap && cap > 0 {
        order.select_nth_unstable_by(cap - 1, by_flux);
        order.truncate(cap);
    }
    order.sort_by(by_flux);

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
    let mut anchor_nodes: HashSet<i64> = HashSet::new();
    for &(i, _) in &selected {
        let e = &graph.edges[i];
        *shown.entry(e.kind).or_insert(0) += 1;
        anchor_nodes.insert(e.source);
        anchor_nodes.insert(e.target);
    }
    let mut totals: HashMap<u16, usize> = HashMap::new();
    for e in &graph.edges {
        if anchor_nodes.contains(&e.source) || anchor_nodes.contains(&e.target) {
            *totals.entry(e.kind).or_insert(0) += 1;
        }
    }
    let mut hidden_by_kind: Vec<(String, usize)> = totals
        .into_iter()
        .filter_map(|(kind, total)| {
            let hidden = total - shown.get(&kind).copied().unwrap_or(0);
            (hidden > 0).then(|| (graph.kinds[kind as usize].clone(), hidden))
        })
        .collect();
    hidden_by_kind.sort();

    let mut node_scores: Vec<ScoredNode> = ids
        .iter()
        .enumerate()
        .filter(|&(i, _)| scores[i] > CONSENSUS_EPSILON * 1.5)
        .map(|(i, &id)| ScoredNode {
            id,
            score: scores[i] / f64::from(degrees[i].max(1)),
        })
        .collect();
    let by_score = |a: &ScoredNode, b: &ScoredNode| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.id.cmp(&b.id))
    };
    if node_scores.len() > SCORED_NODE_POOL {
        node_scores.select_nth_unstable_by(SCORED_NODE_POOL - 1, by_score);
        node_scores.truncate(SCORED_NODE_POOL);
    }
    node_scores.sort_by(by_score);

    RankedNeighborhood {
        selected: selected.into_iter().map(|(i, _)| i).collect(),
        hidden_by_kind,
        node_scores,
    }
}

/// PowerPush (Wu et al., SIGMOD '21): FIFO forward pushes while the frontier
/// is sparse, epoch-thresholded sequential scans once it is dense. Adapted to
/// weighted transitions where a node emitting less than its norm leaks the
/// difference back to the teleport distribution; per-push seed touching is
/// avoided by accumulating that leak into one scalar flushed at phase
/// boundaries. Returned scores include the residual mass, so they always sum
/// to exactly one and the l1 error stays bounded by the residual.
pub fn personalized_pagerank(
    transitions: &Transitions,
    seeds: &[(usize, f64)],
    damping: f64,
) -> Vec<f64> {
    let n = transitions.node_count();
    let seed_total: f64 = seeds.iter().map(|&(_, w)| w.max(0.0)).sum();
    if n == 0 || seed_total <= 0.0 {
        return vec![0.0; n];
    }
    let teleport: Vec<(usize, f64)> = seeds
        .iter()
        .filter(|&&(_, w)| w > 0.0)
        .map(|&(i, w)| (i, w / seed_total))
        .collect();
    let m = transitions.edge_count().max(1) as f64;

    let mut reserve = vec![0.0; n];
    let mut residue = vec![0.0; n];
    for &(i, w) in &teleport {
        residue[i] += w;
    }
    let mut r_sum = 1.0;
    let mut pending_leak = 0.0;

    let push = |v: usize,
                reserve: &mut [f64],
                residue: &mut [f64],
                r_sum: &mut f64,
                pending_leak: &mut f64| {
        let rv = residue[v];
        residue[v] = 0.0;
        reserve[v] += (1.0 - damping) * rv;
        *r_sum -= (1.0 - damping) * rv;
        let spread = damping * rv;
        let total = transitions.out_totals[v];
        let norm = total.max(1.0);
        let (targets, weights) = transitions.out(v);
        for k in 0..targets.len() {
            residue[targets[k] as usize] += spread * f64::from(weights[k]) / norm;
        }
        *pending_leak += spread * (1.0 - total / norm);
    };
    let flush = |residue: &mut [f64], pending_leak: &mut f64| {
        if *pending_leak > 0.0 {
            for &(i, w) in &teleport {
                residue[i] += *pending_leak * w;
            }
            *pending_leak = 0.0;
        }
    };

    let r_max = LAMBDA / m;
    let scan_threshold = (n / 4).max(1);
    let mut queue: std::collections::VecDeque<u32> =
        teleport.iter().map(|&(i, _)| i as u32).collect();
    let mut in_queue = vec![false; n];
    for &(i, _) in &teleport {
        in_queue[i] = true;
    }
    while r_sum > LAMBDA && queue.len() <= scan_threshold {
        let Some(v) = queue.pop_front() else {
            flush(&mut residue, &mut pending_leak);
            let mut refilled = false;
            for &(i, _) in &teleport {
                if residue[i] > transitions.out_len(i).max(1) as f64 * r_max && !in_queue[i] {
                    queue.push_back(i as u32);
                    in_queue[i] = true;
                    refilled = true;
                }
            }
            if !refilled {
                break;
            }
            continue;
        };
        let v = v as usize;
        in_queue[v] = false;
        push(v, &mut reserve, &mut residue, &mut r_sum, &mut pending_leak);
        let (targets, _) = transitions.out(v);
        for &t in targets {
            let t = t as usize;
            if !in_queue[t] && residue[t] > transitions.out_len(t).max(1) as f64 * r_max {
                queue.push_back(t as u32);
                in_queue[t] = true;
            }
        }
    }

    if r_sum > LAMBDA {
        let mut scans = 0usize;
        'epochs: for i in 1..=EPOCHS {
            let epoch_lambda = LAMBDA.powf(f64::from(i) / f64::from(EPOCHS));
            let epoch_r_max = epoch_lambda / m;
            while r_sum > epoch_lambda {
                flush(&mut residue, &mut pending_leak);
                for v in 0..n {
                    if residue[v] > transitions.out_len(v).max(1) as f64 * epoch_r_max {
                        push(v, &mut reserve, &mut residue, &mut r_sum, &mut pending_leak);
                    }
                }
                scans += 1;
                if scans >= MAX_SCANS {
                    break 'epochs;
                }
            }
        }
    }

    flush(&mut residue, &mut pending_leak);
    for v in 0..n {
        reserve[v] += residue[v];
    }
    reserve
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::GraphEdge;

    fn cycle(n: usize) -> Transitions {
        let entries: Vec<(usize, usize, f64)> = (0..n).map(|i| (i, (i + 1) % n, 1.0)).collect();
        Transitions::new(n, &entries)
    }

    fn seed() -> Vec<TermSeeds> {
        vec![TermSeeds {
            seeds: vec![(0, 1.0)],
            weight: 1.0,
        }]
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
        }
    }

    #[test]
    fn low_transfer_rates_leak_mass_back_to_seeds() {
        let full = Transitions::new(2, &[(0, 1, 1.0)]);
        let leaky = Transitions::new(2, &[(0, 1, 0.2)]);
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
        let ranked = rank_neighborhood(&g, &seed(), &rates, None, 20);
        let score = |id: i64| {
            ranked
                .node_scores
                .iter()
                .find(|n| n.id == id)
                .map(|n| n.score)
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
        let ranked = rank_neighborhood(&g, &seed(), &one_way, None, 5);
        assert!(ranked.node_scores.iter().all(|n| n.id != 1));

        let both_ways = HashMap::from([("CITES".to_string(), KindRates::new(0.7))]);
        let ranked = rank_neighborhood(&g, &seed(), &both_ways, None, 5);
        assert!(ranked.node_scores.iter().any(|n| n.id == 1));
    }

    #[test]
    fn focus_boosts_incoming_edges_so_callers_of_the_seed_surface() {
        let g = graph(
            &["CONTAINS", "CALLS"],
            &[("CONTAINS", 0, 2), ("CALLS", 1, 0)],
        );
        let rates = HashMap::from([
            ("CALLS".to_string(), KindRates::new(0.9)),
            ("CONTAINS".to_string(), KindRates::new(0.9)),
        ]);
        let ranked = rank_neighborhood(&g, &seed(), &rates, Some("CALLS"), 1);
        assert_eq!(ranked.selected, vec![1]);

        let ranked = rank_neighborhood(&g, &seed(), &rates, None, 1);
        assert_eq!(ranked.selected, vec![0]);
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
        let ranked = rank_neighborhood(&g, &seed(), &rates, Some("CALLS"), 1);
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
        assert!(
            personalized_pagerank(&Transitions::new(0, &[]), &[(0, 1.0)], PPR_DAMPING).is_empty()
        );
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
        let ranked = rank_neighborhood(&g, &seed(), &rates, None, 10);
        let pos = |id: i64| ranked.node_scores.iter().position(|n| n.id == id).unwrap();
        assert!(pos(1) < pos(2));
        assert!(pos(2) < pos(3));
        assert!(pos(3) < pos(4));
    }

    #[test]
    fn rank_neighborhood_respects_the_cap_and_reports_hidden_counts() {
        let edges: Vec<(&str, i64, i64)> = (0..5).map(|i| ("CALLS", 0i64, 10 + i)).collect();
        let g = graph(&["CALLS"], &edges);
        let rates = HashMap::from([("CALLS".to_string(), KindRates::new(1.0))]);
        let ranked = rank_neighborhood(&g, &seed(), &rates, None, 2);
        assert_eq!(ranked.selected.len(), 2);
        assert_eq!(ranked.hidden_by_kind, vec![("CALLS".to_string(), 3)]);
        assert!(ranked.node_scores.iter().all(|n| n.score > 0.0));

        let empty = rank_neighborhood(&g, &[], &rates, None, 2);
        assert!(empty.selected.is_empty() && empty.node_scores.is_empty());
    }
}
