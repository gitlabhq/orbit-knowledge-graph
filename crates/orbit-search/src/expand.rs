use std::collections::{HashMap, HashSet};

use crate::Edge;
use crate::ppr::{NeighborhoodEdge, rank_neighborhood};

pub const PER_HOP_EDGE_CAP: usize = 600;
pub const FRONTIER_CAP: usize = 150;
pub const EDGE_LIMIT: usize = 40;

pub struct NodeLabel {
    pub label: String,
    pub loc: String,
    pub scoped: bool,
}

pub type ExpandedNeighborhood = (Vec<Edge>, Vec<(String, usize)>);

pub trait NeighborhoodSource {
    type Error;

    fn hop(&self, ids: &[&str], cap: usize) -> Result<Vec<NeighborhoodEdge>, Self::Error>;
    fn degrees(&self, ids: &[&str]) -> Result<HashMap<String, u64>, Self::Error>;
    fn labels(&self, ids: &[&str]) -> Result<HashMap<String, NodeLabel>, Self::Error>;
}

pub fn expand_neighborhood<S: NeighborhoodSource>(
    source: &S,
    seeds: &[(String, f64)],
    kind_weights: &HashMap<String, f64>,
    focus: Option<&str>,
) -> Result<ExpandedNeighborhood, S::Error> {
    let seed_ids: Vec<&str> = seeds.iter().map(|(id, _)| id.as_str()).collect();
    let hop1 = source.hop(&seed_ids, PER_HOP_EDGE_CAP)?;

    let mut seen: HashSet<String> = seed_ids.iter().map(|s| (*s).to_string()).collect();
    let mut hop1_nodes: Vec<String> = Vec::new();
    for e in &hop1 {
        for id in [&e.source, &e.target] {
            if seen.insert(id.clone()) {
                hop1_nodes.push(id.clone());
            }
        }
    }
    let mut degrees = source.degrees(&refs(&hop1_nodes))?;

    let mut frontier: Vec<&str> = hop1_nodes.iter().map(String::as_str).collect();
    frontier.sort_by_key(|id| degrees.get(*id).copied().unwrap_or(0));
    frontier.truncate(FRONTIER_CAP);
    let hop2 = if frontier.is_empty() {
        Vec::new()
    } else {
        source.hop(&frontier, PER_HOP_EDGE_CAP)?
    };

    let mut raw = hop1;
    let mut edge_set: HashSet<NeighborhoodEdge> = raw.iter().cloned().collect();
    for e in hop2 {
        if edge_set.insert(e.clone()) {
            raw.push(e);
        }
    }

    let mut hop2_nodes: Vec<String> = Vec::new();
    for e in &raw {
        for id in [&e.source, &e.target] {
            if seen.insert(id.clone()) {
                hop2_nodes.push(id.clone());
            }
        }
    }
    degrees.extend(source.degrees(&refs(&hop2_nodes))?);

    let all_nodes: Vec<&str> = seen.iter().map(String::as_str).collect();
    let labels = source.labels(&all_nodes)?;

    let edges: Vec<NeighborhoodEdge> = raw
        .into_iter()
        .filter(|e| {
            [e.source.as_str(), e.target.as_str()]
                .iter()
                .all(|id| labels.get(*id).is_none_or(|l| !l.scoped))
        })
        .collect();

    let ranked = rank_neighborhood(&edges, &degrees, seeds, kind_weights, focus, EDGE_LIMIT);
    let display = |id: &str| -> (String, String) {
        match labels.get(id) {
            Some(l) => (l.label.clone(), l.loc.clone()),
            None => (id.to_string(), String::new()),
        }
    };
    let shown = ranked
        .selected
        .into_iter()
        .map(|i| {
            let e = &edges[i];
            let (source_label, source_loc) = display(&e.source);
            let (target_label, target_loc) = display(&e.target);
            Edge {
                kind: e.kind.clone(),
                source: source_label,
                source_loc,
                target: target_label,
                target_loc,
            }
        })
        .collect();
    Ok((shown, ranked.hidden_by_kind))
}

fn refs(ids: &[String]) -> Vec<&str> {
    ids.iter().map(String::as_str).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    struct FakeSource {
        edges: Vec<NeighborhoodEdge>,
        degrees: HashMap<String, u64>,
        scoped: HashSet<String>,
        hop_requests: RefCell<Vec<Vec<String>>>,
    }

    impl FakeSource {
        fn new(edges: Vec<(&str, &str, &str)>) -> Self {
            let edges: Vec<NeighborhoodEdge> = edges
                .into_iter()
                .map(|(kind, source, target)| NeighborhoodEdge {
                    kind: kind.to_string(),
                    source: source.to_string(),
                    target: target.to_string(),
                })
                .collect();
            let mut degrees: HashMap<String, u64> = HashMap::new();
            for e in &edges {
                *degrees.entry(e.source.clone()).or_insert(0) += 1;
                *degrees.entry(e.target.clone()).or_insert(0) += 1;
            }
            Self {
                edges,
                degrees,
                scoped: HashSet::new(),
                hop_requests: RefCell::new(Vec::new()),
            }
        }
    }

    impl NeighborhoodSource for FakeSource {
        type Error = std::convert::Infallible;

        fn hop(&self, ids: &[&str], cap: usize) -> Result<Vec<NeighborhoodEdge>, Self::Error> {
            self.hop_requests
                .borrow_mut()
                .push(ids.iter().map(|s| (*s).to_string()).collect());
            Ok(self
                .edges
                .iter()
                .filter(|e| ids.contains(&e.source.as_str()) || ids.contains(&e.target.as_str()))
                .take(cap)
                .cloned()
                .collect())
        }

        fn degrees(&self, ids: &[&str]) -> Result<HashMap<String, u64>, Self::Error> {
            Ok(ids
                .iter()
                .filter_map(|id| self.degrees.get(*id).map(|d| ((*id).to_string(), *d)))
                .collect())
        }

        fn labels(&self, ids: &[&str]) -> Result<HashMap<String, NodeLabel>, Self::Error> {
            Ok(ids
                .iter()
                .map(|id| {
                    (
                        (*id).to_string(),
                        NodeLabel {
                            label: format!("label_{id}"),
                            loc: String::new(),
                            scoped: self.scoped.contains(*id),
                        },
                    )
                })
                .collect())
        }
    }

    fn seeds(ids: &[&str]) -> Vec<(String, f64)> {
        ids.iter().map(|id| ((*id).to_string(), 1.0)).collect()
    }

    fn weights() -> HashMap<String, f64> {
        HashMap::from([("CALLS".to_string(), 1.0)])
    }

    #[test]
    fn expands_two_hops_and_dedupes_edges_seen_in_both_hops() {
        let source = FakeSource::new(vec![("CALLS", "seed", "mid"), ("CALLS", "mid", "far")]);
        let (edges, hidden) =
            expand_neighborhood(&source, &seeds(&["seed"]), &weights(), None).unwrap();
        let rendered: Vec<String> = edges
            .iter()
            .map(|e| format!("{} -> {}", e.source, e.target))
            .collect();
        assert_eq!(
            rendered,
            vec!["label_seed -> label_mid", "label_mid -> label_far"]
        );
        assert!(hidden.is_empty());
        assert_eq!(source.hop_requests.borrow().len(), 2);
    }

    #[test]
    fn scoped_endpoints_drop_their_edges_before_ranking() {
        let mut source = FakeSource::new(vec![
            ("CALLS", "seed", "kept"),
            ("CALLS", "seed", "local@3:1"),
        ]);
        source.scoped.insert("local@3:1".to_string());
        let (edges, hidden) =
            expand_neighborhood(&source, &seeds(&["seed"]), &weights(), None).unwrap();
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].target, "label_kept");
        assert!(hidden.is_empty());
    }

    #[test]
    fn frontier_is_capped_and_prefers_low_degree_nodes() {
        let mut edge_list: Vec<(String, String, String)> = Vec::new();
        for i in 0..(FRONTIER_CAP + 20) {
            edge_list.push(("CALLS".to_string(), "seed".to_string(), format!("n{i}")));
        }
        for i in 0..30 {
            edge_list.push(("CALLS".to_string(), format!("x{i}"), "n0".to_string()));
        }
        let source = FakeSource::new(
            edge_list
                .iter()
                .map(|(k, s, t)| (k.as_str(), s.as_str(), t.as_str()))
                .collect(),
        );
        expand_neighborhood(&source, &seeds(&["seed"]), &weights(), None).unwrap();
        let requests = source.hop_requests.borrow();
        let frontier = &requests[1];
        assert_eq!(frontier.len(), FRONTIER_CAP);
        assert!(
            !frontier.contains(&"n0".to_string()),
            "the highest-degree node must not be expanded"
        );
    }

    #[test]
    fn unlabeled_nodes_fall_back_to_their_id() {
        struct NoLabels(FakeSource);
        impl NeighborhoodSource for NoLabels {
            type Error = std::convert::Infallible;
            fn hop(&self, ids: &[&str], cap: usize) -> Result<Vec<NeighborhoodEdge>, Self::Error> {
                self.0.hop(ids, cap)
            }
            fn degrees(&self, ids: &[&str]) -> Result<HashMap<String, u64>, Self::Error> {
                self.0.degrees(ids)
            }
            fn labels(&self, _ids: &[&str]) -> Result<HashMap<String, NodeLabel>, Self::Error> {
                Ok(HashMap::new())
            }
        }
        let source = NoLabels(FakeSource::new(vec![("CALLS", "seed", "42")]));
        let (edges, _) = expand_neighborhood(&source, &seeds(&["seed"]), &weights(), None).unwrap();
        assert_eq!(edges[0].source, "seed");
        assert_eq!(edges[0].target, "42");
    }
}
