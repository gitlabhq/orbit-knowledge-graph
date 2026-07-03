//! The universal return type of every graph operation: a [`Subgraph`] of
//! [`MarkedEdge`]s. Terminal answers are projections off it (`node_kinds`,
//! `edge_kinds`, `adjacencies`, `paths`); subgraphs compose under set algebra
//! (`union`, `intersect`, `difference`) so connectivity guards and frontier
//! reasoning are expressions rather than bespoke walks.

use std::collections::{BTreeSet, HashMap};

/// Facts a walk stamps onto a crossed edge. Grows one field per fact ≥2 passes
/// read; [`Mark::merge`] defines how two overlapping walks combine an edge.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EdgeMarks {
    /// Enumerated-path memberships (colored by [`super::OntologyGraph::paths_between`]).
    pub path_ids: Vec<usize>,
}

/// How a mark payload combines when two walks cross the same edge. `path_ids`
/// unions; a scalar mark would take min / assert-equal. Keeps [`Subgraph::union`]
/// well-defined for any future mark without special-casing the caller.
pub trait Mark {
    fn merge(&mut self, other: &Self);
}

impl Mark for EdgeMarks {
    fn merge(&mut self, other: &Self) {
        self.path_ids.extend(other.path_ids.iter().copied());
        self.path_ids.sort_unstable();
        self.path_ids.dedup();
    }
}

/// A relationship kind and the node kind on the far side of the hop.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Adjacency {
    pub relationship_kind: String,
    pub neighbor_kind: String,
}

/// One edge in a [`Subgraph`], carrying its endpoint kinds, depth, and marks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarkedEdge {
    pub from: String,
    pub to: String,
    pub relationship_kind: String,
    pub synthesized: bool,
    pub depth: usize,
    pub marks: EdgeMarks,
}

impl MarkedEdge {
    fn identity(&self) -> (&str, &str, &str) {
        (&self.from, &self.relationship_kind, &self.to)
    }
}

/// The marked view a walk produced.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Subgraph {
    pub edges: Vec<MarkedEdge>,
}

impl Subgraph {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.edges.is_empty()
    }

    /// Far-node kinds reached, excluding `start`.
    #[must_use]
    pub fn node_kinds(&self, start: &str) -> BTreeSet<String> {
        self.edges
            .iter()
            .map(|e| e.to.clone())
            .filter(|n| n != start)
            .collect()
    }

    /// Relationship kinds present on any crossed edge.
    #[must_use]
    pub fn edge_kinds(&self) -> BTreeSet<String> {
        self.edges
            .iter()
            .map(|e| e.relationship_kind.clone())
            .collect()
    }

    /// `(relationship_kind, far_node)` pairs, sorted and deduplicated.
    #[must_use]
    pub fn adjacencies(&self) -> Vec<Adjacency> {
        let set: BTreeSet<(String, String)> = self
            .edges
            .iter()
            .map(|e| (e.relationship_kind.clone(), e.to.clone()))
            .collect();
        set.into_iter()
            .map(|(relationship_kind, neighbor_kind)| Adjacency {
                relationship_kind,
                neighbor_kind,
            })
            .collect()
    }

    /// Ordered edge-kind sequences recovered from the path coloring — one per
    /// enumerated path. Sorted for determinism; empty when unreachable.
    #[must_use]
    pub fn paths(&self) -> Vec<Vec<String>> {
        let mut by_id: HashMap<usize, Vec<(usize, String)>> = HashMap::new();
        for e in &self.edges {
            for &pid in &e.marks.path_ids {
                by_id
                    .entry(pid)
                    .or_default()
                    .push((e.depth, e.relationship_kind.clone()));
            }
        }
        let mut out: Vec<Vec<String>> = by_id
            .into_values()
            .map(|mut seq| {
                seq.sort_by_key(|(d, _)| *d);
                seq.into_iter().map(|(_, k)| k).collect()
            })
            .collect();
        out.sort();
        out
    }

    /// Every edge from both, marks merged on shared `(from, kind, to)` identity.
    #[must_use]
    pub fn union(mut self, other: Subgraph) -> Subgraph {
        for edge in other.edges {
            match self
                .edges
                .iter_mut()
                .find(|e| e.identity() == edge.identity())
            {
                Some(existing) => existing.marks.merge(&edge.marks),
                None => self.edges.push(edge),
            }
        }
        self
    }

    /// Edges whose `(from, kind, to)` identity is present in both.
    #[must_use]
    pub fn intersect(&self, other: &Subgraph) -> Subgraph {
        let keep: BTreeSet<(String, String, String)> = other
            .edges
            .iter()
            .map(|e| (e.from.clone(), e.relationship_kind.clone(), e.to.clone()))
            .collect();
        Subgraph {
            edges: self
                .edges
                .iter()
                .filter(|e| {
                    keep.contains(&(e.from.clone(), e.relationship_kind.clone(), e.to.clone()))
                })
                .cloned()
                .collect(),
        }
    }

    /// Edges in `self` whose identity is absent from `other`.
    #[must_use]
    pub fn difference(&self, other: &Subgraph) -> Subgraph {
        let drop: BTreeSet<(String, String, String)> = other
            .edges
            .iter()
            .map(|e| (e.from.clone(), e.relationship_kind.clone(), e.to.clone()))
            .collect();
        Subgraph {
            edges: self
                .edges
                .iter()
                .filter(|e| {
                    !drop.contains(&(e.from.clone(), e.relationship_kind.clone(), e.to.clone()))
                })
                .cloned()
                .collect(),
        }
    }
}
