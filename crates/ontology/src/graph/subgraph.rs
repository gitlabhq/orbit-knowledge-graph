//! The universal return type of every graph operation: a [`Subgraph`] of
//! [`MarkedEdge`]s. Terminal answers are projections off it (`node_kinds`,
//! `edge_kinds`, `adjacencies`, `paths`); subgraphs compose under set algebra
//! (`union`, `intersect`, `difference`) so connectivity guards and frontier
//! reasoning are expressions rather than bespoke walks.

use std::collections::{BTreeSet, HashMap};

/// Per-hop facts a walk stamps onto a crossed edge. This is the classifier's
/// per-edge output: the passes that emit SQL read these instead of re-deriving.
/// [`Mark::merge`] defines how two overlapping walks combine an edge.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EdgeMarks {
    /// Enumerated-path memberships (colored by [`super::OntologyGraph::paths_between`]).
    pub path_ids: Vec<usize>,
    /// Namespace prefix stamped by scope propagation (restrict A1/A2).
    pub scope_prefix: Option<String>,
    /// The edge keeps both endpoints in one namespace (restrict A3).
    pub scope_preserving: bool,
    /// Minimum access level required for the far node (security A8).
    pub role_floor: Option<u32>,
    /// The far node's table is partitioned (partition C15).
    pub partitioned: bool,
}

/// How a mark payload combines when two walks cross the same edge. Each field
/// merges under its own lattice join: sets union, an `Option<prefix>` asserts
/// agreement (conflict = the multi-namespace bug the plan calls out), a role
/// floor takes the max (most restrictive), booleans OR. Keeps [`Subgraph::union`]
/// well-defined for any mark without special-casing the caller.
pub trait Mark {
    fn merge(&mut self, other: &Self);
}

impl Mark for EdgeMarks {
    fn merge(&mut self, other: &Self) {
        self.path_ids.extend(other.path_ids.iter().copied());
        self.path_ids.sort_unstable();
        self.path_ids.dedup();
        if self.scope_prefix.is_none() {
            self.scope_prefix = other.scope_prefix.clone();
        }
        self.scope_preserving |= other.scope_preserving;
        self.role_floor = self.role_floor.max(other.role_floor);
        self.partitioned |= other.partitioned;
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

    /// Far-node kinds present in both subgraphs — the meet of a forward and a
    /// backward walk, i.e. the kinds a bidirectional frontier spans.
    #[must_use]
    pub fn intersect_nodes(&self, other: &Subgraph) -> BTreeSet<String> {
        let theirs: BTreeSet<&str> = other.edges.iter().map(|e| e.to.as_str()).collect();
        self.edges
            .iter()
            .map(|e| e.to.as_str())
            .filter(|n| theirs.contains(n))
            .map(str::to_string)
            .collect()
    }

    /// Extend each reached node by `expand`, unioning the results into `self`.
    /// Chains walks (neighbors-of-neighbors) without a bespoke second loop.
    #[must_use]
    pub fn then(self, expand: impl Fn(&str) -> Subgraph) -> Subgraph {
        let reached: BTreeSet<String> = self.edges.iter().map(|e| e.to.clone()).collect();
        reached
            .iter()
            .fold(self, |acc, node| acc.union(expand(node)))
    }
}

impl std::fmt::Display for Subgraph {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for e in &self.edges {
            write!(f, "{} --{}--> {}", e.from, e.relationship_kind, e.to)?;
            if let Some(p) = &e.marks.scope_prefix {
                write!(f, " scope={p}")?;
            }
            if let Some(r) = e.marks.role_floor {
                write!(f, " role_floor={r}")?;
            }
            if e.marks.scope_preserving {
                write!(f, " scope_preserving")?;
            }
            if e.marks.partitioned {
                write!(f, " partitioned")?;
            }
            writeln!(f)?;
        }
        Ok(())
    }
}
