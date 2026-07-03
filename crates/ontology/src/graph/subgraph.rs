//! [`Subgraph`] is the return type of every graph op: projectable ([`Subgraph::paths`])
//! and composable under set algebra ([`Subgraph::union`]).

use std::collections::{BTreeSet, HashMap};

/// Per-hop facts stamped onto a crossed edge (the classifier's per-edge output).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EdgeMarks {
    pub path_ids: Vec<usize>,
    pub scope_prefix: Option<String>,
    pub scope_preserving: bool,
    pub role_floor: Option<u32>,
    pub partitioned: bool,
}

/// How a mark combines when two walks cross the same edge; keeps [`Subgraph::union`] total.
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

/// A vertex's role in a classified query graph (plan §3.3).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum VertexRole {
    #[default]
    GraphNode,
    GroupKey,
    Collapsed,
    PathEndpoint,
    Center,
    Dynamic,
}

/// Per-vertex facts stamped by the classifier, keyed by alias in [`Subgraph::nodes`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NodeMarks {
    pub role: VertexRole,
    pub role_floor: Option<u32>,
    pub scope_prefix: Option<String>,
    pub partitioned: bool,
}

impl Mark for NodeMarks {
    fn merge(&mut self, other: &Self) {
        if self.scope_prefix.is_none() {
            self.scope_prefix = other.scope_prefix.clone();
        }
        self.role_floor = self.role_floor.max(other.role_floor);
        self.partitioned |= other.partitioned;
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Adjacency {
    pub relationship_kind: String,
    pub neighbor_kind: String,
}

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

/// Edges carry per-hop marks; `nodes` carry per-vertex marks keyed by alias.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Subgraph {
    pub edges: Vec<MarkedEdge>,
    pub nodes: HashMap<String, NodeMarks>,
}

impl Subgraph {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.edges.is_empty()
    }

    #[must_use]
    pub fn node(&self, alias: &str) -> Option<&NodeMarks> {
        self.nodes.get(alias)
    }

    pub fn mark_node(&mut self, alias: impl Into<String>, marks: NodeMarks) {
        self.nodes
            .entry(alias.into())
            .and_modify(|m| m.merge(&marks))
            .or_insert(marks);
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

    #[must_use]
    pub fn edge_kinds(&self) -> BTreeSet<String> {
        self.edges
            .iter()
            .map(|e| e.relationship_kind.clone())
            .collect()
    }

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

    /// Ordered edge-kind sequences recovered from the path coloring, one per path.
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

    /// Every edge and node from both, marks merged on shared identity.
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
        for (alias, marks) in other.nodes {
            self.mark_node(alias, marks);
        }
        self
    }

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
            nodes: HashMap::new(),
        }
    }

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
            nodes: HashMap::new(),
        }
    }

    /// Far-node kinds in both — the meet of a forward and backward frontier.
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

    /// Extend each reached node by `expand`, unioning results in (chains walks).
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
