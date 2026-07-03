//! One traversal builder over three orthogonal axes: direction, edge selection
//! ([`EdgePred`]), and strategy ([`Strategy::Frontier`] dedups nodes for
//! reachability; [`Strategy::Enumerate`] keeps every distinct path and colors
//! its edges). `neighbors` / `reachable_within` / `paths_between` are presets.

use std::collections::{HashSet, VecDeque};

use petgraph::Direction;
use petgraph::graph::NodeIndex;
use petgraph::visit::EdgeRef;

use super::pred::{EdgePred, any, triple};
use super::subgraph::{MarkedEdge, Subgraph};
use super::{EdgeMeta, OntologyGraph};
use crate::etl::EdgeDirection;

/// One edge crossed during a walk.
#[derive(Debug, Clone, Copy)]
pub struct Hop<'a> {
    pub from: &'a str,
    pub to: &'a str,
    pub relationship_kind: &'a str,
    pub synthesized: bool,
    pub depth: usize,
}

/// Direction to expand a walk frontier. `Both` follows edges of either
/// orientation, which is what pathfinding frontiers, `neighbors direction: both`,
/// and undirected connectivity all need.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dir {
    Outgoing,
    Incoming,
    Both,
}

impl Dir {
    fn petgraph_dirs(self) -> &'static [Direction] {
        match self {
            Self::Outgoing => &[Direction::Outgoing],
            Self::Incoming => &[Direction::Incoming],
            Self::Both => &[Direction::Outgoing, Direction::Incoming],
        }
    }
}

impl From<EdgeDirection> for Dir {
    fn from(d: EdgeDirection) -> Self {
        match d {
            EdgeDirection::Outgoing => Self::Outgoing,
            EdgeDirection::Incoming => Self::Incoming,
        }
    }
}

/// Frontier expansion visits each node once (reachability); enumerate keeps every
/// distinct simple path (cycles broken by on-path membership) and colors edges.
#[derive(Clone)]
enum Strategy {
    Frontier,
    Enumerate { target: Option<String> },
}

/// A configured traversal. Build with [`OntologyGraph::walk`], then `run`.
pub struct Walk<'g> {
    graph: &'g OntologyGraph,
    start: String,
    max_hops: usize,
    dir: Dir,
    pred: EdgePred,
    strategy: Strategy,
}

impl<'g> Walk<'g> {
    pub(super) fn new(graph: &'g OntologyGraph, start: impl Into<String>) -> Self {
        Self {
            graph,
            start: start.into(),
            max_hops: 1,
            dir: Dir::Outgoing,
            pred: any(),
            strategy: Strategy::Frontier,
        }
    }

    #[must_use]
    pub fn hops(mut self, max_hops: usize) -> Self {
        self.max_hops = max_hops;
        self
    }

    #[must_use]
    pub fn dir(mut self, dir: impl Into<Dir>) -> Self {
        self.dir = dir.into();
        self
    }

    #[must_use]
    pub fn filter(mut self, pred: EdgePred) -> Self {
        self.pred = self.pred & pred;
        self
    }

    /// Switch to path enumeration: keep every distinct simple path (not just the
    /// reachable frontier) and color each completed path's edges.
    #[must_use]
    pub fn enumerate(mut self) -> Self {
        self.strategy = Strategy::Enumerate { target: None };
        self
    }

    /// Enumerate only paths ending at `target`.
    #[must_use]
    pub fn enumerate_to(mut self, target: impl Into<String>) -> Self {
        self.strategy = Strategy::Enumerate {
            target: Some(target.into()),
        };
        self
    }

    #[must_use]
    pub fn run(self) -> Subgraph {
        let mut sub = Subgraph::default();
        let Some(&start_ix) = self.graph.index.get(&self.start) else {
            return sub;
        };
        match &self.strategy {
            Strategy::Frontier => self.frontier(start_ix, &mut sub),
            Strategy::Enumerate { target } => {
                let mut walk = PathState {
                    target: target.clone(),
                    on_path: HashSet::from([start_ix]),
                    trail: Vec::new(),
                    next_id: 0,
                };
                self.enumerate_from(start_ix, &mut walk, &mut sub);
            }
        }
        sub
    }

    fn frontier(&self, start_ix: NodeIndex, sub: &mut Subgraph) {
        let mut seen = HashSet::from([start_ix]);
        let mut frontier = VecDeque::from([(start_ix, 0usize)]);
        while let Some((node, depth)) = frontier.pop_front() {
            if depth == self.max_hops {
                continue;
            }
            for &d in self.dir.petgraph_dirs() {
                for e in self.graph.graph.edges_directed(node, d) {
                    let far = if e.source() == node {
                        e.target()
                    } else {
                        e.source()
                    };
                    if let Some(edge) = self.match_edge(node, far, e.weight(), depth + 1) {
                        sub.edges.push(edge);
                        if seen.insert(far) {
                            frontier.push_back((far, depth + 1));
                        }
                    }
                }
            }
        }
    }

    fn enumerate_from(&self, node: NodeIndex, walk: &mut PathState, sub: &mut Subgraph) {
        if walk.trail.len() == self.max_hops {
            return;
        }
        for &d in self.dir.petgraph_dirs() {
            for e in self.graph.graph.edges_directed(node, d) {
                let far = if e.source() == node {
                    e.target()
                } else {
                    e.source()
                };
                if walk.on_path.contains(&far) {
                    continue;
                }
                let Some(edge) = self.match_edge(node, far, e.weight(), walk.trail.len() + 1)
                else {
                    continue;
                };
                let ix = sub.edges.len();
                sub.edges.push(edge);
                walk.trail.push(ix);
                let far_kind = self.graph.graph[far].clone();
                let is_target = walk.target.as_deref() == Some(far_kind.as_str());
                if walk.target.is_none() || is_target {
                    let pid = walk.next_id;
                    walk.next_id += 1;
                    for &edge_ix in &walk.trail {
                        sub.edges[edge_ix].marks.path_ids.push(pid);
                    }
                }
                if !is_target {
                    walk.on_path.insert(far);
                    self.enumerate_from(far, walk, sub);
                    walk.on_path.remove(&far);
                }
                walk.trail.pop();
            }
        }
    }

    fn match_edge(
        &self,
        node: NodeIndex,
        far: NodeIndex,
        meta: &EdgeMeta,
        depth: usize,
    ) -> Option<MarkedEdge> {
        let hop = Hop {
            from: &self.graph.graph[node],
            to: &self.graph.graph[far],
            relationship_kind: &meta.relationship_kind,
            synthesized: meta.synthesized,
            depth,
        };
        self.pred.test(&hop).then(|| MarkedEdge {
            from: hop.from.to_string(),
            to: hop.to.to_string(),
            relationship_kind: hop.relationship_kind.to_string(),
            synthesized: hop.synthesized,
            depth: hop.depth,
            marks: Default::default(),
        })
    }
}

struct PathState {
    target: Option<String>,
    on_path: HashSet<NodeIndex>,
    trail: Vec<usize>,
    next_id: usize,
}

impl OntologyGraph {
    /// Start a traversal from `start`. Configure with `hops`/`dir`/`filter`/
    /// `enumerate`, then `run` for a [`Subgraph`].
    #[must_use]
    pub fn walk(&self, start: impl Into<String>) -> Walk<'_> {
        Walk::new(self, start)
    }

    /// Adjacency leaving (`Outgoing`) or entering (`Incoming`) a node kind,
    /// excluding synthesized FK edges. Project with [`Subgraph::adjacencies`].
    #[must_use]
    pub fn neighbors(&self, node_kind: &str, direction: impl Into<Dir>) -> Subgraph {
        self.walk(node_kind).dir(direction).filter(triple()).run()
    }

    /// Triple edges connecting `a` and `b` in either orientation, filtered to
    /// `types` when non-empty. Project with [`Subgraph::edge_kinds`].
    #[must_use]
    pub fn kinds_connecting(&self, a: &str, b: &str, types: &HashSet<&str>) -> Subgraph {
        let kind_filter = if types.is_empty() {
            any()
        } else {
            super::pred::kinds_in(types)
        };
        self.walk(a)
            .dir(Dir::Both)
            .filter(triple() & super::pred::to(b) & kind_filter)
            .run()
    }

    /// Subgraph reachable from `start` within `max_hops` outgoing edges.
    /// Synthesized FK edges compose with triple hops. Project with
    /// [`Subgraph::node_kinds`].
    #[must_use]
    pub fn reachable_within(&self, start: &str, max_hops: usize) -> Subgraph {
        self.reachable_within_types(start, max_hops, None)
    }

    /// Like [`reachable_within`], but a `Some(types)` set restricts triple edges
    /// to `types`; synthesized FK edges are always traversable.
    #[must_use]
    pub fn reachable_within_types(
        &self,
        start: &str,
        max_hops: usize,
        types: Option<&HashSet<&str>>,
    ) -> Subgraph {
        let pred = match types {
            Some(t) => super::pred::synthesized() | super::pred::kinds_in(t),
            None => any(),
        };
        self.walk(start).hops(max_hops).filter(pred).run()
    }

    /// Whether `node`'s table carries an anchor FK to `anchor` (edge-triple-free synthesis).
    #[must_use]
    pub fn fk_reaches(&self, node: &str, anchor: &str) -> bool {
        !self
            .walk(node)
            .filter(super::pred::synthesized() & super::pred::to(anchor))
            .run()
            .is_empty()
    }

    /// Subgraph of every declared path from `a` to `b` within `max_hops`, edges
    /// colored by the paths they belong to. Project ordered kind-sequences with
    /// [`Subgraph::paths`]; empty when unreachable.
    #[must_use]
    pub fn paths_between(&self, a: &str, b: &str, max_hops: usize) -> Subgraph {
        self.walk(a).hops(max_hops).enumerate_to(b).run()
    }
}
