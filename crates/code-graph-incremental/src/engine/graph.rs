//! The links between the trees of a family: the edges written so far, kept
//! so a phase can follow them from node to node, across files.

use rustc_hash::{FxHashMap, FxHashSet};
use smallvec::SmallVec;

use super::Loc;
use crate::tree::{CallResolution, Edge, EdgeKind};

/// An edge's kind, file and node at one end.
type End = (EdgeKind, u32, u32);

/// The edges of a family, by kind: from each node, to each node, and at each
/// site.
#[derive(Default)]
pub struct Graph {
    incoming: FxHashSet<End>,
    outgoing: FxHashMap<End, SmallVec<[Loc; 2]>>,
    unresolved: FxHashMap<End, SmallVec<[Loc; 2]>>,
    sites: FxHashMap<End, SmallVec<[(Loc, Loc); 1]>>,
}

impl Graph {
    pub fn new<'a>(edges: impl IntoIterator<Item = &'a Edge>) -> Self {
        let mut graph = Self::default();
        for e in edges {
            let (from, to) = (e.from_loc(), e.to_loc());
            let key = (e.kind, e.from_tree, e.from_node);
            graph.incoming.insert((e.kind, e.to_tree, e.to_node));
            graph.outgoing.entry(key).or_default().push(to);
            if e.call_resolution == CallResolution::Unknown {
                graph.unresolved.entry(key).or_default().push(to);
            }
            if let Some(site) = e.site {
                graph
                    .sites
                    .entry((e.kind, e.from_tree, site))
                    .or_default()
                    .push((from, to));
            }
        }
        graph
    }

    /// The targets of `from`'s edges of `kind`.
    pub fn targets(&self, kind: EdgeKind, from: Loc) -> &[Loc] {
        self.outgoing
            .get(&(kind, from.fi, from.node))
            .map_or(&[], |t| t.as_slice())
    }

    /// The targets of `from`'s edges of `kind` whose call resolution is unknown.
    pub fn unresolved(&self, kind: EdgeKind, from: Loc) -> &[Loc] {
        self.unresolved
            .get(&(kind, from.fi, from.node))
            .map_or(&[], |t| t.as_slice())
    }

    /// The edges of `kind` made at `site`, as (from, to).
    pub fn at_site(&self, kind: EdgeKind, site: Loc) -> &[(Loc, Loc)] {
        self.sites
            .get(&(kind, site.fi, site.node))
            .map_or(&[], |t| t.as_slice())
    }

    /// Whether `at` has an edge of `kind` into it, or out of it.
    pub fn has(&self, kind: EdgeKind, incoming: bool, at: Loc) -> bool {
        let key = (kind, at.fi, at.node);
        match incoming {
            true => self.incoming.contains(&key),
            false => self.outgoing.contains_key(&key),
        }
    }
}
