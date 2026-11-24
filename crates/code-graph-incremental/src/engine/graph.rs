//! The links between the trees of a family: the edges written so far, kept
//! so a phase can follow them from node to node, across files.

use std::sync::OnceLock;

use rustc_hash::FxHashMap;
use smallvec::SmallVec;

use super::Loc;
use crate::intern::Lang;
use crate::tree::{Edge, EdgeAttribute, EdgeKind};

/// An edge's kind, file and node at one end.
pub type End = (EdgeKind, u32, u32);

#[derive(Default)]
pub struct Graph {
    edges: Vec<Edge>,
    attributes: Vec<EdgeAttribute>,
    outgoing: OnceLock<FxHashMap<End, SmallVec<[Loc; 2]>>>,
    by_attribute: Vec<OnceLock<FxHashMap<End, SmallVec<[(Loc, Loc); 1]>>>>,
}

/// Every end a graph read can look an edge up by: its source, its target,
pub fn ends<'a>(attributes: &'a [EdgeAttribute], e: &'a Edge) -> impl Iterator<Item = End> + 'a {
    let at = attributes
        .iter()
        .enumerate()
        .filter(|(_, a)| a.node)
        .filter_map(move |(slot, _)| Some((e.kind, e.from_tree, e.attr(slot)?)));
    [
        (e.kind, e.from_tree, e.from_node),
        (e.kind, e.to_tree, e.to_node),
    ]
    .into_iter()
    .chain(at)
}

fn from(e: &Edge) -> End {
    (e.kind, e.from_tree, e.from_node)
}

impl Graph {
    pub fn new<'a>(lang: &Lang, edges: impl IntoIterator<Item = &'a Edge>) -> Self {
        let attributes = lang.edge_attributes().to_vec();
        Self {
            edges: edges.into_iter().copied().collect(),
            by_attribute: attributes.iter().map(|_| OnceLock::new()).collect(),
            attributes,
            ..Self::default()
        }
    }

    /// The targets of `from`'s edges of `kind`.
    pub fn targets(&self, kind: EdgeKind, at: Loc) -> &[Loc] {
        let outgoing = self.outgoing.get_or_init(|| {
            let mut index: FxHashMap<End, SmallVec<[Loc; 2]>> = FxHashMap::default();
            for e in &self.edges {
                index.entry(from(e)).or_default().push(e.to_loc());
            }
            index
        });
        outgoing
            .get(&(kind, at.fi, at.node))
            .map_or(&[], |t| t.as_slice())
    }

    pub fn by_attribute(&self, slot: usize, kind: EdgeKind, at: Loc) -> &[(Loc, Loc)] {
        let Some(lock) = self.by_attribute.get(slot) else {
            return &[];
        };
        let node = self.attributes[slot].node;
        let index = lock.get_or_init(|| {
            let mut index: FxHashMap<End, SmallVec<[(Loc, Loc); 1]>> = FxHashMap::default();
            for e in &self.edges {
                let key = match (node, e.attr(slot)) {
                    (true, Some(node)) => (e.kind, e.from_tree, node),
                    (false, None) => from(e),
                    _ => continue,
                };
                index
                    .entry(key)
                    .or_default()
                    .push((e.from_loc(), e.to_loc()));
            }
            index
        });
        index
            .get(&(kind, at.fi, at.node))
            .map_or(&[], |t| t.as_slice())
    }
}
