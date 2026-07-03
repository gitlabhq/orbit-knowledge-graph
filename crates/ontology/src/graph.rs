//! petgraph-backed topology plus per-triple/per-node indexes over the flat [`Ontology`].
//!
//! The whole traversal surface is three pieces, mirroring `treesitter-visit`'s
//! `Pred`/`Extract`: a chainable [`EdgePred`] (the edge-filter algebra), a
//! [`Visitor`] with `enter`/`leave`, and one recursive [`OntologyGraph::traverse`]
//! interpreter. Every reduction ([`OntologyGraph::neighbors`],
//! [`OntologyGraph::reachable_within`], [`OntologyGraph::paths_between`], …) is a
//! thin reducer over that one primitive.

use std::collections::{BTreeSet, HashMap, HashSet};

use petgraph::Direction;
use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::visit::EdgeRef;

use crate::etl::EdgeDirection;
use crate::{DenormDirection, EdgeVariantScope, Ontology, strip_schema_version_prefix};

/// A relationship kind and the node kind on the far side of the hop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Adjacency {
    pub relationship_kind: String,
    pub neighbor_kind: String,
}

/// One edge crossed during a [`OntologyGraph::traverse`].
#[derive(Debug, Clone, Copy)]
pub struct Hop<'a> {
    pub from: &'a str,
    pub to: &'a str,
    pub relationship_kind: &'a str,
    pub synthesized: bool,
    pub depth: usize,
}

/// Direction to expand a traversal frontier. `Both` follows edges of either
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

/// Flow control returned by [`Visitor::enter`]: the dedup / cycle / short-circuit
/// knob. `Prune` skips expansion past this edge's far node; `Stop` ends the walk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Flow {
    Continue,
    Prune,
    Stop,
}

/// `enter` fires per crossed edge; `leave` fires after that edge's subtree is
/// exhausted, so a reducer can undo per-path state on ascent. Accumulate-only
/// reducers never override `leave`, so any `FnMut(&Hop) -> Flow` is a `Visitor`.
pub trait Visitor {
    fn enter(&mut self, hop: &Hop<'_>) -> Flow;
    fn leave(&mut self, _hop: &Hop<'_>) {}
}

impl<F: FnMut(&Hop<'_>) -> Flow> Visitor for F {
    fn enter(&mut self, hop: &Hop<'_>) -> Flow {
        self(hop)
    }
}

/// Composable boolean over a single [`Hop`] — the edge-filter analog of
/// `treesitter-visit`'s `Match`. Chain with `&`, `|`, `!`.
#[derive(Clone)]
pub enum EdgePred {
    Any,
    Synthesized,
    To(String),
    KindsIn(HashSet<String>),
    And(Box<EdgePred>, Box<EdgePred>),
    Or(Box<EdgePred>, Box<EdgePred>),
    Not(Box<EdgePred>),
}

impl EdgePred {
    fn test(&self, hop: &Hop<'_>) -> bool {
        match self {
            EdgePred::Any => true,
            EdgePred::Synthesized => hop.synthesized,
            EdgePred::To(n) => hop.to == n,
            EdgePred::KindsIn(set) => set.contains(hop.relationship_kind),
            EdgePred::And(a, b) => a.test(hop) && b.test(hop),
            EdgePred::Or(a, b) => a.test(hop) || b.test(hop),
            EdgePred::Not(p) => !p.test(hop),
        }
    }
}

impl std::ops::BitAnd for EdgePred {
    type Output = EdgePred;
    fn bitand(self, rhs: EdgePred) -> EdgePred {
        EdgePred::And(Box::new(self), Box::new(rhs))
    }
}

impl std::ops::BitOr for EdgePred {
    type Output = EdgePred;
    fn bitor(self, rhs: EdgePred) -> EdgePred {
        EdgePred::Or(Box::new(self), Box::new(rhs))
    }
}

impl std::ops::Not for EdgePred {
    type Output = EdgePred;
    fn not(self) -> EdgePred {
        EdgePred::Not(Box::new(self))
    }
}

/// Any edge.
#[must_use]
pub fn any() -> EdgePred {
    EdgePred::Any
}

/// A synthesized FK edge.
#[must_use]
pub fn synthesized() -> EdgePred {
    EdgePred::Synthesized
}

/// A declared triple edge (not FK-synthesized).
#[must_use]
pub fn triple() -> EdgePred {
    !EdgePred::Synthesized
}

/// The far node kind equals `node`.
#[must_use]
pub fn to(node: &str) -> EdgePred {
    EdgePred::To(node.to_string())
}

/// The relationship kind is in `types` (empty set matches nothing).
#[must_use]
pub fn kinds_in(types: &HashSet<&str>) -> EdgePred {
    EdgePred::KindsIn(types.iter().map(|s| (*s).to_string()).collect())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EdgeMeta {
    relationship_kind: String,
    synthesized: bool,
}

/// Query-independent facts about one `(kind, source, target)` edge triple.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeTemplate {
    pub scope: Option<EdgeVariantScope>,
    pub scope_preserving: bool,
    pub destination_table: String,
    pub fk_column: Option<String>,
}

/// Static, query-independent facts about one node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeTemplate {
    pub destination_table: String,
    pub sort_key: Vec<String>,
    pub has_traversal_path: bool,
    pub global: bool,
    pub path_scopable: bool,
    pub role_floor: Option<u32>,
    pub redaction_id_column: Option<String>,
}

/// Materialized topology and per-triple/per-node templates over an [`Ontology`].
#[derive(Debug, Clone)]
pub struct OntologyGraph {
    graph: DiGraph<String, EdgeMeta>,
    index: HashMap<String, NodeIndex>,
    table_to_node: HashMap<String, String>,
    anchor_fk: Vec<(String, String)>,
    anchor_nodes: BTreeSet<String>,
    global_nodes: BTreeSet<String>,
    edge_templates: HashMap<(String, String, String), EdgeTemplate>,
    node_templates: HashMap<String, NodeTemplate>,
    denorm_coverage: HashMap<(String, String, DenormDirection), BTreeSet<String>>,
}

impl PartialEq for OntologyGraph {
    fn eq(&self, other: &Self) -> bool {
        self.table_to_node == other.table_to_node
            && self.anchor_fk == other.anchor_fk
            && self.anchor_nodes == other.anchor_nodes
            && self.global_nodes == other.global_nodes
            && self.edge_templates == other.edge_templates
            && self.node_templates == other.node_templates
            && self.denorm_coverage == other.denorm_coverage
            && self.sorted_edges() == other.sorted_edges()
    }
}

impl Eq for OntologyGraph {}

impl OntologyGraph {
    #[must_use]
    pub fn build(ontology: &Ontology) -> Self {
        let mut graph = DiGraph::new();
        let mut index: HashMap<String, NodeIndex> = HashMap::new();
        let mut edge_templates = HashMap::new();
        let mut anchor_fk_seen: HashMap<String, String> = HashMap::new();
        let mut anchor_fk = Vec::new();
        let mut anchor_nodes = BTreeSet::new();

        let node_of = |graph: &mut DiGraph<String, EdgeMeta>,
                       index: &mut HashMap<String, NodeIndex>,
                       kind: &str| {
            *index
                .entry(kind.to_string())
                .or_insert_with(|| graph.add_node(kind.to_string()))
        };

        for edge in ontology.edges() {
            let source = node_of(&mut graph, &mut index, &edge.source_kind);
            let target = node_of(&mut graph, &mut index, &edge.target_kind);
            graph.add_edge(
                source,
                target,
                EdgeMeta {
                    relationship_kind: edge.relationship_kind.clone(),
                    synthesized: false,
                },
            );

            let scope_preserving = edge
                .scope
                .is_some_and(EdgeVariantScope::is_scope_preserving);
            edge_templates.insert(
                (
                    edge.relationship_kind.clone(),
                    edge.source_kind.clone(),
                    edge.target_kind.clone(),
                ),
                EdgeTemplate {
                    scope: edge.scope,
                    scope_preserving,
                    destination_table: edge.destination_table.clone(),
                    fk_column: edge.fk_column.clone(),
                },
            );

            if edge.scope == Some(EdgeVariantScope::NamespaceAnchor)
                && let Some(fk) = edge.fk_column.as_deref()
                && !anchor_fk_seen.contains_key(fk)
            {
                anchor_fk_seen.insert(fk.to_string(), edge.target_kind.clone());
                anchor_fk.push((fk.to_string(), edge.target_kind.clone()));
            }
        }

        for lookup in ontology.traversal_path_lookups() {
            anchor_nodes.insert(lookup.entity.clone());
        }

        let mut table_to_node = HashMap::new();
        let mut node_templates = HashMap::new();
        let mut global_nodes = BTreeSet::new();
        for node in ontology.nodes() {
            table_to_node.insert(
                strip_schema_version_prefix(&node.destination_table).to_string(),
                node.name.clone(),
            );
            if node.global {
                global_nodes.insert(node.name.clone());
            }
            let columns: HashSet<&str> = node
                .storage
                .columns
                .iter()
                .map(|c| c.name.as_str())
                .collect();
            for (fk, anchor) in &anchor_fk {
                if columns.contains(fk.as_str()) {
                    let source = node_of(&mut graph, &mut index, &node.name);
                    let target = node_of(&mut graph, &mut index, anchor);
                    graph.add_edge(
                        source,
                        target,
                        EdgeMeta {
                            relationship_kind: format!("__fk_{fk}"),
                            synthesized: true,
                        },
                    );
                }
            }
            node_templates.insert(
                node.name.clone(),
                NodeTemplate {
                    destination_table: node.destination_table.clone(),
                    sort_key: node.sort_key.clone(),
                    has_traversal_path: node.has_traversal_path,
                    global: node.global,
                    path_scopable: ontology.is_path_scopable(&node.name),
                    role_floor: node
                        .redaction
                        .as_ref()
                        .map(|r| r.required_role.as_access_level()),
                    redaction_id_column: node.redaction.as_ref().map(|r| r.id_column.clone()),
                },
            );
        }

        let mut denorm_coverage: HashMap<(String, String, DenormDirection), BTreeSet<String>> =
            HashMap::new();
        for denorm in ontology.denormalized_properties() {
            denorm_coverage
                .entry((
                    denorm.node_kind.clone(),
                    denorm.property_name.clone(),
                    denorm.direction.clone(),
                ))
                .or_default()
                .insert(denorm.relationship_kind.clone());
        }

        Self {
            graph,
            index,
            table_to_node,
            anchor_fk,
            anchor_nodes,
            global_nodes,
            edge_templates,
            node_templates,
            denorm_coverage,
        }
    }

    fn sorted_edges(&self) -> BTreeSet<(String, String, String, bool)> {
        self.graph
            .edge_references()
            .map(|e| {
                (
                    self.graph[e.source()].clone(),
                    self.graph[e.target()].clone(),
                    e.weight().relationship_kind.clone(),
                    e.weight().synthesized,
                )
            })
            .collect()
    }

    /// Whether the graph has real edge adjacency between named node kinds. False
    /// for a bare test scaffold whose edges declare no endpoint kinds.
    #[must_use]
    pub fn has_edges(&self) -> bool {
        self.graph.edge_references().any(|e| {
            !e.weight().synthesized
                && !self.graph[e.source()].is_empty()
                && !self.graph[e.target()].is_empty()
        })
    }

    /// The single traversal primitive. Crosses every edge matching `pred`,
    /// bounded to `max_hops`, expanding in `dir`. The visitor's [`Flow`] governs
    /// dedup (`Prune`) and short-circuit (`Stop`); [`Visitor::leave`] fires on
    /// ascent so a reducer can undo per-path state. Every adjacency, reachability,
    /// and path query is a thin reducer over this.
    pub fn traverse(
        &self,
        start: &str,
        max_hops: usize,
        dir: impl Into<Dir>,
        pred: &EdgePred,
        visitor: &mut impl Visitor,
    ) {
        if let Some(&start_ix) = self.index.get(start) {
            self.walk_from(start_ix, 0, max_hops, dir.into(), pred, visitor);
        }
    }

    fn walk_from(
        &self,
        node: NodeIndex,
        depth: usize,
        max_hops: usize,
        dir: Dir,
        pred: &EdgePred,
        visitor: &mut impl Visitor,
    ) -> Flow {
        if depth == max_hops {
            return Flow::Continue;
        }
        for &d in dir.petgraph_dirs() {
            for e in self.graph.edges_directed(node, d) {
                let far = if e.source() == node {
                    e.target()
                } else {
                    e.source()
                };
                let hop = Hop {
                    from: &self.graph[node],
                    to: &self.graph[far],
                    relationship_kind: &e.weight().relationship_kind,
                    synthesized: e.weight().synthesized,
                    depth: depth + 1,
                };
                if !pred.test(&hop) {
                    continue;
                }
                match visitor.enter(&hop) {
                    Flow::Stop => return Flow::Stop,
                    Flow::Prune => {}
                    Flow::Continue => {
                        if self.walk_from(far, depth + 1, max_hops, dir, pred, visitor)
                            == Flow::Stop
                        {
                            return Flow::Stop;
                        }
                        visitor.leave(&hop);
                    }
                }
            }
        }
        Flow::Continue
    }

    /// Adjacency leaving (`Outgoing`) or entering (`Incoming`) a node kind,
    /// excluding synthesized FK edges.
    #[must_use]
    pub fn neighbors(&self, node_kind: &str, direction: impl Into<Dir>) -> Vec<Adjacency> {
        let mut out = BTreeSet::new();
        self.traverse(node_kind, 1, direction, &triple(), &mut |h: &Hop<'_>| {
            out.insert((h.relationship_kind.to_string(), h.to.to_string()));
            Flow::Prune
        });
        out.into_iter()
            .map(|(relationship_kind, neighbor_kind)| Adjacency {
                relationship_kind,
                neighbor_kind,
            })
            .collect()
    }

    /// Relationship kinds connecting `a` and `b` in either orientation, filtered
    /// to `types` when non-empty. Direction is folded in because the lowerer
    /// matches a triple regardless of the query's declared direction.
    #[must_use]
    pub fn kinds_connecting(&self, a: &str, b: &str, types: &HashSet<&str>) -> BTreeSet<String> {
        let kind_filter = if types.is_empty() {
            any()
        } else {
            kinds_in(types)
        };
        let pred = triple() & to(b) & kind_filter;
        let mut kinds = BTreeSet::new();
        self.traverse(a, 1, Dir::Both, &pred, &mut |h: &Hop<'_>| {
            kinds.insert(h.relationship_kind.to_string());
            Flow::Prune
        });
        kinds
    }

    /// Node kinds reachable from `start` within `max_hops` outgoing edges,
    /// excluding `start`. Synthesized FK edges compose with triple hops.
    #[must_use]
    pub fn reachable_within(&self, start: &str, max_hops: usize) -> BTreeSet<String> {
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
    ) -> BTreeSet<String> {
        let pred = match types {
            Some(t) => synthesized() | kinds_in(t),
            None => any(),
        };
        let mut reached = BTreeSet::new();
        let mut seen: HashSet<String> = HashSet::new();
        self.traverse(start, max_hops, Dir::Outgoing, &pred, &mut |h: &Hop<'_>| {
            if h.to != start {
                reached.insert(h.to.to_string());
            }
            if seen.insert(h.to.to_string()) {
                Flow::Continue
            } else {
                Flow::Prune
            }
        });
        reached
    }

    /// Whether `node`'s table carries an anchor FK to `anchor` (edge-triple-free synthesis).
    #[must_use]
    pub fn fk_reaches(&self, node: &str, anchor: &str) -> bool {
        let mut found = false;
        self.traverse(
            node,
            1,
            Dir::Outgoing,
            &(synthesized() & to(anchor)),
            &mut |_: &Hop<'_>| {
                found = true;
                Flow::Stop
            },
        );
        found
    }

    /// Every declared edge-kind sequence from `a` to `b` within `max_hops`,
    /// following triple and FK edges. Backs pathfinding frontier enumeration
    /// without a SQL unroll; the empty vec means unreachable.
    #[must_use]
    pub fn paths_between(&self, a: &str, b: &str, max_hops: usize) -> Vec<Vec<String>> {
        let mut collector = PathCollector {
            target: b,
            stack: Vec::new(),
            on_path: HashSet::from([a.to_string()]),
            out: Vec::new(),
        };
        self.traverse(a, max_hops, Dir::Outgoing, &any(), &mut collector);
        collector.out
    }

    /// Node kind backing a physical table (tolerating a `v{N}_` prefix); `None` for edge/CTE/unknown tables.
    #[must_use]
    pub fn table_to_node(&self, table: &str) -> Option<&str> {
        self.table_to_node
            .get(strip_schema_version_prefix(table))
            .map(String::as_str)
    }

    /// `(fk_column, anchor_entity)` pairs from `namespace_anchor` variants, deduped by column.
    #[must_use = "returns the mapping iterator without mutating the graph"]
    pub fn anchor_fk_mappings(&self) -> impl Iterator<Item = (&str, &str)> {
        self.anchor_fk
            .iter()
            .map(|(fk, anchor)| (fk.as_str(), anchor.as_str()))
    }

    #[must_use]
    pub fn is_anchor(&self, entity: &str) -> bool {
        self.anchor_nodes.contains(entity)
    }

    #[must_use]
    pub fn is_global(&self, entity: &str) -> bool {
        self.global_nodes.contains(entity)
    }

    #[must_use]
    pub fn edge_template(&self, kind: &str, source: &str, target: &str) -> Option<&EdgeTemplate> {
        self.edge_templates
            .get(&(kind.to_string(), source.to_string(), target.to_string()))
    }

    #[must_use]
    pub fn node_template(&self, entity: &str) -> Option<&NodeTemplate> {
        self.node_templates.get(entity)
    }

    /// Static per-node facts for the node backing a physical `table`. Resolves
    /// `table → node → template` in one call, so the security pass reads a
    /// table's role floor / scopability / global-ness without a per-alias scan.
    #[must_use]
    pub fn table_facts(&self, table: &str) -> Option<&NodeTemplate> {
        self.node_template(self.table_to_node(table)?)
    }

    /// Whether the `(kind, source, target)` triple keeps both endpoints in one
    /// namespace. Reads the per-triple template rather than rescanning edges.
    #[must_use]
    pub fn is_scope_preserving(&self, kind: &str, source: &str, target: &str) -> bool {
        self.edge_template(kind, source, target)
            .is_some_and(|t| t.scope_preserving)
    }

    /// The scope variant for the `(kind, source, target)` triple, if annotated.
    #[must_use]
    pub fn edge_scope(&self, kind: &str, source: &str, target: &str) -> Option<EdgeVariantScope> {
        self.edge_template(kind, source, target)
            .and_then(|t| t.scope)
    }

    /// Relationship kinds carrying `entity`'s `prop` on their edge table in `direction`.
    #[must_use]
    pub fn denorm_kinds(
        &self,
        entity: &str,
        prop: &str,
        direction: DenormDirection,
    ) -> Option<&BTreeSet<String>> {
        self.denorm_coverage
            .get(&(entity.to_string(), prop.to_string(), direction))
    }
}

/// The one reducer needing backtracking: push the crossed kind on `enter`, pop
/// it (and clear the far node from the current path) on `leave`, so a node on one
/// exhausted branch stays traversable on a sibling branch.
struct PathCollector<'a> {
    target: &'a str,
    stack: Vec<String>,
    on_path: HashSet<String>,
    out: Vec<Vec<String>>,
}

impl Visitor for PathCollector<'_> {
    fn enter(&mut self, hop: &Hop<'_>) -> Flow {
        if self.on_path.contains(hop.to) {
            return Flow::Prune;
        }
        self.stack.push(hop.relationship_kind.to_string());
        if hop.to == self.target {
            self.out.push(self.stack.clone());
            self.stack.pop();
            Flow::Prune
        } else {
            self.on_path.insert(hop.to.to_string());
            Flow::Continue
        }
    }

    fn leave(&mut self, hop: &Hop<'_>) {
        self.on_path.remove(hop.to);
        self.stack.pop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EdgeEntity;

    fn edge(kind: &str, source: &str, target: &str) -> EdgeEntity {
        EdgeEntity {
            relationship_kind: kind.to_string(),
            source: source.to_string(),
            source_kind: source.to_string(),
            target: target.to_string(),
            target_kind: target.to_string(),
            destination_table: "gl_edge".to_string(),
            fk_column: None,
            scope: None,
        }
    }

    fn graph_of(variants: &[(&str, &str, &str)]) -> OntologyGraph {
        let nodes: BTreeSet<&str> = variants.iter().flat_map(|&(_, s, t)| [s, t]).collect();
        let kinds: BTreeSet<&str> = variants.iter().map(|&(k, _, _)| k).collect();
        let mut ont = Ontology::new().with_nodes(nodes).with_edges(kinds);
        for &(kind, s, t) in variants {
            ont = ont.with_edge_variant(edge(kind, s, t));
        }
        ont.graph()
    }

    fn set(items: &[&str]) -> BTreeSet<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    fn embedded() -> OntologyGraph {
        Ontology::load_embedded().unwrap().graph()
    }

    #[test]
    fn neighbors_are_directional() {
        let g = graph_of(&[("R", "A", "B"), ("R", "B", "C")]);
        assert_eq!(
            g.neighbors("B", EdgeDirection::Outgoing)[0].neighbor_kind,
            "C"
        );
        assert_eq!(
            g.neighbors("B", EdgeDirection::Incoming)[0].neighbor_kind,
            "A"
        );
    }

    #[test]
    fn kinds_connecting_folds_direction_and_filters_types() {
        let g = graph_of(&[("R", "A", "B"), ("S", "A", "B")]);
        let none = HashSet::new();
        assert_eq!(g.kinds_connecting("A", "B", &none), set(&["R", "S"]));
        assert_eq!(g.kinds_connecting("B", "A", &none), set(&["R", "S"]));
        assert_eq!(
            g.kinds_connecting("A", "B", &HashSet::from(["R"])),
            set(&["R"])
        );
        assert!(g.kinds_connecting("A", "C", &none).is_empty());
    }

    #[test]
    fn reachable_within_respects_budget_and_terminates_on_cycles() {
        let chain = graph_of(&[("R", "A", "B"), ("R", "B", "C"), ("S", "C", "D")]);
        assert_eq!(chain.reachable_within("A", 0), set(&[]));
        assert_eq!(chain.reachable_within("A", 1), set(&["B"]));
        assert_eq!(chain.reachable_within("A", 3), set(&["B", "C", "D"]));

        let cycle = graph_of(&[("R", "X", "Y"), ("R", "Y", "X")]);
        assert_eq!(cycle.reachable_within("X", 10), set(&["Y"]));
    }

    #[test]
    fn reachable_within_types_follows_only_declared_kinds() {
        let g = graph_of(&[("R", "A", "B"), ("S", "B", "C")]);
        let only_r = HashSet::from(["R"]);
        assert_eq!(g.reachable_within_types("A", 2, Some(&only_r)), set(&["B"]));
        assert_eq!(g.reachable_within("A", 2), set(&["B", "C"]));
    }

    #[test]
    fn table_to_node_strips_version_prefix() {
        let g = embedded();
        assert_eq!(g.table_to_node("gl_project"), Some("Project"));
        assert_eq!(g.table_to_node("v42_gl_project"), Some("Project"));
        assert_eq!(g.table_to_node("gl_edge"), None);
    }

    #[test]
    fn fk_reaches_is_directional() {
        let g = embedded();
        assert!(g.fk_reaches("File", "Project"));
        assert!(!g.fk_reaches("Project", "File"));
    }

    #[test]
    fn fk_edges_are_excluded_from_neighbors_and_kinds() {
        let g = embedded();
        let none = HashSet::new();
        assert!(g.kinds_connecting("File", "Project", &none).is_empty());
        assert!(
            g.neighbors("File", EdgeDirection::Outgoing)
                .iter()
                .all(|a| !a.relationship_kind.starts_with("__fk_"))
        );
    }

    #[test]
    fn reachable_composes_fk_with_triple_hops() {
        let g = embedded();
        assert!(g.reachable_within("File", 1).contains("Project"));
    }

    #[test]
    fn traverse_both_reaches_either_orientation() {
        let g = graph_of(&[("R", "A", "B"), ("S", "C", "A")]);
        let mut reached = BTreeSet::new();
        g.traverse("A", 1, Dir::Both, &triple(), &mut |h: &Hop<'_>| {
            reached.insert(h.to.to_string());
            Flow::Prune
        });
        assert_eq!(reached, set(&["B", "C"]));
    }

    #[test]
    fn traverse_exposes_per_hop_depth_for_frontier_enumeration() {
        use std::collections::BTreeMap;
        let g = graph_of(&[("R", "A", "B"), ("R", "B", "C")]);
        let mut by_depth: BTreeMap<usize, BTreeSet<String>> = BTreeMap::new();
        g.traverse("A", 2, Dir::Outgoing, &any(), &mut |h: &Hop<'_>| {
            by_depth
                .entry(h.depth)
                .or_default()
                .insert(h.to.to_string());
            Flow::Continue
        });
        assert_eq!(by_depth[&1], set(&["B"]));
        assert_eq!(by_depth[&2], set(&["C"]));
    }

    #[test]
    fn anchor_fk_mappings_are_deduplicated_by_column() {
        let g = embedded();
        let mapped: Vec<_> = g.anchor_fk_mappings().collect();
        let columns: BTreeSet<&str> = mapped.iter().map(|(fk, _)| *fk).collect();
        assert_eq!(columns.len(), mapped.len());
        assert!(mapped.contains(&("project_id", "Project")));
    }

    #[test]
    fn table_facts_resolve_table_to_node_template() {
        let g = embedded();
        assert!(g.table_facts("gl_user").unwrap().global);
        assert!(g.table_facts("v42_gl_user").unwrap().global);
        assert!(g.table_facts("gl_edge").is_none());
    }

    #[test]
    fn scope_accessors_read_the_template() {
        let g = embedded();
        assert!(g.is_scope_preserving("CONTAINS", "Group", "Project"));
        assert_eq!(
            g.edge_scope("CONTAINS", "Group", "Project"),
            Some(EdgeVariantScope::SameNamespace)
        );
    }

    #[test]
    fn paths_between_enumerates_kind_sequences() {
        let g = graph_of(&[("R", "A", "B"), ("S", "B", "C"), ("T", "A", "C")]);
        let mut paths = g.paths_between("A", "C", 3);
        paths.sort();
        assert_eq!(
            paths,
            vec![
                vec!["R".to_string(), "S".to_string()],
                vec!["T".to_string()],
            ]
        );
        assert!(g.paths_between("C", "A", 3).is_empty());
    }

    #[test]
    fn paths_between_finds_routes_through_shared_interior_nodes() {
        let g = graph_of(&[
            ("R", "A", "X"),
            ("R", "X", "T"),
            ("R", "A", "Y"),
            ("R", "Y", "X"),
        ]);
        let mut paths = g.paths_between("A", "T", 4);
        paths.sort();
        assert_eq!(
            paths,
            vec![
                vec!["R".to_string(), "R".to_string()],
                vec!["R".to_string(), "R".to_string(), "R".to_string()],
            ]
        );
    }

    #[test]
    fn edge_pred_chains_like_a_boolean() {
        let g = graph_of(&[("R", "A", "B"), ("S", "A", "C")]);
        let pred = triple() & to("B");
        let mut hits = BTreeSet::new();
        g.traverse("A", 1, Dir::Outgoing, &pred, &mut |h: &Hop<'_>| {
            hits.insert(h.to.to_string());
            Flow::Prune
        });
        assert_eq!(hits, set(&["B"]));
    }

    #[test]
    fn templates_carry_static_facts() {
        let g = embedded();
        let contains = g.edge_template("CONTAINS", "Group", "Project").unwrap();
        assert!(contains.scope_preserving);
        assert_eq!(contains.destination_table, "gl_edge");
        assert!(g.node_template("User").unwrap().global);
        assert!(g.is_global("User"));
    }
}
