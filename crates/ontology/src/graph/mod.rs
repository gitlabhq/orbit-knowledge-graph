//! petgraph-backed topology plus per-triple/per-node indexes over the flat
//! [`Ontology`]. The traversal surface is four composable pieces:
//!
//! - [`pred`] — the chainable edge-filter algebra ([`EdgePred`], `&`/`|`/`!`).
//! - [`walk`] — one [`Walk`] builder over direction, filter, and strategy.
//! - [`subgraph`] — the universal return type ([`Subgraph`]) with projections
//!   (`node_kinds`, `paths`, …) and set algebra (`union`/`intersect`/`difference`).
//! - this module — the materialized graph and its per-triple/per-node indexes.

mod pred;
mod subgraph;
mod walk;

pub use pred::{EdgeFn, EdgePred, any, kinds_in, synthesized, to, triple};
pub use subgraph::{Adjacency, EdgeMarks, Mark, MarkedEdge, Subgraph};
pub use walk::{Dir, Hop, Walk};

use std::collections::{BTreeSet, HashMap, HashSet};

use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::visit::EdgeRef;

use crate::{DenormDirection, EdgeVariantScope, Ontology, strip_schema_version_prefix};

#[derive(Debug, Clone)]
pub(crate) struct EdgeMeta {
    pub(crate) relationship_kind: String,
    pub(crate) synthesized: bool,
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
    pub(super) graph: DiGraph<String, EdgeMeta>,
    pub(super) index: HashMap<String, NodeIndex>,
    table_to_node: HashMap<String, String>,
    anchor_fk: Vec<(String, String)>,
    anchor_nodes: BTreeSet<String>,
    global_nodes: BTreeSet<String>,
    edge_templates: HashMap<(String, String, String), EdgeTemplate>,
    node_templates: HashMap<String, NodeTemplate>,
    denorm_coverage: HashMap<(String, String, DenormDirection), BTreeSet<String>>,
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EdgeEntity, etl::EdgeDirection};

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
            g.neighbors("B", EdgeDirection::Outgoing).adjacencies()[0].neighbor_kind,
            "C"
        );
        assert_eq!(
            g.neighbors("B", EdgeDirection::Incoming).adjacencies()[0].neighbor_kind,
            "A"
        );
    }

    #[test]
    fn kinds_connecting_folds_direction_and_filters_types() {
        let g = graph_of(&[("R", "A", "B"), ("S", "A", "B")]);
        let none = HashSet::new();
        assert_eq!(
            g.kinds_connecting("A", "B", &none).edge_kinds(),
            set(&["R", "S"])
        );
        assert_eq!(
            g.kinds_connecting("B", "A", &none).edge_kinds(),
            set(&["R", "S"])
        );
        assert_eq!(
            g.kinds_connecting("A", "B", &HashSet::from(["R"]))
                .edge_kinds(),
            set(&["R"])
        );
        assert!(g.kinds_connecting("A", "C", &none).is_empty());
    }

    #[test]
    fn reachable_within_respects_budget_and_terminates_on_cycles() {
        let chain = graph_of(&[("R", "A", "B"), ("R", "B", "C"), ("S", "C", "D")]);
        assert_eq!(chain.reachable_within("A", 0).node_kinds("A"), set(&[]));
        assert_eq!(chain.reachable_within("A", 1).node_kinds("A"), set(&["B"]));
        assert_eq!(
            chain.reachable_within("A", 3).node_kinds("A"),
            set(&["B", "C", "D"])
        );

        let cycle = graph_of(&[("R", "X", "Y"), ("R", "Y", "X")]);
        assert_eq!(cycle.reachable_within("X", 10).node_kinds("X"), set(&["Y"]));
    }

    #[test]
    fn reachable_within_types_follows_only_declared_kinds() {
        let g = graph_of(&[("R", "A", "B"), ("S", "B", "C")]);
        let only_r = HashSet::from(["R"]);
        assert_eq!(
            g.reachable_within_types("A", 2, Some(&only_r))
                .node_kinds("A"),
            set(&["B"])
        );
        assert_eq!(g.reachable_within("A", 2).node_kinds("A"), set(&["B", "C"]));
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
                .adjacencies()
                .iter()
                .all(|a| !a.relationship_kind.starts_with("__fk_"))
        );
    }

    #[test]
    fn reachable_composes_fk_with_triple_hops() {
        let g = embedded();
        assert!(
            g.reachable_within("File", 1)
                .node_kinds("File")
                .contains("Project")
        );
    }

    #[test]
    fn walk_both_reaches_either_orientation() {
        let g = graph_of(&[("R", "A", "B"), ("S", "C", "A")]);
        let reached = g
            .walk("A")
            .dir(Dir::Both)
            .filter(triple())
            .run()
            .node_kinds("A");
        assert_eq!(reached, set(&["B", "C"]));
    }

    #[test]
    fn walk_exposes_per_hop_depth_for_frontier_enumeration() {
        use std::collections::BTreeMap;
        let g = graph_of(&[("R", "A", "B"), ("R", "B", "C")]);
        let mut by_depth: BTreeMap<usize, BTreeSet<String>> = BTreeMap::new();
        for e in g.walk("A").hops(2).run().edges {
            by_depth.entry(e.depth).or_default().insert(e.to);
        }
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
        assert_eq!(
            g.paths_between("A", "C", 3).paths(),
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
        assert_eq!(
            g.paths_between("A", "T", 4).paths(),
            vec![
                vec!["R".to_string(), "R".to_string()],
                vec!["R".to_string(), "R".to_string(), "R".to_string()],
            ]
        );
    }

    #[test]
    fn edge_pred_chains_like_a_boolean() {
        let g = graph_of(&[("R", "A", "B"), ("S", "A", "C")]);
        let hits = g.walk("A").filter(triple() & to("B")).run().node_kinds("A");
        assert_eq!(hits, set(&["B"]));
    }

    #[test]
    fn walk_enumerate_colors_every_path() {
        let g = graph_of(&[("R", "A", "B"), ("S", "B", "C"), ("T", "A", "C")]);
        assert_eq!(
            g.walk("A").hops(3).enumerate_to("C").run().paths(),
            vec![
                vec!["R".to_string(), "S".to_string()],
                vec!["T".to_string()],
            ]
        );
    }

    #[test]
    fn subgraph_union_merges_marks_on_shared_edges() {
        let g = graph_of(&[("R", "A", "B"), ("S", "B", "C"), ("T", "A", "C")]);
        let left = g.walk("A").hops(2).enumerate_to("C").run();
        let right = g.walk("A").hops(1).enumerate_to("C").run();
        let merged = left.clone().union(right);
        assert_eq!(
            merged.paths(),
            left.union(g.paths_between("A", "C", 1)).paths()
        );
    }

    #[test]
    fn subgraph_intersect_and_difference_key_on_edge_identity() {
        let g = graph_of(&[("R", "A", "B"), ("S", "B", "C"), ("T", "A", "C")]);
        let two_hop = g.reachable_within("A", 2);
        let one_hop = g.reachable_within("A", 1);
        assert_eq!(
            two_hop.intersect(&one_hop).node_kinds("A"),
            set(&["B", "C"])
        );
        assert_eq!(two_hop.difference(&one_hop).node_kinds("A"), set(&["C"]));
    }

    #[test]
    fn mark_stamps_and_merges_per_hop_facts() {
        let g = graph_of(&[("R", "A", "B"), ("S", "A", "B")]);
        let sub = g
            .walk("A")
            .filter(triple())
            .mark(|hop, m| {
                m.role_floor = Some(hop.relationship_kind.len() as u32);
                m.scope_preserving = hop.relationship_kind == "R";
            })
            .run();
        let r = sub
            .edges
            .iter()
            .find(|e| e.relationship_kind == "R")
            .unwrap();
        assert_eq!(r.marks.role_floor, Some(1));
        assert!(r.marks.scope_preserving);
    }

    #[test]
    fn min_cost_path_prefers_the_cheaper_route() {
        let g = graph_of(&[("R", "A", "B"), ("S", "B", "D"), ("T", "A", "D")]);
        let unit = EdgeFn::of(|_: &Hop<'_>| 1u32);
        assert_eq!(
            g.min_cost_path("A", "D", &unit),
            Some(vec!["T".to_string()])
        );

        let cheap_long = EdgeFn::of(|h: &Hop<'_>| if h.relationship_kind == "T" { 10 } else { 1 });
        assert_eq!(
            g.min_cost_path("A", "D", &cheap_long),
            Some(vec!["R".to_string(), "S".to_string()])
        );
    }

    #[test]
    fn then_chains_neighbors_of_neighbors() {
        let g = graph_of(&[("R", "A", "B"), ("S", "B", "C")]);
        let two = g
            .neighbors("A", EdgeDirection::Outgoing)
            .then(|n| g.neighbors(n, EdgeDirection::Outgoing));
        assert_eq!(two.node_kinds("A"), set(&["B", "C"]));
    }

    #[test]
    fn intersect_nodes_meets_forward_and_backward() {
        let g = graph_of(&[("R", "A", "M"), ("S", "Z", "M")]);
        let forward = g.walk("A").dir(Dir::Outgoing).run();
        let backward = g.walk("Z").dir(Dir::Outgoing).run();
        assert_eq!(forward.intersect_nodes(&backward), set(&["M"]));
    }

    #[test]
    fn display_renders_marked_edges() {
        let g = graph_of(&[("R", "A", "B")]);
        let sub = g
            .walk("A")
            .filter(triple())
            .mark(|_, m| m.role_floor = Some(30))
            .run();
        assert_eq!(format!("{sub}"), "A --R--> B role_floor=30\n");
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
