//! Materialized adjacency, table→node, template, and reachability indexes over the flat [`Ontology`] maps.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};

use crate::etl::EdgeDirection;
use crate::{DenormDirection, EdgeVariantScope, Ontology, strip_schema_version_prefix};

/// A relationship kind and the node kind on the far side of the hop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Adjacency {
    pub relationship_kind: String,
    pub neighbor_kind: String,
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OntologyGraph {
    outgoing: BTreeMap<String, Vec<Adjacency>>,
    incoming: BTreeMap<String, Vec<Adjacency>>,
    table_to_node: HashMap<String, String>,
    anchor_fk: Vec<(String, String)>,
    fk_reachable: BTreeMap<String, BTreeSet<String>>,
    anchor_nodes: BTreeSet<String>,
    global_nodes: BTreeSet<String>,
    edge_templates: HashMap<(String, String, String), EdgeTemplate>,
    node_templates: HashMap<String, NodeTemplate>,
    denorm_coverage: HashMap<(String, String, DenormDirection), BTreeSet<String>>,
}

impl OntologyGraph {
    #[must_use]
    pub fn build(ontology: &Ontology) -> Self {
        let mut outgoing: BTreeMap<String, Vec<Adjacency>> = BTreeMap::new();
        let mut incoming: BTreeMap<String, Vec<Adjacency>> = BTreeMap::new();
        let mut edge_templates = HashMap::new();
        let mut anchor_fk_seen: HashMap<String, String> = HashMap::new();
        let mut anchor_fk = Vec::new();
        let mut anchor_nodes = BTreeSet::new();

        for edge in ontology.edges() {
            outgoing
                .entry(edge.source_kind.clone())
                .or_default()
                .push(Adjacency {
                    relationship_kind: edge.relationship_kind.clone(),
                    neighbor_kind: edge.target_kind.clone(),
                });
            incoming
                .entry(edge.target_kind.clone())
                .or_default()
                .push(Adjacency {
                    relationship_kind: edge.relationship_kind.clone(),
                    neighbor_kind: edge.source_kind.clone(),
                });

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

        for adjacencies in outgoing.values_mut().chain(incoming.values_mut()) {
            adjacencies.sort_by(|a, b| {
                a.relationship_kind
                    .cmp(&b.relationship_kind)
                    .then_with(|| a.neighbor_kind.cmp(&b.neighbor_kind))
            });
            adjacencies.dedup();
        }

        for lookup in ontology.traversal_path_lookups() {
            anchor_nodes.insert(lookup.entity.clone());
        }

        let mut table_to_node = HashMap::new();
        let mut node_templates = HashMap::new();
        let mut global_nodes = BTreeSet::new();
        let mut fk_reachable: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
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
                    fk_reachable
                        .entry(node.name.clone())
                        .or_default()
                        .insert(anchor.clone());
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
            outgoing,
            incoming,
            table_to_node,
            anchor_fk,
            fk_reachable,
            anchor_nodes,
            global_nodes,
            edge_templates,
            node_templates,
            denorm_coverage,
        }
    }

    /// Adjacency leaving (`Outgoing`) or entering (`Incoming`) a node kind.
    #[must_use]
    pub fn neighbors(&self, node_kind: &str, direction: EdgeDirection) -> &[Adjacency] {
        let map = match direction {
            EdgeDirection::Outgoing => &self.outgoing,
            EdgeDirection::Incoming => &self.incoming,
        };
        map.get(node_kind).map_or(&[], Vec::as_slice)
    }

    /// Relationship kinds connecting `a` and `b` in either orientation, filtered
    /// to `types` when non-empty. Direction is folded in because the lowerer
    /// matches a triple regardless of the query's declared direction.
    #[must_use]
    pub fn kinds_connecting(&self, a: &str, b: &str, types: &HashSet<&str>) -> BTreeSet<&str> {
        self.neighbors(a, EdgeDirection::Outgoing)
            .iter()
            .chain(self.neighbors(a, EdgeDirection::Incoming))
            .filter(|adj| adj.neighbor_kind == b)
            .filter(|adj| types.is_empty() || types.contains(adj.relationship_kind.as_str()))
            .map(|adj| adj.relationship_kind.as_str())
            .collect()
    }

    /// Node kinds reachable from `start` within `max_hops` outgoing edges, excluding `start`.
    #[must_use]
    pub fn reachable_within(&self, start: &str, max_hops: usize) -> BTreeSet<String> {
        self.reachable_within_types(start, max_hops, None)
    }

    /// Like [`reachable_within`], but only traverses edges whose relationship
    /// kind is in `types` when `Some` (any kind when `None`).
    #[must_use]
    pub fn reachable_within_types(
        &self,
        start: &str,
        max_hops: usize,
        types: Option<&HashSet<&str>>,
    ) -> BTreeSet<String> {
        let mut visited: HashSet<&str> = HashSet::from([start]);
        let mut reached = BTreeSet::new();
        let mut frontier: VecDeque<(&str, usize)> = VecDeque::from([(start, 0usize)]);

        while let Some((kind, depth)) = frontier.pop_front() {
            if depth == max_hops {
                continue;
            }
            for adj in self.neighbors(kind, EdgeDirection::Outgoing) {
                if types.is_some_and(|t| !t.contains(adj.relationship_kind.as_str())) {
                    continue;
                }
                if visited.insert(adj.neighbor_kind.as_str()) {
                    reached.insert(adj.neighbor_kind.clone());
                    frontier.push_back((adj.neighbor_kind.as_str(), depth + 1));
                }
            }
        }
        reached
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

    /// Whether `node`'s table carries an anchor FK to `anchor` (edge-triple-free synthesis).
    #[must_use]
    pub fn fk_reaches(&self, node: &str, anchor: &str) -> bool {
        self.fk_reachable
            .get(node)
            .is_some_and(|anchors| anchors.contains(anchor))
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

    fn set_ref<'a>(items: &[&'a str]) -> BTreeSet<&'a str> {
        items.iter().copied().collect()
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
        assert_eq!(g.kinds_connecting("A", "B", &none), set_ref(&["R", "S"]));
        assert_eq!(g.kinds_connecting("B", "A", &none), set_ref(&["R", "S"]));
        assert_eq!(
            g.kinds_connecting("A", "B", &HashSet::from(["R"])),
            set_ref(&["R"])
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
    fn anchor_fk_mappings_are_deduplicated_by_column() {
        let g = embedded();
        let mapped: Vec<_> = g.anchor_fk_mappings().collect();
        let columns: BTreeSet<&str> = mapped.iter().map(|(fk, _)| *fk).collect();
        assert_eq!(columns.len(), mapped.len());
        assert!(mapped.contains(&("project_id", "Project")));
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
