//! Dependency graph for relationship-based generation.
//!
//! Builds a directed graph from the config's relationship definitions and
//! performs topological sort to determine generation order.

use crate::synth::config::{EdgeRatio, GenerationConfig, RelationshipConfig};
use crate::synth::constants::{PARENT_TO_CHILD_EDGE, PARENT_TO_CHILD_PREFIX};
use anyhow::{Result, bail};
use ontology::Ontology;
use std::collections::{HashMap, HashSet, VecDeque};

/// Separator between entity type and depth level in epsilon node names.
const EPSILON_DEPTH_SEPARATOR: char = '@';

#[derive(Debug, Clone)]
pub struct ParentEdge {
    /// The edge relationship kind (e.g., "CONTAINS", "IN_PROJECT").
    pub edge_type: String,
    /// The parent node type (e.g., "Group", "Project").
    pub parent_kind: String,
    /// The ratio/probability for this relationship.
    pub ratio: EdgeRatio,
    /// Whether this is a parent-to-child edge (vs child-to-parent).
    /// Parent-to-child: source is parent, target is child (e.g., CONTAINS)
    /// Child-to-parent: source is child, target is parent (e.g., IN_PROJECT)
    pub parent_to_child: bool,
}

#[derive(Debug)]
pub struct DependencyGraph {
    generation_order: Vec<String>,
    parent_edges: HashMap<String, Vec<ParentEdge>>,
    roots: HashSet<String>,
    /// Entity types that are parents (have children in relationships).
    parent_types: HashSet<String>,
    /// Maps epsilon depth-level node names to their real entity type.
    /// e.g., "Group@1" → "Group", "Group@2" → "Group"
    epsilon_to_real: HashMap<String, String>,
}

impl DependencyGraph {
    pub fn build(config: &GenerationConfig, ontology: &Ontology) -> Result<Self> {
        let mut parent_edges: HashMap<String, Vec<ParentEdge>> = HashMap::new();
        let mut roots: HashSet<String> = HashSet::new();
        let mut all_nodes: HashSet<String> = HashSet::new();
        let mut epsilon_to_real: HashMap<String, String> = HashMap::new();
        // Reverse index: real type → its epsilon names. Built in pass 1, used in pass 2
        // to fan out children to all depth levels in O(1) per parent type.
        let mut expanded_types: HashMap<String, Vec<String>> = HashMap::new();

        for node_type in config.roots.keys() {
            roots.insert(node_type.clone());
            all_nodes.insert(node_type.clone());
        }

        // Two-pass processing of relationships:
        //
        // Pass 1: Detect self-referential edges (source == target) with Recursive ratios
        //         and expand them into epsilon depth-level nodes.
        //         e.g., "Group -> Group" { count: 2, max_depth: 3 }
        //         becomes: Group → Group@1 → Group@2 → Group@3
        //
        // Pass 2: Process all other edges normally, fanning out to epsilon levels
        //         when a parent type was expanded in pass 1.

        for (edge_type, variants) in &config.relationships.edges {
            for (variant_key, ratio) in variants {
                let (source, target) = RelationshipConfig::parse_variant_key(variant_key)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "Invalid variant key '{}' in edge type '{}'. Expected 'Source -> Target'.",
                            variant_key,
                            edge_type
                        )
                    })?;

                if source != target {
                    continue;
                }

                let EdgeRatio::Recursive { count, max_depth } = ratio else {
                    bail!(
                        "Self-referential edge '{}: {} -> {}' requires {{ count, max_depth }} format. \
                         Plain counts create cycles in the dependency graph.",
                        edge_type,
                        source,
                        target,
                    );
                };

                if source.contains(EPSILON_DEPTH_SEPARATOR) {
                    bail!(
                        "Entity type '{}' contains the epsilon separator '{}'. \
                         This would collide with generated epsilon node names.",
                        source,
                        EPSILON_DEPTH_SEPARATOR,
                    );
                }

                Self::validate_edge(ontology, edge_type, &source, &target)?;

                let parent_to_child = Self::is_parent_to_child(edge_type);

                for depth in 1..=*max_depth {
                    let epsilon_name = format!("{}{}{}", source, EPSILON_DEPTH_SEPARATOR, depth);
                    let parent_name = if depth == 1 {
                        source.clone()
                    } else {
                        format!("{}{}{}", source, EPSILON_DEPTH_SEPARATOR, depth - 1)
                    };

                    epsilon_to_real.insert(epsilon_name.clone(), source.clone());
                    expanded_types
                        .entry(source.clone())
                        .or_default()
                        .push(epsilon_name.clone());
                    all_nodes.insert(epsilon_name.clone());

                    parent_edges
                        .entry(epsilon_name)
                        .or_default()
                        .push(ParentEdge {
                            edge_type: edge_type.clone(),
                            parent_kind: parent_name,
                            ratio: EdgeRatio::Count(*count),
                            parent_to_child,
                        });
                }
            }
        }

        // Pass 2: non-self-referential edges
        for (edge_type, variants) in &config.relationships.edges {
            for (variant_key, ratio) in variants {
                let (source, target) = RelationshipConfig::parse_variant_key(variant_key)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "Invalid variant key '{}' in edge type '{}'. Expected 'Source -> Target'.",
                            variant_key,
                            edge_type
                        )
                    })?;

                if source == target {
                    continue; // Handled in pass 1
                }

                if matches!(ratio, EdgeRatio::Recursive { .. }) {
                    bail!(
                        "Recursive {{ count, max_depth }} is only valid for self-referential edges \
                         (source == target). Edge '{}: {} -> {}' has different source and target.",
                        edge_type,
                        source,
                        target,
                    );
                }

                Self::validate_edge(ontology, edge_type, &source, &target)?;

                let parent_to_child = Self::is_parent_to_child(edge_type);
                let (parent_kind, child_kind) = if parent_to_child {
                    (source.clone(), target.clone())
                } else {
                    (target.clone(), source.clone())
                };

                all_nodes.insert(parent_kind.clone());
                all_nodes.insert(child_kind.clone());

                // If the parent was expanded into epsilon depth-level nodes,
                // add parent edges from each epsilon level to the child.
                // This ensures children (e.g., Project) are generated for every
                // depth level of the hierarchy (Group, Group@1, Group@2, ...).
                if let Some(epsilon_names) = expanded_types.get(&parent_kind) {
                    parent_edges
                        .entry(child_kind.clone())
                        .or_default()
                        .push(ParentEdge {
                            edge_type: edge_type.clone(),
                            parent_kind: parent_kind.clone(),
                            ratio: ratio.clone(),
                            parent_to_child,
                        });

                    for epsilon_name in epsilon_names {
                        parent_edges
                            .entry(child_kind.clone())
                            .or_default()
                            .push(ParentEdge {
                                edge_type: edge_type.clone(),
                                parent_kind: epsilon_name.clone(),
                                ratio: ratio.clone(),
                                parent_to_child,
                            });
                    }
                } else {
                    parent_edges
                        .entry(child_kind)
                        .or_default()
                        .push(ParentEdge {
                            edge_type: edge_type.clone(),
                            parent_kind,
                            ratio: ratio.clone(),
                            parent_to_child,
                        });
                }
            }
        }

        for node in &all_nodes {
            if !roots.contains(node)
                && !parent_edges.contains_key(node)
                && !epsilon_to_real.contains_key(node)
            {
                bail!(
                    "Node type '{}' is neither a root nor has a parent relationship defined.",
                    node
                );
            }
        }

        // Collect all entity types that are parents (have children)
        let parent_types: HashSet<String> = parent_edges
            .values()
            .flat_map(|edges| edges.iter().map(|e| e.parent_kind.clone()))
            .collect();

        let generation_order = Self::topological_sort(&roots, &parent_edges, &all_nodes)?;

        Ok(Self {
            generation_order,
            parent_edges,
            roots,
            parent_types,
            epsilon_to_real,
        })
    }

    /// Check if an entity type is a parent (has children in relationships).
    pub fn is_parent_type(&self, node_type: &str) -> bool {
        self.parent_types.contains(node_type)
    }

    /// Validate that a node type exists in the ontology.
    pub fn validate_node(ontology: &Ontology, node_type: &str) -> Result<()> {
        if ontology.get_node(node_type).is_none() {
            bail!(
                "Node type '{}' not found in ontology. Available: {:?}",
                node_type,
                ontology.nodes().map(|n| &n.name).collect::<Vec<_>>()
            );
        }
        Ok(())
    }

    /// Validate that an edge variant (type + source → target) exists in the ontology.
    pub fn validate_edge(
        ontology: &Ontology,
        edge_type: &str,
        source: &str,
        target: &str,
    ) -> Result<()> {
        let found = ontology.edges().any(|e| {
            e.relationship_kind == edge_type && e.source_kind == source && e.target_kind == target
        });

        if !found {
            bail!(
                "Edge '{}' with variant '{} -> {}' not found in ontology.",
                edge_type,
                source,
                target
            );
        }

        Ok(())
    }

    /// Determine edge directionality from the ontology naming convention.
    ///
    /// Parent-to-child edges (source is parent, target is child):
    ///   - `CONTAINS` — Group→Group, Group→Project, etc.
    ///   - `HAS_*`    — Pipeline→Stage, MergeRequest→Diff, etc.
    ///
    /// Child-to-parent edges (source is child, target is parent):
    ///   - `IN_*`     — MergeRequest→Project, Note→Project, etc.
    ///
    /// All other edges are associations and should not appear in
    /// the relationship config (they belong in associations).
    fn is_parent_to_child(edge_type: &str) -> bool {
        edge_type == PARENT_TO_CHILD_EDGE || edge_type.starts_with(PARENT_TO_CHILD_PREFIX)
    }

    /// Kahn's algorithm
    fn topological_sort(
        _roots: &HashSet<String>,
        parent_edges: &HashMap<String, Vec<ParentEdge>>,
        all_nodes: &HashSet<String>,
    ) -> Result<Vec<String>> {
        // Build adjacency list (parent -> children)
        let mut children: HashMap<String, Vec<String>> = HashMap::new();
        let mut in_degree: HashMap<String, usize> = HashMap::new();

        for node in all_nodes {
            in_degree.insert(node.clone(), 0);
        }

        for (child, parents) in parent_edges {
            for parent_edge in parents {
                children
                    .entry(parent_edge.parent_kind.clone())
                    .or_default()
                    .push(child.clone());
                *in_degree.get_mut(child).unwrap() += 1;
            }
        }

        // Start with nodes that have no incoming edges (roots)
        let mut queue: VecDeque<String> = in_degree
            .iter()
            .filter(|(_, deg)| **deg == 0)
            .map(|(node, _)| node.clone())
            .collect();

        let mut result = Vec::new();

        while let Some(node) = queue.pop_front() {
            result.push(node.clone());

            if let Some(node_children) = children.get(&node) {
                for child in node_children {
                    let deg = in_degree.get_mut(child).unwrap();
                    *deg -= 1;
                    if *deg == 0 {
                        queue.push_back(child.clone());
                    }
                }
            }
        }

        if result.len() != all_nodes.len() {
            let remaining: Vec<_> = all_nodes.iter().filter(|n| !result.contains(n)).collect();
            bail!(
                "Cyclic dependency detected in relationship config. Remaining nodes: {:?}",
                remaining
            );
        }

        Ok(result)
    }

    pub fn generation_order(&self) -> &[String] {
        &self.generation_order
    }

    pub fn parent_edges(&self, node_type: &str) -> Option<&[ParentEdge]> {
        self.parent_edges.get(node_type).map(|v| v.as_slice())
    }

    pub fn is_root(&self, node_type: &str) -> bool {
        self.roots.contains(node_type)
    }

    pub fn roots(&self) -> &HashSet<String> {
        &self.roots
    }

    /// Resolve a possibly-epsilon node type to its real entity type.
    /// Returns the input unchanged if it's not an epsilon node.
    pub fn resolve_type<'a>(&'a self, node_type: &'a str) -> &'a str {
        self.epsilon_to_real
            .get(node_type)
            .map(|s| s.as_str())
            .unwrap_or(node_type)
    }

    /// Whether the given node type is an epsilon depth-level node.
    pub fn is_epsilon(&self, node_type: &str) -> bool {
        self.epsilon_to_real.contains_key(node_type)
    }

    /// Get the epsilon-to-real name mapping (for registry compaction).
    pub fn epsilon_to_real(&self) -> &HashMap<String, String> {
        &self.epsilon_to_real
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_parent_to_child() {
        // CONTAINS is always parent-to-child
        assert!(DependencyGraph::is_parent_to_child("CONTAINS"));

        // HAS_* edges are parent-to-child
        assert!(DependencyGraph::is_parent_to_child("HAS_STAGE"));
        assert!(DependencyGraph::is_parent_to_child("HAS_JOB"));
        assert!(DependencyGraph::is_parent_to_child("HAS_DIFF"));
        assert!(DependencyGraph::is_parent_to_child("HAS_FILE"));
        assert!(DependencyGraph::is_parent_to_child("HAS_NOTE"));
        assert!(DependencyGraph::is_parent_to_child("HAS_LABEL"));
        assert!(DependencyGraph::is_parent_to_child("HAS_FINDING"));
        assert!(DependencyGraph::is_parent_to_child("HAS_IDENTIFIER"));
        // Future HAS_* edges would also match automatically
        assert!(DependencyGraph::is_parent_to_child("HAS_FUTURE_EDGE"));

        // IN_* edges are child-to-parent (not parent-to-child)
        assert!(!DependencyGraph::is_parent_to_child("IN_PROJECT"));
        assert!(!DependencyGraph::is_parent_to_child("IN_GROUP"));
        assert!(!DependencyGraph::is_parent_to_child("IN_MILESTONE"));

        // Association edges are not parent-to-child
        assert!(!DependencyGraph::is_parent_to_child("AUTHORED"));
        assert!(!DependencyGraph::is_parent_to_child("MEMBER_OF"));
        assert!(!DependencyGraph::is_parent_to_child("ASSIGNED"));
    }

    #[test]
    fn test_topological_sort_simple() {
        let mut roots = HashSet::new();
        roots.insert("A".to_string());

        let mut parent_edges = HashMap::new();
        parent_edges.insert(
            "B".to_string(),
            vec![ParentEdge {
                edge_type: "TEST".to_string(),
                parent_kind: "A".to_string(),
                ratio: EdgeRatio::Count(1),
                parent_to_child: true,
            }],
        );
        parent_edges.insert(
            "C".to_string(),
            vec![ParentEdge {
                edge_type: "TEST".to_string(),
                parent_kind: "B".to_string(),
                ratio: EdgeRatio::Count(1),
                parent_to_child: true,
            }],
        );

        let mut all_nodes = HashSet::new();
        all_nodes.insert("A".to_string());
        all_nodes.insert("B".to_string());
        all_nodes.insert("C".to_string());

        let order = DependencyGraph::topological_sort(&roots, &parent_edges, &all_nodes).unwrap();

        // A must come before B, B must come before C
        let pos_a = order.iter().position(|x| x == "A").unwrap();
        let pos_b = order.iter().position(|x| x == "B").unwrap();
        let pos_c = order.iter().position(|x| x == "C").unwrap();

        assert!(pos_a < pos_b);
        assert!(pos_b < pos_c);
    }

    #[test]
    fn test_topological_sort_cycle_detection() {
        let roots = HashSet::new();

        let mut parent_edges = HashMap::new();
        parent_edges.insert(
            "A".to_string(),
            vec![ParentEdge {
                edge_type: "TEST".to_string(),
                parent_kind: "B".to_string(),
                ratio: EdgeRatio::Count(1),
                parent_to_child: true,
            }],
        );
        parent_edges.insert(
            "B".to_string(),
            vec![ParentEdge {
                edge_type: "TEST".to_string(),
                parent_kind: "A".to_string(),
                ratio: EdgeRatio::Count(1),
                parent_to_child: true,
            }],
        );

        let mut all_nodes = HashSet::new();
        all_nodes.insert("A".to_string());
        all_nodes.insert("B".to_string());

        let result = DependencyGraph::topological_sort(&roots, &parent_edges, &all_nodes);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Cyclic dependency")
        );
    }

    #[test]
    fn test_epsilon_node_expansion() {
        use crate::synth::config::{GenerationConfig, RelationshipConfig};

        let ontology = ontology::Ontology::load_embedded().unwrap();

        let mut roots = HashMap::new();
        roots.insert("Group".to_string(), 2);

        let mut rel_edges = HashMap::new();
        let mut contains_variants = HashMap::new();
        contains_variants.insert(
            "Group -> Group".to_string(),
            EdgeRatio::Recursive {
                count: 2,
                max_depth: 3,
            },
        );
        contains_variants.insert("Group -> Project".to_string(), EdgeRatio::Count(3));
        rel_edges.insert("CONTAINS".to_string(), contains_variants);

        let config = GenerationConfig {
            roots,
            relationships: RelationshipConfig { edges: rel_edges },
            ..Default::default()
        };

        let graph = DependencyGraph::build(&config, &ontology).unwrap();

        // Epsilon nodes should exist
        assert!(graph.is_epsilon("Group@1"));
        assert!(graph.is_epsilon("Group@2"));
        assert!(graph.is_epsilon("Group@3"));
        assert!(!graph.is_epsilon("Group"));
        assert!(!graph.is_epsilon("Project"));

        // resolve_type maps epsilon to real
        assert_eq!(graph.resolve_type("Group@1"), "Group");
        assert_eq!(graph.resolve_type("Group@2"), "Group");
        assert_eq!(graph.resolve_type("Group@3"), "Group");
        assert_eq!(graph.resolve_type("Group"), "Group");
        assert_eq!(graph.resolve_type("Project"), "Project");

        // Generation order: Group before Group@1 before Group@2 before Group@3
        let order = graph.generation_order();
        let pos_group = order.iter().position(|x| x == "Group").unwrap();
        let pos_g1 = order.iter().position(|x| x == "Group@1").unwrap();
        let pos_g2 = order.iter().position(|x| x == "Group@2").unwrap();
        let pos_g3 = order.iter().position(|x| x == "Group@3").unwrap();
        let pos_project = order.iter().position(|x| x == "Project").unwrap();

        assert!(pos_group < pos_g1);
        assert!(pos_g1 < pos_g2);
        assert!(pos_g2 < pos_g3);
        // Project depends on Group + all epsilon levels, so after Group@3
        assert!(pos_g3 < pos_project);

        // Project should have parent edges from Group, Group@1, Group@2, Group@3
        let project_parents = graph.parent_edges("Project").unwrap();
        let parent_kinds: Vec<&str> = project_parents
            .iter()
            .map(|e| e.parent_kind.as_str())
            .collect();
        assert!(parent_kinds.contains(&"Group"));
        assert!(parent_kinds.contains(&"Group@1"));
        assert!(parent_kinds.contains(&"Group@2"));
        assert!(parent_kinds.contains(&"Group@3"));

        // Group@1 has parent edge from Group
        let g1_parents = graph.parent_edges("Group@1").unwrap();
        assert_eq!(g1_parents.len(), 1);
        assert_eq!(g1_parents[0].parent_kind, "Group");
        assert_eq!(g1_parents[0].ratio, EdgeRatio::Count(2));

        // Group@2 has parent edge from Group@1
        let g2_parents = graph.parent_edges("Group@2").unwrap();
        assert_eq!(g2_parents.len(), 1);
        assert_eq!(g2_parents[0].parent_kind, "Group@1");
    }
}
