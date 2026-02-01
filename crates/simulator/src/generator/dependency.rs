//! Dependency graph for relationship-based generation.
//!
//! Builds a directed graph from the config's relationship definitions and
//! performs topological sort to determine generation order.

use crate::config::{EdgeRatio, GenerationConfig, RelationshipConfig};
use anyhow::{Result, bail};
use ontology::Ontology;
use std::collections::{HashMap, HashSet, VecDeque};

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
}

impl DependencyGraph {
    pub fn build(config: &GenerationConfig, ontology: &Ontology) -> Result<Self> {
        let mut parent_edges: HashMap<String, Vec<ParentEdge>> = HashMap::new();
        let mut roots: HashSet<String> = HashSet::new();
        let mut all_nodes: HashSet<String> = HashSet::new();

        for node_type in config.roots.keys() {
            roots.insert(node_type.clone());
            all_nodes.insert(node_type.clone());
        }

        // Build parent edges from relationships config
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

                Self::validate_edge(ontology, edge_type, &source, &target)?;

                let parent_to_child = Self::is_parent_to_child(edge_type);
                let (parent_kind, child_kind) = if parent_to_child {
                    (source.clone(), target.clone())
                } else {
                    (target.clone(), source.clone())
                };

                all_nodes.insert(parent_kind.clone());
                all_nodes.insert(child_kind.clone());

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

        for node in &all_nodes {
            if !roots.contains(node) && !parent_edges.contains_key(node) {
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
        })
    }

    /// Check if an entity type is a parent (has children in relationships).
    pub fn is_parent_type(&self, node_type: &str) -> bool {
        self.parent_types.contains(node_type)
    }

    fn validate_edge(
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

    // TODO: should be derived from the ontology
    fn is_parent_to_child(edge_type: &str) -> bool {
        matches!(
            edge_type,
            "CONTAINS"
                | "HAS_STAGE"
                | "HAS_JOB"
                | "HAS_FILE"
                | "HAS_NOTE"
                | "HAS_LABEL"
                | "HAS_FINDING"
                | "HAS_IDENTIFIER"
                | "HAS_DIFF"
        )
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_parent_to_child() {
        assert!(DependencyGraph::is_parent_to_child("CONTAINS"));
        assert!(DependencyGraph::is_parent_to_child("HAS_STAGE"));
        assert!(DependencyGraph::is_parent_to_child("HAS_JOB"));
        assert!(DependencyGraph::is_parent_to_child("HAS_DIFF"));
        assert!(!DependencyGraph::is_parent_to_child("IN_PROJECT"));
        assert!(!DependencyGraph::is_parent_to_child("IN_GROUP"));
        assert!(!DependencyGraph::is_parent_to_child("AUTHORED"));
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
}
