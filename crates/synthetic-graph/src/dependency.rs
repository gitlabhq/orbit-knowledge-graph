use std::collections::{HashMap, HashSet, VecDeque};

use anyhow::{bail, Result};
use ontology::Ontology;

use crate::config::{EdgeRatio, GenerationConfig, RelationshipConfig};

/// A parent edge in the dependency graph: describes how a child entity type
/// relates to its parent.
#[derive(Debug, Clone)]
pub struct ParentEdge {
    pub edge_type: String,
    pub parent_kind: String,
    pub ratio: EdgeRatio,
    /// `true` when source is parent and target is child (e.g., CONTAINS).
    /// `false` when source is child and target is parent (e.g., IN_PROJECT).
    pub parent_to_child: bool,
}

/// Directed acyclic graph of entity type dependencies, used to determine
/// the order in which entity types must be generated.
#[derive(Debug)]
pub struct DependencyGraph {
    generation_order: Vec<String>,
    parent_edges: HashMap<String, Vec<ParentEdge>>,
    roots: HashSet<String>,
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

        let parent_types: HashSet<String> = parent_edges
            .values()
            .flat_map(|edges| edges.iter().map(|e| e.parent_kind.clone()))
            .collect();

        let generation_order = Self::topological_sort(&parent_edges, &all_nodes)?;

        Ok(Self {
            generation_order,
            parent_edges,
            roots,
            parent_types,
        })
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

    fn topological_sort(
        parent_edges: &HashMap<String, Vec<ParentEdge>>,
        all_nodes: &HashSet<String>,
    ) -> Result<Vec<String>> {
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
                "Cycle detected in dependency graph. Remaining nodes: {:?}",
                remaining
            );
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_ontology() -> Ontology {
        use ontology::EdgeEntity;

        let mut ontology = Ontology::new().with_nodes(["Group", "Project", "MergeRequest"]);

        fn edge(kind: &str, source_kind: &str, target_kind: &str) -> EdgeEntity {
            EdgeEntity {
                relationship_kind: kind.to_string(),
                source: "source_id".to_string(),
                source_kind: source_kind.to_string(),
                target: "target_id".to_string(),
                target_kind: target_kind.to_string(),
            }
        }

        for e in [
            edge("CONTAINS", "Group", "Project"),
            edge("IN_PROJECT", "MergeRequest", "Project"),
        ] {
            ontology.add_edge(e);
        }

        ontology
    }

    #[test]
    fn test_dependency_graph_basic() {
        let ontology = test_ontology();
        let config = GenerationConfig {
            organizations: 1,
            roots: [("Group".to_string(), 10)].into(),
            relationships: RelationshipConfig {
                edges: [
                    (
                        "CONTAINS".to_string(),
                        [("Group -> Project".to_string(), EdgeRatio::Count(5))].into(),
                    ),
                    (
                        "IN_PROJECT".to_string(),
                        [("MergeRequest -> Project".to_string(), EdgeRatio::Count(10))].into(),
                    ),
                ]
                .into(),
            },
            ..default_gen_config()
        };

        let graph = DependencyGraph::build(&config, &ontology).unwrap();
        let order = graph.generation_order();

        let group_pos = order.iter().position(|n| n == "Group").unwrap();
        let project_pos = order.iter().position(|n| n == "Project").unwrap();
        let mr_pos = order.iter().position(|n| n == "MergeRequest").unwrap();

        assert!(group_pos < project_pos);
        assert!(project_pos < mr_pos);
    }

    fn default_gen_config() -> GenerationConfig {
        GenerationConfig {
            organizations: 1,
            roots: HashMap::new(),
            relationships: RelationshipConfig::default(),
            associations: crate::config::AssociationConfig::default(),
            subgroups: crate::config::SubgroupConfig::default(),
            batch_size: 1000,
            seed: 42,
        }
    }
}
