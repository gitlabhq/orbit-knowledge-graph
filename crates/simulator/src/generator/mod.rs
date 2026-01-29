//! Data generation from ontology definitions.
//!
//! Generates entities in topological order based on relationship definitions,
//! creating edges inline during generation. This ensures referential integrity
//! and proper traversal ID inheritance.

mod batch;
mod dependency;
mod fake_data;
mod traversal;

pub use batch::BatchBuilder;
pub use dependency::{DependencyGraph, ParentEdge};
pub use fake_data::FakeValueGenerator;
pub use traversal::{EntityContext, EntityRegistry, TraversalIdGenerator};

use crate::arrow_schema::ToArrowSchema;
use crate::config::{Config, EdgeRatio};
use anyhow::Result;
use arrow::record_batch::RecordBatch;
use fake::rand::Rng;
use fake::rand::seq::SliceRandom;
use ontology::{NodeEntity, Ontology};
use std::collections::HashMap;
use std::sync::Arc;

/// Edge record for storage.
#[derive(Debug, Clone)]
pub struct EdgeRecord {
    pub relationship_kind: String,
    pub source: i64,
    pub source_kind: String,
    pub target: i64,
    pub target_kind: String,
}

/// Generated data for an organization.
#[derive(Debug, Default)]
pub struct OrganizationData {
    /// Node batches by node type name.
    pub nodes: HashMap<String, Vec<RecordBatch>>,
    /// Edge records.
    pub edges: Vec<EdgeRecord>,
}

pub struct Generator {
    ontology: Ontology,
    config: Config,
    /// Dependency graph determining generation order.
    dependency_graph: DependencyGraph,
}

impl Generator {
    pub fn new(ontology: Ontology, config: Config) -> Result<Self> {
        let dependency_graph = DependencyGraph::build(&config.generation, &ontology)?;

        Ok(Self {
            ontology,
            config,
            dependency_graph,
        })
    }

    pub fn generate_organization(&self, org_id: u32) -> Result<OrganizationData> {
        let mut data = OrganizationData::default();
        let mut registry = EntityRegistry::new(org_id);
        let mut rng = fake::rand::thread_rng();

        for node_type in self.dependency_graph.generation_order() {
            let node = self
                .ontology
                .nodes()
                .find(|n| n.name == *node_type)
                .ok_or_else(|| {
                    anyhow::anyhow!("Node type '{}' not found in ontology", node_type)
                })?;

            if self.dependency_graph.is_root(node_type) {
                let count = self
                    .config
                    .generation
                    .roots
                    .get(node_type)
                    .copied()
                    .unwrap_or(0);

                if count > 0 {
                    let (batches, edges) =
                        self.generate_root_entities(node, org_id, count, &mut registry, &mut rng)?;
                    data.nodes.insert(node_type.clone(), batches);
                    data.edges.extend(edges);
                }
            } else if let Some(parent_edges) = self.dependency_graph.parent_edges(node_type) {
                let (batches, edges) = self.generate_child_entities(
                    node,
                    org_id,
                    parent_edges,
                    &mut registry,
                    &mut rng,
                )?;
                if !batches.is_empty() {
                    data.nodes.insert(node_type.clone(), batches);
                }
                data.edges.extend(edges);
            }
        }

        let association_edges = self.generate_association_edges(&registry, &mut rng);
        data.edges.extend(association_edges);

        Ok(data)
    }

    fn generate_root_entities(
        &self,
        node: &NodeEntity,
        org_id: u32,
        count: usize,
        registry: &mut EntityRegistry,
        _rng: &mut impl Rng,
    ) -> Result<(Vec<RecordBatch>, Vec<EdgeRecord>)> {
        let schema = Arc::new(node.to_arrow_schema());
        let mut builder = BatchBuilder::new(node, schema, self.config.generation.batch_size);
        let mut edges = Vec::new();
        let is_group = node.name == "Group";

        for _ in 0..count {
            let (entity_id, traversal_id) = if is_group {
                let ns_id = registry.next_namespace_id();
                let trav = format!("{}/{}/", org_id, ns_id);
                (ns_id, trav)
            } else {
                let eid = registry.next_entity_id();
                (eid, org_id.to_string())
            };

            builder.add_row(org_id, traversal_id.clone(), entity_id);
            let ctx = EntityContext::new(entity_id, traversal_id);
            registry.add(&node.name, ctx.clone());

            if is_group && self.config.generation.subgroups.max_depth > 0 {
                self.generate_subgroup_hierarchy(
                    org_id,
                    &ctx,
                    1,
                    registry,
                    &mut builder,
                    &mut edges,
                )?;
            }
        }

        Ok((builder.finish(), edges))
    }

    /// Recursively generate subgroup hierarchy with CONTAINS edges.
    fn generate_subgroup_hierarchy(
        &self,
        org_id: u32,
        parent: &EntityContext,
        depth: usize,
        registry: &mut EntityRegistry,
        builder: &mut BatchBuilder,
        edges: &mut Vec<EdgeRecord>,
    ) -> Result<()> {
        let subgroup_config = &self.config.generation.subgroups;
        if depth > subgroup_config.max_depth {
            return Ok(());
        }

        for _ in 0..subgroup_config.per_group {
            let ns_id = registry.next_namespace_id();
            let traversal_id = format!("{}{}/", parent.traversal_id, ns_id);

            builder.add_row(org_id, traversal_id.clone(), ns_id);
            let ctx = EntityContext::new(ns_id, traversal_id);
            registry.add("Group", ctx.clone());

            edges.push(EdgeRecord {
                relationship_kind: "CONTAINS".to_string(),
                source: parent.id,
                source_kind: "Group".to_string(),
                target: ns_id,
                target_kind: "Group".to_string(),
            });

            self.generate_subgroup_hierarchy(org_id, &ctx, depth + 1, registry, builder, edges)?;
        }

        Ok(())
    }

    fn generate_child_entities(
        &self,
        node: &NodeEntity,
        org_id: u32,
        parent_edges: &[ParentEdge],
        registry: &mut EntityRegistry,
        rng: &mut impl Rng,
    ) -> Result<(Vec<RecordBatch>, Vec<EdgeRecord>)> {
        let schema = Arc::new(node.to_arrow_schema());
        let mut builder = BatchBuilder::new(node, schema, self.config.generation.batch_size);
        let mut edges = Vec::new();
        let is_group = node.name == "Group";

        for parent_edge in parent_edges {
            let parents = match registry.get(&parent_edge.parent_kind) {
                Some(p) if !p.is_empty() => p.to_vec(),
                _ => continue,
            };

            for parent in &parents {
                let child_count = parent_edge.ratio.sample_with_variance(rng);

                for _ in 0..child_count {
                    let (entity_id, traversal_id) = if is_group {
                        let ns_id = registry.next_namespace_id();
                        let trav = format!("{}{}/", parent.traversal_id, ns_id);
                        (ns_id, trav)
                    } else {
                        let eid = registry.next_entity_id();
                        (eid, parent.traversal_id.clone())
                    };

                    builder.add_row(org_id, traversal_id.clone(), entity_id);
                    registry.add(&node.name, EntityContext::new(entity_id, traversal_id));

                    let (source, source_kind, target, target_kind) = if parent_edge.parent_to_child
                    {
                        // Parent -> Child (e.g., CONTAINS: Group -> Project)
                        (
                            parent.id,
                            parent_edge.parent_kind.clone(),
                            entity_id,
                            node.name.clone(),
                        )
                    } else {
                        // Child -> Parent (e.g., IN_PROJECT: MergeRequest -> Project)
                        (
                            entity_id,
                            node.name.clone(),
                            parent.id,
                            parent_edge.parent_kind.clone(),
                        )
                    };

                    edges.push(EdgeRecord {
                        relationship_kind: parent_edge.edge_type.clone(),
                        source,
                        source_kind,
                        target,
                        target_kind,
                    });
                }
            }
        }

        Ok((builder.finish(), edges))
    }

    /// Generate association edges between existing entities.
    ///
    /// Unlike relationship edges (which are created when generating child entities),
    /// association edges connect entities that already exist without generating new ones.
    ///
    /// Config format: `"Source -> Target": ratio`
    /// Semantics: For each TARGET entity, sample RATIO source entities to link.
    /// Example: `"User -> MergeRequest": 1` means each MR gets 1 author (User).
    fn generate_association_edges(
        &self,
        registry: &EntityRegistry,
        rng: &mut impl Rng,
    ) -> Vec<EdgeRecord> {
        let mut edges = Vec::new();

        for (edge_type, source_kind, target_kind, ratio) in
            self.config.generation.associations.all_associations()
        {
            let sources = match registry.get(&source_kind) {
                Some(entities) if !entities.is_empty() => entities,
                _ => continue,
            };

            let targets = match registry.get(&target_kind) {
                Some(entities) if !entities.is_empty() => entities,
                _ => continue,
            };

            // For each target entity, sample source entities to create edges
            for target in targets {
                let edge_count = match &ratio {
                    EdgeRatio::Count(n) => *n,
                    EdgeRatio::Probability(p) => {
                        if rng.gen_bool(*p) {
                            1
                        } else {
                            0
                        }
                    }
                };

                if edge_count == 0 {
                    continue;
                }

                let selected_sources: Vec<_> = if edge_count >= sources.len() {
                    sources.iter().collect()
                } else {
                    sources.choose_multiple(rng, edge_count).collect()
                };

                for source in selected_sources {
                    edges.push(EdgeRecord {
                        relationship_kind: edge_type.clone(),
                        source: source.id,
                        source_kind: source_kind.clone(),
                        target: target.id,
                        target_kind: target_kind.clone(),
                    });
                }
            }
        }

        edges
    }

    pub fn print_plan(&self) {
        let cfg = &self.config.generation;

        println!("Generation plan:");
        println!("  Organizations: {}", cfg.organizations);
        println!();

        println!("  Root entities:");
        for (node_type, count) in &cfg.roots {
            let total = count * cfg.organizations as usize;
            println!("    {}: {} per org = {} total", node_type, count, total);
        }

        if cfg.subgroups.max_depth > 0 {
            let root_groups = cfg.roots.get("Group").copied().unwrap_or(0);
            let mut total_groups = root_groups;
            let mut groups_at_level = root_groups;
            for _ in 1..=cfg.subgroups.max_depth {
                groups_at_level *= cfg.subgroups.per_group;
                total_groups += groups_at_level;
            }
            println!(
                "    (with subgroups: {} levels x {} per group = {} total groups per org)",
                cfg.subgroups.max_depth, cfg.subgroups.per_group, total_groups
            );
        }
        println!();

        println!(
            "  Generation order ({} types):",
            self.dependency_graph.generation_order().len()
        );
        for (i, node_type) in self.dependency_graph.generation_order().iter().enumerate() {
            let is_root = self.dependency_graph.is_root(node_type);
            let marker = if is_root { "(root)" } else { "" };
            println!("    {}. {} {}", i + 1, node_type, marker);
        }
        println!();

        println!("  Relationships:");
        for (edge_type, variants) in &cfg.relationships.edges {
            for (variant, ratio) in variants {
                let ratio_str = match ratio {
                    EdgeRatio::Count(n) => format!("{} per parent", n),
                    EdgeRatio::Probability(p) => format!("{:.0}% chance", p * 100.0),
                };
                println!("    {}: {} ({})", edge_type, variant, ratio_str);
            }
        }
        println!();

        if !cfg.associations.edges.is_empty() {
            println!("  Associations (per target entity):");
            for (edge_type, variants) in &cfg.associations.edges {
                for (variant, ratio) in variants {
                    let ratio_str = match ratio {
                        EdgeRatio::Count(n) => format!("{} per target", n),
                        EdgeRatio::Probability(p) => format!("{:.0}% chance", p * 100.0),
                    };
                    println!("    {}: {} ({})", edge_type, variant, ratio_str);
                }
            }
            println!();
        }
    }
}
