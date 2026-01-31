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
pub use traversal::{EntityContext, EntityRegistry, TraversalPathGenerator};

use crate::arrow_schema::ToArrowSchema;
use crate::config::{Config, EdgeRatio};
use crate::parquet::StreamingEdgeWriter;
use anyhow::Result;
use arrow::record_batch::RecordBatch;
use rand::Rng;
use ontology::{NodeEntity, Ontology};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

/// Interned string type for edge records to avoid millions of small allocations.
pub type IStr = Arc<str>;

/// Edge record for storage.
#[derive(Debug, Clone)]
pub struct EdgeRecord {
    pub relationship_kind: IStr,
    pub source: i64,
    pub source_kind: IStr,
    pub target: i64,
    pub target_kind: IStr,
}

/// Generated data for an organization.
#[derive(Debug, Default)]
pub struct OrganizationData {
    /// Node batches by node type name.
    pub nodes: HashMap<String, Vec<RecordBatch>>,
    /// Edge records.
    pub edges: Vec<EdgeRecord>,
}

/// Generated node data only (edges streamed separately).
#[derive(Debug, Default)]
pub struct OrganizationNodes {
    /// Node batches by node type name.
    pub nodes: HashMap<String, Vec<RecordBatch>>,
}

/// Simple string interner for edge record strings.
#[derive(Debug, Default)]
struct StringInterner {
    cache: HashMap<String, IStr>,
}

impl StringInterner {
    fn intern(&mut self, s: &str) -> IStr {
        if let Some(cached) = self.cache.get(s) {
            return cached.clone();
        }
        let interned: IStr = s.into();
        self.cache.insert(s.to_string(), interned.clone());
        interned
    }
}

pub struct Generator {
    ontology: Ontology,
    config: Config,
    /// Dependency graph determining generation order.
    dependency_graph: DependencyGraph,
    /// Global entity ID counter (shared across all orgs for unique IDs).
    global_entity_counter: AtomicI64,
    /// String interner for edge records.
    interner: std::sync::Mutex<StringInterner>,
}

impl Generator {
    pub fn new(ontology: Ontology, config: Config) -> Result<Self> {
        let dependency_graph = DependencyGraph::build(&config.generation, &ontology)?;

        Ok(Self {
            ontology,
            config,
            dependency_graph,
            global_entity_counter: AtomicI64::new(1),
            interner: std::sync::Mutex::new(StringInterner::default()),
        })
    }

    /// Intern a string for use in edge records.
    fn intern(&self, s: &str) -> IStr {
        self.interner.lock().unwrap().intern(s)
    }

    /// Get the next globally unique entity ID.
    pub fn next_entity_id(&self) -> i64 {
        self.global_entity_counter.fetch_add(1, Ordering::SeqCst)
    }

    pub fn generate_organization(&self, org_id: u32) -> Result<OrganizationData> {
        let mut data = OrganizationData::default();
        let mut registry = EntityRegistry::new(org_id);
        let mut rng = rand::thread_rng();

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
                let (batches, edges) =
                    self.generate_child_entities(node, parent_edges, &mut registry, &mut rng)?;
                if !batches.is_empty() {
                    data.nodes.insert(node_type.clone(), batches);
                }
                data.edges.extend(edges);
            }
        }

        // Compact registry to free traversal path memory before associations
        registry.compact();

        let association_edges = self.generate_association_edges(&registry, &mut rng);
        data.edges.extend(association_edges);

        Ok(data)
    }

    /// Generate organization data with streaming edge output.
    /// Edges are written directly to the StreamingEdgeWriter, reducing peak memory.
    pub fn generate_organization_streaming(
        &self,
        org_id: u32,
        edge_writer: &mut StreamingEdgeWriter,
    ) -> Result<OrganizationNodes> {
        let mut data = OrganizationNodes::default();
        let mut registry = EntityRegistry::new(org_id);
        let mut rng = rand::thread_rng();

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
                    let batches = self.generate_root_entities_streaming(
                        node,
                        org_id,
                        count,
                        &mut registry,
                        &mut rng,
                        edge_writer,
                    )?;
                    data.nodes.insert(node_type.clone(), batches);
                }
            } else if let Some(parent_edges) = self.dependency_graph.parent_edges(node_type) {
                let batches = self.generate_child_entities_streaming(
                    node,
                    parent_edges,
                    &mut registry,
                    &mut rng,
                    edge_writer,
                )?;
                if !batches.is_empty() {
                    data.nodes.insert(node_type.clone(), batches);
                }
            }
        }

        // Compact registry to free traversal path memory before associations
        registry.compact();

        self.generate_association_edges_streaming(&registry, &mut rng, edge_writer)?;

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
        let mut builder = BatchBuilder::with_seed(
            node,
            schema,
            self.config.generation.batch_size,
            self.config.generation.seed,
        );
        let mut edges = Vec::new();
        let is_group = node.name == "Group";

        for _ in 0..count {
            let (entity_id, traversal_path) = if is_group {
                let ns_id = registry.next_namespace_id();
                let trav = format!("{}/{}/", org_id, ns_id);
                (ns_id, trav)
            } else {
                // Non-group root entities (like Users) get org-level paths.
                // Note: The query engine skips traversal path security filters
                // for Users since their visibility is determined through
                // MEMBER_OF relationships to Groups, not path hierarchy.
                let eid = self.next_entity_id();
                (eid, format!("{}/", org_id))
            };

            builder.add_row(traversal_path.clone(), entity_id);
            let ctx = EntityContext::new(entity_id, traversal_path);
            registry.add(&node.name, ctx.clone());

            if is_group && self.config.generation.subgroups.max_depth > 0 {
                self.generate_subgroup_hierarchy(&ctx, 1, registry, &mut builder, &mut edges)?;
            }
        }

        Ok((builder.finish(), edges))
    }

    /// Recursively generate subgroup hierarchy with CONTAINS edges.
    fn generate_subgroup_hierarchy(
        &self,
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
            let traversal_path = format!("{}{}/", parent.traversal_path, ns_id);

            builder.add_row(traversal_path.clone(), ns_id);
            let ctx = EntityContext::new(ns_id, traversal_path);
            registry.add("Group", ctx.clone());

            edges.push(EdgeRecord {
                relationship_kind: self.intern("CONTAINS"),
                source: parent.id,
                source_kind: self.intern("Group"),
                target: ns_id,
                target_kind: self.intern("Group"),
            });

            self.generate_subgroup_hierarchy(&ctx, depth + 1, registry, builder, edges)?;
        }

        Ok(())
    }

    fn generate_child_entities(
        &self,
        node: &NodeEntity,
        parent_edges: &[ParentEdge],
        registry: &mut EntityRegistry,
        rng: &mut impl Rng,
    ) -> Result<(Vec<RecordBatch>, Vec<EdgeRecord>)> {
        let schema = Arc::new(node.to_arrow_schema());
        let mut builder = BatchBuilder::with_seed(
            node,
            schema,
            self.config.generation.batch_size,
            self.config.generation.seed,
        );
        let mut edges = Vec::new();
        let is_group = node.name == "Group";
        // Only store full context for parent types; leaves only need IDs for associations
        let is_parent_type = self.dependency_graph.is_parent_type(&node.name);

        for parent_edge in parent_edges {
            // Clone parent IDs and paths to avoid borrow conflict with registry.add()
            let parents: Vec<_> = match registry.get(&parent_edge.parent_kind) {
                Some(p) if !p.is_empty() => p.iter().map(|e| (e.id, e.traversal_path.clone())).collect(),
                _ => continue,
            };

            // Intern strings once per parent_edge type
            let rel_kind = self.intern(&parent_edge.edge_type);
            let parent_kind_str = self.intern(&parent_edge.parent_kind);
            let node_name_str = self.intern(&node.name);

            for (parent_id, parent_path) in &parents {
                let child_count = parent_edge.ratio.sample_with_variance(rng);

                for _ in 0..child_count {
                    let (entity_id, traversal_path) = if is_group {
                        let ns_id = registry.next_namespace_id();
                        let trav = format!("{}{}/", parent_path, ns_id);
                        (ns_id, trav)
                    } else {
                        let eid = self.next_entity_id();
                        (eid, parent_path.clone())
                    };

                    builder.add_row(traversal_path.clone(), entity_id);
                    
                    // For leaf entities, only store ID (saves ~44 bytes per entity)
                    if is_parent_type {
                        registry.add(&node.name, EntityContext::new(entity_id, traversal_path));
                    } else {
                        registry.add_id_only(&node.name, entity_id);
                    }

                    let (source, source_kind, target, target_kind) = if parent_edge.parent_to_child
                    {
                        (*parent_id, parent_kind_str.clone(), entity_id, node_name_str.clone())
                    } else {
                        (entity_id, node_name_str.clone(), *parent_id, parent_kind_str.clone())
                    };

                    edges.push(EdgeRecord {
                        relationship_kind: rel_kind.clone(),
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
    /// The iteration direction determines which side we iterate over:
    /// - `per: target` (default): For each target, sample sources to link
    /// - `per: source`: For each source, sample targets to link
    fn generate_association_edges(
        &self,
        registry: &EntityRegistry,
        rng: &mut impl Rng,
    ) -> Vec<EdgeRecord> {
        use crate::config::IterationDirection;

        let mut edges = Vec::new();

        for (edge_type, source_kind, target_kind, ratio, direction) in
            self.config.generation.associations.all_associations()
        {
            // Use compacted ID-only data
            let source_ids = match registry.get_ids_slice(&source_kind) {
                Some(ids) if !ids.is_empty() => ids,
                _ => continue,
            };

            let target_ids = match registry.get_ids_slice(&target_kind) {
                Some(ids) if !ids.is_empty() => ids,
                _ => continue,
            };

            // Intern strings once per association type
            let rel_kind = self.intern(&edge_type);
            let src_kind = self.intern(&source_kind);
            let tgt_kind = self.intern(&target_kind);

            // Determine which side to iterate over based on direction
            let (iterate_over, sample_from) = match direction {
                IterationDirection::Target => (target_ids, source_ids),
                IterationDirection::Source => (source_ids, target_ids),
            };

            let sample_len = sample_from.len();

            for &primary_id in iterate_over {
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

                let count = edge_count.min(sample_len);
                for _ in 0..count {
                    let idx = rng.gen_range(0..sample_len);
                    let secondary_id = sample_from[idx];

                    let (source_id, target_id) = match direction {
                        IterationDirection::Target => (secondary_id, primary_id),
                        IterationDirection::Source => (primary_id, secondary_id),
                    };

                    edges.push(EdgeRecord {
                        relationship_kind: rel_kind.clone(),
                        source: source_id,
                        source_kind: src_kind.clone(),
                        target: target_id,
                        target_kind: tgt_kind.clone(),
                    });
                }
            }
        }

        edges
    }

    // ==================== Streaming variants ====================
    // These write edges directly to StreamingEdgeWriter instead of accumulating

    fn generate_root_entities_streaming(
        &self,
        node: &NodeEntity,
        org_id: u32,
        count: usize,
        registry: &mut EntityRegistry,
        _rng: &mut impl Rng,
        edge_writer: &mut StreamingEdgeWriter,
    ) -> Result<Vec<RecordBatch>> {
        let schema = Arc::new(node.to_arrow_schema());
        let mut builder = BatchBuilder::with_seed(
            node,
            schema,
            self.config.generation.batch_size,
            self.config.generation.seed,
        );
        let is_group = node.name == "Group";

        for _ in 0..count {
            let (entity_id, traversal_path) = if is_group {
                let ns_id = registry.next_namespace_id();
                let trav = format!("{}/{}/", org_id, ns_id);
                (ns_id, trav)
            } else {
                let eid = self.next_entity_id();
                (eid, format!("{}/", org_id))
            };

            builder.add_row(traversal_path.clone(), entity_id);
            let ctx = EntityContext::new(entity_id, traversal_path);
            registry.add(&node.name, ctx.clone());

            if is_group && self.config.generation.subgroups.max_depth > 0 {
                self.generate_subgroup_hierarchy_streaming(
                    &ctx,
                    1,
                    registry,
                    &mut builder,
                    edge_writer,
                )?;
            }
        }

        Ok(builder.finish())
    }

    fn generate_subgroup_hierarchy_streaming(
        &self,
        parent: &EntityContext,
        depth: usize,
        registry: &mut EntityRegistry,
        builder: &mut BatchBuilder,
        edge_writer: &mut StreamingEdgeWriter,
    ) -> Result<()> {
        let subgroup_config = &self.config.generation.subgroups;
        if depth > subgroup_config.max_depth {
            return Ok(());
        }

        for _ in 0..subgroup_config.per_group {
            let ns_id = registry.next_namespace_id();
            let traversal_path = format!("{}{}/", parent.traversal_path, ns_id);

            builder.add_row(traversal_path.clone(), ns_id);
            let ctx = EntityContext::new(ns_id, traversal_path);
            registry.add("Group", ctx.clone());

            edge_writer.push(EdgeRecord {
                relationship_kind: self.intern("CONTAINS"),
                source: parent.id,
                source_kind: self.intern("Group"),
                target: ns_id,
                target_kind: self.intern("Group"),
            })?;

            self.generate_subgroup_hierarchy_streaming(&ctx, depth + 1, registry, builder, edge_writer)?;
        }

        Ok(())
    }

    fn generate_child_entities_streaming(
        &self,
        node: &NodeEntity,
        parent_edges: &[ParentEdge],
        registry: &mut EntityRegistry,
        rng: &mut impl Rng,
        edge_writer: &mut StreamingEdgeWriter,
    ) -> Result<Vec<RecordBatch>> {
        let schema = Arc::new(node.to_arrow_schema());
        let mut builder = BatchBuilder::with_seed(
            node,
            schema,
            self.config.generation.batch_size,
            self.config.generation.seed,
        );
        let is_group = node.name == "Group";
        let is_parent_type = self.dependency_graph.is_parent_type(&node.name);

        for parent_edge in parent_edges {
            let parents: Vec<_> = match registry.get(&parent_edge.parent_kind) {
                Some(p) if !p.is_empty() => p.iter().map(|e| (e.id, e.traversal_path.clone())).collect(),
                _ => continue,
            };

            let rel_kind = self.intern(&parent_edge.edge_type);
            let parent_kind_str = self.intern(&parent_edge.parent_kind);
            let node_name_str = self.intern(&node.name);

            for (parent_id, parent_path) in &parents {
                let child_count = parent_edge.ratio.sample_with_variance(rng);

                for _ in 0..child_count {
                    let (entity_id, traversal_path) = if is_group {
                        let ns_id = registry.next_namespace_id();
                        let trav = format!("{}{}/", parent_path, ns_id);
                        (ns_id, trav)
                    } else {
                        let eid = self.next_entity_id();
                        (eid, parent_path.clone())
                    };

                    builder.add_row(traversal_path.clone(), entity_id);

                    if is_parent_type {
                        registry.add(&node.name, EntityContext::new(entity_id, traversal_path));
                    } else {
                        registry.add_id_only(&node.name, entity_id);
                    }

                    let (source, source_kind, target, target_kind) = if parent_edge.parent_to_child {
                        (*parent_id, parent_kind_str.clone(), entity_id, node_name_str.clone())
                    } else {
                        (entity_id, node_name_str.clone(), *parent_id, parent_kind_str.clone())
                    };

                    edge_writer.push(EdgeRecord {
                        relationship_kind: rel_kind.clone(),
                        source,
                        source_kind,
                        target,
                        target_kind,
                    })?;
                }
            }
        }

        Ok(builder.finish())
    }

    fn generate_association_edges_streaming(
        &self,
        registry: &EntityRegistry,
        rng: &mut impl Rng,
        edge_writer: &mut StreamingEdgeWriter,
    ) -> Result<()> {
        use crate::config::IterationDirection;

        for (edge_type, source_kind, target_kind, ratio, direction) in
            self.config.generation.associations.all_associations()
        {
            let source_ids = match registry.get_ids_slice(&source_kind) {
                Some(ids) if !ids.is_empty() => ids,
                _ => continue,
            };

            let target_ids = match registry.get_ids_slice(&target_kind) {
                Some(ids) if !ids.is_empty() => ids,
                _ => continue,
            };

            let rel_kind = self.intern(&edge_type);
            let src_kind = self.intern(&source_kind);
            let tgt_kind = self.intern(&target_kind);

            let (iterate_over, sample_from) = match direction {
                IterationDirection::Target => (target_ids, source_ids),
                IterationDirection::Source => (source_ids, target_ids),
            };

            let sample_len = sample_from.len();

            for &primary_id in iterate_over {
                let edge_count = match &ratio {
                    EdgeRatio::Count(n) => *n,
                    EdgeRatio::Probability(p) => {
                        if rng.gen_bool(*p) { 1 } else { 0 }
                    }
                };

                if edge_count == 0 {
                    continue;
                }

                let count = edge_count.min(sample_len);
                for _ in 0..count {
                    let idx = rng.gen_range(0..sample_len);
                    let secondary_id = sample_from[idx];

                    let (source_id, target_id) = match direction {
                        IterationDirection::Target => (secondary_id, primary_id),
                        IterationDirection::Source => (primary_id, secondary_id),
                    };

                    edge_writer.push(EdgeRecord {
                        relationship_kind: rel_kind.clone(),
                        source: source_id,
                        source_kind: src_kind.clone(),
                        target: target_id,
                        target_kind: tgt_kind.clone(),
                    })?;
                }
            }
        }

        Ok(())
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
            println!("  Associations:");
            for (edge_type, variants) in &cfg.associations.edges {
                for (variant, value) in variants {
                    let ratio = value.ratio();
                    let direction = value.iteration_direction();
                    let per_str = match direction {
                        crate::config::IterationDirection::Target => "per target",
                        crate::config::IterationDirection::Source => "per source",
                    };
                    let ratio_str = match ratio {
                        EdgeRatio::Count(n) => format!("{} {}", n, per_str),
                        EdgeRatio::Probability(p) => {
                            format!("{:.0}% chance {}", p * 100.0, per_str)
                        }
                    };
                    println!("    {}: {} ({})", edge_type, variant, ratio_str);
                }
            }
            println!();
        }
    }
}
