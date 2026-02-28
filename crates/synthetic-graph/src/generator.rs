use std::collections::HashMap;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use ontology::Ontology;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tracing::info;

use crate::batch::arrow_schema::node_to_arrow_schema;
use crate::batch::BatchBuilder;
use crate::config::{GenerationConfig, IterationDirection};
use crate::dependency::DependencyGraph;
use crate::ids::IdAllocator;
use crate::traversal::{EntityContext, EntityRegistry};

/// Interned string type for zero-copy edge records.
pub type IStr = Arc<str>;

/// A single edge in the generated graph.
#[derive(Debug, Clone)]
pub struct EdgeRecord {
    pub traversal_path: IStr,
    pub relationship_kind: IStr,
    pub source: i64,
    pub source_kind: IStr,
    pub target: i64,
    pub target_kind: IStr,
}

/// Generated data for a single organization: nodes as Arrow RecordBatches
/// plus edge records.
pub struct OrganizationData {
    pub nodes: HashMap<String, Vec<RecordBatch>>,
    pub edges: Vec<EdgeRecord>,
}

/// Generated nodes for a single organization (edges handled via callback).
pub struct OrganizationNodes {
    pub nodes: HashMap<String, Vec<RecordBatch>>,
}

/// Simple string interner to reduce Arc allocations for repeated strings.
#[derive(Default)]
struct StringInterner {
    cache: HashMap<String, IStr>,
}

impl StringInterner {
    fn intern(&mut self, s: &str) -> IStr {
        if let Some(existing) = self.cache.get(s) {
            existing.clone()
        } else {
            let interned: IStr = Arc::from(s);
            self.cache.insert(s.to_string(), interned.clone());
            interned
        }
    }
}

/// Core graph generator. Produces synthetic SDLC data driven by ontology
/// definitions and configuration.
///
/// # Usage
///
/// ```ignore
/// let ontology = Ontology::load_embedded()?;
/// let config = GraphConfig::load("config.yaml")?;
/// let generator = Generator::new(ontology, config.generation)?;
///
/// // In-memory generation
/// let data = generator.generate_organization(1);
///
/// // Streaming generation (edges via callback)
/// generator.generate_organization_streaming(1, |edge| { /* write edge */ });
/// ```
pub struct Generator {
    ontology: Ontology,
    config: GenerationConfig,
    dependency_graph: DependencyGraph,
    global_entity_counter: IdAllocator,
    interner: std::sync::Mutex<StringInterner>,
}

impl Generator {
    pub fn new(ontology: Ontology, config: GenerationConfig) -> anyhow::Result<Self> {
        let dependency_graph = DependencyGraph::build(&config, &ontology)?;
        Ok(Self {
            ontology,
            config,
            dependency_graph,
            global_entity_counter: IdAllocator::new(1),
            interner: std::sync::Mutex::new(StringInterner::default()),
        })
    }

    pub fn ontology(&self) -> &Ontology {
        &self.ontology
    }

    pub fn config(&self) -> &GenerationConfig {
        &self.config
    }

    pub fn dependency_graph(&self) -> &DependencyGraph {
        &self.dependency_graph
    }

    fn next_entity_id(&self) -> i64 {
        self.global_entity_counter.next()
    }

    fn intern(&self, s: &str) -> IStr {
        self.interner.lock().unwrap().intern(s)
    }

    /// Generate all data for one organization in memory.
    pub fn generate_organization(&self, org_id: u32) -> OrganizationData {
        let mut rng = StdRng::seed_from_u64(self.config.seed.wrapping_add(org_id as u64));
        let mut registry = EntityRegistry::new(org_id);
        let mut all_nodes: HashMap<String, Vec<RecordBatch>> = HashMap::new();
        let mut all_edges: Vec<EdgeRecord> = Vec::new();

        self.generate_entities(&mut rng, &mut registry, &mut all_nodes, &mut all_edges);

        registry.compact();
        self.generate_associations(&mut rng, &registry, &mut all_edges);

        OrganizationData {
            nodes: all_nodes,
            edges: all_edges,
        }
    }

    /// Generate data for one organization, streaming edges to a callback.
    pub fn generate_organization_streaming<F>(
        &self,
        org_id: u32,
        mut on_edge: F,
    ) -> OrganizationNodes
    where
        F: FnMut(EdgeRecord),
    {
        let mut rng = StdRng::seed_from_u64(self.config.seed.wrapping_add(org_id as u64));
        let mut registry = EntityRegistry::new(org_id);
        let mut all_nodes: HashMap<String, Vec<RecordBatch>> = HashMap::new();
        let mut edges_buf: Vec<EdgeRecord> = Vec::new();

        self.generate_entities(&mut rng, &mut registry, &mut all_nodes, &mut edges_buf);
        for edge in edges_buf.drain(..) {
            on_edge(edge);
        }

        registry.compact();
        self.generate_associations(&mut rng, &registry, &mut edges_buf);
        for edge in edges_buf.drain(..) {
            on_edge(edge);
        }

        OrganizationNodes { nodes: all_nodes }
    }

    /// Get the registry for an organization (for state building).
    pub fn build_registry(&self, org_id: u32) -> EntityRegistry {
        let mut rng = StdRng::seed_from_u64(self.config.seed.wrapping_add(org_id as u64));
        let mut registry = EntityRegistry::new(org_id);
        let mut nodes: HashMap<String, Vec<RecordBatch>> = HashMap::new();
        let mut edges: Vec<EdgeRecord> = Vec::new();
        self.generate_entities(&mut rng, &mut registry, &mut nodes, &mut edges);
        registry
    }

    fn generate_entities(
        &self,
        rng: &mut StdRng,
        registry: &mut EntityRegistry,
        all_nodes: &mut HashMap<String, Vec<RecordBatch>>,
        all_edges: &mut Vec<EdgeRecord>,
    ) {
        let org_id = registry.org_id();

        for node_type in self.dependency_graph.generation_order() {
            let node_entity = match self.ontology.get_node(node_type) {
                Some(n) => n,
                None => continue,
            };

            let schema = Arc::new(node_to_arrow_schema(node_entity));
            let is_parent = self.dependency_graph.is_parent_type(node_type);

            if self.dependency_graph.is_root(node_type) {
                let count = self.config.roots.get(node_type).copied().unwrap_or(0);
                let mut builder = BatchBuilder::with_seed(
                    node_entity,
                    schema,
                    self.config.batch_size,
                    Some(self.config.seed),
                );

                if node_type == "Group" {
                    self.generate_root_groups(
                        rng,
                        registry,
                        &mut builder,
                        all_edges,
                        org_id,
                        count,
                    );
                } else {
                    for _ in 0..count {
                        let id = self.next_entity_id();
                        let traversal_path = format!("{}/", org_id);
                        builder.add_row(traversal_path.clone(), id);
                        if is_parent {
                            registry.add(node_type, EntityContext::new(id, traversal_path));
                        } else {
                            registry.add_id_only(node_type, id);
                        }
                    }
                }

                all_nodes
                    .entry(node_type.to_string())
                    .or_default()
                    .extend(builder.finish());
            } else if let Some(parent_edges) = self.dependency_graph.parent_edges(node_type) {
                let mut builder = BatchBuilder::with_seed(
                    node_entity,
                    schema,
                    self.config.batch_size,
                    Some(self.config.seed),
                );

                for parent_edge in parent_edges {
                    let parents = match registry.get(&parent_edge.parent_kind) {
                        Some(p) => p.to_vec(),
                        None => continue,
                    };

                    for parent in &parents {
                        let child_count = parent_edge.ratio.sample_with_variance(rng);

                        for _ in 0..child_count {
                            let id = self.next_entity_id();
                            let ctx = EntityContext::child(parent, id);
                            builder.add_row(ctx.traversal_path.clone(), id);

                            let edge = if parent_edge.parent_to_child {
                                EdgeRecord {
                                    traversal_path: self.intern(&parent.traversal_path),
                                    relationship_kind: self.intern(&parent_edge.edge_type),
                                    source: parent.id,
                                    source_kind: self.intern(&parent_edge.parent_kind),
                                    target: id,
                                    target_kind: self.intern(node_type),
                                }
                            } else {
                                EdgeRecord {
                                    traversal_path: self.intern(&ctx.traversal_path),
                                    relationship_kind: self.intern(&parent_edge.edge_type),
                                    source: id,
                                    source_kind: self.intern(node_type),
                                    target: parent.id,
                                    target_kind: self.intern(&parent_edge.parent_kind),
                                }
                            };
                            all_edges.push(edge);

                            if is_parent {
                                registry.add(node_type, ctx);
                            } else {
                                registry.add_id_only(node_type, id);
                            }
                        }
                    }
                }

                all_nodes
                    .entry(node_type.to_string())
                    .or_default()
                    .extend(builder.finish());
            }
        }
    }

    fn generate_root_groups(
        &self,
        rng: &mut StdRng,
        registry: &mut EntityRegistry,
        builder: &mut BatchBuilder,
        edges: &mut Vec<EdgeRecord>,
        org_id: u32,
        count: usize,
    ) {
        for _ in 0..count {
            let ns_id = registry.next_namespace_id();
            let group_id = self.next_entity_id();
            let ctx = EntityContext::root_group(org_id, ns_id);
            builder.add_row(ctx.traversal_path.clone(), group_id);

            let real_ctx = EntityContext::new(group_id, ctx.traversal_path);
            self.generate_subgroups(rng, registry, builder, edges, &real_ctx, 1);
            registry.add("Group", real_ctx);
        }
    }

    #[allow(clippy::only_used_in_recursion)]
    fn generate_subgroups(
        &self,
        rng: &mut StdRng,
        registry: &mut EntityRegistry,
        builder: &mut BatchBuilder,
        edges: &mut Vec<EdgeRecord>,
        parent: &EntityContext,
        depth: usize,
    ) {
        if depth >= self.config.subgroups.max_depth {
            return;
        }

        let count = self.config.subgroups.per_group;
        for _ in 0..count {
            let ns_id = registry.next_namespace_id();
            let subgroup_id = self.next_entity_id();
            let ctx = EntityContext::subgroup(parent, ns_id);
            builder.add_row(ctx.traversal_path.clone(), subgroup_id);

            edges.push(EdgeRecord {
                traversal_path: self.intern(&parent.traversal_path),
                relationship_kind: self.intern("CONTAINS"),
                source: parent.id,
                source_kind: self.intern("Group"),
                target: subgroup_id,
                target_kind: self.intern("Group"),
            });

            let real_ctx = EntityContext::new(subgroup_id, ctx.traversal_path);
            self.generate_subgroups(rng, registry, builder, edges, &real_ctx, depth + 1);
            registry.add("Group", real_ctx);
        }
    }

    fn generate_associations(
        &self,
        rng: &mut StdRng,
        registry: &EntityRegistry,
        edges: &mut Vec<EdgeRecord>,
    ) {
        let org_traversal: IStr = self.intern("0/");

        for (edge_type, source_kind, target_kind, ratio, direction) in
            self.config.associations.all_associations()
        {
            let (iterate_kind, other_kind) = match direction {
                IterationDirection::Target => (&target_kind, &source_kind),
                IterationDirection::Source => (&source_kind, &target_kind),
            };

            let iterate_ids = registry.get_ids(iterate_kind);
            let other_ids = registry.get_ids(other_kind);

            if iterate_ids.is_empty() || other_ids.is_empty() {
                continue;
            }

            for &iterate_id in &iterate_ids {
                let count = ratio.sample_with_variance(rng);
                for _ in 0..count {
                    let other_id = other_ids[rng.gen_range(0..other_ids.len())];

                    let (source, target) = match direction {
                        IterationDirection::Target => (other_id, iterate_id),
                        IterationDirection::Source => (iterate_id, other_id),
                    };

                    edges.push(EdgeRecord {
                        traversal_path: org_traversal.clone(),
                        relationship_kind: self.intern(&edge_type),
                        source,
                        source_kind: self.intern(&source_kind),
                        target,
                        target_kind: self.intern(&target_kind),
                    });
                }
            }
        }
    }

    /// Print a summary of the generation plan to tracing.
    pub fn print_plan(&self) {
        info!("generation plan:");
        info!("  organizations: {}", self.config.organizations);
        for node_type in self.dependency_graph.generation_order() {
            if self.dependency_graph.is_root(node_type) {
                let count = self.config.roots.get(node_type).copied().unwrap_or(0);
                info!("  root: {} x {}", node_type, count);
            } else if let Some(parent_edges) = self.dependency_graph.parent_edges(node_type) {
                for pe in parent_edges {
                    info!(
                        "  child: {} via {} (parent: {}, ratio: {:?})",
                        node_type, pe.edge_type, pe.parent_kind, pe.ratio
                    );
                }
            }
        }
        for (edge_type, source, target, ratio, direction) in
            self.config.associations.all_associations()
        {
            info!(
                "  association: {} ({} -> {}, ratio: {:?}, iterate: {:?})",
                edge_type, source, target, ratio, direction
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ontology::EdgeEntity;

    fn test_ontology() -> Ontology {
        let mut ontology = Ontology::new()
            .with_nodes(["Group", "Project", "MergeRequest", "User"])
            .with_fields("Group", [("name", ontology::DataType::String)])
            .with_fields("Project", [("name", ontology::DataType::String)])
            .with_fields("MergeRequest", [("title", ontology::DataType::String)])
            .with_fields("User", [("name", ontology::DataType::String)]);

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
            edge("AUTHORED", "User", "MergeRequest"),
        ] {
            ontology.add_edge(e);
        }

        ontology
    }

    fn test_config() -> GenerationConfig {
        GenerationConfig {
            organizations: 1,
            roots: [("Group".to_string(), 2), ("User".to_string(), 5)].into(),
            relationships: crate::config::RelationshipConfig {
                edges: [
                    (
                        "CONTAINS".to_string(),
                        [(
                            "Group -> Project".to_string(),
                            crate::config::EdgeRatio::Count(3),
                        )]
                        .into(),
                    ),
                    (
                        "IN_PROJECT".to_string(),
                        [(
                            "MergeRequest -> Project".to_string(),
                            crate::config::EdgeRatio::Count(2),
                        )]
                        .into(),
                    ),
                ]
                .into(),
            },
            associations: crate::config::AssociationConfig {
                edges: [(
                    "AUTHORED".to_string(),
                    [(
                        "User -> MergeRequest".to_string(),
                        crate::config::AssociationEdgeValue::Simple(
                            crate::config::EdgeRatio::Count(1),
                        ),
                    )]
                    .into(),
                )]
                .into(),
            },
            subgroups: crate::config::SubgroupConfig {
                max_depth: 2,
                per_group: 1,
            },
            batch_size: 1000,
            seed: 42,
        }
    }

    #[test]
    fn test_generate_organization() {
        let ontology = test_ontology();
        let config = test_config();
        let generator = Generator::new(ontology, config).unwrap();

        let data = generator.generate_organization(1);

        assert!(data.nodes.contains_key("Group"));
        assert!(data.nodes.contains_key("Project"));
        assert!(data.nodes.contains_key("MergeRequest"));
        assert!(data.nodes.contains_key("User"));
        assert!(!data.edges.is_empty());

        let group_rows: usize = data.nodes["Group"].iter().map(|b| b.num_rows()).sum();
        assert!(group_rows >= 2, "should have at least 2 root groups");

        let has_contains = data
            .edges
            .iter()
            .any(|e| e.relationship_kind.as_ref() == "CONTAINS");
        let has_in_project = data
            .edges
            .iter()
            .any(|e| e.relationship_kind.as_ref() == "IN_PROJECT");
        let has_authored = data
            .edges
            .iter()
            .any(|e| e.relationship_kind.as_ref() == "AUTHORED");

        assert!(has_contains, "should have CONTAINS edges");
        assert!(has_in_project, "should have IN_PROJECT edges");
        assert!(has_authored, "should have AUTHORED edges");
    }

    #[test]
    fn test_generate_streaming() {
        let ontology = test_ontology();
        let config = test_config();
        let generator = Generator::new(ontology, config).unwrap();

        let mut edge_count = 0;
        let nodes = generator.generate_organization_streaming(1, |_edge| {
            edge_count += 1;
        });

        assert!(!nodes.nodes.is_empty());
        assert!(edge_count > 0);
    }
}
