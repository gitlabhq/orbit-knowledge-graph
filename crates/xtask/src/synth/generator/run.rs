//! Synthetic SDLC data generator.
//!
//! Generates entities in topological order based on relationship definitions,
//! creating edges inline during generation. This ensures referential integrity
//! and proper traversal ID inheritance.

use super::batch::BatchBuilder;
use super::dependency::{DependencyGraph, ParentEdge};
use super::fake_data::FakeDataPools;
use super::parquet_writer::{ParquetWriter, StreamingEdgeWriter};
use super::traversal::{EntityContext, EntityRegistry};
use crate::synth::arrow_schema::ToArrowSchema;
use crate::synth::config::{Config, EdgeRatio};
use crate::synth::constants::ASSOCIATION_TRAVERSAL_PATH;
use anyhow::Result;
use arrow::record_batch::RecordBatch;
use ontology::{NodeEntity, Ontology};
use rand::{Rng, RngExt};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

// ── Public types ──────────────────────────────────────────────────────

/// Interned string type for edge records to avoid millions of small allocations.
pub type IStr = Arc<str>;

/// Edge record for storage.
#[derive(Debug, Clone)]
pub struct EdgeRecord {
    pub traversal_path: IStr,
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

// ── Internals ─────────────────────────────────────────────────────────

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

// ── Generator ─────────────────────────────────────────────────────────

pub struct Generator {
    ontology: Ontology,
    config: Config,
    dependency_graph: DependencyGraph,
    global_entity_counter: AtomicI64,
    interner: std::sync::Mutex<StringInterner>,
    pools: &'static FakeDataPools,
}

impl std::fmt::Debug for Generator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Generator")
            .field("config", &self.config)
            .field("dependency_graph", &self.dependency_graph)
            .finish_non_exhaustive()
    }
}

impl Generator {
    pub fn new(ontology: Ontology, config: Config) -> Result<Self> {
        use crate::synth::config::FakeDataConfig;

        Self::validate_config(&config, &ontology)?;
        let dependency_graph = DependencyGraph::build(&config.generation, &ontology)?;

        let fake_data_config = FakeDataConfig::load(&config.generation.fake_data_path)?;
        let pools = FakeDataPools::intern(fake_data_config);

        Ok(Self {
            ontology,
            config,
            dependency_graph,
            global_entity_counter: AtomicI64::new(1),
            interner: std::sync::Mutex::new(StringInterner::default()),
            pools,
        })
    }

    fn validate_config(config: &Config, ontology: &Ontology) -> Result<()> {
        let generation = &config.generation;

        if generation.organizations == 0 {
            anyhow::bail!("generation.organizations must be > 0");
        }
        if generation.batch_size == 0 {
            anyhow::bail!("generation.batch_size must be > 0");
        }

        for node_type in generation.roots.keys() {
            DependencyGraph::validate_node(ontology, node_type)
                .map_err(|e| anyhow::anyhow!("roots: {}", e))?;
        }

        DependencyGraph::validate_node(ontology, &generation.namespace_entity)
            .map_err(|e| anyhow::anyhow!("namespace_entity: {}", e))?;

        for (edge_type, variants) in &generation.relationships.edges {
            for variant_key in variants.keys() {
                let (source, target) =
                    crate::synth::config::RelationshipConfig::parse_variant_key(variant_key)
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "relationships: invalid variant key '{}' in edge type '{}'. \
                                 Expected 'Source -> Target'.",
                                variant_key,
                                edge_type
                            )
                        })?;
                DependencyGraph::validate_node(ontology, &source)
                    .map_err(|e| anyhow::anyhow!("relationships.{}: {}", edge_type, e))?;
                DependencyGraph::validate_node(ontology, &target)
                    .map_err(|e| anyhow::anyhow!("relationships.{}: {}", edge_type, e))?;
                DependencyGraph::validate_edge(ontology, edge_type, &source, &target)
                    .map_err(|e| anyhow::anyhow!("relationships: {}", e))?;
            }
        }

        for (edge_type, variants) in &generation.associations.edges {
            for variant_key in variants.keys() {
                let (source, target) =
                    crate::synth::config::RelationshipConfig::parse_variant_key(variant_key)
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "associations: invalid variant key '{}' in edge type '{}'. \
                                 Expected 'Source -> Target'.",
                                variant_key,
                                edge_type
                            )
                        })?;
                DependencyGraph::validate_node(ontology, &source)
                    .map_err(|e| anyhow::anyhow!("associations.{}: {}", edge_type, e))?;
                DependencyGraph::validate_node(ontology, &target)
                    .map_err(|e| anyhow::anyhow!("associations.{}: {}", edge_type, e))?;
                DependencyGraph::validate_edge(ontology, edge_type, &source, &target)
                    .map_err(|e| anyhow::anyhow!("associations: {}", e))?;
            }
        }

        Self::validate_schema_config(&config.clickhouse.schema, ontology)?;

        Ok(())
    }

    fn validate_schema_config(
        schema: &crate::synth::config::SchemaConfig,
        ontology: &Ontology,
    ) -> Result<()> {
        use ontology::constants::EDGE_RESERVED_COLUMNS;

        let universal_node_columns: std::collections::HashSet<&str> = {
            let mut cols: std::collections::HashSet<&str> =
                std::iter::once(ontology::constants::TRAVERSAL_PATH_COLUMN).collect();
            for col in ontology::constants::NODE_RESERVED_COLUMNS {
                cols.insert(col);
            }
            cols
        };

        let edge_columns: std::collections::HashSet<&str> =
            EDGE_RESERVED_COLUMNS.iter().copied().collect();

        for col in &schema.node_order_by {
            if !universal_node_columns.contains(col.as_str()) {
                anyhow::bail!(
                    "schema.node_order_by: column '{}' is not a universal node column. \
                     Valid columns (present on all node tables): {:?}",
                    col,
                    universal_node_columns.iter().collect::<Vec<_>>()
                );
            }
        }

        for col in &schema.node_primary_key {
            if !universal_node_columns.contains(col.as_str()) {
                anyhow::bail!(
                    "schema.node_primary_key: column '{}' is not a universal node column. \
                     Valid columns (present on all node tables): {:?}",
                    col,
                    universal_node_columns.iter().collect::<Vec<_>>()
                );
            }
        }

        for col in &schema.edge_order_by {
            if !edge_columns.contains(col.as_str()) {
                anyhow::bail!(
                    "schema.edge_order_by: column '{}' is not a valid edge column. \
                     Valid columns: {:?}",
                    col,
                    EDGE_RESERVED_COLUMNS
                );
            }
        }

        for col in &schema.edge_primary_key {
            if !edge_columns.contains(col.as_str()) {
                anyhow::bail!(
                    "schema.edge_primary_key: column '{}' is not a valid edge column. \
                     Valid columns: {:?}",
                    col,
                    EDGE_RESERVED_COLUMNS
                );
            }
        }

        for idx in &schema.indexes {
            let valid_columns = Self::resolve_valid_columns(
                &idx.table,
                ontology,
                &universal_node_columns,
                &edge_columns,
            )?;
            if !valid_columns.contains(idx.expression.as_str()) {
                anyhow::bail!(
                    "schema.indexes[{}]: expression '{}' is not a valid column for table '{}'. \
                     Valid columns: {:?}",
                    idx.name,
                    idx.expression,
                    idx.table,
                    valid_columns.iter().collect::<Vec<_>>()
                );
            }
        }

        for proj in &schema.projections {
            let valid_columns = Self::resolve_valid_columns(
                &proj.table,
                ontology,
                &universal_node_columns,
                &edge_columns,
            )?;
            for col in &proj.columns {
                if !valid_columns.contains(col.as_str()) {
                    anyhow::bail!(
                        "schema.projections[{}]: column '{}' is not a valid column for table '{}'. \
                         Valid columns: {:?}",
                        proj.name,
                        col,
                        proj.table,
                        valid_columns.iter().collect::<Vec<_>>()
                    );
                }
            }
            for col in &proj.order_by {
                if !valid_columns.contains(col.as_str()) {
                    anyhow::bail!(
                        "schema.projections[{}]: order_by column '{}' is not a valid column for table '{}'. \
                         Valid columns: {:?}",
                        proj.name,
                        col,
                        proj.table,
                        valid_columns.iter().collect::<Vec<_>>()
                    );
                }
            }
        }

        Ok(())
    }

    fn resolve_valid_columns<'a>(
        table_pattern: &str,
        ontology: &'a Ontology,
        universal_node_columns: &std::collections::HashSet<&'a str>,
        edge_columns: &std::collections::HashSet<&'a str>,
    ) -> Result<std::collections::HashSet<&'a str>> {
        use crate::synth::constants::{TABLE_PATTERN_ALL_NODES, TABLE_PATTERN_EDGES};
        match table_pattern {
            TABLE_PATTERN_EDGES => Ok(edge_columns.clone()),
            TABLE_PATTERN_ALL_NODES => Ok(universal_node_columns.clone()),
            specific => {
                if let Some(node) = ontology
                    .nodes()
                    .find(|n| ontology.table_name(&n.name).is_ok_and(|t| t == specific))
                {
                    let mut cols: std::collections::HashSet<&str> =
                        universal_node_columns.iter().copied().collect();
                    cols.insert(ontology::constants::TRAVERSAL_PATH_COLUMN);
                    for field in &node.fields {
                        cols.insert(&field.name);
                    }
                    Ok(cols)
                } else if specific == ontology::constants::EDGE_TABLE {
                    Ok(edge_columns.clone())
                } else {
                    anyhow::bail!(
                        "unknown table '{}'. Use '*' for all node tables, 'edges' for the edge table, \
                         or a specific node table name (e.g., 'gl_user').",
                        specific
                    );
                }
            }
        }
    }

    fn intern(&self, s: &str) -> IStr {
        self.interner.lock().unwrap().intern(s)
    }

    pub fn ontology(&self) -> &ontology::Ontology {
        &self.ontology
    }

    pub fn next_entity_id(&self) -> i64 {
        self.global_entity_counter.fetch_add(1, Ordering::SeqCst)
    }

    pub fn generate_organization(&self, org_id: u32) -> Result<OrganizationData> {
        let mut data = OrganizationData::default();
        let mut registry = EntityRegistry::new(org_id);
        let mut rng = rand::rng();

        for node_type in self.dependency_graph.generation_order() {
            let real_type = self.dependency_graph.resolve_type(node_type);
            let node = self
                .ontology
                .nodes()
                .find(|n| n.name == real_type)
                .ok_or_else(|| {
                    anyhow::anyhow!("Node type '{}' not found in ontology", real_type)
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
                    data.nodes
                        .entry(real_type.to_string())
                        .or_default()
                        .extend(batches);
                    data.edges.extend(edges);
                }
            } else if let Some(parent_edges) = self.dependency_graph.parent_edges(node_type) {
                let (batches, edges) = self.generate_child_entities(
                    node,
                    node_type,
                    parent_edges,
                    &mut registry,
                    &mut rng,
                )?;
                if !batches.is_empty() {
                    data.nodes
                        .entry(real_type.to_string())
                        .or_default()
                        .extend(batches);
                }
                data.edges.extend(edges);
            }
        }

        registry.compact_with_aliases(self.dependency_graph.epsilon_to_real());

        let association_edges = self.generate_association_edges(&registry, &mut rng);
        data.edges.extend(association_edges);

        Ok(data)
    }

    pub fn generate_organization_streaming(
        &self,
        org_id: u32,
        edge_writer: &mut StreamingEdgeWriter,
    ) -> Result<OrganizationNodes> {
        let mut data = OrganizationNodes::default();
        let mut registry = EntityRegistry::new(org_id);
        let mut rng = rand::rng();

        for node_type in self.dependency_graph.generation_order() {
            let real_type = self.dependency_graph.resolve_type(node_type);
            let node = self
                .ontology
                .nodes()
                .find(|n| n.name == real_type)
                .ok_or_else(|| {
                    anyhow::anyhow!("Node type '{}' not found in ontology", real_type)
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
                    data.nodes
                        .entry(real_type.to_string())
                        .or_default()
                        .extend(batches);
                }
            } else if let Some(parent_edges) = self.dependency_graph.parent_edges(node_type) {
                let batches = self.generate_child_entities_streaming(
                    node,
                    node_type,
                    parent_edges,
                    &mut registry,
                    &mut rng,
                    edge_writer,
                )?;
                if !batches.is_empty() {
                    data.nodes
                        .entry(real_type.to_string())
                        .or_default()
                        .extend(batches);
                }
            }
        }

        registry.compact_with_aliases(self.dependency_graph.epsilon_to_real());
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
            self.pools,
        );
        let is_namespace_entity = node.name == self.config.generation.namespace_entity;

        for _ in 0..count {
            let (entity_id, traversal_path) = if is_namespace_entity {
                let ns_id = registry.next_namespace_id();
                (ns_id, format!("{}/{}/", org_id, ns_id))
            } else {
                (self.next_entity_id(), format!("{}/", org_id))
            };

            builder.add_row(traversal_path.clone(), entity_id);
            registry.add(&node.name, EntityContext::new(entity_id, traversal_path));
        }

        Ok((builder.finish(), Vec::new()))
    }

    fn generate_child_entities(
        &self,
        node: &NodeEntity,
        registry_key: &str,
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
            self.pools,
        );
        let mut edges = Vec::new();
        let is_namespace_entity = node.name == self.config.generation.namespace_entity;
        let is_parent_type = self.dependency_graph.is_parent_type(registry_key);

        for parent_edge in parent_edges {
            let parents: Vec<_> = match registry.get(&parent_edge.parent_kind) {
                Some(p) if !p.is_empty() => {
                    p.iter().map(|e| (e.id, e.traversal_path.clone())).collect()
                }
                _ => continue,
            };

            let rel_kind = self.intern(&parent_edge.edge_type);
            let parent_real_type = self.dependency_graph.resolve_type(&parent_edge.parent_kind);
            let parent_kind_str = self.intern(parent_real_type);
            let node_name_str = self.intern(&node.name);

            for (parent_id, parent_path) in &parents {
                let child_count = parent_edge.ratio.sample_with_variance(rng);

                for _ in 0..child_count {
                    let (entity_id, traversal_path) = if is_namespace_entity {
                        let ns_id = registry.next_namespace_id();
                        (ns_id, format!("{}{}/", parent_path, ns_id))
                    } else {
                        (self.next_entity_id(), parent_path.clone())
                    };

                    builder.add_row(traversal_path.clone(), entity_id);

                    if is_parent_type {
                        registry.add(registry_key, EntityContext::new(entity_id, traversal_path));
                    } else {
                        registry.add_id_only(registry_key, entity_id);
                    }

                    let (source, source_kind, target, target_kind) = if parent_edge.parent_to_child
                    {
                        (
                            *parent_id,
                            parent_kind_str.clone(),
                            entity_id,
                            node_name_str.clone(),
                        )
                    } else {
                        (
                            entity_id,
                            node_name_str.clone(),
                            *parent_id,
                            parent_kind_str.clone(),
                        )
                    };

                    edges.push(EdgeRecord {
                        traversal_path: self.intern(parent_path),
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

    fn generate_association_edges(
        &self,
        registry: &EntityRegistry,
        rng: &mut impl Rng,
    ) -> Vec<EdgeRecord> {
        use crate::synth::config::IterationDirection;

        let mut edges = Vec::new();

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
                let edge_count = ratio.sample(rng);
                if edge_count == 0 {
                    continue;
                }

                let count = edge_count.min(sample_len);
                for _ in 0..count {
                    let secondary_id = sample_from[rng.random_range(0..sample_len)];
                    let (source_id, target_id) = match direction {
                        IterationDirection::Target => (secondary_id, primary_id),
                        IterationDirection::Source => (primary_id, secondary_id),
                    };

                    edges.push(EdgeRecord {
                        traversal_path: self.intern(ASSOCIATION_TRAVERSAL_PATH),
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

    // ── Streaming variants ────────────────────────────────────────────

    fn generate_root_entities_streaming(
        &self,
        node: &NodeEntity,
        org_id: u32,
        count: usize,
        registry: &mut EntityRegistry,
        _rng: &mut impl Rng,
        _edge_writer: &mut StreamingEdgeWriter,
    ) -> Result<Vec<RecordBatch>> {
        let schema = Arc::new(node.to_arrow_schema());
        let mut builder = BatchBuilder::with_seed(
            node,
            schema,
            self.config.generation.batch_size,
            self.config.generation.seed,
            self.pools,
        );
        let is_namespace_entity = node.name == self.config.generation.namespace_entity;

        for _ in 0..count {
            let (entity_id, traversal_path) = if is_namespace_entity {
                let ns_id = registry.next_namespace_id();
                (ns_id, format!("{}/{}/", org_id, ns_id))
            } else {
                (self.next_entity_id(), format!("{}/", org_id))
            };

            builder.add_row(traversal_path.clone(), entity_id);
            registry.add(&node.name, EntityContext::new(entity_id, traversal_path));
        }

        Ok(builder.finish())
    }

    fn generate_child_entities_streaming(
        &self,
        node: &NodeEntity,
        registry_key: &str,
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
            self.pools,
        );
        let is_namespace_entity = node.name == self.config.generation.namespace_entity;
        let is_parent_type = self.dependency_graph.is_parent_type(registry_key);

        for parent_edge in parent_edges {
            let parents: Vec<_> = match registry.get(&parent_edge.parent_kind) {
                Some(p) if !p.is_empty() => {
                    p.iter().map(|e| (e.id, e.traversal_path.clone())).collect()
                }
                _ => continue,
            };

            let rel_kind = self.intern(&parent_edge.edge_type);
            let parent_real_type = self.dependency_graph.resolve_type(&parent_edge.parent_kind);
            let parent_kind_str = self.intern(parent_real_type);
            let node_name_str = self.intern(&node.name);

            for (parent_id, parent_path) in &parents {
                let child_count = parent_edge.ratio.sample_with_variance(rng);

                for _ in 0..child_count {
                    let (entity_id, traversal_path) = if is_namespace_entity {
                        let ns_id = registry.next_namespace_id();
                        (ns_id, format!("{}{}/", parent_path, ns_id))
                    } else {
                        (self.next_entity_id(), parent_path.clone())
                    };

                    builder.add_row(traversal_path.clone(), entity_id);

                    if is_parent_type {
                        registry.add(registry_key, EntityContext::new(entity_id, traversal_path));
                    } else {
                        registry.add_id_only(registry_key, entity_id);
                    }

                    let (source, source_kind, target, target_kind) = if parent_edge.parent_to_child
                    {
                        (
                            *parent_id,
                            parent_kind_str.clone(),
                            entity_id,
                            node_name_str.clone(),
                        )
                    } else {
                        (
                            entity_id,
                            node_name_str.clone(),
                            *parent_id,
                            parent_kind_str.clone(),
                        )
                    };

                    edge_writer.push(EdgeRecord {
                        traversal_path: self.intern(parent_path),
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
        use crate::synth::config::IterationDirection;

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
                let edge_count = ratio.sample(rng);
                if edge_count == 0 {
                    continue;
                }

                let count = edge_count.min(sample_len);
                for _ in 0..count {
                    let secondary_id = sample_from[rng.random_range(0..sample_len)];
                    let (source_id, target_id) = match direction {
                        IterationDirection::Target => (secondary_id, primary_id),
                        IterationDirection::Source => (primary_id, secondary_id),
                    };

                    edge_writer.push(EdgeRecord {
                        traversal_path: self.intern(ASSOCIATION_TRAVERSAL_PATH),
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

        for (edge_type, variants) in &cfg.relationships.edges {
            for (variant_key, ratio) in variants {
                if let EdgeRatio::Recursive { count, max_depth } = ratio
                    && let Some((source, _)) =
                        crate::synth::config::RelationshipConfig::parse_variant_key(variant_key)
                {
                    let root_count = cfg.roots.get(&source).copied().unwrap_or(0);
                    let mut total = root_count;
                    let mut level_count = root_count;
                    for _ in 1..=*max_depth {
                        level_count *= count;
                        total += level_count;
                    }
                    println!(
                        "    (with {}: {} {} levels x {} per parent = {} total per org)",
                        edge_type,
                        source.to_lowercase(),
                        max_depth,
                        count,
                        total,
                    );
                }
            }
        }
        println!();

        println!(
            "  Generation order ({} types):",
            self.dependency_graph.generation_order().len()
        );
        for (i, node_type) in self.dependency_graph.generation_order().iter().enumerate() {
            let marker = if self.dependency_graph.is_root(node_type) {
                " (root)"
            } else if self.dependency_graph.is_epsilon(node_type) {
                " (epsilon)"
            } else {
                ""
            };
            println!("    {}. {}{}", i + 1, node_type, marker);
        }
        println!();

        println!("  Relationships:");
        for (edge_type, variants) in &cfg.relationships.edges {
            for (variant, ratio) in variants {
                let ratio_str = match ratio {
                    EdgeRatio::Count(n) => format!("{} per parent", n),
                    EdgeRatio::Probability(p) => format!("{:.0}% chance", p * 100.0),
                    EdgeRatio::Recursive { count, max_depth } => {
                        format!("{} per parent x {} depth levels", count, max_depth)
                    }
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
                        crate::synth::config::IterationDirection::Target => "per target",
                        crate::synth::config::IterationDirection::Source => "per source",
                    };
                    let ratio_str = match ratio {
                        EdgeRatio::Count(n) | EdgeRatio::Recursive { count: n, .. } => {
                            format!("{} {}", n, per_str)
                        }
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

// ── CLI entry point ───────────────────────────────────────────────────

pub fn run(config_path: &Path, dry_run: bool, force: bool) -> Result<()> {
    println!("GitLab Knowledge Graph Generator");
    println!("=================================\n");

    println!("Loading config from {:?}...", config_path);
    let config = Config::load(config_path)?;

    let writer = ParquetWriter::new(&config.generation.output_dir);

    if !force && config.generation.skip_if_present && writer.data_exists() {
        println!(
            "Data already exists in {:?}, skipping generation.",
            config.generation.output_dir
        );
        println!("Use --force to regenerate.");
        return Ok(());
    }

    println!(
        "Loading ontology from {:?}...",
        config.generation.ontology_path
    );
    let ontology = Ontology::load_from_dir(&config.generation.ontology_path)?;
    println!(
        "Loaded {} node types and {} edge types\n",
        ontology.node_count(),
        ontology.edge_count()
    );

    let generator = Generator::new(ontology.clone(), config.clone())?;
    generator.print_plan();

    if dry_run {
        println!("Dry run - not executing.");
        return Ok(());
    }

    println!("Output directory: {:?}\n", config.generation.output_dir);

    if Path::new(&config.generation.output_dir).exists() {
        println!("Removing existing data directory...");
        std::fs::remove_dir_all(&config.generation.output_dir)?;
    }

    std::fs::create_dir_all(&config.generation.output_dir)?;

    let overall_start = std::time::Instant::now();

    for org_id in 1..=config.generation.organizations {
        println!(
            "=== Organization {}/{} ===",
            org_id, config.generation.organizations
        );

        let mut edge_writer = writer.create_edge_writer(org_id, generator.ontology())?;

        let gen_start = std::time::Instant::now();
        let org_nodes = generator.generate_organization_streaming(org_id, &mut edge_writer)?;
        let gen_elapsed = gen_start.elapsed().as_secs_f64();

        let node_count: usize = org_nodes
            .nodes
            .values()
            .map(|batches| batches.iter().map(|b| b.num_rows()).sum::<usize>())
            .sum();

        let edge_count = edge_writer.count();

        println!(
            "  Generated {} nodes + {} edges ({:.1}s)",
            node_count, edge_count, gen_elapsed
        );

        let write_start = std::time::Instant::now();

        writer.write_organization_nodes(org_id, &org_nodes)?;
        edge_writer.close()?;

        let write_elapsed = write_start.elapsed().as_secs_f64();
        println!("  Written to Parquet ({:.1}s)\n", write_elapsed);
    }

    writer.write_manifest(&ontology, config.generation.organizations)?;

    println!(
        "Done! Total time: {:.1}s",
        overall_start.elapsed().as_secs_f64()
    );
    println!("Data written to: {:?}", config.generation.output_dir);

    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::synth::config::{
        AssociationConfig, AssociationEdgeValue, Config, EdgeRatio, GenerationConfig,
        RelationshipConfig,
    };
    use arrow::array::Array;
    use std::collections::{HashMap, HashSet};

    fn fake_data_path() -> String {
        crate::synth::fixture_path(crate::synth::constants::DEFAULT_FAKE_DATA_PATH)
    }

    fn extract_ids(batches: &[RecordBatch]) -> Vec<i64> {
        let mut ids = Vec::new();
        for batch in batches {
            let schema = batch.schema();
            let id_idx = schema
                .fields()
                .iter()
                .position(|f| f.name() == "id")
                .expect("batch should have an 'id' column");
            let id_col = batch
                .column(id_idx)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("id column should be Int64");
            for i in 0..id_col.len() {
                ids.push(id_col.value(i));
            }
        }
        ids
    }

    fn test_config_and_ontology() -> (Config, Ontology) {
        let ontology = Ontology::load_embedded().expect("should load embedded ontology");

        let mut roots = HashMap::new();
        roots.insert("User".to_string(), 5);
        roots.insert("Group".to_string(), 2);

        let mut rel_edges = HashMap::new();
        let mut contains_variants = HashMap::new();
        contains_variants.insert(
            "Group -> Group".to_string(),
            EdgeRatio::Recursive {
                count: 1,
                max_depth: 1,
            },
        );
        contains_variants.insert("Group -> Project".to_string(), EdgeRatio::Count(3));
        rel_edges.insert("CONTAINS".to_string(), contains_variants);

        let mut in_project_variants = HashMap::new();
        in_project_variants.insert("MergeRequest -> Project".to_string(), EdgeRatio::Count(2));
        rel_edges.insert("IN_PROJECT".to_string(), in_project_variants);

        let mut assoc_edges = HashMap::new();
        let mut authored_variants = HashMap::new();
        authored_variants.insert(
            "User -> MergeRequest".to_string(),
            AssociationEdgeValue::Simple(EdgeRatio::Count(1)),
        );
        assoc_edges.insert("AUTHORED".to_string(), authored_variants);

        let config = Config {
            generation: GenerationConfig {
                organizations: 1,
                roots,
                relationships: RelationshipConfig { edges: rel_edges },
                associations: AssociationConfig { edges: assoc_edges },
                batch_size: 100,
                seed: Some(42),
                fake_data_path: fake_data_path(),
                ..Default::default()
            },
            ..Default::default()
        };

        (config, ontology)
    }

    #[test]
    fn test_generator_produces_nodes_and_edges() {
        let (config, ontology) = test_config_and_ontology();
        let generator = Generator::new(ontology, config).unwrap();
        let data = generator.generate_organization(1).unwrap();

        assert!(data.nodes.contains_key("User"));
        assert!(data.nodes.contains_key("Group"));
        assert!(data.nodes.contains_key("Project"));
        assert!(data.nodes.contains_key("MergeRequest"));
        assert!(!data.edges.is_empty());

        let edge_types: HashSet<&str> = data
            .edges
            .iter()
            .map(|e| e.relationship_kind.as_ref())
            .collect();
        assert!(edge_types.contains("CONTAINS"));
        assert!(edge_types.contains("IN_PROJECT"));
        assert!(edge_types.contains("AUTHORED"));
    }

    #[test]
    fn test_generator_unique_entity_ids_per_type() {
        let (config, ontology) = test_config_and_ontology();
        let generator = Generator::new(ontology, config).unwrap();
        let data = generator.generate_organization(1).unwrap();

        for (node_type, batches) in &data.nodes {
            let ids = extract_ids(batches);
            let unique: HashSet<i64> = ids.iter().copied().collect();
            assert_eq!(ids.len(), unique.len(), "Duplicate IDs in {}", node_type);
        }
    }

    #[test]
    fn test_generator_edge_references_valid_ids() {
        let (config, ontology) = test_config_and_ontology();
        let generator = Generator::new(ontology, config).unwrap();
        let data = generator.generate_organization(1).unwrap();

        let all_ids: HashSet<i64> = data.nodes.values().flat_map(|b| extract_ids(b)).collect();

        for edge in &data.edges {
            assert!(
                all_ids.contains(&edge.source),
                "Edge {:?} source {} not found",
                edge.relationship_kind,
                edge.source
            );
            assert!(
                all_ids.contains(&edge.target),
                "Edge {:?} target {} not found",
                edge.relationship_kind,
                edge.target
            );
        }
    }

    #[test]
    fn test_generator_association_edges_use_sentinel_path() {
        let (config, ontology) = test_config_and_ontology();
        let generator = Generator::new(ontology, config).unwrap();
        let data = generator.generate_organization(1).unwrap();

        let authored: Vec<_> = data
            .edges
            .iter()
            .filter(|e| e.relationship_kind.as_ref() == "AUTHORED")
            .collect();
        assert!(!authored.is_empty());
        for edge in &authored {
            assert_eq!(edge.traversal_path.as_ref(), ASSOCIATION_TRAVERSAL_PATH);
        }
    }

    #[test]
    fn test_generator_namespace_entity_extends_traversal_paths() {
        let (config, ontology) = test_config_and_ontology();
        let generator = Generator::new(ontology, config).unwrap();
        let data = generator.generate_organization(1).unwrap();

        for batch in &data.nodes["Group"] {
            let tp_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();
            for i in 0..tp_col.len() {
                let depth = tp_col.value(i).split('/').filter(|s| !s.is_empty()).count();
                assert!(
                    depth >= 2,
                    "Group path '{}' should have depth >= 2",
                    tp_col.value(i)
                );
            }
        }

        for batch in &data.nodes["User"] {
            let tp_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();
            for i in 0..tp_col.len() {
                let depth = tp_col.value(i).split('/').filter(|s| !s.is_empty()).count();
                assert_eq!(
                    depth,
                    1,
                    "User path '{}' should have depth 1",
                    tp_col.value(i)
                );
            }
        }
    }

    #[test]
    fn test_generator_recursive_hierarchy_edges() {
        let (config, ontology) = test_config_and_ontology();
        let ns_entity = config.generation.namespace_entity.clone();
        let generator = Generator::new(ontology, config).unwrap();
        let data = generator.generate_organization(1).unwrap();

        let hierarchy_edges: Vec<_> = data
            .edges
            .iter()
            .filter(|e| {
                e.relationship_kind.as_ref() == "CONTAINS"
                    && e.source_kind.as_ref() == ns_entity.as_str()
                    && e.target_kind.as_ref() == ns_entity.as_str()
            })
            .collect();

        assert!(
            hierarchy_edges.len() >= 2 && hierarchy_edges.len() <= 4,
            "Expected 2-4 CONTAINS {ns}->{ns} edges, got {}",
            hierarchy_edges.len(),
            ns = ns_entity
        );

        for edge in &hierarchy_edges {
            assert_ne!(edge.source, edge.target);
        }
    }

    #[test]
    fn test_generator_global_entity_counter_across_orgs() {
        let (config, ontology) = test_config_and_ontology();
        let generator = Generator::new(ontology, config).unwrap();

        let data1 = generator.generate_organization(1).unwrap();
        let data2 = generator.generate_organization(2).unwrap();

        let collect_non_ns_ids = |data: &OrganizationData| -> HashSet<i64> {
            data.nodes
                .iter()
                .filter(|(k, _)| k.as_str() != "Group")
                .flat_map(|(_, b)| extract_ids(b))
                .collect()
        };

        let ids1 = collect_non_ns_ids(&data1);
        let ids2 = collect_non_ns_ids(&data2);
        let overlap: Vec<_> = ids1.intersection(&ids2).collect();
        assert!(
            overlap.is_empty(),
            "Non-namespace IDs should not overlap: {:?}",
            overlap
        );
    }

    // --- Validation tests ---

    #[test]
    fn test_validation_rejects_unknown_root_entity() {
        let ontology = Ontology::load_embedded().unwrap();
        let mut config = Config::default();
        config.generation.fake_data_path = fake_data_path();
        config.generation.roots.insert("FakeEntity".to_string(), 10);
        let err = Generator::new(ontology, config).unwrap_err();
        assert!(err.to_string().contains("roots:") && err.to_string().contains("FakeEntity"));
    }

    #[test]
    fn test_validation_rejects_unknown_namespace_entity() {
        let ontology = Ontology::load_embedded().unwrap();
        let mut config = Config::default();
        config.generation.fake_data_path = fake_data_path();
        config.generation.namespace_entity = "FakeNamespace".to_string();
        config.generation.roots.insert("User".to_string(), 1);
        let err = Generator::new(ontology, config).unwrap_err();
        assert!(
            err.to_string().contains("namespace_entity:")
                && err.to_string().contains("FakeNamespace")
        );
    }

    #[test]
    fn test_validation_rejects_self_referential_without_recursive() {
        let ontology = Ontology::load_embedded().unwrap();
        let mut config = Config::default();
        config.generation.fake_data_path = fake_data_path();
        config.generation.roots.insert("Group".to_string(), 1);
        let mut contains = HashMap::new();
        contains.insert("Group -> Group".to_string(), EdgeRatio::Count(2));
        config
            .generation
            .relationships
            .edges
            .insert("CONTAINS".to_string(), contains);
        let err = Generator::new(ontology, config).unwrap_err();
        assert!(
            err.to_string().contains("Self-referential")
                && err.to_string().contains("count, max_depth")
        );
    }

    #[test]
    fn test_validation_rejects_recursive_on_non_self_referential() {
        let ontology = Ontology::load_embedded().unwrap();
        let mut config = Config::default();
        config.generation.fake_data_path = fake_data_path();
        config.generation.roots.insert("Group".to_string(), 1);
        let mut contains = HashMap::new();
        contains.insert(
            "Group -> Project".to_string(),
            EdgeRatio::Recursive {
                count: 2,
                max_depth: 3,
            },
        );
        config
            .generation
            .relationships
            .edges
            .insert("CONTAINS".to_string(), contains);
        let err = Generator::new(ontology, config).unwrap_err();
        assert!(err.to_string().contains("only valid for self-referential"));
    }

    #[test]
    fn test_validation_rejects_unknown_association_edge() {
        let ontology = Ontology::load_embedded().unwrap();
        let (mut config, _) = test_config_and_ontology();
        let mut fake_variants = HashMap::new();
        fake_variants.insert(
            "User -> Project".to_string(),
            AssociationEdgeValue::Simple(EdgeRatio::Count(1)),
        );
        config
            .generation
            .associations
            .edges
            .insert("NONEXISTENT_EDGE".to_string(), fake_variants);
        let err = Generator::new(ontology, config).unwrap_err();
        assert!(
            err.to_string().contains("associations:")
                && err.to_string().contains("NONEXISTENT_EDGE")
        );
    }

    #[test]
    fn test_validation_rejects_unknown_association_node() {
        let ontology = Ontology::load_embedded().unwrap();
        let (mut config, _) = test_config_and_ontology();
        let mut fake_variants = HashMap::new();
        fake_variants.insert(
            "FakeNode -> User".to_string(),
            AssociationEdgeValue::Simple(EdgeRatio::Count(1)),
        );
        config
            .generation
            .associations
            .edges
            .insert("AUTHORED".to_string(), fake_variants);
        let err = Generator::new(ontology, config).unwrap_err();
        assert!(
            err.to_string().contains("associations.AUTHORED:")
                && err.to_string().contains("FakeNode")
        );
    }

    #[test]
    fn test_validation_rejects_zero_organizations() {
        let ontology = Ontology::load_embedded().unwrap();
        let mut config = Config::default();
        config.generation.fake_data_path = fake_data_path();
        config.generation.organizations = 0;
        let err = Generator::new(ontology, config).unwrap_err();
        assert!(err.to_string().contains("organizations must be > 0"));
    }

    #[test]
    fn test_validation_rejects_zero_batch_size() {
        let ontology = Ontology::load_embedded().unwrap();
        let mut config = Config::default();
        config.generation.fake_data_path = fake_data_path();
        config.generation.batch_size = 0;
        let err = Generator::new(ontology, config).unwrap_err();
        assert!(err.to_string().contains("batch_size must be > 0"));
    }

    #[test]
    fn test_validation_rejects_unknown_relationship_edge() {
        let ontology = Ontology::load_embedded().unwrap();
        let mut config = Config::default();
        config.generation.fake_data_path = fake_data_path();
        config.generation.roots.insert("User".to_string(), 1);
        config.generation.roots.insert("Group".to_string(), 1);
        let mut fake_variants = HashMap::new();
        fake_variants.insert("Group -> Project".to_string(), EdgeRatio::Count(1));
        config
            .generation
            .relationships
            .edges
            .insert("BOGUS_EDGE".to_string(), fake_variants);
        let err = Generator::new(ontology, config).unwrap_err();
        assert!(
            err.to_string().contains("relationships:") && err.to_string().contains("BOGUS_EDGE")
        );
    }

    #[test]
    fn test_validation_rejects_unknown_relationship_node() {
        let ontology = Ontology::load_embedded().unwrap();
        let mut config = Config::default();
        config.generation.fake_data_path = fake_data_path();
        config.generation.roots.insert("Group".to_string(), 1);
        let mut variants = HashMap::new();
        variants.insert("Group -> FakeChild".to_string(), EdgeRatio::Count(1));
        config
            .generation
            .relationships
            .edges
            .insert("CONTAINS".to_string(), variants);
        let err = Generator::new(ontology, config).unwrap_err();
        assert!(
            err.to_string().contains("relationships.CONTAINS:")
                && err.to_string().contains("FakeChild")
        );
    }

    #[test]
    fn test_validation_rejects_invalid_edge_order_by_column() {
        let ontology = Ontology::load_embedded().unwrap();
        let mut config = Config::default();
        config.generation.fake_data_path = fake_data_path();
        config.clickhouse.schema.edge_order_by =
            vec!["traversal_path".to_string(), "bogus_column".to_string()];
        let err = Generator::new(ontology, config).unwrap_err();
        assert!(
            err.to_string().contains("edge_order_by") && err.to_string().contains("bogus_column")
        );
    }

    #[test]
    fn test_validation_rejects_invalid_edge_primary_key_column() {
        let ontology = Ontology::load_embedded().unwrap();
        let mut config = Config::default();
        config.generation.fake_data_path = fake_data_path();
        config.clickhouse.schema.edge_primary_key = vec!["not_a_column".to_string()];
        let err = Generator::new(ontology, config).unwrap_err();
        assert!(
            err.to_string().contains("edge_primary_key")
                && err.to_string().contains("not_a_column")
        );
    }

    #[test]
    fn test_validation_rejects_invalid_node_order_by_column() {
        let ontology = Ontology::load_embedded().unwrap();
        let mut config = Config::default();
        config.generation.fake_data_path = fake_data_path();
        config.clickhouse.schema.node_order_by =
            vec!["traversal_path".to_string(), "username".to_string()];
        let err = Generator::new(ontology, config).unwrap_err();
        assert!(err.to_string().contains("node_order_by") && err.to_string().contains("username"));
    }

    #[test]
    fn test_validation_rejects_invalid_node_primary_key_column() {
        let ontology = Ontology::load_embedded().unwrap();
        let mut config = Config::default();
        config.generation.fake_data_path = fake_data_path();
        config.clickhouse.schema.node_primary_key = vec!["nonexistent".to_string()];
        let err = Generator::new(ontology, config).unwrap_err();
        assert!(
            err.to_string().contains("node_primary_key") && err.to_string().contains("nonexistent")
        );
    }

    #[test]
    fn test_validation_accepts_valid_schema_columns() {
        let ontology = Ontology::load_embedded().unwrap();
        let mut config = Config::default();
        config.generation.fake_data_path = fake_data_path();
        config.clickhouse.schema.node_order_by =
            vec!["traversal_path".to_string(), "id".to_string()];
        config.clickhouse.schema.node_primary_key =
            vec!["traversal_path".to_string(), "id".to_string()];
        config.clickhouse.schema.edge_order_by = vec![
            "traversal_path".into(),
            "source_id".into(),
            "source_kind".into(),
            "target_id".into(),
            "target_kind".into(),
        ];
        config.clickhouse.schema.edge_primary_key = vec![
            "traversal_path".into(),
            "source_id".into(),
            "source_kind".into(),
            "target_id".into(),
            "target_kind".into(),
        ];
        Generator::new(ontology, config).unwrap();
    }

    #[test]
    fn test_validation_rejects_invalid_index_expression() {
        let ontology = Ontology::load_embedded().unwrap();
        let mut config = Config::default();
        config.generation.fake_data_path = fake_data_path();
        config.clickhouse.schema.indexes = vec![crate::synth::config::IndexConfig {
            name: "idx_bad".into(),
            table: "edges".into(),
            expression: "nonexistent_col".into(),
            index_type: "bloom_filter".into(),
            granularity: 4,
        }];
        let err = Generator::new(ontology, config).unwrap_err();
        assert!(
            err.to_string().contains("indexes[idx_bad]")
                && err.to_string().contains("nonexistent_col")
        );
    }

    #[test]
    fn test_validation_rejects_invalid_index_table() {
        let ontology = Ontology::load_embedded().unwrap();
        let mut config = Config::default();
        config.generation.fake_data_path = fake_data_path();
        config.clickhouse.schema.indexes = vec![crate::synth::config::IndexConfig {
            name: "idx_bad".into(),
            table: "fake_table".into(),
            expression: "id".into(),
            index_type: "minmax".into(),
            granularity: 4,
        }];
        let err = Generator::new(ontology, config).unwrap_err();
        assert!(
            err.to_string().contains("unknown table") && err.to_string().contains("fake_table")
        );
    }

    #[test]
    fn test_validation_accepts_valid_index_on_edges() {
        let ontology = Ontology::load_embedded().unwrap();
        let mut config = Config::default();
        config.generation.fake_data_path = fake_data_path();
        config.clickhouse.schema.indexes = vec![crate::synth::config::IndexConfig {
            name: "idx_rel".into(),
            table: "edges".into(),
            expression: "relationship_kind".into(),
            index_type: "bloom_filter".into(),
            granularity: 4,
        }];
        Generator::new(ontology, config).unwrap();
    }

    #[test]
    fn test_validation_rejects_invalid_projection_column() {
        let ontology = Ontology::load_embedded().unwrap();
        let mut config = Config::default();
        config.generation.fake_data_path = fake_data_path();
        config.clickhouse.schema.projections = vec![crate::synth::config::ProjectionConfig {
            name: "proj_bad".into(),
            table: "edges".into(),
            columns: vec!["source_kind".into(), "fake_col".into()],
            order_by: vec!["target_id".into()],
        }];
        let err = Generator::new(ontology, config).unwrap_err();
        assert!(
            err.to_string().contains("projections[proj_bad]")
                && err.to_string().contains("fake_col")
        );
    }

    #[test]
    fn test_validation_rejects_invalid_projection_order_by() {
        let ontology = Ontology::load_embedded().unwrap();
        let mut config = Config::default();
        config.generation.fake_data_path = fake_data_path();
        config.clickhouse.schema.projections = vec![crate::synth::config::ProjectionConfig {
            name: "proj_bad".into(),
            table: "edges".into(),
            columns: vec!["source_kind".into()],
            order_by: vec!["bad_order_col".into()],
        }];
        let err = Generator::new(ontology, config).unwrap_err();
        assert!(
            err.to_string().contains("projections[proj_bad]")
                && err.to_string().contains("bad_order_col")
        );
    }

    #[test]
    fn test_validation_accepts_valid_projection() {
        let ontology = Ontology::load_embedded().unwrap();
        let mut config = Config::default();
        config.generation.fake_data_path = fake_data_path();
        config.clickhouse.schema.projections = vec![crate::synth::config::ProjectionConfig {
            name: "by_target".into(),
            table: "edges".into(),
            columns: vec![
                "source_kind".into(),
                "source_id".into(),
                "relationship_kind".into(),
                "target_kind".into(),
                "target_id".into(),
            ],
            order_by: vec![
                "target_id".into(),
                "target_kind".into(),
                "relationship_kind".into(),
            ],
        }];
        Generator::new(ontology, config).unwrap();
    }

    #[test]
    fn test_validation_accepts_yaml_configs() {
        let ontology = Ontology::load_embedded().unwrap();

        let mut config = Config::load(crate::synth::fixture_path("simulator.yaml")).unwrap();
        config.generation.fake_data_path = fake_data_path();
        Generator::new(ontology.clone(), config).unwrap();

        let mut config_slim =
            Config::load(crate::synth::fixture_path("simulator-slim.yaml")).unwrap();
        config_slim.generation.fake_data_path = fake_data_path();
        Generator::new(ontology, config_slim).unwrap();
    }
}
