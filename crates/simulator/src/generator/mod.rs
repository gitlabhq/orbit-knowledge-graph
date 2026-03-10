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
pub use fake_data::{FakeDataPools, FakeValueGenerator};
pub use traversal::{EntityContext, EntityRegistry, TraversalPathGenerator};

use crate::arrow_schema::ToArrowSchema;
use crate::config::{Config, EdgeRatio};
use crate::parquet::StreamingEdgeWriter;
use anyhow::Result;
use arrow::record_batch::RecordBatch;
use ontology::{NodeEntity, Ontology};
use rand::Rng;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

/// Interned string type for edge records to avoid millions of small allocations.
pub type IStr = Arc<str>;

/// Traversal path for association edges (AUTHORED, MEMBER_OF, etc.).
///
/// Association edges link entities across namespace boundaries and don't
/// belong to a specific namespace path. This sentinel value tells the
/// query engine to skip traversal-path-based security filtering for
/// these edges (they are authorized through the source/target entities
/// themselves).
use crate::constants::ASSOCIATION_TRAVERSAL_PATH;

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
    /// Interned fake data pools (program-lifetime, leaked).
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
        use crate::config::FakeDataConfig;

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

    /// Validate all config references against the ontology.
    fn validate_config(config: &Config, ontology: &Ontology) -> Result<()> {
        let generation = &config.generation;

        // --- Numeric sanity checks ---

        if generation.organizations == 0 {
            anyhow::bail!("generation.organizations must be > 0");
        }
        if generation.batch_size == 0 {
            anyhow::bail!("generation.batch_size must be > 0");
        }

        // --- Ontology entity validation ---

        // Validate root entity types
        for node_type in generation.roots.keys() {
            DependencyGraph::validate_node(ontology, node_type)
                .map_err(|e| anyhow::anyhow!("roots: {}", e))?;
        }

        // Validate namespace entity
        DependencyGraph::validate_node(ontology, &generation.namespace_entity)
            .map_err(|e| anyhow::anyhow!("namespace_entity: {}", e))?;

        // Validate relationship edge types + source/target variants
        for (edge_type, variants) in &generation.relationships.edges {
            for variant_key in variants.keys() {
                let (source, target) = crate::config::RelationshipConfig::parse_variant_key(
                    variant_key,
                )
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

        // Validate association edge types + source/target variants
        for (edge_type, variants) in &generation.associations.edges {
            for variant_key in variants.keys() {
                let (source, target) = crate::config::RelationshipConfig::parse_variant_key(
                    variant_key,
                )
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

        // --- Schema column validation ---

        Self::validate_schema_config(&config.clickhouse.schema, ontology)?;

        Ok(())
    }

    /// Validate ClickHouse schema config columns against ontology definitions.
    fn validate_schema_config(
        schema: &crate::config::SchemaConfig,
        ontology: &Ontology,
    ) -> Result<()> {
        use ontology::constants::EDGE_RESERVED_COLUMNS;

        // Columns that exist on every node table: system column + reserved columns
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

        // Validate node ORDER BY columns
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

        // Validate node PRIMARY KEY columns
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

        // Validate edge ORDER BY columns
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

        // Validate edge PRIMARY KEY columns
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

        // Validate indexes
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

        // Validate projections
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

    /// Resolve valid column names for a table pattern used in indexes/projections.
    ///
    /// - `"edges"` → edge reserved columns
    /// - `"*"` → universal node columns (traversal_path + id)
    /// - specific table name → must be a known ontology table, returns all its columns
    fn resolve_valid_columns<'a>(
        table_pattern: &str,
        ontology: &'a Ontology,
        universal_node_columns: &std::collections::HashSet<&'a str>,
        edge_columns: &std::collections::HashSet<&'a str>,
    ) -> Result<std::collections::HashSet<&'a str>> {
        use crate::constants::{TABLE_PATTERN_ALL_NODES, TABLE_PATTERN_EDGES};
        match table_pattern {
            TABLE_PATTERN_EDGES => Ok(edge_columns.clone()),
            TABLE_PATTERN_ALL_NODES => Ok(universal_node_columns.clone()),
            specific => {
                // Check if it's a known node table
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
            // Virtual nodes (e.g., "Group@1") resolve to real entity types for ontology lookup
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

        // Compact registry, merging epsilon entries into real type names
        registry.compact_with_aliases(self.dependency_graph.epsilon_to_real());

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

        // Compact registry, merging epsilon entries into real type names
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
                let trav = format!("{}/{}/", org_id, ns_id);
                (ns_id, trav)
            } else {
                // Non-namespace root entities (like Users) get org-level paths.
                // Note: The query engine skips traversal path security filters
                // for Users since their visibility is determined through
                // MEMBER_OF relationships to Groups, not path hierarchy.
                let eid = self.next_entity_id();
                (eid, format!("{}/", org_id))
            };

            builder.add_row(traversal_path.clone(), entity_id);
            let ctx = EntityContext::new(entity_id, traversal_path);
            registry.add(&node.name, ctx);
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
        // Only store full context for parent types; leaves only need IDs for associations.
        // Check against registry_key (possibly epsilon) since the dependency graph
        // references epsilon names as parents.
        let is_parent_type = self.dependency_graph.is_parent_type(registry_key);

        for parent_edge in parent_edges {
            // Clone parent IDs and paths to avoid borrow conflict with registry.add()
            let parents: Vec<_> = match registry.get(&parent_edge.parent_kind) {
                Some(p) if !p.is_empty() => {
                    p.iter().map(|e| (e.id, e.traversal_path.clone())).collect()
                }
                _ => continue,
            };

            // Intern strings using real type names (resolve epsilon) for edge records
            let rel_kind = self.intern(&parent_edge.edge_type);
            let parent_real_type = self.dependency_graph.resolve_type(&parent_edge.parent_kind);
            let parent_kind_str = self.intern(parent_real_type);
            let node_name_str = self.intern(&node.name);

            for (parent_id, parent_path) in &parents {
                let child_count = parent_edge.ratio.sample_with_variance(rng);

                for _ in 0..child_count {
                    let (entity_id, traversal_path) = if is_namespace_entity {
                        let ns_id = registry.next_namespace_id();
                        let trav = format!("{}{}/", parent_path, ns_id);
                        (ns_id, trav)
                    } else {
                        let eid = self.next_entity_id();
                        (eid, parent_path.clone())
                    };

                    builder.add_row(traversal_path.clone(), entity_id);

                    // Store under registry_key (possibly epsilon) so entities at
                    // different depth levels are kept separate during generation
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
                let edge_count = ratio.sample(rng);

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

    // ==================== Streaming variants ====================
    // These write edges directly to StreamingEdgeWriter instead of accumulating

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
                let trav = format!("{}/{}/", org_id, ns_id);
                (ns_id, trav)
            } else {
                let eid = self.next_entity_id();
                (eid, format!("{}/", org_id))
            };

            builder.add_row(traversal_path.clone(), entity_id);
            let ctx = EntityContext::new(entity_id, traversal_path);
            registry.add(&node.name, ctx);
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
                        let trav = format!("{}{}/", parent_path, ns_id);
                        (ns_id, trav)
                    } else {
                        let eid = self.next_entity_id();
                        (eid, parent_path.clone())
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
                let edge_count = ratio.sample(rng);

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

        // Show recursive hierarchy expansion for self-referential edges
        for (edge_type, variants) in &cfg.relationships.edges {
            for (variant_key, ratio) in variants {
                if let EdgeRatio::Recursive { count, max_depth } = ratio
                    && let Some((source, _)) =
                        crate::config::RelationshipConfig::parse_variant_key(variant_key)
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
                        crate::config::IterationDirection::Target => "per target",
                        crate::config::IterationDirection::Source => "per source",
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        AssociationConfig, AssociationEdgeValue, Config, EdgeRatio, GenerationConfig,
        RelationshipConfig,
    };
    use arrow::array::Array;
    use std::collections::{HashMap, HashSet};

    /// Extract all entity IDs from a set of batches by looking up the "id" column by name.
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

    /// Build a minimal config + ontology for generator testing.
    /// Uses the embedded ontology so edge validation passes.
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

        // Should have produced nodes for each configured type
        assert!(data.nodes.contains_key("User"), "Should generate Users");
        assert!(data.nodes.contains_key("Group"), "Should generate Groups");
        assert!(
            data.nodes.contains_key("Project"),
            "Should generate Projects"
        );
        assert!(
            data.nodes.contains_key("MergeRequest"),
            "Should generate MergeRequests"
        );

        // Should have edges
        assert!(!data.edges.is_empty(), "Should generate edges");

        // Check edge types
        let edge_types: HashSet<&str> = data
            .edges
            .iter()
            .map(|e| e.relationship_kind.as_ref())
            .collect();
        assert!(
            edge_types.contains("CONTAINS"),
            "Should have CONTAINS edges"
        );
        assert!(
            edge_types.contains("IN_PROJECT"),
            "Should have IN_PROJECT edges"
        );
        assert!(
            edge_types.contains("AUTHORED"),
            "Should have AUTHORED association edges"
        );
    }

    #[test]
    fn test_generator_unique_entity_ids_per_type() {
        let (config, ontology) = test_config_and_ontology();
        let generator = Generator::new(ontology, config).unwrap();
        let data = generator.generate_organization(1).unwrap();

        // IDs must be unique within each node type.
        // Note: namespace entities (Groups) use a per-org counter while
        // non-namespace entities use a global counter, so IDs can
        // legitimately collide across types (e.g., Group 2 and User 2).
        for (node_type, batches) in &data.nodes {
            let ids = extract_ids(batches);
            let unique: HashSet<i64> = ids.iter().copied().collect();
            assert_eq!(
                ids.len(),
                unique.len(),
                "Duplicate IDs found in {}: {} total, {} unique",
                node_type,
                ids.len(),
                unique.len()
            );
        }
    }

    #[test]
    fn test_generator_edge_references_valid_ids() {
        let (config, ontology) = test_config_and_ontology();
        let generator = Generator::new(ontology, config).unwrap();
        let data = generator.generate_organization(1).unwrap();

        let all_ids: HashSet<i64> = data
            .nodes
            .values()
            .flat_map(|batches| extract_ids(batches))
            .collect();

        for edge in &data.edges {
            assert!(
                all_ids.contains(&edge.source),
                "Edge {:?} source {} not found in generated entities",
                edge.relationship_kind,
                edge.source
            );
            assert!(
                all_ids.contains(&edge.target),
                "Edge {:?} target {} not found in generated entities",
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

        let authored_edges: Vec<_> = data
            .edges
            .iter()
            .filter(|e| e.relationship_kind.as_ref() == "AUTHORED")
            .collect();

        assert!(!authored_edges.is_empty());
        for edge in &authored_edges {
            assert_eq!(
                edge.traversal_path.as_ref(),
                ASSOCIATION_TRAVERSAL_PATH,
                "Association edges should use the sentinel traversal path"
            );
        }
    }

    #[test]
    fn test_generator_namespace_entity_extends_traversal_paths() {
        let (config, ontology) = test_config_and_ontology();
        let generator = Generator::new(ontology, config).unwrap();
        let data = generator.generate_organization(1).unwrap();

        // Group traversal paths should have depth > 1 (org/ns/)
        let group_batches = &data.nodes["Group"];
        for batch in group_batches {
            let tp_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();
            for i in 0..tp_col.len() {
                let path = tp_col.value(i);
                let depth = path.split('/').filter(|s| !s.is_empty()).count();
                assert!(
                    depth >= 2,
                    "Group traversal path '{}' should have depth >= 2",
                    path
                );
            }
        }

        // Non-namespace entities (e.g., User) should have simpler paths
        let user_batches = &data.nodes["User"];
        for batch in user_batches {
            let tp_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();
            for i in 0..tp_col.len() {
                let path = tp_col.value(i);
                let depth = path.split('/').filter(|s| !s.is_empty()).count();
                assert_eq!(
                    depth, 1,
                    "User traversal path '{}' should have depth 1 (org-level)",
                    path
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

        // Find CONTAINS Group→Group edges (the recursive hierarchy edges)
        let hierarchy_edges: Vec<_> = data
            .edges
            .iter()
            .filter(|e| {
                e.relationship_kind.as_ref() == "CONTAINS"
                    && e.source_kind.as_ref() == ns_entity.as_str()
                    && e.target_kind.as_ref() == ns_entity.as_str()
            })
            .collect();

        // With 2 root groups and { count: 1, max_depth: 1 }:
        // sample_with_variance on count=1 gives range [1, 2], so expect 2-4 edges
        assert!(
            hierarchy_edges.len() >= 2 && hierarchy_edges.len() <= 4,
            "Expected 2-4 CONTAINS {ns}->{ns} edges (2 roots x ~1 child), got {}",
            hierarchy_edges.len(),
            ns = ns_entity
        );

        // All edges should have valid structure
        for edge in &hierarchy_edges {
            assert_eq!(edge.source_kind.as_ref(), ns_entity.as_str());
            assert_eq!(edge.target_kind.as_ref(), ns_entity.as_str());
            assert_ne!(edge.source, edge.target, "Edge should not self-loop");
        }
    }

    #[test]
    fn test_generator_global_entity_counter_across_orgs() {
        let (config, ontology) = test_config_and_ontology();
        let generator = Generator::new(ontology, config).unwrap();

        let data1 = generator.generate_organization(1).unwrap();
        let data2 = generator.generate_organization(2).unwrap();

        // Collect non-namespace entity IDs from both orgs.
        // Namespace entities (Groups) use per-org counters so they can overlap.
        // Non-namespace entities use the global counter so they must be unique.
        let collect_non_ns_ids = |data: &OrganizationData, ns_entity: &str| -> HashSet<i64> {
            data.nodes
                .iter()
                .filter(|(k, _)| k.as_str() != ns_entity)
                .flat_map(|(_, batches)| extract_ids(batches))
                .collect()
        };

        let ids1 = collect_non_ns_ids(&data1, "Group");
        let ids2 = collect_non_ns_ids(&data2, "Group");

        assert!(!ids1.is_empty(), "Org 1 should have non-namespace entities");
        assert!(!ids2.is_empty(), "Org 2 should have non-namespace entities");

        let overlap: Vec<_> = ids1.intersection(&ids2).collect();
        assert!(
            overlap.is_empty(),
            "Non-namespace entity IDs should not overlap across orgs: {:?}",
            overlap
        );
    }

    // --- Validation tests ---

    #[test]
    fn test_validation_rejects_unknown_root_entity() {
        let ontology = Ontology::load_embedded().unwrap();
        let mut config = Config::default();
        config.generation.roots.insert("FakeEntity".to_string(), 10);

        let err = Generator::new(ontology, config).unwrap_err();
        assert!(
            err.to_string().contains("roots:") && err.to_string().contains("FakeEntity"),
            "Expected root validation error, got: {}",
            err
        );
    }

    #[test]
    fn test_validation_rejects_unknown_namespace_entity() {
        let ontology = Ontology::load_embedded().unwrap();
        let mut config = Config::default();
        config.generation.namespace_entity = "FakeNamespace".to_string();
        config.generation.roots.insert("User".to_string(), 1);

        let err = Generator::new(ontology, config).unwrap_err();
        assert!(
            err.to_string().contains("namespace_entity:")
                && err.to_string().contains("FakeNamespace"),
            "Expected namespace_entity validation error, got: {}",
            err
        );
    }

    #[test]
    fn test_validation_rejects_self_referential_without_recursive() {
        let ontology = Ontology::load_embedded().unwrap();
        let mut config = Config::default();
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
                && err.to_string().contains("count, max_depth"),
            "Expected self-referential edge validation error, got: {}",
            err
        );
    }

    #[test]
    fn test_validation_rejects_recursive_on_non_self_referential() {
        let ontology = Ontology::load_embedded().unwrap();
        let mut config = Config::default();
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
        assert!(
            err.to_string().contains("only valid for self-referential"),
            "Expected non-self-referential Recursive validation error, got: {}",
            err
        );
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
                && err.to_string().contains("NONEXISTENT_EDGE"),
            "Expected association edge validation error, got: {}",
            err
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
                && err.to_string().contains("FakeNode"),
            "Expected association node validation error, got: {}",
            err
        );
    }

    #[test]
    fn test_validation_rejects_zero_organizations() {
        let ontology = Ontology::load_embedded().unwrap();
        let mut config = Config::default();
        config.generation.organizations = 0;

        let err = Generator::new(ontology, config).unwrap_err();
        assert!(
            err.to_string().contains("organizations must be > 0"),
            "Expected organizations validation error, got: {}",
            err
        );
    }

    #[test]
    fn test_validation_rejects_zero_batch_size() {
        let ontology = Ontology::load_embedded().unwrap();
        let mut config = Config::default();
        config.generation.batch_size = 0;

        let err = Generator::new(ontology, config).unwrap_err();
        assert!(
            err.to_string().contains("batch_size must be > 0"),
            "Expected batch_size validation error, got: {}",
            err
        );
    }

    #[test]
    fn test_validation_rejects_unknown_relationship_edge() {
        let ontology = Ontology::load_embedded().unwrap();
        let mut config = Config::default();
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
            err.to_string().contains("relationships:") && err.to_string().contains("BOGUS_EDGE"),
            "Expected relationship edge validation error, got: {}",
            err
        );
    }

    #[test]
    fn test_validation_rejects_unknown_relationship_node() {
        let ontology = Ontology::load_embedded().unwrap();
        let mut config = Config::default();
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
                && err.to_string().contains("FakeChild"),
            "Expected relationship node validation error, got: {}",
            err
        );
    }

    #[test]
    fn test_validation_rejects_invalid_edge_order_by_column() {
        let ontology = Ontology::load_embedded().unwrap();
        let mut config = Config::default();
        config.clickhouse.schema.edge_order_by =
            vec!["traversal_path".to_string(), "bogus_column".to_string()];

        let err = Generator::new(ontology, config).unwrap_err();
        assert!(
            err.to_string().contains("edge_order_by") && err.to_string().contains("bogus_column"),
            "Expected edge_order_by validation error, got: {}",
            err
        );
    }

    #[test]
    fn test_validation_rejects_invalid_edge_primary_key_column() {
        let ontology = Ontology::load_embedded().unwrap();
        let mut config = Config::default();
        config.clickhouse.schema.edge_primary_key = vec!["not_a_column".to_string()];

        let err = Generator::new(ontology, config).unwrap_err();
        assert!(
            err.to_string().contains("edge_primary_key")
                && err.to_string().contains("not_a_column"),
            "Expected edge_primary_key validation error, got: {}",
            err
        );
    }

    #[test]
    fn test_validation_rejects_invalid_node_order_by_column() {
        let ontology = Ontology::load_embedded().unwrap();
        let mut config = Config::default();
        config.clickhouse.schema.node_order_by =
            vec!["traversal_path".to_string(), "username".to_string()];

        let err = Generator::new(ontology, config).unwrap_err();
        assert!(
            err.to_string().contains("node_order_by") && err.to_string().contains("username"),
            "Expected node_order_by validation error, got: {}",
            err
        );
    }

    #[test]
    fn test_validation_rejects_invalid_node_primary_key_column() {
        let ontology = Ontology::load_embedded().unwrap();
        let mut config = Config::default();
        config.clickhouse.schema.node_primary_key = vec!["nonexistent".to_string()];

        let err = Generator::new(ontology, config).unwrap_err();
        assert!(
            err.to_string().contains("node_primary_key") && err.to_string().contains("nonexistent"),
            "Expected node_primary_key validation error, got: {}",
            err
        );
    }

    #[test]
    fn test_validation_accepts_valid_schema_columns() {
        let ontology = Ontology::load_embedded().unwrap();
        let mut config = Config::default();
        config.clickhouse.schema.node_order_by =
            vec!["traversal_path".to_string(), "id".to_string()];
        config.clickhouse.schema.node_primary_key =
            vec!["traversal_path".to_string(), "id".to_string()];
        config.clickhouse.schema.edge_order_by = vec![
            "traversal_path".to_string(),
            "source_id".to_string(),
            "source_kind".to_string(),
            "target_id".to_string(),
            "target_kind".to_string(),
        ];
        config.clickhouse.schema.edge_primary_key = vec![
            "traversal_path".to_string(),
            "source_id".to_string(),
            "source_kind".to_string(),
            "target_id".to_string(),
            "target_kind".to_string(),
        ];

        // Should not error
        Generator::new(ontology, config).unwrap();
    }

    #[test]
    fn test_validation_rejects_invalid_index_expression() {
        let ontology = Ontology::load_embedded().unwrap();
        let mut config = Config::default();
        config.clickhouse.schema.indexes = vec![crate::config::IndexConfig {
            name: "idx_bad".to_string(),
            table: "edges".to_string(),
            expression: "nonexistent_col".to_string(),
            index_type: "bloom_filter".to_string(),
            granularity: 4,
        }];

        let err = Generator::new(ontology, config).unwrap_err();
        assert!(
            err.to_string().contains("indexes[idx_bad]")
                && err.to_string().contains("nonexistent_col"),
            "Expected index expression validation error, got: {}",
            err
        );
    }

    #[test]
    fn test_validation_rejects_invalid_index_table() {
        let ontology = Ontology::load_embedded().unwrap();
        let mut config = Config::default();
        config.clickhouse.schema.indexes = vec![crate::config::IndexConfig {
            name: "idx_bad".to_string(),
            table: "fake_table".to_string(),
            expression: "id".to_string(),
            index_type: "minmax".to_string(),
            granularity: 4,
        }];

        let err = Generator::new(ontology, config).unwrap_err();
        assert!(
            err.to_string().contains("unknown table") && err.to_string().contains("fake_table"),
            "Expected index table validation error, got: {}",
            err
        );
    }

    #[test]
    fn test_validation_accepts_valid_index_on_edges() {
        let ontology = Ontology::load_embedded().unwrap();
        let mut config = Config::default();
        config.clickhouse.schema.indexes = vec![crate::config::IndexConfig {
            name: "idx_rel".to_string(),
            table: "edges".to_string(),
            expression: "relationship_kind".to_string(),
            index_type: "bloom_filter".to_string(),
            granularity: 4,
        }];

        Generator::new(ontology, config).unwrap();
    }

    #[test]
    fn test_validation_rejects_invalid_projection_column() {
        let ontology = Ontology::load_embedded().unwrap();
        let mut config = Config::default();
        config.clickhouse.schema.projections = vec![crate::config::ProjectionConfig {
            name: "proj_bad".to_string(),
            table: "edges".to_string(),
            columns: vec!["source_kind".to_string(), "fake_col".to_string()],
            order_by: vec!["target_id".to_string()],
        }];

        let err = Generator::new(ontology, config).unwrap_err();
        assert!(
            err.to_string().contains("projections[proj_bad]")
                && err.to_string().contains("fake_col"),
            "Expected projection column validation error, got: {}",
            err
        );
    }

    #[test]
    fn test_validation_rejects_invalid_projection_order_by() {
        let ontology = Ontology::load_embedded().unwrap();
        let mut config = Config::default();
        config.clickhouse.schema.projections = vec![crate::config::ProjectionConfig {
            name: "proj_bad".to_string(),
            table: "edges".to_string(),
            columns: vec!["source_kind".to_string()],
            order_by: vec!["bad_order_col".to_string()],
        }];

        let err = Generator::new(ontology, config).unwrap_err();
        assert!(
            err.to_string().contains("projections[proj_bad]")
                && err.to_string().contains("bad_order_col"),
            "Expected projection order_by validation error, got: {}",
            err
        );
    }

    #[test]
    fn test_validation_accepts_valid_projection() {
        let ontology = Ontology::load_embedded().unwrap();
        let mut config = Config::default();
        config.clickhouse.schema.projections = vec![crate::config::ProjectionConfig {
            name: "by_target".to_string(),
            table: "edges".to_string(),
            columns: vec![
                "source_kind".to_string(),
                "source_id".to_string(),
                "relationship_kind".to_string(),
                "target_kind".to_string(),
                "target_id".to_string(),
            ],
            order_by: vec![
                "target_id".to_string(),
                "target_kind".to_string(),
                "relationship_kind".to_string(),
            ],
        }];

        Generator::new(ontology, config).unwrap();
    }

    #[test]
    fn test_validation_accepts_yaml_configs() {
        let ontology = Ontology::load_embedded().unwrap();

        // Validate the actual simulator.yaml
        let config = Config::load("simulator.yaml").unwrap();
        Generator::new(ontology.clone(), config).unwrap();

        // Validate the actual simulator-slim.yaml
        let config_slim = Config::load("simulator-slim.yaml").unwrap();
        Generator::new(ontology, config_slim).unwrap();
    }
}
