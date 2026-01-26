//! Data generation from ontology definitions.
//!
//! This module generates fake data entirely from ontology definitions:
//! - Node types and their fields come from `ontology.nodes()`
//! - Edge types and their source/target kinds come from `ontology.edges()`
//! - No hardcoded entity names or relationships

mod batch;
mod fake_data;
mod traversal;

pub use batch::BatchBuilder;
pub use fake_data::FakeValueGenerator;
pub use traversal::TraversalIdGenerator;

use crate::arrow_schema::ToArrowSchema;
use crate::clickhouse::ClickHouseWriter;
use crate::config::Config;
use anyhow::Result;
use arrow::record_batch::RecordBatch;
use fake::rand::Rng;
use fake::rand::seq::SliceRandom;
use indicatif::{ProgressBar, ProgressStyle};
use ontology::{EdgeEntity, NodeEntity, Ontology};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

/// Edge data to be written to ClickHouse.
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

/// Main generator that produces fake data from ontology definitions.
///
/// This generator is fully ontology-driven:
/// - Iterates `ontology.nodes()` to generate all node types
/// - Iterates `ontology.edges()` to generate all edge types
/// - No hardcoded entity names
pub struct Generator {
    ontology: Ontology,
    config: Config,
    next_id: Arc<AtomicI64>,
    /// Traversal ID generators per organization.
    traversal_ids: HashMap<u32, TraversalIdGenerator>,
}

impl Generator {
    /// Create a new generator.
    pub fn new(ontology: Ontology, config: Config) -> Self {
        // Pre-generate traversal IDs for each organization
        let mut traversal_ids = HashMap::new();
        for org_id in 1..=config.num_organizations {
            let traversal_gen = TraversalIdGenerator::new(
                org_id,
                config.traversal_ids_per_org,
                config.max_traversal_depth,
            );
            traversal_ids.insert(org_id, traversal_gen);
        }

        Self {
            ontology,
            config,
            next_id: Arc::new(AtomicI64::new(1)),
            traversal_ids,
        }
    }

    /// Generate a unique ID.
    pub fn next_id(&self) -> i64 {
        self.next_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Run the full generation and import pipeline.
    pub async fn run(&self) -> Result<()> {
        let writer = ClickHouseWriter::new(&self.config.clickhouse_url);

        // Create schemas
        println!("Creating ClickHouse schemas...");
        writer.create_schemas(&self.ontology).await?;

        // Generate data per organization
        let pb = ProgressBar::new(self.config.num_organizations as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} organizations ({msg})",
                )
                .unwrap()
                .progress_chars("##-"),
        );

        for org_id in 1..=self.config.num_organizations {
            pb.set_message(format!("generating org {}", org_id));

            let org_data = self.generate_organization(org_id)?;

            pb.set_message(format!("writing org {} to ClickHouse", org_id));
            writer
                .write_organization_data(&self.ontology, &org_data)
                .await?;

            pb.inc(1);
        }

        pb.finish_with_message("complete");

        // Print statistics
        writer.print_statistics(&self.ontology).await?;

        Ok(())
    }

    /// Generate all data for a single organization.
    ///
    /// This is fully ontology-driven:
    /// 1. Generate nodes for each type in `ontology.nodes()`
    /// 2. Generate edges for each type in `ontology.edges()`
    pub fn generate_organization(&self, org_id: u32) -> Result<OrganizationData> {
        let mut data = OrganizationData::default();
        let mut id_map: HashMap<String, Vec<i64>> = HashMap::new();
        let traversal_gen = self
            .traversal_ids
            .get(&org_id)
            .expect("traversal IDs exist");

        // Phase 1: Generate all nodes from ontology
        for node in self.ontology.nodes() {
            let count = self.config.count_for(&node.name);
            let (batches, ids) = self.generate_node_batches(node, org_id, traversal_gen, count)?;
            data.nodes.insert(node.name.clone(), batches);
            id_map.insert(node.name.clone(), ids);
        }

        // Phase 2: Generate all edges from ontology
        for edge in self.ontology.edges() {
            self.generate_edges_for_type(edge, &id_map, &mut data.edges);
        }

        Ok(data)
    }

    /// Generate batches for a node type.
    fn generate_node_batches(
        &self,
        node: &NodeEntity,
        org_id: u32,
        traversal_gen: &TraversalIdGenerator,
        count: usize,
    ) -> Result<(Vec<RecordBatch>, Vec<i64>)> {
        let schema = Arc::new(node.to_arrow_schema());
        let mut builder = BatchBuilder::new(node, schema, self.config.batch_size);
        let mut all_ids = Vec::with_capacity(count);
        let mut rng = fake::rand::thread_rng();

        for _ in 0..count {
            let id = self.next_id();
            all_ids.push(id);
            let traversal_id = traversal_gen.random(&mut rng).to_string();
            builder.add_row(org_id, traversal_id, id);
        }

        let batches = builder.finish();
        Ok((batches, all_ids))
    }

    /// Generate edges for a specific edge type from the ontology.
    ///
    /// Uses the edge's `source_kind` and `target_kind` to find the right nodes.
    fn generate_edges_for_type(
        &self,
        edge: &EdgeEntity,
        id_map: &HashMap<String, Vec<i64>>,
        edges: &mut Vec<EdgeRecord>,
    ) {
        let source_ids = match id_map.get(&edge.source_kind) {
            Some(ids) if !ids.is_empty() => ids,
            _ => return, // No source nodes of this type
        };

        let target_ids = match id_map.get(&edge.target_kind) {
            Some(ids) if !ids.is_empty() => ids,
            _ => return, // No target nodes of this type
        };

        let mut rng = fake::rand::thread_rng();

        // For each source node, create edges to random target nodes
        for &source_id in source_ids {
            // Determine how many edges to create (1 to edges_per_source)
            let num_edges = rng.gen_range(1..=self.config.edges_per_source);
            let num_edges = num_edges.min(target_ids.len());

            // Pick random targets (avoid self-loops if source_kind == target_kind)
            let targets: Vec<i64> = if edge.source_kind == edge.target_kind {
                // Same type: avoid self-loops
                target_ids
                    .iter()
                    .filter(|&&id| id != source_id)
                    .copied()
                    .collect::<Vec<_>>()
                    .choose_multiple(&mut rng, num_edges)
                    .copied()
                    .collect()
            } else {
                // Different types: pick any
                target_ids
                    .choose_multiple(&mut rng, num_edges)
                    .copied()
                    .collect()
            };

            for target_id in targets {
                edges.push(EdgeRecord {
                    relationship_kind: edge.relationship_kind.clone(),
                    source: source_id,
                    source_kind: edge.source_kind.clone(),
                    target: target_id,
                    target_kind: edge.target_kind.clone(),
                });
            }
        }
    }

    /// Print generation plan based on ontology.
    pub fn print_plan(&self) {
        println!("Generation plan (from ontology):");
        println!("  Organizations: {}", self.config.num_organizations);
        println!(
            "  Traversal IDs: {} per org (max depth {})",
            self.config.traversal_ids_per_org, self.config.max_traversal_depth
        );
        println!();

        println!("  Node types ({}):", self.ontology.node_count());
        for node in self.ontology.nodes() {
            let count = self.config.count_for(&node.name);
            let total = count * self.config.num_organizations as usize;
            println!(
                "    {}: {} per org = {} total ({} fields)",
                node.name,
                count,
                total,
                node.fields.len()
            );
        }
        println!();

        println!("  Edge types ({}):", self.ontology.edge_count());
        for edge in self.ontology.edges() {
            println!(
                "    {}: {} -> {}",
                edge.relationship_kind, edge.source_kind, edge.target_kind
            );
        }
        println!();
    }
}
