//! Data generation from ontology definitions.

mod batch;
mod fake_data;
mod traversal;

pub use batch::BatchBuilder;
pub use fake_data::FakeValueGenerator;
pub use traversal::TraversalIdGenerator;

use crate::arrow_schema::ToArrowSchema;
use crate::config::Config;
use anyhow::Result;
use arrow::record_batch::RecordBatch;
use fake::rand::Rng;
use fake::rand::seq::SliceRandom;
use ontology::{EdgeEntity, NodeEntity, Ontology};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

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

/// Generates fake data from ontology definitions.
pub struct Generator {
    ontology: Ontology,
    config: Config,
    next_id: Arc<AtomicI64>,
    traversal_ids: HashMap<u32, TraversalIdGenerator>,
}

impl Generator {
    pub fn new(ontology: Ontology, config: Config) -> Self {
        let mut traversal_ids = HashMap::new();
        for org_id in 1..=config.generation.organizations {
            let traversal_gen = TraversalIdGenerator::new(
                org_id,
                config.generation.traversal.ids_per_org,
                config.generation.traversal.max_depth,
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

    /// Generate data for a single organization.
    pub fn generate_organization(&self, org_id: u32) -> Result<OrganizationData> {
        let mut data = OrganizationData::default();
        let mut id_map: HashMap<String, Vec<i64>> = HashMap::new();
        let traversal_gen = self
            .traversal_ids
            .get(&org_id)
            .expect("traversal IDs exist");

        for node in self.ontology.nodes() {
            let count = self.config.node_count(&node.name);
            if count > 0 {
                let (batches, ids) =
                    self.generate_node_batches(node, org_id, traversal_gen, count)?;
                data.nodes.insert(node.name.clone(), batches);
                id_map.insert(node.name.clone(), ids);
            }
        }

        for edge in self.ontology.edges() {
            self.generate_edges_for_type(edge, &id_map, &mut data.edges);
        }

        Ok(data)
    }

    fn generate_node_batches(
        &self,
        node: &NodeEntity,
        org_id: u32,
        traversal_gen: &TraversalIdGenerator,
        count: usize,
    ) -> Result<(Vec<RecordBatch>, Vec<i64>)> {
        let schema = Arc::new(node.to_arrow_schema());
        let mut builder = BatchBuilder::new(node, schema, self.config.generation.batch_size);

        let start_id = self.next_id.fetch_add(count as i64, Ordering::SeqCst);
        let all_ids: Vec<i64> = (start_id..(start_id + count as i64)).collect();

        let mut rng = fake::rand::thread_rng();

        for &id in &all_ids {
            let traversal_id = traversal_gen.random(&mut rng).to_string();
            builder.add_row(org_id, traversal_id, id);
        }

        let batches = builder.finish();
        Ok((batches, all_ids))
    }

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
        let same_type = edge.source_kind == edge.target_kind;

        for &source_id in source_ids {
            let num_edges = rng.gen_range(1..=self.config.generation.edges.per_source);

            let targets: Vec<i64> = if same_type {
                // Sample with retry to avoid self-loops
                let mut selected = Vec::with_capacity(num_edges);
                let max_attempts = num_edges * 3; // Retry limit
                let mut attempts = 0;

                while selected.len() < num_edges && attempts < max_attempts && target_ids.len() > 1
                {
                    let idx = rng.gen_range(0..target_ids.len());
                    let candidate = target_ids[idx];

                    if candidate != source_id && !selected.contains(&candidate) {
                        selected.push(candidate);
                    }
                    attempts += 1;
                }
                selected
            } else {
                let num_edges = num_edges.min(target_ids.len());
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

    pub fn print_plan(&self) {
        let cfg = &self.config.generation;

        println!("Generation plan:");
        println!("  Organizations: {}", cfg.organizations);
        println!(
            "  Traversal IDs: {} per org (max depth {})",
            cfg.traversal.ids_per_org, cfg.traversal.max_depth
        );
        println!();

        println!("  Node types ({}):", self.ontology.node_count());
        for node in self.ontology.nodes() {
            let count = self.config.node_count(&node.name);
            let total = count * cfg.organizations as usize;
            println!(
                "    {}: {} per org = {} total ({} fields)",
                node.name, count, total, node.fields.len()
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
