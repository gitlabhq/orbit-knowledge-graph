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
use ontology::{EdgeEntity, NodeEntity, Ontology};
use rayon::prelude::*;
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

    /// Run the full generation and import pipeline (sequential).
    pub async fn run(&self) -> Result<()> {
        let writer = ClickHouseWriter::new(&self.config.clickhouse_url);

        // Create schemas
        println!("Creating ClickHouse schemas...");
        writer.create_schemas(&self.ontology).await?;

        // Generate data per organization
        println!(
            "\nGenerating data for {} organization(s)...",
            self.config.num_organizations
        );
        let overall_start = std::time::Instant::now();

        let mut total_gen_time = 0.0;
        let mut total_write_time = 0.0;

        for org_id in 1..=self.config.num_organizations {
            println!("\n=== Organization {}/{} ===", org_id, self.config.num_organizations);

            // Generate all data first (no writes during generation)
            println!("  Generating nodes...");
            let gen_start = std::time::Instant::now();
            let org_data = self.generate_organization_with_logging(org_id, true)?;
            let gen_elapsed = gen_start.elapsed().as_secs_f64();
            total_gen_time += gen_elapsed;
            
            let node_count: usize = org_data.nodes.values()
                .map(|batches| batches.iter().map(|b| b.num_rows()).sum::<usize>())
                .sum();
            println!("  Generation complete: {} nodes + {} edges ({:.1}s)", 
                node_count, org_data.edges.len(), gen_elapsed);

            // Now write everything to ClickHouse
            println!("  Writing to ClickHouse...");
            let write_start = std::time::Instant::now();
            writer.write_organization_data(&self.ontology, &org_data).await?;
            let write_elapsed = write_start.elapsed().as_secs_f64();
            total_write_time += write_elapsed;
            
            println!("  Write complete ({:.1}s)", write_elapsed);
        }

        println!(
            "\n=== Summary ===\nTotal: gen:{:.1}s + write:{:.1}s = {:.1}s",
            total_gen_time,
            total_write_time,
            overall_start.elapsed().as_secs_f64()
        );

        // Print statistics
        writer.print_statistics(&self.ontology).await?;

        Ok(())
    }

    /// Run the full generation and import pipeline (parallel generation).
    pub async fn run_parallel(&self) -> Result<()> {
        let writer = ClickHouseWriter::new(&self.config.clickhouse_url);

        // Create schemas
        println!("Creating ClickHouse schemas...");
        writer.create_schemas(&self.ontology).await?;

        // Generate data per organization
        println!(
            "\nGenerating data for {} organization(s) in parallel...",
            self.config.num_organizations
        );
        let overall_start = std::time::Instant::now();

        // Generate all organizations in parallel
        println!("\n=== Parallel Generation Phase ===");
        let gen_start = std::time::Instant::now();
        
        let org_data_vec: Vec<_> = (1..=self.config.num_organizations)
            .into_par_iter()
            .map(|org_id| {
                let start = std::time::Instant::now();
                println!("  [Org {}] Starting generation...", org_id);
                
                let result = self.generate_organization(org_id);
                
                match &result {
                    Ok(data) => {
                        let node_count: usize = data.nodes.values()
                            .map(|batches| batches.iter().map(|b| b.num_rows()).sum::<usize>())
                            .sum();
                        println!(
                            "  [Org {}] ✓ Generated {} nodes + {} edges in {:.1}s",
                            org_id,
                            node_count,
                            data.edges.len(),
                            start.elapsed().as_secs_f64()
                        );
                    }
                    Err(e) => {
                        eprintln!("  [Org {}] ✗ Error: {}", org_id, e);
                    }
                }
                
                (org_id, result)
            })
            .collect();

        let gen_elapsed = gen_start.elapsed().as_secs_f64();
        println!("\nAll organizations generated in {:.1}s", gen_elapsed);

        // Write all organizations sequentially (ClickHouse client isn't Send)
        println!("\n=== Sequential Write Phase ===");
        let write_start = std::time::Instant::now();
        
        for (org_id, result) in org_data_vec {
            let org_data = result?;
            
            let start = std::time::Instant::now();
            println!("  [Org {}] Writing to ClickHouse...", org_id);
            
            writer
                .write_organization_data(&self.ontology, &org_data)
                .await?;
            
            println!("  [Org {}] ✓ Written in {:.1}s", org_id, start.elapsed().as_secs_f64());
        }

        let write_elapsed = write_start.elapsed().as_secs_f64();
        println!("\nAll organizations written in {:.1}s", write_elapsed);

        println!(
            "\n=== Summary ===\nTotal: gen:{:.1}s + write:{:.1}s = {:.1}s",
            gen_elapsed,
            write_elapsed,
            overall_start.elapsed().as_secs_f64()
        );

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
        self.generate_organization_with_logging(org_id, false)
    }

    /// Generate all data for a single organization (with optional verbose logging).
    fn generate_organization_with_logging(&self, org_id: u32, verbose: bool) -> Result<OrganizationData> {
        let mut data = OrganizationData::default();
        let mut id_map: HashMap<String, Vec<i64>> = HashMap::new();
        let traversal_gen = self
            .traversal_ids
            .get(&org_id)
            .expect("traversal IDs exist");

        // Phase 1: Generate all nodes from ontology
        for node in self.ontology.nodes() {
            let count = self.config.count_for(&node.name);
            if count > 0 {
                if verbose {
                    print!("    {} ({} nodes)... ", node.name, count);
                    std::io::Write::flush(&mut std::io::stdout()).ok();
                }
                
                let start = std::time::Instant::now();
                let (batches, ids) = self.generate_node_batches(node, org_id, traversal_gen, count)?;
                
                if verbose {
                    println!("✓ {:.1}s", start.elapsed().as_secs_f64());
                }
                
                data.nodes.insert(node.name.clone(), batches);
                id_map.insert(node.name.clone(), ids);
            }
        }

        // Phase 2: Generate all edges from ontology
        if verbose {
            println!("    Generating edges...");
        }
        
        let edge_start = std::time::Instant::now();
        for edge in self.ontology.edges() {
            if verbose {
                print!("      {} ({} -> {})... ", 
                    edge.relationship_kind, edge.source_kind, edge.target_kind);
                std::io::Write::flush(&mut std::io::stdout()).ok();
            }
            
            let edge_type_start = std::time::Instant::now();
            let edge_count_before = data.edges.len();
            self.generate_edges_for_type(edge, &id_map, &mut data.edges);
            let edges_added = data.edges.len() - edge_count_before;
            let edge_type_elapsed = edge_type_start.elapsed().as_secs_f64();
            
            if verbose {
                if edges_added > 0 {
                    println!("{} edges ({:.1}s, {:.0} edges/s)", 
                        edges_added, 
                        edge_type_elapsed,
                        edges_added as f64 / edge_type_elapsed.max(0.001));
                } else {
                    println!("0 edges (skipped)");
                }
            }
        }
        
        if verbose {
            println!("    Total edges: {} ({:.1}s)", data.edges.len(), edge_start.elapsed().as_secs_f64());
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
        
        // Pre-allocate IDs efficiently
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
        let same_type = edge.source_kind == edge.target_kind;

        // For each source node, create edges to random target nodes
        for &source_id in source_ids {
            // Determine how many edges to create (1 to edges_per_source)
            let num_edges = rng.gen_range(1..=self.config.edges_per_source);
            
            // Pick random targets
            let targets: Vec<i64> = if same_type {
                // Optimized self-loop avoidance: sample with retry instead of filtering
                let mut selected = Vec::with_capacity(num_edges);
                let max_attempts = num_edges * 3; // Retry limit
                let mut attempts = 0;
                
                while selected.len() < num_edges && attempts < max_attempts && target_ids.len() > 1 {
                    let idx = rng.gen_range(0..target_ids.len());
                    let candidate = target_ids[idx];
                    
                    if candidate != source_id && !selected.contains(&candidate) {
                        selected.push(candidate);
                    }
                    attempts += 1;
                }
                selected
            } else {
                // Different types: direct sampling
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
