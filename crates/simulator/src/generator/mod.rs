//! Data generation from ontology definitions.
//!
//! Thin wrapper around [`synthetic_graph::Generator`] that adds
//! simulator-specific streaming (via [`StreamingEdgeWriter`]) and
//! `println!`-based plan output.

pub use synthetic_graph::batch::BatchBuilder;
pub use synthetic_graph::dependency::{DependencyGraph, ParentEdge};
pub use synthetic_graph::fake_values::FakeValueGenerator;
pub use synthetic_graph::generator::{EdgeRecord, IStr, OrganizationData, OrganizationNodes};
pub use synthetic_graph::traversal::{EntityContext, EntityRegistry, TraversalPathGenerator};

use crate::config::Config;
use crate::parquet::StreamingEdgeWriter;
use anyhow::Result;
use ontology::Ontology;

/// Simulator-level generator wrapping [`synthetic_graph::Generator`].
///
/// Adds streaming edge support via [`StreamingEdgeWriter`] and human-
/// readable plan output via `println!`.
pub struct Generator {
    inner: synthetic_graph::Generator,
}

impl Generator {
    pub fn new(ontology: Ontology, config: Config) -> Result<Self> {
        let sg_config = config.generation.to_synthetic_graph_config();
        let inner = synthetic_graph::Generator::new(ontology, sg_config)?;
        Ok(Self { inner })
    }

    pub fn ontology(&self) -> &Ontology {
        self.inner.ontology()
    }

    pub fn dependency_graph(&self) -> &DependencyGraph {
        self.inner.dependency_graph()
    }

    /// Generate all data for one organization in memory.
    pub fn generate_organization(&self, org_id: u32) -> OrganizationData {
        self.inner.generate_organization(org_id)
    }

    /// Generate organization data with streaming edge output.
    /// Edges are written directly to the [`StreamingEdgeWriter`], reducing peak memory.
    pub fn generate_organization_streaming(
        &self,
        org_id: u32,
        edge_writer: &mut StreamingEdgeWriter,
    ) -> Result<OrganizationNodes> {
        let mut edge_err: Option<anyhow::Error> = None;
        let nodes = self.inner.generate_organization_streaming(org_id, |edge| {
            if edge_err.is_none() && let Err(e) = edge_writer.push(edge) {
                edge_err = Some(e);
            }
        });

        if let Some(err) = edge_err {
            return Err(err);
        }

        Ok(nodes)
    }

    pub fn print_plan(&self) {
        let cfg = self.inner.config();
        let dep = self.inner.dependency_graph();

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
            dep.generation_order().len()
        );
        for (i, node_type) in dep.generation_order().iter().enumerate() {
            let is_root = dep.is_root(node_type);
            let marker = if is_root { "(root)" } else { "" };
            println!("    {}. {} {}", i + 1, node_type, marker);
        }
        println!();

        println!("  Relationships:");
        for (edge_type, variants) in &cfg.relationships.edges {
            for (variant, ratio) in variants {
                let ratio_str = match ratio {
                    synthetic_graph::config::EdgeRatio::Count(n) => format!("{} per parent", n),
                    synthetic_graph::config::EdgeRatio::Probability(p) => {
                        format!("{:.0}% chance", p * 100.0)
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
                    let (ratio, direction) = match value {
                        synthetic_graph::config::AssociationEdgeValue::Simple(r) => {
                            (r, synthetic_graph::config::IterationDirection::Target)
                        }
                        synthetic_graph::config::AssociationEdgeValue::Extended(ext) => {
                            (&ext.ratio, ext.iterate.clone())
                        }
                    };
                    let per_str = match direction {
                        synthetic_graph::config::IterationDirection::Target => "per target",
                        synthetic_graph::config::IterationDirection::Source => "per source",
                    };
                    let ratio_str = match ratio {
                        synthetic_graph::config::EdgeRatio::Count(n) => {
                            format!("{} {}", n, per_str)
                        }
                        synthetic_graph::config::EdgeRatio::Probability(p) => {
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
