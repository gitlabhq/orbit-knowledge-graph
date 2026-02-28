use std::collections::HashMap;
use std::io::{BufReader, BufWriter};
use std::path::Path;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use rand::Rng;
use serde::{Deserialize, Serialize};

/// Metadata about the generated graph state, including high-water marks
/// for ID allocation and per-entity-type ID ranges for sampling.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphMetadata {
    pub next_entity_id: i64,
    pub next_namespace_id: i64,
    pub last_watermark: DateTime<Utc>,
    pub enabled_namespaces: Vec<EnabledNamespaceRef>,
    pub entity_ranges: HashMap<String, IdRange>,
}

/// A contiguous range of entity IDs for sampling existing entities.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdRange {
    pub first_id: i64,
    pub count: usize,
}

impl IdRange {
    pub fn sample(&self, rng: &mut impl Rng) -> i64 {
        if self.count == 0 {
            return self.first_id;
        }
        self.first_id + rng.gen_range(0..self.count as i64)
    }
}

/// Reference to an enabled namespace (root group + organization).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnabledNamespaceRef {
    pub root_namespace_id: i64,
    pub organization_id: i64,
}

/// An entry in the path hierarchy, recording an entity's position in the graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PathEntry {
    pub entity_type: String,
    pub id: i64,
    pub traversal_path: String,
    pub namespace_id: Option<i64>,
}

/// Persistent state of a generated graph, used to resume continuous mode
/// or skip re-seeding.
///
/// Serialized as gzip-compressed JSON for compact storage.
#[derive(Serialize, Deserialize)]
pub struct GraphState {
    pub metadata: GraphMetadata,
    pub path_entries: Vec<PathEntry>,
}

const STATE_FILENAME: &str = "graph_state.json.gz";

impl GraphState {
    /// Save state to a gzip-compressed JSON file in the given directory.
    pub fn save(&self, dir: &Path) -> Result<()> {
        std::fs::create_dir_all(dir)
            .with_context(|| format!("failed to create state directory: {}", dir.display()))?;

        let state_path = dir.join(STATE_FILENAME);
        let file = std::fs::File::create(&state_path)
            .with_context(|| format!("failed to create state file: {}", state_path.display()))?;

        let encoder = GzEncoder::new(BufWriter::new(file), Compression::fast());
        serde_json::to_writer(encoder, self)
            .with_context(|| format!("failed to write state to {}", state_path.display()))?;

        Ok(())
    }

    /// Load state from a previously saved file.
    pub fn load(dir: &Path) -> Result<Self> {
        let state_path = dir.join(STATE_FILENAME);

        let file = std::fs::File::open(&state_path).with_context(|| {
            format!(
                "failed to open state file: {} (did you run initial generation first?)",
                state_path.display()
            )
        })?;

        let decoder = GzDecoder::new(BufReader::new(file));
        let state: Self = serde_json::from_reader(decoder)
            .with_context(|| format!("failed to parse state from {}", state_path.display()))?;

        Ok(state)
    }

    /// Build a `GraphState` from an `EntityRegistry` after generation completes.
    pub fn from_registry(
        registry: &crate::traversal::EntityRegistry,
        entity_ranges: HashMap<String, IdRange>,
        enabled_namespaces: Vec<EnabledNamespaceRef>,
    ) -> Self {
        let mut path_entries = Vec::new();

        for (node_type, contexts) in registry.all_entities() {
            for ctx in contexts {
                path_entries.push(PathEntry {
                    entity_type: node_type.clone(),
                    id: ctx.id,
                    traversal_path: ctx.traversal_path.clone(),
                    namespace_id: None,
                });
            }
        }

        Self {
            metadata: GraphMetadata {
                next_entity_id: registry.current_entity_id(),
                next_namespace_id: registry.current_namespace_id(),
                last_watermark: Utc::now(),
                enabled_namespaces,
                entity_ranges,
            },
            path_entries,
        }
    }
}

/// Result of a continuous mode run.
#[derive(Debug, Default)]
pub struct ContinuousResult {
    pub cycles_completed: usize,
    pub total_inserts: usize,
    pub total_updates: usize,
    pub total_deletes: usize,
}
