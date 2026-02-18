use std::collections::HashMap;
use std::io::{BufReader, BufWriter};
use std::path::Path;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HierarchyMetadata {
    pub next_entity_id: i64,
    pub next_namespace_id: i64,
    pub last_watermark: DateTime<Utc>,
    pub enabled_namespaces: Vec<EnabledNamespaceRef>,
    pub entity_ranges: HashMap<String, IdRange>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdRange {
    pub first_id: i64,
    pub count: usize,
}

impl IdRange {
    pub fn sample(&self, rng: &mut impl rand::Rng) -> i64 {
        if self.count == 0 {
            return self.first_id;
        }
        self.first_id + rng.gen_range(0..self.count as i64)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnabledNamespaceRef {
    pub root_namespace_id: i64,
    pub organization_id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HierarchyPathEntry {
    pub entity_type: String,
    pub id: i64,
    pub traversal_path: String,
    pub namespace_id: Option<i64>,
}

#[derive(Serialize, Deserialize)]
pub struct HierarchyState {
    pub metadata: HierarchyMetadata,
    pub path_entries: Vec<HierarchyPathEntry>,
}

const STATE_FILENAME: &str = "hierarchy_state.json.gz";

impl HierarchyState {
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

    pub fn load(dir: &Path) -> Result<Self> {
        let state_path = dir.join(STATE_FILENAME);

        let file = std::fs::File::open(&state_path).with_context(|| {
            format!(
                "failed to open state file: {} (did you run a seed first?)",
                state_path.display()
            )
        })?;

        let decoder = GzDecoder::new(BufReader::new(file));
        let state: Self = serde_json::from_reader(decoder)
            .with_context(|| format!("failed to parse state from {}", state_path.display()))?;

        Ok(state)
    }
}
