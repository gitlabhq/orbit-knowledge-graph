//! The seed directory: rows for each ClickHouse table, the repositories the fake Orbit Next
//! serves, the Slack workspaces the fake Slack serves, and the callers the page switches between.

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::Context;
use integration_testkit::seeded_remote::SeededDataExtraction;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Map, Value};

use crate::callers::Callers;

pub struct Seed {
    pub tables: Vec<TableRows>,
    pub orbit_next: OrbitNext,
    pub slack: SeededDataExtraction,
    pub callers: Callers,
    pub examples: Vec<Example>,
}

/// A starter query the page offers, and the caller it first runs as.
#[derive(Debug, Clone, serde::Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Example {
    pub title: String,
    pub caller: String,
    pub query: String,
}

/// The rows of one ClickHouse table, as JSON objects keyed by column.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableRows {
    pub table: String,
    pub rows: Vec<Map<String, Value>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrbitNext {
    pub repositories: BTreeMap<i64, Repository>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Repository {
    pub traversal_path: String,
    /// The files at the head of each branch.
    pub branches: BTreeMap<String, Vec<File>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct File {
    pub path: String,
    pub content: String,
}

fn read<T: DeserializeOwned>(path: &Path) -> anyhow::Result<T> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    orbit_utils::yaml::from_str(&text).with_context(|| format!("parsing {}", path.display()))
}

impl Seed {
    pub fn read(dir: &Path) -> anyhow::Result<Self> {
        let graph = dir.join("clickhouse");
        let mut files: Vec<_> = std::fs::read_dir(&graph)
            .with_context(|| format!("reading {}", graph.display()))?
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .map(|entry| entry.path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "yaml"))
            .collect();
        files.sort();
        let tables = files
            .iter()
            .map(|path| read(path))
            .collect::<anyhow::Result<_>>()?;
        Ok(Self {
            tables,
            orbit_next: read(&dir.join("orbit_next.yaml"))?,
            slack: read(&dir.join("slack.yaml"))?,
            callers: read(&dir.join("callers.yaml"))?,
            examples: read(&dir.join("examples.yaml"))?,
        })
    }
}

/// The language Orbit Next indexes a file as, by its extension.
pub fn language(path: &str) -> &'static str {
    match path.rsplit('.').next().unwrap_or_default() {
        "rs" => "rust",
        "rb" => "ruby",
        "py" => "python",
        "go" => "go",
        "ts" => "typescript",
        "js" => "javascript",
        _ => "text",
    }
}
