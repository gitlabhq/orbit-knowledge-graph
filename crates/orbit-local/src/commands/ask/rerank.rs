use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use orbit_search::{CorpusRow, Reranker};

const PASSAGE_LINES: usize = 12;
const PASSAGE_LINE_CHARS: usize = 200;

pub(super) struct CrossEncoder {
    model: orbit_rerank::Reranker,
    root: PathBuf,
}

impl CrossEncoder {
    pub(super) fn load(root: &Path) -> Result<Self> {
        let model = match std::env::var_os("ORBIT_RERANK_MODEL_DIR").filter(|d| !d.is_empty()) {
            Some(dir) => orbit_rerank::Reranker::load_dir(Path::new(&dir))
                .with_context(|| format!("failed to load reranker from {}", dir.display()))?,
            None => bundled()?,
        };
        Ok(Self {
            model,
            root: root.to_path_buf(),
        })
    }

    fn passage(&self, row: &CorpusRow, files: &mut HashMap<String, Option<String>>) -> String {
        let (path, start) = row
            .loc
            .rsplit_once(':')
            .and_then(|(p, l)| l.parse::<usize>().ok().map(|l| (p, l)))
            .unwrap_or((row.loc.as_str(), 0));
        let mut text = format!("{} {} in {path}", row.kind, row.fqn);
        let content = files
            .entry(path.to_string())
            .or_insert_with(|| std::fs::read_to_string(self.root.join(path)).ok());
        if let Some(content) = content {
            for (_, line) in super::slice_lines(content, start, row.end_line, PASSAGE_LINES) {
                text.push('\n');
                text.extend(line.trim().chars().take(PASSAGE_LINE_CHARS));
            }
        }
        text
    }
}

impl Reranker for CrossEncoder {
    fn rescore(&self, question: &str, rows: &[CorpusRow]) -> Vec<f64> {
        let mut files = HashMap::new();
        let passages: Vec<String> = rows.iter().map(|r| self.passage(r, &mut files)).collect();
        match self.model.score(question, &passages) {
            Ok(scores) => scores.into_iter().map(f64::from).collect(),
            Err(err) => {
                eprintln!("note: reranker failed, keeping lexical order: {err:#}");
                Vec::new()
            }
        }
    }
}

#[cfg(orbit_reranker_bundle)]
fn bundled() -> Result<orbit_rerank::Reranker> {
    orbit_rerank::Reranker::from_parts(
        include_str!(concat!(env!("ORBIT_RERANK_BUNDLE_DIR"), "/config.json")),
        include_bytes!(concat!(
            env!("ORBIT_RERANK_BUNDLE_DIR"),
            "/model.safetensors"
        ))
        .to_vec(),
        include_bytes!(concat!(env!("ORBIT_RERANK_BUNDLE_DIR"), "/tokenizer.json")),
    )
    .context("failed to load the bundled reranker")
}

#[cfg(not(orbit_reranker_bundle))]
fn bundled() -> Result<orbit_rerank::Reranker> {
    anyhow::bail!("no reranker embedded in this build; run `cargo xtask rerank-bundle` and rebuild")
}
