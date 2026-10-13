use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Instant;

use anyhow::{Context as _, Result};
use code_graph_incremental::treesitter::SupportLang;
use code_graph_incremental::{Env, State};
use serde::{Deserialize, Serialize};

use super::input;
use super::output::{ChangeCounts, Exporter, FamilyOutput, IncrementalOutput, Run};
use crate::database::GraphWriter;
use crate::workspace::{self, GitInfo};

const STATE_FILE: &str = "index.json";

#[derive(Serialize, Deserialize)]
struct SavedIndex {
    commit_sha: String,
    dirty: Vec<String>,
    families: Vec<String>,
    owner: Option<String>,
}

pub(super) struct Project<'a> {
    pub git: &'a GitInfo,
    pub writer: &'a GraphWriter,
    pub ontology: &'a ontology::Ontology,
    pub state_dir: PathBuf,
    pub budget_ms: u64,
    pub stats: bool,
}

pub(super) enum FamilyWork {
    Index(Vec<String>),
    Reindex {
        paths: Vec<String>,
        removed: Vec<String>,
    },
    Keep,
}

impl Project<'_> {
    pub(super) fn index_all(&self) -> Result<IncrementalOutput> {
        let started = Instant::now();
        let repository = input::directory(&self.git.repo_path)?;
        tracing::info!(
            "walk {:.2}s, {} files",
            started.elapsed().as_secs_f64(),
            repository.usage().files
        );
        let families = input::split(&repository, None);
        let owner = families.values().next().map(|(family, _)| *family);
        let work = families
            .into_values()
            .map(|(family, paths)| (family, FamilyWork::Index(paths)));
        let families = self.run_families(work, owner)?;
        Ok(self.output("index", started, families, None))
    }

    pub(super) fn index_changed(&self) -> Result<IncrementalOutput> {
        let started = Instant::now();
        let saved = self.load_state()?;
        let mut changes = workspace::git_changes(&self.git.repo_path, &saved.commit_sha)?;
        for path in &saved.dirty {
            match self.git.repo_path.join(path).is_file() {
                true => changes.changed.push(path.clone()),
                false => changes.removed.push(path.clone()),
            }
        }
        let changes = changes.settled();
        let counts = ChangeCounts {
            changed: changes.changed.len(),
            removed: changes.removed.len(),
        };
        tracing::info!(
            "{} changed, {} removed since {}",
            counts.changed,
            counts.removed,
            &saved.commit_sha[..8.min(saved.commit_sha.len())]
        );
        let owner = saved.owner.as_deref().and_then(SupportLang::from_family);
        let changed = input::paths(&self.git.repo_path, changes.changed)?;
        let mut changed_families = input::split(&changed, owner);
        let mut removed = input::split_removed(changes.removed, owner);
        let mut work = BTreeMap::new();
        for name in &saved.families {
            let family = SupportLang::from_family(name).with_context(|| {
                format!("{STATE_FILE} names a language family this build does not know: {name}")
            })?;
            let paths = changed_families
                .remove(family.family())
                .map_or_else(Vec::new, |(_, paths)| paths);
            let removed = removed.remove(family.family()).unwrap_or_default();
            let job = match paths.is_empty() && removed.is_empty() {
                true => FamilyWork::Keep,
                false => FamilyWork::Reindex { paths, removed },
            };
            work.insert(family.family(), (family, job));
        }
        for (family, paths) in changed_families.into_values() {
            work.insert(family.family(), (family, FamilyWork::Index(paths)));
        }
        let families = self.run_families(work.into_values(), owner)?;
        Ok(self.output("reindex", started, families, Some(counts)))
    }

    fn run_families(
        &self,
        work: impl IntoIterator<Item = (SupportLang, FamilyWork)>,
        owner: Option<SupportLang>,
    ) -> Result<Vec<FamilyOutput>> {
        self.writer.clear(self.ontology)?;
        let mut exporter = Exporter::new(self.git, self.writer, self.ontology, self.stats);
        std::fs::create_dir_all(&self.state_dir)?;
        let mut families = Vec::new();
        let mut outputs = Vec::new();
        for (family, job) in work {
            families.push(family.family().to_string());
            let snapshot = self.snapshot_path(family);
            let mut env;
            let run = match job {
                FamilyWork::Index(paths) => {
                    env = Env::for_lang(family)?;
                    env.limits.total_ms = self.budget_ms;
                    Run::Index(input::paths(&self.git.repo_path, paths)?)
                }
                FamilyWork::Reindex { paths, removed } => {
                    let (loaded, state) = State::load(&snapshot, family)
                        .with_context(|| format!("failed to load {}", snapshot.display()))?;
                    env = loaded;
                    env.limits.total_ms = self.budget_ms;
                    Run::Reindex {
                        repository: input::paths(&self.git.repo_path, paths)?,
                        state,
                        removed,
                    }
                }
                FamilyWork::Keep => {
                    let (loaded, state) = State::load(&snapshot, family)
                        .with_context(|| format!("failed to load {}", snapshot.display()))?;
                    env = loaded;
                    Run::Keep(state)
                }
            };
            outputs.push(exporter.family(family, &env, run, &snapshot)?);
        }
        self.save_state(&families, owner)?;
        Ok(outputs)
    }

    fn output(
        &self,
        mode: &'static str,
        started: Instant,
        families: Vec<FamilyOutput>,
        changes: Option<ChangeCounts>,
    ) -> IncrementalOutput {
        IncrementalOutput {
            repository: super::super::repository_name(self.git),
            path: self.git.repo_path.to_string_lossy().to_string(),
            mode,
            time_seconds: started.elapsed().as_secs_f64(),
            families,
            changes,
            state_dir: self.state_dir.to_string_lossy().to_string(),
        }
    }

    fn snapshot_path(&self, family: SupportLang) -> PathBuf {
        self.state_dir
            .join(format!("graph.{}.bin", family.family()))
    }

    fn save_state(&self, families: &[String], owner: Option<SupportLang>) -> Result<()> {
        let dirty = workspace::git_working_changes(&self.git.repo_path)?;
        let saved = SavedIndex {
            commit_sha: self.git.commit_sha.clone(),
            dirty: dirty.paths().cloned().collect(),
            families: families.to_vec(),
            owner: owner.map(|lang| lang.family().to_string()),
        };
        std::fs::write(
            self.state_dir.join(STATE_FILE),
            serde_json::to_vec_pretty(&saved)?,
        )?;
        Ok(())
    }

    fn load_state(&self) -> Result<SavedIndex> {
        let path = self.state_dir.join(STATE_FILE);
        let bytes = std::fs::read(&path).with_context(|| {
            format!(
                "no incremental state for {}; run `orbit index --ff=inc` first",
                self.git.repo_path.display()
            )
        })?;
        serde_json::from_slice(&bytes).with_context(|| format!("failed to read {}", path.display()))
    }
}
