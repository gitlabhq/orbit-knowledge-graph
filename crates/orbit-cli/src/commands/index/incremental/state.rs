use std::collections::BTreeMap;
use std::path::PathBuf;

use anyhow::{Context as _, Result};
use code_graph_incremental::pipeline::{Context, Display, Report};
use code_graph_incremental::treesitter::SupportLang;
use code_graph_incremental::{Env, State, templates};
use serde::{Deserialize, Serialize};

use super::input;
use super::output::{Exporter, FamilyOutput};
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

enum Work {
    Index(Vec<String>),
    Reindex {
        paths: Vec<String>,
        removed: Vec<String>,
    },
    Keep,
}

impl Project<'_> {
    pub(super) fn index_all(&self) -> Result<Vec<FamilyOutput>> {
        let repository = input::directory(&self.git.repo_path)?;
        let families = input::split(&repository, None);
        let owner = families.values().next().map(|(family, _)| *family);
        let work = families
            .into_values()
            .map(|(family, paths)| (family, Work::Index(paths)));
        self.run(work, owner)
    }

    pub(super) fn index_changed(&self) -> Result<Vec<FamilyOutput>> {
        let saved = self.load_state()?;
        let mut changes = workspace::git_changes(&self.git.repo_path, &saved.commit_sha)?;
        for path in &saved.dirty {
            match self.git.repo_path.join(path).is_file() {
                true => changes.changed.push(path.clone()),
                false => changes.removed.push(path.clone()),
            }
        }
        let changes = changes.settled();
        let owner = saved.owner.as_deref().and_then(SupportLang::from_family);
        let changed = input::paths(&self.git.repo_path, changes.changed)?;
        let mut changed = input::split(&changed, owner);
        let mut removed = input::split_removed(changes.removed, owner);
        let mut work = BTreeMap::new();
        for name in &saved.families {
            let family = SupportLang::from_family(name).with_context(|| {
                format!("{STATE_FILE} names a language family this build does not know: {name}")
            })?;
            let paths = changed
                .remove(family.family())
                .map_or_else(Vec::new, |(_, paths)| paths);
            let removed = removed.remove(family.family()).unwrap_or_default();
            let next = match paths.is_empty() && removed.is_empty() {
                true => Work::Keep,
                false => Work::Reindex { paths, removed },
            };
            work.insert(family.family(), (family, next));
        }
        for (family, paths) in changed.into_values() {
            work.insert(family.family(), (family, Work::Index(paths)));
        }
        self.run(work.into_values(), owner)
    }

    fn run(
        &self,
        work: impl IntoIterator<Item = (SupportLang, Work)>,
        owner: Option<SupportLang>,
    ) -> Result<Vec<FamilyOutput>> {
        self.writer.clear(self.ontology)?;
        let mut exporter = Exporter::new(self.git, self.writer, self.ontology);
        std::fs::create_dir_all(&self.state_dir)?;
        let mut families = Vec::new();
        let mut outputs = Vec::new();
        for (family, work) in work {
            families.push(family.family().to_string());
            let snapshot = self.snapshot_path(family);
            let mut env;
            let (report, state) = match work {
                Work::Index(paths) => {
                    env = Env::for_lang(family)?;
                    env.limits.total_ms = self.budget_ms;
                    let (context, displayed) = templates::index(Context::new(
                        &env,
                        input::paths(&self.git.repo_path, paths)?,
                    ))?
                    .then(Display)?
                    .finish();
                    (context.report, displayed.state)
                }
                Work::Reindex { paths, removed } => {
                    let (loaded, state) = State::load(&snapshot, family)
                        .with_context(|| format!("failed to load {}", snapshot.display()))?;
                    env = loaded;
                    env.limits.total_ms = self.budget_ms;
                    let (context, displayed) = templates::reindex(
                        Context::new(&env, input::paths(&self.git.repo_path, paths)?),
                        state,
                        removed,
                    )?
                    .then(Display)?
                    .finish();
                    (context.report, displayed.state)
                }
                Work::Keep => {
                    let (loaded, state) = State::load(&snapshot, family)
                        .with_context(|| format!("failed to load {}", snapshot.display()))?;
                    env = loaded;
                    outputs.push(exporter.write(
                        family,
                        &env,
                        &state,
                        &Report::default(),
                        self.stats,
                    )?);
                    continue;
                }
            };
            state.save(&env, &snapshot)?;
            outputs.push(exporter.write(family, &env, &state, &report, self.stats)?);
        }
        self.save_state(&families, owner)?;
        Ok(outputs)
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
