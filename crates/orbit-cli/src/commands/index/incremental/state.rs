use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Context as _, Result};
use code_graph_incremental::pipeline::{Context, Display, Report};
use code_graph_incremental::treesitter::SupportLang;
use code_graph_incremental::{Env, State, templates};

use super::RepositoryBars;
use super::input;
use super::output::{Exporter, FamilyOutput};
use crate::database::{GraphWriter, manifest};
use crate::tui;
use crate::workspace::{self, GitInfo};

pub(super) struct Project<'a> {
    pub git: &'a GitInfo,
    pub writer: &'a GraphWriter,
    pub ontology: &'a ontology::Ontology,
    pub manifest_path: &'a std::path::Path,
    pub state_dir: PathBuf,
    pub budget_ms: u64,
    pub stats: bool,
    pub cancel: &'a Arc<AtomicBool>,
    pub progress: Option<&'a Arc<RepositoryBars>>,
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
        let owner = saved.owner.as_deref().and_then(SupportLang::from_family);
        let families = saved
            .families
            .iter()
            .map(|name| {
                SupportLang::from_family(name).with_context(|| {
                    format!(
                        "incremental state names a language family this build does not know: {name}"
                    )
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let changes = self.changes(&saved, owner, &families)?;
        let changed = input::paths(&self.git.repo_path, changes.changed)?;
        let mut changed = input::split(&changed, owner);
        let mut removed = input::split_removed(changes.removed, owner, &families);
        let mut work = BTreeMap::new();
        for family in families {
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

    fn changes(
        &self,
        saved: &manifest::IncrementalState,
        owner: Option<SupportLang>,
        families: &[SupportLang],
    ) -> Result<workspace::GitChanges> {
        let repo = &self.git.repo_path;
        let mut changes = workspace::git_committed_changes(repo, &saved.commit_sha)?;
        let working = workspace::git_working_changes(repo)?;
        for path in working.paths().chain(&saved.dirty_paths) {
            match repo.join(path).is_file() {
                true if self.edited_since_read(path, owner, families) => {
                    changes.changed.push(path.clone())
                }
                true => {}
                false if !saved.dirty_removed.contains(path) => changes.removed.push(path.clone()),
                false => {}
            }
        }
        Ok(changes.settled())
    }

    fn edited_since_read(
        &self,
        path: &str,
        owner: Option<SupportLang>,
        families: &[SupportLang],
    ) -> bool {
        let modified = |path: &std::path::Path| {
            std::fs::metadata(path)
                .and_then(|meta| meta.modified())
                .ok()
        };
        let Some(edited) = modified(&self.git.repo_path.join(path)) else {
            return true;
        };
        let readers = input::readers(path, owner, families);
        readers.is_empty()
            || readers.iter().any(|family| {
                modified(&self.snapshot_path(*family)).is_none_or(|read| edited >= read)
            })
    }

    fn run(
        &self,
        work: impl IntoIterator<Item = (SupportLang, Work)>,
        owner: Option<SupportLang>,
    ) -> Result<Vec<FamilyOutput>> {
        let work: Vec<_> = work.into_iter().collect();
        for (family, job) in &work {
            if !matches!(job, Work::Index(_)) {
                let snapshot = self.snapshot_path(*family);
                std::fs::File::open(&snapshot)
                    .with_context(|| format!("failed to open {}", snapshot.display()))?;
            }
        }
        self.writer.clear(self.ontology)?;
        let mut exporter = Exporter::new(self.git, self.writer, self.ontology);
        std::fs::create_dir_all(&self.state_dir)?;
        if let Some(bars) = self.progress {
            let files = |work: &Work| match work {
                Work::Index(paths) | Work::Reindex { paths, .. } => Some(paths.len()),
                Work::Keep => None,
            };
            let counts: Vec<_> = work
                .iter()
                .map(|(family, work)| (family.family(), files(work)))
                .collect();
            bars.begin(&counts);
        }
        let mut families = Vec::new();
        let mut outputs = Vec::new();
        for (family, work) in work {
            families.push(family.family().to_string());
            outputs.push(self.family(family, work, &mut exporter)?);
        }
        self.save_state(&families, owner)?;
        Ok(outputs)
    }

    fn family(
        &self,
        family: SupportLang,
        work: Work,
        exporter: &mut Exporter,
    ) -> Result<FamilyOutput> {
        if self.cancelled() {
            return Err(tui::cancelled("Indexing cancelled."));
        }
        let snapshot = self.snapshot_path(family);
        let read_at = std::time::SystemTime::now();
        let mut env;
        let (report, state) = match work {
            Work::Index(paths) => {
                env = Env::for_lang(family)?;
                env.limits.total_ms = self.budget_ms;
                let context = self.context(&env, paths)?;
                let (context, displayed) = templates::index(context)?.then(Display)?.finish();
                (context.report, displayed.state)
            }
            Work::Reindex { paths, removed } => {
                let (loaded, state) = State::load(&snapshot, family)
                    .with_context(|| format!("failed to load {}", snapshot.display()))?;
                env = loaded;
                env.limits.total_ms = self.budget_ms;
                let context = self.context(&env, paths)?;
                let (context, displayed) = templates::reindex(context, state, removed)?
                    .then(Display)?
                    .finish();
                (context.report, displayed.state)
            }
            Work::Keep => {
                let (env, state) = State::load(&snapshot, family)
                    .with_context(|| format!("failed to load {}", snapshot.display()))?;
                return exporter.write(family, &env, &state, &Report::default(), self.stats);
            }
        };
        save_snapshot(&env, &state, &snapshot, read_at)?;
        exporter.write(family, &env, &state, &report, self.stats)
    }

    fn context<'e>(&self, env: &'e Env, paths: Vec<String>) -> Result<Context<'e>> {
        let files = paths.len();
        let mut context = Context::new(env, input::paths(&self.git.repo_path, paths)?);
        let cancel = self.cancel.clone();
        context.run = context
            .run
            .with_cancellation(Arc::new(move || cancel.load(Ordering::Relaxed)));
        Ok(match self.progress {
            Some(bars) => context.observe(bars.family(files)),
            None => context,
        })
    }

    fn cancelled(&self) -> bool {
        self.cancel.load(Ordering::Relaxed)
    }

    fn snapshot_path(&self, family: SupportLang) -> PathBuf {
        snapshot_file(&self.state_dir, family)
    }

    fn save_state(&self, families: &[String], owner: Option<SupportLang>) -> Result<()> {
        let dirty = workspace::git_working_changes(&self.git.repo_path)?;
        manifest::save_incremental_state(
            self.manifest_path,
            self.git.project_id,
            &manifest::IncrementalState {
                commit_sha: self.git.commit_sha.clone(),
                dirty_paths: dirty.paths().cloned().collect(),
                dirty_removed: dirty.removed,
                families: families.to_vec(),
                owner: owner.map(|lang| lang.family().to_string()),
            },
        )
    }

    fn load_state(&self) -> Result<manifest::IncrementalState> {
        manifest::load_incremental_state(self.manifest_path, self.git.project_id)?.with_context(
            || {
                format!(
                    "no incremental state for {}; run `orbit index` first",
                    self.git.repo_path.display()
                )
            },
        )
    }
}

fn snapshot_file(state_dir: &std::path::Path, family: SupportLang) -> PathBuf {
    state_dir.join(format!("graph.{}.bin", family.family()))
}

fn save_snapshot(
    env: &Env,
    state: &State,
    snapshot: &std::path::Path,
    read_at: std::time::SystemTime,
) -> Result<()> {
    state.save(env, snapshot)?;
    std::fs::File::options()
        .write(true)
        .open(snapshot)?
        .set_modified(read_at)?;
    Ok(())
}
