use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Context as _, Result};
use code_graph_incremental::pipeline::{Context, Display, Report};
use code_graph_incremental::treesitter::SupportLang;
use code_graph_incremental::{Env, State, templates};
use rustc_hash::FxHashSet;

use super::RepositoryBars;
use super::input;
use super::output::{self, Exporter, FamilyOutput};
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
    pub working: workspace::GitChanges,
}

/// Which rows of the graph a run writes: all of them, after clearing it, or
/// only those of the files the run read, resolved again, or removed.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Rows {
    All,
    Changed,
}

/// A family's graph after this run read its files.
struct Indexed {
    env: Env,
    report: Report,
    state: State,
    touched: FxHashSet<u32>,
    removed: Vec<String>,
    labels_changed: bool,
}

/// What one language family wrote, and the files it now holds.
struct Written {
    output: FamilyOutput,
    labels: Vec<String>,
    labels_changed: bool,
}

impl Written {
    fn new(output: FamilyOutput, state: &State, labels_changed: bool) -> Self {
        Self {
            output,
            labels: state.trees.iter().map(|tree| tree.label.clone()).collect(),
            labels_changed,
        }
    }
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
        self.run(work, owner, Rows::All)
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
        self.run(work.into_values(), owner, Rows::Changed)
    }

    fn changes(
        &self,
        saved: &manifest::IncrementalState,
        owner: Option<SupportLang>,
        families: &[SupportLang],
    ) -> Result<workspace::GitChanges> {
        let repo = &self.git.repo_path;
        let mut changes = workspace::git_committed_changes(repo, &saved.commit_sha)?;
        let indexed: FxHashSet<String> = families
            .iter()
            .flat_map(|family| State::labels(&self.snapshot_path(*family)).unwrap_or_default())
            .collect();
        for path in self.working.paths().chain(&saved.dirty_paths) {
            match repo.join(path).is_file() {
                true if !indexed.contains(path)
                    || self.edited_since_read(path, owner, families) =>
                {
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
        mut rows: Rows,
    ) -> Result<Vec<FamilyOutput>> {
        let work: Vec<_> = work.into_iter().collect();
        for (family, job) in &work {
            if !matches!(job, Work::Index(_)) {
                let snapshot = self.snapshot_path(*family);
                std::fs::File::open(&snapshot)
                    .with_context(|| format!("failed to open {}", snapshot.display()))?;
            }
        }
        if rows == Rows::Changed && !self.writer.is_complete(&self.git.commit_sha)? {
            rows = Rows::All;
        }
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
        let families: Vec<String> = work
            .iter()
            .map(|(family, _)| family.family().to_string())
            .collect();
        let outputs = self.writer.transaction(|| {
            if rows == Rows::All {
                self.writer.clear(self.ontology)?;
            }
            let mut exporter = Exporter::new(self.git, self.writer, self.ontology);
            let mut labels = Vec::new();
            let mut labels_changed = false;
            let mut outputs = Vec::new();
            for (family, work) in work {
                let written = self.family(family, work, rows, &mut exporter)?;
                labels.extend(written.labels);
                labels_changed |= written.labels_changed;
                outputs.push(written.output);
            }
            if rows == Rows::Changed && labels_changed {
                labels.sort_unstable();
                labels.dedup();
                let paths: Vec<&str> = labels.iter().map(String::as_str).collect();
                exporter.write_directories(&paths)?;
            }
            Ok(outputs)
        })?;
        self.save_state(&families, owner)?;
        Ok(outputs)
    }

    fn family(
        &self,
        family: SupportLang,
        work: Work,
        rows: Rows,
        exporter: &mut Exporter,
    ) -> Result<Written> {
        if self.cancelled() {
            return Err(tui::cancelled("Indexing cancelled."));
        }
        let snapshot = self.snapshot_path(family);
        let read_at = std::time::SystemTime::now();
        let indexed = match work {
            Work::Keep => return self.keep(family, rows, exporter),
            Work::Index(paths) => self.index_family(family, paths)?,
            Work::Reindex { paths, removed } => self.reindex_family(family, paths, removed)?,
        };
        save_snapshot(&indexed, &snapshot, read_at)?;
        let only = match rows {
            Rows::All => None,
            Rows::Changed => {
                self.writer.delete_files(&indexed.removed)?;
                Some(&indexed.touched)
            }
        };
        let output = exporter.write(
            family,
            &indexed.env,
            &indexed.state,
            &indexed.report,
            self.stats,
            only,
        )?;
        Ok(Written::new(output, &indexed.state, indexed.labels_changed))
    }

    /// A family with no changes writes nothing unless the graph is written
    /// whole; it reads only its file list.
    fn keep(&self, family: SupportLang, rows: Rows, exporter: &mut Exporter) -> Result<Written> {
        let snapshot = self.snapshot_path(family);
        let failed = || format!("failed to load {}", snapshot.display());
        if rows == Rows::Changed {
            let labels = State::labels(&snapshot).with_context(failed)?;
            return Ok(Written {
                output: output::family_output(family, labels.len(), &Report::default(), false),
                labels,
                labels_changed: false,
            });
        }
        let (env, state) = State::load(&snapshot, family).with_context(failed)?;
        let output = exporter.write(family, &env, &state, &Report::default(), self.stats, None)?;
        Ok(Written::new(output, &state, false))
    }

    fn index_family(&self, family: SupportLang, paths: Vec<String>) -> Result<Indexed> {
        let mut env = Env::for_lang(family)?;
        env.limits.total_ms = self.budget_ms;
        let context = self.context(&env, paths)?;
        let (context, displayed) = templates::index(context)?.then(Display)?.finish();
        let report = context.report;
        let state = displayed.state;
        Ok(Indexed {
            touched: (0..state.trees.len() as u32).collect(),
            env,
            report,
            state,
            removed: Vec::new(),
            labels_changed: true,
        })
    }

    /// Reads `paths` into the family's saved graph and resolves again what
    /// depends on them.
    fn reindex_family(
        &self,
        family: SupportLang,
        paths: Vec<String>,
        removed: Vec<String>,
    ) -> Result<Indexed> {
        let snapshot = self.snapshot_path(family);
        let (mut env, state) = State::load(&snapshot, family)
            .with_context(|| format!("failed to load {}", snapshot.display()))?;
        env.limits.total_ms = self.budget_ms;
        let known: FxHashSet<&str> = state.trees.iter().map(|t| t.label.as_str()).collect();
        let labels_changed =
            !removed.is_empty() || paths.iter().any(|path| !known.contains(path.as_str()));
        drop(known);
        let context = self.context(&env, paths)?;
        let resolved = templates::reindex(context, state, removed.clone())?;
        let touched = resolved.value().touched.clone();
        let (context, displayed) = resolved.then(Display)?.finish();
        let state = displayed.state;
        let report = context.report;
        Ok(Indexed {
            env,
            report,
            state,
            touched,
            removed,
            labels_changed,
        })
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
        manifest::save_incremental_state(
            self.manifest_path,
            self.git.project_id,
            &manifest::IncrementalState {
                commit_sha: self.git.commit_sha.clone(),
                dirty_paths: self.working.paths().cloned().collect(),
                dirty_removed: self.working.removed.clone(),
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
    indexed: &Indexed,
    snapshot: &std::path::Path,
    read_at: std::time::SystemTime,
) -> Result<()> {
    indexed
        .state
        .save_changed(&indexed.env, snapshot, &indexed.touched)?;
    std::fs::File::options()
        .write(true)
        .open(snapshot)?
        .set_modified(read_at)?;
    Ok(())
}
