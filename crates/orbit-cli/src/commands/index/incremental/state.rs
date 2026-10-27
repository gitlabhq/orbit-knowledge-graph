use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Context as _, Result};
use arrow::record_batch::RecordBatch;
use code_graph_incremental::pipeline::{Context, Display, Report};
use code_graph_incremental::treesitter::SupportLang;
use code_graph_incremental::{Env, State, templates};
use rustc_hash::FxHashSet;

use super::RepositoryBars;
use super::input;
use super::output::{self, Exporter, FamilyOutput, Replacement};
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

impl Project<'_> {
    /// Indexes every file and rewrites the whole graph. The snapshots go in
    /// place once the rows are written.
    pub(super) fn index_all(&self) -> Result<Vec<FamilyOutput>> {
        let repository = input::directory(&self.git.repo_path)?;
        let families = input::split(&repository, None);
        let owner = families.values().next().map(|(family, _)| *family);
        let names: Vec<String> = families.keys().map(|name| name.to_string()).collect();
        self.begin_progress(
            families
                .values()
                .map(|(family, paths)| (family.family(), Some(paths.len()))),
        );
        std::fs::create_dir_all(&self.state_dir)?;
        let mut staged = Vec::new();
        let written = self.writer.in_transaction(|| {
            self.writer.clear(self.ontology)?;
            let mut exporter = Exporter::new(self.git, self.writer, self.ontology);
            let mut outputs = Vec::new();
            for (family, paths) in families.into_values() {
                if self.cancelled() {
                    return Err(tui::cancelled("Indexing cancelled."));
                }
                let read_at = std::time::SystemTime::now();
                let mut env = Env::for_lang(family)?;
                env.limits.total_ms = self.budget_ms;
                let context = self.context(&env, paths)?;
                let (context, displayed) = templates::index(context)?.then(Display)?.finish();
                let snapshot = self.snapshot_path(family);
                staged.push(stage(&env, &displayed.state, &snapshot, None, read_at)?);
                outputs.push(exporter.write(
                    family,
                    &env,
                    &displayed.state,
                    &context.report,
                    self.stats,
                )?);
            }
            Ok(outputs)
        });
        self.finish(written, &staged, &names, owner)
    }

    /// Reads the files changed since the last run and replaces only their
    /// rows, the same way a refresh does.
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
        let saved_files = saved_files(&self.state_dir, &families);
        let changes = self.changes(&saved, owner, &families, &saved_files)?;
        let reader = Reader {
            git: self.git,
            ontology: self.ontology,
            state_dir: &self.state_dir,
            owner,
            families: &families,
            saved_files: &saved_files,
            stats: self.stats,
            context: &|env, paths| self.context(env, paths),
        };
        let work = reader.work(changes.changed, changes.removed)?;
        let mut names: Vec<&'static str> = families
            .iter()
            .map(|family| family.family())
            .chain(work.keys().copied())
            .collect();
        names.sort_unstable();
        names.dedup();
        self.begin_progress(names.iter().map(|name| {
            let files = work.get(name).map(|(_, paths, _)| paths.len());
            (*name, files)
        }));
        let refresh = reader.read(work, self.budget_ms, &|| self.cancelled())?;
        let written = self
            .writer
            .in_transaction(|| output::apply_refresh(self.writer, self.ontology, &refresh));
        let outputs: Vec<FamilyOutput> = names
            .iter()
            .filter_map(|name| {
                let read = refresh.families.iter().find(|f| f.family.family() == *name);
                match read {
                    Some(family) => Some(family.output.clone()),
                    None => families.iter().find(|f| f.family() == *name).map(|family| {
                        let files = saved_files.get(name).map_or(0, Vec::len);
                        output::family_output(*family, files, &Report::default(), false)
                    }),
                }
            })
            .collect();
        let names: Vec<String> = names.iter().map(|name| name.to_string()).collect();
        self.finish(written.map(|()| outputs), &refresh.families, &names, owner)
    }

    /// Puts the staged snapshots in place once the rows are written, and
    /// drops them when they are not.
    fn finish<T>(
        &self,
        written: Result<T>,
        staged: &[Refreshed],
        families: &[String],
        owner: Option<SupportLang>,
    ) -> Result<T> {
        match written {
            Ok(value) => {
                commit_snapshots(staged)?;
                self.save_state(families, owner)?;
                Ok(value)
            }
            Err(error) => {
                discard_snapshots(staged);
                Err(error)
            }
        }
    }

    fn begin_progress<'f>(&self, families: impl Iterator<Item = (&'f str, Option<usize>)>) {
        if let Some(bars) = self.progress {
            bars.begin(&families.collect::<Vec<_>>());
        }
    }

    fn changes(
        &self,
        saved: &manifest::IncrementalState,
        owner: Option<SupportLang>,
        families: &[SupportLang],
        saved_files: &SavedFiles,
    ) -> Result<workspace::GitChanges> {
        let repo = &self.git.repo_path;
        let mut changes = workspace::git_committed_changes(repo, &saved.commit_sha)?;
        let indexed: FxHashSet<&str> = saved_files.values().flatten().map(String::as_str).collect();
        for path in self.working.paths().chain(&saved.dirty_paths) {
            match repo.join(path).is_file() {
                true if !indexed.contains(path.as_str())
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
                    "no incremental state for {}; run `orbit workspace index` first",
                    self.git.repo_path.display()
                )
            },
        )
    }
}

fn snapshot_file(state_dir: &std::path::Path, family: SupportLang) -> PathBuf {
    state_dir.join(format!("graph.{}.bin", family.family()))
}

/// Freshness compares a file's mtime with the time its family last read it.
fn set_read_time(snapshot: &std::path::Path, read_at: std::time::SystemTime) -> Result<()> {
    std::fs::File::options()
        .write(true)
        .open(snapshot)?
        .set_modified(read_at)?;
    Ok(())
}

pub(super) fn changed_since_snapshot(
    repo: &std::path::Path,
    state_dir: &std::path::Path,
    files: &[&str],
) -> Vec<String> {
    let modified = |path: &std::path::Path| {
        std::fs::metadata(path)
            .and_then(|meta| meta.modified())
            .ok()
    };
    files
        .iter()
        .filter(|file| {
            let Some(language) =
                SupportLang::from_path(file).filter(|language| language.has_rules())
            else {
                return false;
            };
            let snapshot = modified(&snapshot_file(state_dir, language));
            let file = modified(&repo.join(file));
            matches!((file, snapshot), (Some(file), Some(snapshot)) if file > snapshot)
        })
        .map(|file| file.to_string())
        .collect()
}

/// A family's new rows, and its snapshot written beside the live one until the rows are applied.
pub(super) struct Refreshed {
    pub family: SupportLang,
    pub replacement: Option<Replacement>,
    pub output: FamilyOutput,
    pending: PathBuf,
    snapshot: PathBuf,
}

/// What a refresh writes: each family's rows, and the directories again
/// when files came or went.
pub(super) struct Refresh {
    pub families: Vec<Refreshed>,
    pub directories: Option<Vec<(String, RecordBatch)>>,
}

fn pending_file(snapshot: &std::path::Path) -> PathBuf {
    let mut name = snapshot.as_os_str().to_owned();
    name.push(".pending");
    PathBuf::from(name)
}

/// Writes the snapshot of `state` beside the live one at `snapshot`.
/// `touched` names the files that changed since that one was read; without
/// it every block is written.
fn stage(
    env: &Env,
    state: &State,
    snapshot: &std::path::Path,
    touched: Option<&FxHashSet<u32>>,
    read_at: std::time::SystemTime,
) -> Result<Refreshed> {
    let pending = pending_file(snapshot);
    let staged = state
        .stage(env, snapshot, &pending, touched)
        .map_err(anyhow::Error::from)
        .and_then(|()| set_read_time(&pending, read_at));
    if let Err(error) = staged {
        let _ = std::fs::remove_file(&pending);
        return Err(error);
    }
    Ok(Refreshed {
        family: env.lang_id,
        replacement: None,
        output: output::family_output(env.lang_id, state.trees.len(), &Report::default(), false),
        pending,
        snapshot: snapshot.to_path_buf(),
    })
}

/// Puts each pending snapshot in place. The rename keeps the read time that freshness compares against.
pub(super) fn commit_snapshots(refreshed: &[Refreshed]) -> Result<()> {
    for family in refreshed {
        State::commit_staged(&family.pending, &family.snapshot)
            .with_context(|| format!("failed to save {}", family.snapshot.display()))?;
    }
    Ok(())
}

/// Drops the pending snapshots, so the edited files still count as changed after a failed write.
pub(super) fn discard_snapshots(refreshed: &[Refreshed]) {
    for family in refreshed {
        let _ = std::fs::remove_file(&family.pending);
    }
}

/// The working-tree files a refresh must read and remove: edited since
/// their family read them, or not in any snapshot yet, as after a
/// `git mv`; and gone, though a snapshot still holds them.
pub(super) fn refresh_changes(
    repo: &std::path::Path,
    state_dir: &std::path::Path,
    saved_files: &SavedFiles,
    working: workspace::GitChanges,
    files: Vec<String>,
) -> (Vec<String>, Vec<String>) {
    let indexed: FxHashSet<&str> = saved_files.values().flatten().map(String::as_str).collect();
    let mut candidates: Vec<String> = working.changed.into_iter().chain(files).collect();
    candidates.sort();
    candidates.dedup();
    let candidates: Vec<&str> = candidates.iter().map(String::as_str).collect();
    let mut changed = changed_since_snapshot(repo, state_dir, &candidates);
    changed.extend(
        candidates
            .iter()
            .filter(|path| !indexed.contains(**path) && repo.join(path).is_file())
            .filter(|path| SupportLang::from_path(path).is_some_and(|lang| lang.has_rules()))
            .map(|path| path.to_string()),
    );
    changed.sort();
    changed.dedup();
    let removed = working
        .removed
        .into_iter()
        .filter(|path| indexed.contains(path.as_str()) && !repo.join(path).exists())
        .collect();
    (changed, removed)
}

/// Each family's files, as its saved snapshot lists them.
pub(super) type SavedFiles = BTreeMap<&'static str, Vec<String>>;

pub(super) fn saved_files(state_dir: &std::path::Path, families: &[SupportLang]) -> SavedFiles {
    families
        .iter()
        .map(|family| {
            let labels = State::labels(&snapshot_file(state_dir, *family)).unwrap_or_default();
            (family.family(), labels)
        })
        .collect()
}

type ContextFor<'a> = dyn for<'e> Fn(&'e Env, Vec<String>) -> Result<Context<'e>> + 'a;

/// How `orbit index` and a refresh read changed files into the saved graph.
pub(super) struct Reader<'a> {
    pub git: &'a GitInfo,
    pub ontology: &'a ontology::Ontology,
    pub state_dir: &'a std::path::Path,
    pub owner: Option<SupportLang>,
    pub families: &'a [SupportLang],
    pub saved_files: &'a SavedFiles,
    pub stats: bool,
    pub context: &'a ContextFor<'a>,
}

type FamilyWork = (SupportLang, Vec<String>, Vec<String>);

impl Reader<'_> {
    /// The files each family reads again and drops.
    pub(super) fn work(
        &self,
        changed: Vec<String>,
        removed: Vec<String>,
    ) -> Result<BTreeMap<&'static str, FamilyWork>> {
        let repository = input::paths(&self.git.repo_path, changed)?;
        let mut work: BTreeMap<&'static str, FamilyWork> = BTreeMap::new();
        for (family, paths) in input::split(&repository, self.owner).into_values() {
            work.entry(family.family())
                .or_insert((family, Vec::new(), Vec::new()))
                .1 = paths;
        }
        for (name, paths) in input::split_removed(removed, self.owner, self.families) {
            if let Some(family) = SupportLang::from_family(name) {
                work.entry(name)
                    .or_insert((family, Vec::new(), Vec::new()))
                    .2 = paths;
            }
        }
        Ok(work)
    }

    /// Reads each family's files, builds their new rows, and stages each
    /// snapshot beside the live one. Nothing is in place until the rows are
    /// written and [`commit_snapshots`] runs.
    pub(super) fn read(
        &self,
        work: BTreeMap<&'static str, FamilyWork>,
        budget_ms: u64,
        cancelled: &dyn Fn() -> bool,
    ) -> Result<Refresh> {
        let read_at = std::time::SystemTime::now();
        let mut refreshed: Vec<Refreshed> = Vec::new();
        let mut labels: BTreeMap<&'static str, Vec<String>> = BTreeMap::new();
        let mut labels_changed = false;
        for (family, paths, removed) in work.into_values() {
            let read = match cancelled() {
                true => Err(tui::cancelled("Indexing cancelled.")),
                false => self.read_family(family, paths, removed, budget_ms, read_at),
            };
            match read {
                Ok((family_refreshed, family_labels, changed)) => {
                    labels_changed |= changed;
                    labels.insert(family.family(), family_labels);
                    refreshed.push(family_refreshed);
                }
                Err(error) => {
                    discard_snapshots(&refreshed);
                    return Err(error);
                }
            }
        }
        let directories = match labels_changed {
            false => None,
            true => Some(self.directories(labels)?),
        };
        Ok(Refresh {
            families: refreshed,
            directories,
        })
    }

    /// Reads `paths` into one family and drops `removed`. A family the
    /// snapshots do not have yet is indexed whole. Returns the family's files
    /// now, and whether the set of them changed.
    fn read_family(
        &self,
        family: SupportLang,
        paths: Vec<String>,
        removed: Vec<String>,
        budget_ms: u64,
        read_at: std::time::SystemTime,
    ) -> Result<(Refreshed, Vec<String>, bool)> {
        let snapshot = snapshot_file(self.state_dir, family);
        let (mut env, state) = match self.families.contains(&family) {
            true => State::load(&snapshot, family)
                .with_context(|| format!("failed to load {}", snapshot.display()))?,
            false => {
                let env = Env::for_lang(family)?;
                let state = State::new(&env);
                (env, state)
            }
        };
        env.limits.total_ms = budget_ms;
        let before = state.trees.len();
        let known: FxHashSet<&str> = state.trees.iter().map(|tree| tree.label.as_str()).collect();
        let added = paths.iter().any(|path| !known.contains(path.as_str()));
        drop(known);
        let resolved = templates::reindex((self.context)(&env, paths)?, state, removed.clone())?;
        let touched = resolved.value().touched.clone();
        let (context, displayed) = resolved.then(Display)?.finish();
        let state = displayed.state;
        let labels: Vec<String> = state.trees.iter().map(|tree| tree.label.clone()).collect();
        let files: FxHashSet<String> = touched
            .iter()
            .filter_map(|&index| state.trees.get(index as usize))
            .map(|tree| tree.label.to_string())
            .chain(removed)
            .collect();
        let replacement = output::replacement(self.git, self.ontology, &env, &state, &files)?;
        let mut staged = stage(&env, &state, &snapshot, Some(&touched), read_at)?;
        staged.replacement = Some(replacement);
        staged.output =
            output::family_output(family, state.trees.len(), &context.report, self.stats);
        Ok((staged, labels, added || state.trees.len() != before))
    }

    /// The directory rows of every family's files: those just read, and the
    /// saved file lists of the rest.
    fn directories(
        &self,
        read: BTreeMap<&'static str, Vec<String>>,
    ) -> Result<Vec<(String, RecordBatch)>> {
        let mut paths: Vec<String> = self
            .saved_files
            .iter()
            .filter(|(family, _)| !read.contains_key(*family))
            .flat_map(|(_, files)| files.iter().cloned())
            .collect();
        paths.extend(read.into_values().flatten());
        paths.sort_unstable();
        paths.dedup();
        let paths: Vec<&str> = paths.iter().map(String::as_str).collect();
        output::directory_rows(self.git, self.ontology, &paths)
    }
}
