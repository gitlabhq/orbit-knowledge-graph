use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result};
use code_graph_incremental::pipeline::Observer;
use code_graph_incremental::treesitter::SupportLang;

use ontology::Ontology;

use crate::database::schema::graph_lacks_commit;
use crate::database::{GraphWriter, lock_index_state, manifest, wait_timeout};
use crate::{tui, workspace};

mod detached;
mod input;
mod output;
mod state;

pub(crate) use detached::index_detached;
pub(crate) use output::FamilyOutput;
use state::Project;

#[derive(Default)]
pub(crate) struct Options {
    pub stats: bool,
    pub verbose: bool,
    pub db: Option<PathBuf>,
    pub budget_seconds: Option<u64>,
    pub cancel: Arc<AtomicBool>,
    pub progress: bool,
    /// How long to wait for a running index of the same repository; `None` waits the default.
    pub wait: Option<Duration>,
    /// Skip a repository whose revision is already indexed, once any running index finished.
    pub skip_current: bool,
    /// A file to create once this index holds a repository's locks and shows as `indexing`.
    pub ready_file: Option<PathBuf>,
}

#[derive(serde::Serialize)]
pub(crate) struct IndexOutput {
    repository: String,
    path: String,
    time_seconds: f64,
    graph: GraphStats,
    processing: ProcessingStats,
    database_path: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    detailed: Vec<FamilyOutput>,
}

#[derive(serde::Serialize)]
struct GraphStats {
    directories: usize,
    files: usize,
    definitions: usize,
    imported_symbols: usize,
    relationships: usize,
}

#[derive(serde::Serialize)]
struct ProcessingStats {
    skipped_files: usize,
    errored_files: usize,
}

fn summary(
    git: &workspace::GitInfo,
    writer: &GraphWriter,
    db_path: &std::path::Path,
    started: std::time::Instant,
    families: Vec<FamilyOutput>,
    stats: bool,
) -> Result<IndexOutput> {
    let rows = |table: &str| writer.count(&format!("SELECT count(*) FROM {table}"));
    Ok(IndexOutput {
        repository: git
            .repo_path
            .file_name()
            .map(|name| name.to_string_lossy().to_string())
            .unwrap_or_default(),
        path: git.repo_path.display().to_string(),
        time_seconds: started.elapsed().as_secs_f64(),
        graph: GraphStats {
            directories: rows("gl_directory")?,
            files: rows("gl_file")?,
            definitions: rows("gl_definition")?,
            imported_symbols: rows("gl_imported_symbol")?,
            relationships: rows("gl_edge")?,
        },
        processing: ProcessingStats {
            skipped_files: families.iter().map(|family| family.skipped_files).sum(),
            errored_files: writer
                .count("SELECT count(*) FROM gl_file WHERE reason LIKE 'fault_%'")?,
        },
        database_path: db_path.display().to_string(),
        detailed: if stats { families } else { Vec::new() },
    })
}

impl IndexOutput {
    pub(crate) fn path(&self) -> &str {
        &self.path
    }

    pub(crate) fn database_path(&self) -> &str {
        &self.database_path
    }

    pub(crate) fn slowest_files(&self) -> Vec<String> {
        self.detailed
            .iter()
            .flat_map(FamilyOutput::slowest_lines)
            .collect()
    }
}

pub(super) struct RepositoryBars {
    group: tui::ProgressGroup,
    phases: OnceLock<PhaseBars>,
}

struct PhaseBars {
    parse: tui::Bar,
    resolve: tui::Bar,
}

impl RepositoryBars {
    fn open(git: &workspace::GitInfo) -> Self {
        let name = git.repo_path.file_name().unwrap_or_default();
        Self {
            group: tui::progress_group(format!(
                "{}  {} @ {}",
                name.to_string_lossy(),
                git.branch,
                git.short_sha()
            )),
            phases: OnceLock::new(),
        }
    }

    pub(super) fn begin(&self, families: &[(&str, Option<usize>)]) {
        let mut changed: Vec<_> = families
            .iter()
            .filter_map(|(family, files)| files.map(|files| (*family, files)))
            .collect();
        changed.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(b.0)));
        let total = changed.iter().map(|(_, files)| files).sum();
        let mut note = vec![count_noun(total, "file")];
        let names: Vec<_> = changed.iter().take(3).map(|(family, _)| *family).collect();
        match changed.len().saturating_sub(3) {
            _ if names.is_empty() => {}
            0 => note.push(names.join(", ")),
            hidden => note.push(format!("{} +{hidden} more", names.join(", "))),
        }
        let unchanged: Vec<_> = families
            .iter()
            .filter(|(_, files)| files.is_none())
            .map(|(family, _)| *family)
            .collect();
        if !unchanged.is_empty() {
            note.push(format!("{} unchanged", unchanged.join(", ")));
        }
        self.group.note(note.join(" · "));
        self.phases.get_or_init(|| PhaseBars {
            parse: self.group.bar("Parse  ", total),
            resolve: self.group.bar("Resolve", total),
        });
    }

    pub(super) fn family(self: &Arc<Self>, files: usize) -> FamilyBars {
        FamilyBars {
            bars: self.clone(),
            files,
        }
    }

    fn close(&self, output: &IndexOutput) {
        if let Some(phases) = self.phases.get() {
            phases
                .parse
                .finish(count_noun(output.graph.definitions, "definition"));
            phases.resolve.finish(format!(
                "{} · {:.1} s",
                count_noun(output.graph.relationships, "relationship"),
                output.time_seconds
            ));
        }
        self.group.close();
    }

    fn fail(&self, error: &anyhow::Error) {
        let cancelled = tui::is_cancelled(error);
        if let Some(phases) = self.phases.get() {
            for bar in [&phases.parse, &phases.resolve] {
                match cancelled {
                    true => bar.cancel_at_current_count(),
                    false => bar.fail_at_current_count(),
                }
            }
        }
        match cancelled {
            true => self.group.cancel(),
            false => self.group.fail(format!("{error:#}")),
        }
    }
}

pub(super) struct FamilyBars {
    bars: Arc<RepositoryBars>,
    files: usize,
}

impl Observer for FamilyBars {
    fn finished(&mut self, phase: &str, _: Duration) {
        let Some(phases) = self.bars.phases.get() else {
            return;
        };
        match phase {
            "resolve" => phases.resolve.advance(self.files),
            phase if phase.starts_with("parse") => phases.parse.advance(self.files),
            _ => {}
        }
    }
}

fn count_noun(count: usize, noun: &str) -> String {
    let plural = match count {
        1 => "",
        _ => "s",
    };
    format!("{} {noun}{plural}", tui::format_with_thousands(count))
}

pub(crate) fn index(path: PathBuf, options: Options) -> Result<Vec<IndexOutput>> {
    let workspace = workspace::Workspace::open_default()?;
    let manifest_path = std::path::absolute(workspace.manifest_path())?;
    let repos = workspace.resolve_repos(&path)?;
    if repos.is_empty() {
        let folder = dunce::canonicalize(&path).unwrap_or(path);
        return Err(super::NoRepositoryFound(folder).into());
    }
    let ontology = Ontology::load_embedded().context("failed to load embedded ontology")?;
    let wait = options.wait.unwrap_or_else(wait_timeout);
    let ready = options.ready_file.clone().map(detached::ReadyFile);
    let mut outputs = Vec::new();
    for repo in repos {
        let started = std::time::Instant::now();
        let git = match workspace::git_info(&repo) {
            Ok(git) => git,
            Err(error) => {
                manifest::record_git_info_failure(&manifest_path, &repo, &format!("{error:#}"));
                return Err(error);
            }
        };
        let state_dir = workspace.var_dir(git.project_id);
        let _state_lock = lock_index_state(&state_dir, &git.repo_path, wait)?;
        let db_path = workspace::resolve_db_path(options.db.clone(), &git)?;
        if options.skip_current && !graph_lacks_commit(&db_path, &git.commit_sha)? {
            continue;
        }
        let snapshot = options.db.is_none()
            && db_path.exists()
            && manifest::load_incremental_state(&manifest_path, git.project_id)?.is_some();
        let writer = GraphWriter::open_waiting(&db_path, &git, wait)?;
        let progress = options
            .progress
            .then(|| Arc::new(RepositoryBars::open(&git)));
        manifest::record_status(
            &manifest_path,
            &git,
            manifest::RepoStatus::Indexing,
            None,
            &db_path,
        )?;
        if let Some(ready) = &ready {
            ready.signal()?;
        }
        let project = Project {
            git: &git,
            writer: &writer,
            ontology: &ontology,
            manifest_path: &manifest_path,
            state_dir,
            budget_ms: options
                .budget_seconds
                .map_or(u64::MAX, |seconds| seconds.saturating_mul(1000)),
            stats: options.stats,
            cancel: &options.cancel,
            progress: progress.as_ref(),
        };
        let cancelled = || options.cancel.load(Ordering::Relaxed);
        let output = match snapshot {
            true => project.index_changed().or_else(|error| match cancelled() {
                true => Err(error),
                false => {
                    orbit_log_fields::warn!(
                        safe(ERROR_TYPE) = "incremental_index",
                        expanded(ERROR_MESSAGE) = %format_args!("{error:#}"),
                        "incremental index failed; falling back to full index"
                    );
                    project.index_all()
                }
            }),
            false => project.index_all(),
        }
        .and_then(|output| writer.complete(&git.commit_sha).map(|()| output));
        let output = match output {
            Ok(output) => output,
            Err(error) => {
                let error = match cancelled() {
                    true => tui::cancelled("Indexing cancelled."),
                    false => error,
                };
                if let Some(bars) = &progress {
                    bars.fail(&error);
                }
                manifest::record_status(
                    &manifest_path,
                    &git,
                    manifest::RepoStatus::Error,
                    Some(&format!("{error:#}")),
                    &db_path,
                )?;
                return Err(error);
            }
        };
        manifest::record_status(
            &manifest_path,
            &git,
            manifest::RepoStatus::Indexed,
            None,
            &db_path,
        )?;
        let output = summary(&git, &writer, &db_path, started, output, options.stats)?;
        if let Some(bars) = &progress {
            bars.close(&output);
        }
        outputs.push(output);
    }
    Ok(outputs)
}

pub(crate) enum Freshness {
    NotIncremental,
    Fresh,
    Refreshed,
    Stale(Vec<String>),
}

const REFRESHING: &str = "refreshing";
const QUEUED: &str = "refresh-queued";
const REFRESH_WAIT: Duration = Duration::from_secs(2);

fn lock_file(state_dir: &Path, name: &str) -> Result<std::fs::File> {
    std::fs::create_dir_all(state_dir)?;
    Ok(std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(state_dir.join(name))?)
}

fn try_lock(state_dir: &Path, name: &str) -> Result<Option<std::fs::File>> {
    let file = lock_file(state_dir, name)?;
    match file.try_lock() {
        Ok(()) => Ok(Some(file)),
        Err(std::fs::TryLockError::WouldBlock) => Ok(None),
        Err(std::fs::TryLockError::Error(error)) => Err(error.into()),
    }
}

fn refresh_wait() -> Duration {
    std::env::var("ORBIT_REFRESH_WAIT_MS")
        .ok()
        .and_then(|ms| ms.parse().ok())
        .map_or(REFRESH_WAIT, Duration::from_millis)
}

pub(super) fn detached_command() -> Option<Command> {
    let mut command = Command::new(std::env::current_exe().ok()?);
    command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    #[cfg(unix)]
    std::os::unix::process::CommandExt::process_group(&mut command, 0);
    Some(command)
}

fn spawn_refresh(repo: &Path, db: Option<&Path>, files: &[String]) -> Option<Child> {
    let mut command = detached_command()?;
    command.arg("refresh").arg(repo);
    if let Some(db) = db {
        command.arg("--db").arg(db);
    }
    command.arg("--").args(files).spawn().ok()
}

fn finished_within(mut child: Child, wait: Duration) -> bool {
    let deadline = Instant::now() + wait;
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return true,
            Ok(None) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(20)),
            _ => return false,
        }
    }
}

struct Incremental {
    git: workspace::GitInfo,
    saved: manifest::IncrementalState,
    state_dir: PathBuf,
}

fn incremental(repo: &Path) -> Result<Option<Incremental>> {
    let workspace = workspace::Workspace::open_default()?;
    let manifest_path = std::path::absolute(workspace.manifest_path())?;
    let git = workspace::git_info(repo)?;
    let Some(saved) = manifest::load_incremental_state(&manifest_path, git.project_id)? else {
        return Ok(None);
    };
    let state_dir = workspace.var_dir(git.project_id);
    Ok(Some(Incremental {
        git,
        saved,
        state_dir,
    }))
}

pub(crate) fn refresh(
    repo: &std::path::Path,
    db: Option<PathBuf>,
    files: &[&str],
) -> Result<Freshness> {
    let Some(Incremental { git, state_dir, .. }) = incremental(repo)? else {
        return Ok(Freshness::NotIncremental);
    };
    let changed = state::changed_since_snapshot(&git.repo_path, &state_dir, files);
    if changed.is_empty() {
        return Ok(Freshness::Fresh);
    }
    let finished = spawn_refresh(&git.repo_path, db.as_deref(), &changed)
        .is_some_and(|child| finished_within(child, refresh_wait()));
    if !finished {
        return Ok(Freshness::Stale(changed));
    }
    let changed = state::changed_since_snapshot(&git.repo_path, &state_dir, files);
    Ok(match changed.is_empty() {
        true => Freshness::Refreshed,
        false => Freshness::Stale(changed),
    })
}

pub(crate) fn refresh_now(repo: PathBuf, db: Option<PathBuf>, files: Vec<String>) -> Result<()> {
    let Some(Incremental {
        git,
        saved,
        state_dir,
    }) = incremental(&repo)?
    else {
        return Ok(());
    };
    let Some(queued) = try_lock(&state_dir, QUEUED)? else {
        return Ok(());
    };
    let refreshing = lock_file(&state_dir, REFRESHING)?;
    refreshing.lock()?;
    drop(queued);
    let mut candidates: Vec<String> = workspace::git_working_changes(&git.repo_path)?
        .changed
        .into_iter()
        .chain(files)
        .collect();
    candidates.sort();
    candidates.dedup();
    let candidates: Vec<&str> = candidates.iter().map(String::as_str).collect();
    let changed = state::changed_since_snapshot(&git.repo_path, &state_dir, &candidates);
    if changed.is_empty() {
        return Ok(());
    }
    let _state_lock = lock_index_state(&state_dir, &git.repo_path, crate::database::BRIEF_WAIT)?;
    let ontology = Ontology::load_embedded().context("failed to load embedded ontology")?;
    let owner = saved.owner.as_deref().and_then(SupportLang::from_family);
    let refreshed = state::refresh_families(&git, &ontology, &state_dir, owner, changed)?;
    // Snapshots go in place only after the rows land, so a failed write keeps the files stale.
    let written = workspace::resolve_db_path(db, &git)
        .and_then(|path| GraphWriter::open(&path, &git))
        .and_then(|writer| {
            writer.in_transaction(|| {
                refreshed
                    .iter()
                    .try_for_each(|family| output::apply(&writer, &family.replacement))
            })
        });
    match written {
        Ok(()) => state::commit_snapshots(&refreshed),
        Err(error) => {
            state::discard_snapshots(&refreshed);
            Err(error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_marker_left_by_a_killed_refresh_does_not_block_the_next_one() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(REFRESHING), "").unwrap();
        let held = try_lock(dir.path(), REFRESHING).unwrap();
        assert!(held.is_some());
        assert!(try_lock(dir.path(), REFRESHING).unwrap().is_none());
        drop(held);
        assert!(try_lock(dir.path(), REFRESHING).unwrap().is_some());
    }
}
