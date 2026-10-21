use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result, bail};
use code_graph_incremental::treesitter::SupportLang;

use ontology::Ontology;

use crate::database::{GraphWriter, lock_incremental_state, manifest};
use crate::workspace;

mod input;
mod output;
mod state;

pub(crate) use output::FamilyOutput;
use state::Project;

pub(crate) struct Options {
    pub stats: bool,
    pub verbose: bool,
    pub db: Option<PathBuf>,
    pub budget_seconds: Option<u64>,
}

pub(crate) fn index(path: PathBuf, options: Options) -> Result<Vec<Vec<FamilyOutput>>> {
    run(path, options, Mode::Full)
}

pub(crate) fn reindex(path: PathBuf, options: Options) -> Result<Vec<Vec<FamilyOutput>>> {
    run(path, options, Mode::Changed)
}

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Full,
    Changed,
}

fn run(path: PathBuf, options: Options, mode: Mode) -> Result<Vec<Vec<FamilyOutput>>> {
    super::install_tracing(options.verbose, false);
    let workspace = workspace::Workspace::open_default()?;
    let manifest_path = std::path::absolute(workspace.manifest_path())?;
    let repos = workspace.resolve_repos(&path)?;
    if repos.is_empty() {
        bail!("no git repository found in {}", path.display());
    }
    let ontology = Ontology::load_embedded().context("failed to load embedded ontology")?;
    let mut outputs = Vec::new();
    for repo in repos {
        let git = match workspace::git_info(&repo) {
            Ok(git) => git,
            Err(error) => {
                manifest::record_git_info_failure(&manifest_path, &repo, &format!("{error:#}"));
                return Err(error);
            }
        };
        let state_dir = workspace.var_dir(git.project_id);
        let _state_lock = lock_incremental_state(&state_dir)?;
        let db_path = workspace::resolve_db_path(options.db.clone(), &git)?;
        let writer = GraphWriter::open(&db_path, &git)?;
        manifest::record_status(
            &manifest_path,
            &git,
            manifest::RepoStatus::Indexing,
            None,
            &db_path,
        )?;
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
        };
        let output = match mode {
            Mode::Full => project.index_all(),
            Mode::Changed => project.index_changed(),
        }
        .and_then(|output| writer.complete(&git.commit_sha).map(|()| output));
        let output = match output {
            Ok(output) => output,
            Err(error) => {
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

fn spawn_refresh(repo: &Path, db: Option<&Path>, files: &[String]) -> Option<Child> {
    let mut command = Command::new(std::env::current_exe().ok()?);
    command.arg("refresh").arg(repo);
    if let Some(db) = db {
        command.arg("--db").arg(db);
    }
    command
        .arg("--")
        .args(files)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    #[cfg(unix)]
    std::os::unix::process::CommandExt::process_group(&mut command, 0);
    command.spawn().ok()
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

pub(crate) fn refresh(
    repo: &std::path::Path,
    db: Option<PathBuf>,
    files: &[&str],
) -> Result<Freshness> {
    let workspace = workspace::Workspace::open_default()?;
    let manifest_path = std::path::absolute(workspace.manifest_path())?;
    let git = workspace::git_info(repo)?;
    if manifest::load_incremental_state(&manifest_path, git.project_id)?.is_none() {
        return Ok(Freshness::NotIncremental);
    }
    let state_dir = workspace.var_dir(git.project_id);
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

/// One refresh runs at a time and at most one waits behind it. The waiting
/// one refreshes every uncommitted edit, so edits made during a long refresh
/// land together in the next one.
pub(crate) fn refresh_now(repo: PathBuf, db: Option<PathBuf>, files: Vec<String>) -> Result<()> {
    let workspace = workspace::Workspace::open_default()?;
    let manifest_path = std::path::absolute(workspace.manifest_path())?;
    let git = workspace::git_info(&repo)?;
    let Some(saved) = manifest::load_incremental_state(&manifest_path, git.project_id)? else {
        return Ok(());
    };
    let state_dir = workspace.var_dir(git.project_id);
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
    let _state_lock = lock_incremental_state(&state_dir)?;
    let ontology = Ontology::load_embedded().context("failed to load embedded ontology")?;
    let owner = saved.owner.as_deref().and_then(SupportLang::from_family);
    let replacements = state::refresh_families(&git, &ontology, &state_dir, owner, changed)?;
    let writer = GraphWriter::open(&workspace::resolve_db_path(db, &git)?, &git)?;
    for replacement in &replacements {
        output::apply(&writer, replacement)?;
    }
    Ok(())
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
