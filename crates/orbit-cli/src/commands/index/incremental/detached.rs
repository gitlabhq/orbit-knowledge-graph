use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result};

use super::{Options, detached_command};
use crate::database::{index_is_running, wait_timeout};
use crate::workspace;

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);
const KEPT_FILES: usize = 4;

pub(crate) enum DetachedStatus {
    Indexing,
    Indexed,
    Queued,
}

pub(crate) struct DetachedIndex {
    pub pid: u32,
    pub status: DetachedStatus,
    pub log: PathBuf,
}

/// The child still waits for its locks; only the caller gives up.
#[derive(Debug)]
pub(crate) struct HandshakeTimedOut {
    pid: u32,
    waited: Duration,
    log: PathBuf,
}

impl std::fmt::Display for HandshakeTimedOut {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "the background index (pid {}) is still waiting for readers of its revision after {} s \
             and keeps waiting; see {}",
            self.pid,
            self.waited.as_secs(),
            self.log.display()
        )
    }
}

impl std::error::Error for HandshakeTimedOut {}

/// Created by the detached child once it holds a repository's locks and the manifest shows
/// `indexing`. It holds the child's pid, written to a temporary name and renamed, so a reader
/// of the file never sees a partial one. The child removes it when it finishes, including
/// after a parent that timed out or died.
pub(super) struct ReadyFile {
    path: PathBuf,
    signalled: bool,
}

impl ReadyFile {
    pub(super) fn new(path: &Path) -> Self {
        Self {
            path: path.to_path_buf(),
            signalled: false,
        }
    }

    pub(super) fn signal(&mut self) -> Result<()> {
        if std::mem::replace(&mut self.signalled, true) {
            return Ok(());
        }
        let staged = self.path.with_extension("ready.tmp");
        std::fs::write(&staged, std::process::id().to_string())
            .and_then(|()| std::fs::rename(&staged, &self.path))
            .context("failed to signal that the index holds its locks")
    }
}

impl Drop for ReadyFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn signalled_by(ready: &Path, pid: u32) -> bool {
    std::fs::read_to_string(ready).is_ok_and(|content| content.trim() == pid.to_string())
}

/// Starts `orbit workspace index --wait` detached and returns once that child reports, through
/// a ready file, that it holds a repository's state and revision locks. Only the child can
/// say so: a lock held by another index or by a reader says nothing about it, and a reader
/// started after this returns must wait for the child instead of reading the old graph. If
/// another index already holds a repository's state lock, the child is queued behind it and
/// this returns at once. Waiting for open readers can delay the return, up to the handshake
/// timeout. Indexing a directory of repositories signals on whichever repository the child
/// locks first.
pub(crate) fn index_detached(path: &Path, options: &Options) -> Result<DetachedIndex> {
    let workspace = workspace::Workspace::open_default()?;
    let repos = workspace.resolve_repos(path)?;
    if repos.is_empty() {
        let folder = dunce::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
        return Err(super::super::NoRepositoryFound(folder).into());
    }
    let state_dirs: Vec<PathBuf> = repos
        .iter()
        .filter_map(|repo| workspace::git_info(repo).ok())
        .map(|git| workspace.var_dir(git.project_id))
        .collect();
    let busy = state_dirs.iter().try_fold(false, |busy, dir| {
        index_is_running(dir).map(|running| busy || running)
    })?;
    let files = state_dirs
        .first()
        .context("cannot find the repository's state directory")?;
    std::fs::create_dir_all(files)?;
    prune(files, "log");
    prune(files, "ready");
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos());
    let stem = format!("detached-{nonce}-{}", std::process::id());
    let log = files.join(format!("{stem}.log"));
    let ready = files.join(format!("{stem}.ready"));

    let mut command = detached_command().context("cannot locate the orbit executable")?;
    command
        .args(["workspace", "index", "--wait", "--ready-file"])
        .arg(&ready)
        .arg(path)
        .stderr(std::fs::File::create(&log).context("failed to open the detached index log")?);
    if let Some(db) = &options.db {
        command.arg("--db").arg(db);
    }
    if let Some(seconds) = options.budget_seconds {
        command.arg("--budget").arg(seconds.to_string());
    }
    let mut child = command
        .spawn()
        .context("failed to start the background index")?;
    let pid = child.id();
    let started = |status| DetachedIndex {
        pid,
        status,
        log: log.clone(),
    };
    if busy {
        return Ok(started(DetachedStatus::Queued));
    }

    let _cleanup = ReadyFile::new(&ready);
    let timeout = HANDSHAKE_TIMEOUT.min(wait_timeout());
    let began = Instant::now();
    loop {
        if signalled_by(&ready, pid) {
            return Ok(started(DetachedStatus::Indexing));
        }
        if let Some(status) = child.try_wait()? {
            anyhow::ensure!(
                status.success(),
                "the background index exited early ({status}); see {}",
                log.display()
            );
            return Ok(started(DetachedStatus::Indexed));
        }
        if began.elapsed() >= timeout {
            return Err(HandshakeTimedOut {
                pid,
                waited: began.elapsed(),
                log,
            }
            .into());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Keeps the newest few `detached-*.<extension>` files, which sort by creation time.
fn prune(dir: &Path, extension: &str) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    let mut files: Vec<PathBuf> = entries
        .filter_map(|entry| Some(entry.ok()?.path()))
        .filter(|path| {
            path.extension().is_some_and(|ext| ext == extension)
                && path
                    .file_name()
                    .is_some_and(|name| name.to_string_lossy().starts_with("detached-"))
        })
        .collect();
    files.sort();
    let excess = files.len().saturating_sub(KEPT_FILES);
    for stale in &files[..excess] {
        let _ = std::fs::remove_file(stale);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_ready_file_counts_only_for_the_pid_that_wrote_it() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("detached-1-2.ready");
        let mut ready = ReadyFile::new(&path);
        assert!(!signalled_by(&path, std::process::id()));

        ready.signal().unwrap();

        assert!(signalled_by(&path, std::process::id()));
        assert!(!signalled_by(&path, std::process::id() + 1));
        std::fs::write(&path, "999999").unwrap();
        assert!(!signalled_by(&path, std::process::id()));
    }

    #[test]
    fn a_finished_child_removes_its_ready_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("detached-1-2.ready");
        let mut ready = ReadyFile::new(&path);
        ready.signal().unwrap();
        assert!(path.exists());

        drop(ready);

        assert!(!path.exists());
    }

    #[test]
    fn pruning_keeps_the_newest_files_of_one_kind() {
        let dir = tempfile::tempdir().unwrap();
        for n in 0..7 {
            std::fs::write(dir.path().join(format!("detached-{n}-1.log")), "").unwrap();
        }
        std::fs::write(dir.path().join("detached-0-1.ready"), "").unwrap();

        prune(dir.path(), "log");

        let mut kept: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        kept.sort();
        assert_eq!(
            kept,
            [
                "detached-0-1.ready",
                "detached-3-1.log",
                "detached-4-1.log",
                "detached-5-1.log",
                "detached-6-1.log"
            ]
        );
    }
}
