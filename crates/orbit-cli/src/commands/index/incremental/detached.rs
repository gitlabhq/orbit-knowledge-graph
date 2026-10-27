use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result};

use super::{Options, detached_command};
use crate::database::{WAIT_TIMED_OUT_EXIT_CODE, WaitTimedOut, index_is_running, wait_timeout};
use crate::workspace;

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) struct DetachedIndex {
    pub pid: u32,
    pub status: &'static str,
    pub log: PathBuf,
}

pub(super) struct ReadyFile(pub(super) PathBuf);

impl ReadyFile {
    pub(super) fn signal(&self) -> Result<()> {
        std::fs::write(&self.0, "").context("failed to signal that the index holds its locks")
    }
}

impl Drop for ReadyFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// Returns once the child holds the first repository's locks, or at once when it is queued. Only
/// the child can say so: a reader started after this returns must wait for it, not read the old
/// graph.
pub(crate) fn index_detached(path: &Path, options: &Options) -> Result<DetachedIndex> {
    let workspace = workspace::Workspace::open_default()?;
    let repos = workspace.resolve_repos(path)?;
    if repos.is_empty() {
        let folder = dunce::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
        return Err(super::super::NoRepositoryFound(folder).into());
    }
    let state_dirs = repos
        .iter()
        .map(|repo| Ok(workspace.var_dir(workspace::git_info(repo)?.project_id)))
        .collect::<Result<Vec<PathBuf>>>()?;
    let queued = state_dirs
        .iter()
        .map(|dir| index_is_running(dir))
        .collect::<Result<Vec<_>>>()?
        .contains(&true);
    let dir = &state_dirs[0];
    std::fs::create_dir_all(dir)?;
    let log = dir.join("detached-index.log");
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos());
    let ready = ReadyFile(dir.join(format!("detached-{}-{nonce}.ready", std::process::id())));

    let mut command = detached_command().context("cannot locate the orbit executable")?;
    command
        .args(["workspace", "index", "--wait", "--ready-file"])
        .arg(&ready.0)
        .arg(path)
        .stderr(
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&log)
                .context("failed to open the detached index log")?,
        );
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
    if queued {
        return Ok(started("queued"));
    }

    let timeout = HANDSHAKE_TIMEOUT.min(wait_timeout());
    let began = Instant::now();
    while !ready.0.exists() {
        if let Some(status) = child.try_wait()? {
            if status.success() {
                return Ok(started("indexed"));
            }
            let message = format!(
                "the background index exited ({status}); see {}",
                log.display()
            );
            return Err(if status.code() == Some(WAIT_TIMED_OUT_EXIT_CODE) {
                WaitTimedOut(message).into()
            } else {
                anyhow::anyhow!(message)
            });
        }
        if began.elapsed() >= timeout {
            return Err(WaitTimedOut(format!(
                "the background index (pid {pid}) is still waiting for readers of its revision; \
                 see {}",
                log.display()
            ))
            .into());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    Ok(started("indexing"))
}
