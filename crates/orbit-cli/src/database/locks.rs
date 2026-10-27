use std::fmt;
use std::fs::{File, OpenOptions, TryLockError};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime};

use anyhow::Result;

/// How long a short critical section (manifest edits, refreshes) may wait for its lock.
pub const BRIEF_WAIT: Duration = Duration::from_secs(5);
/// How long reads and indexes wait on a running index, so a cold first use outlasts one.
const DEFAULT_WAIT: Duration = Duration::from_secs(600);
/// Exit code of a command that gave up waiting for a lock (`EX_TEMPFAIL`).
pub const WAIT_TIMED_OUT_EXIT_CODE: i32 = 75;
const FIRST_NOTICE_AFTER: Duration = Duration::from_secs(1);
const NOTICE_EVERY: Duration = Duration::from_secs(15);

pub fn wait_timeout() -> Duration {
    std::env::var("ORBIT_WAIT_TIMEOUT_MS")
        .ok()
        .and_then(|ms| ms.parse().ok())
        .map_or(DEFAULT_WAIT, Duration::from_millis)
}

#[derive(Debug)]
pub struct WaitTimedOut(pub String);

impl fmt::Display for WaitTimedOut {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for WaitTimedOut {}

pub fn lock_database(path: &Path) -> Result<File> {
    lock_database_waiting(path, BRIEF_WAIT)
}

pub fn lock_database_waiting(path: &Path, timeout: Duration) -> Result<File> {
    acquire_database_lock(path, false, timeout, None)
}

/// Takes a repository's state lock, which an index holds for its whole run. A wait on it is a
/// wait for that index, so the notice and the timeout name `repo` and the index's start time.
pub fn lock_index_state(state_dir: &Path, repo: &Path, timeout: Duration) -> Result<File> {
    acquire_database_lock(state_dir, false, timeout, Some(repo))
}

pub fn read_lock_database(path: &Path) -> Result<Option<File>> {
    if !path.try_exists()? {
        return Ok(None);
    }
    let lock = acquire_database_lock(path, true, wait_timeout(), Some(path))?;
    Ok(path.try_exists()?.then_some(lock))
}

pub fn index_is_running(state_dir: &Path) -> Result<bool> {
    match open_lock_file(state_dir)?.try_lock_shared() {
        Ok(()) => Ok(false),
        Err(TryLockError::WouldBlock) => Ok(true),
        Err(TryLockError::Error(error)) => Err(error.into()),
    }
}

fn open_lock_file(path: &Path) -> Result<File> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut lock_path = path.as_os_str().to_owned();
    lock_path.push(".lock");
    Ok(OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(PathBuf::from(lock_path))?)
}

/// `indexing` names what a waiter reports it is waiting to see indexed; the holder is then a
/// writer, so the lock file's mtime is the index's start.
fn acquire_database_lock(
    path: &Path,
    shared: bool,
    timeout: Duration,
    indexing: Option<&Path>,
) -> Result<File> {
    let file = open_lock_file(path)?;
    let started = Instant::now();
    let mut next_notice = FIRST_NOTICE_AFTER;
    loop {
        let result = if shared {
            file.try_lock_shared()
        } else {
            file.try_lock()
        };
        match result {
            Ok(()) => {
                if !shared {
                    let _ = file.set_modified(SystemTime::now());
                }
                return Ok(file);
            }
            Err(TryLockError::WouldBlock) => {
                let waited = started.elapsed();
                let target = match indexing {
                    Some(subject) => match held_for(&file) {
                        Some(age) => format!(
                            "the index of {} (started {} s ago)",
                            subject.display(),
                            age.as_secs()
                        ),
                        None => format!("the index of {}", subject.display()),
                    },
                    None => format!("{} to be released", path.display()),
                };
                if waited >= timeout {
                    return Err(WaitTimedOut(format!(
                        "gave up waiting for {target} after {} s. Retry, or raise ORBIT_WAIT_TIMEOUT_MS.",
                        waited.as_secs()
                    ))
                    .into());
                }
                if waited >= next_notice {
                    next_notice = waited + NOTICE_EVERY;
                    eprintln!("orbit: waiting for {target}");
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(TryLockError::Error(error)) => return Err(error.into()),
        }
    }
}

fn held_for(file: &File) -> Option<Duration> {
    file.metadata().ok()?.modified().ok()?.elapsed().ok()
}
