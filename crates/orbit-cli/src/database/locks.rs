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
pub struct WaitTimedOut {
    path: PathBuf,
    waited: Duration,
    indexing_for: Option<Duration>,
}

impl fmt::Display for WaitTimedOut {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let waited = self.waited.as_secs();
        match self.indexing_for {
            Some(started_ago) => write!(
                f,
                "{} is still indexing (started {} s ago); gave up after waiting {waited} s. \
                 Retry, or raise ORBIT_WAIT_TIMEOUT_MS.",
                self.path.display(),
                started_ago.as_secs()
            ),
            None => write!(
                f,
                "local database {} is busy; gave up after waiting {waited} s. \
                 Retry when its readers or writer finish.",
                self.path.display()
            ),
        }
    }
}

impl std::error::Error for WaitTimedOut {}

pub fn lock_database(path: &Path) -> Result<File> {
    lock_database_waiting(path, BRIEF_WAIT)
}

pub fn lock_database_waiting(path: &Path, timeout: Duration) -> Result<File> {
    acquire_database_lock(path, false, timeout)
}

pub fn read_lock_database(path: &Path) -> Result<Option<File>> {
    if !path.try_exists()? {
        return Ok(None);
    }
    let lock = acquire_database_lock(path, true, wait_timeout())?;
    Ok(path.try_exists()?.then_some(lock))
}

pub fn index_is_running(state_dir: &Path) -> Result<bool> {
    let file = open_lock_file(state_dir)?;
    match file.try_lock_shared() {
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

fn acquire_database_lock(path: &Path, shared: bool, timeout: Duration) -> Result<File> {
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
                    // A blocked reader reads the holder's start time from the file's mtime.
                    let _ = file.set_modified(SystemTime::now());
                }
                return Ok(file);
            }
            Err(TryLockError::WouldBlock) => {
                let waited = started.elapsed();
                // Only a writer blocks a reader, so the mtime is the running index's start.
                let indexing_for = shared.then(|| held_for(&file)).flatten();
                if waited >= timeout {
                    return Err(WaitTimedOut {
                        path: path.to_path_buf(),
                        waited,
                        indexing_for,
                    }
                    .into());
                }
                if waited >= next_notice {
                    next_notice = waited + NOTICE_EVERY;
                    announce(path, indexing_for);
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

fn announce(path: &Path, indexing_for: Option<Duration>) {
    match indexing_for {
        Some(started_ago) => eprintln!(
            "orbit: waiting for the index of {} (started {} s ago)",
            path.display(),
            started_ago.as_secs()
        ),
        None => eprintln!("orbit: waiting for {} to be released", path.display()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_reader_times_out_with_a_typed_error_naming_the_index_age() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("graph.duckdb");
        std::fs::write(&db, "").unwrap();
        let _writer = lock_database(&db).unwrap();

        let error = acquire_database_lock(&db, true, Duration::from_millis(100)).unwrap_err();

        let timed_out = error.downcast_ref::<WaitTimedOut>().expect("typed error");
        assert!(timed_out.indexing_for.is_some());
        assert!(error.to_string().contains("is still indexing (started"));
    }

    #[test]
    fn a_writer_waits_for_readers_to_leave() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("graph.duckdb");
        std::fs::write(&db, "").unwrap();
        let reader = read_lock_database(&db).unwrap().unwrap();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(200));
            drop(reader);
        });

        lock_database_waiting(&db, Duration::from_secs(5)).unwrap();
    }

    #[test]
    fn the_state_lock_reports_a_running_index() {
        let dir = tempfile::tempdir().unwrap();
        let state = dir.path().join("var/1");
        assert!(!index_is_running(&state).unwrap());
        let _index = lock_database(&state).unwrap();
        assert!(index_is_running(&state).unwrap());
    }
}
