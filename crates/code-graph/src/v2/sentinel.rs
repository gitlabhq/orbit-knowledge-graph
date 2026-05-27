//! Watchdog sentinel for per-file timeout enforcement.
//!
//! The sentinel runs on a dedicated thread, monitoring active files
//! across all pipeline phases. When a file exceeds its timeout, the
//! sentinel sets its kill flag, causing the worker to bail out at the
//! next check point via `Result<T, Killed>`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// Error returned when the sentinel kills a file's processing.
#[derive(Debug)]
pub struct Killed;

/// Message from a worker thread to the sentinel.
pub enum SentinelMsg {
    /// A file has started processing. Includes a kill flag the sentinel
    /// can set to abort the file.
    FileStart {
        id: u64,
        path: String,
        kill: Arc<AtomicBool>,
    },
    /// A file has finished processing (success or failure).
    FileDone { id: u64 },
    /// Shut down the sentinel thread.
    Shutdown,
}

/// Handle for workers to communicate with the sentinel.
#[derive(Clone)]
pub struct SentinelHandle {
    tx: crossbeam_channel::Sender<SentinelMsg>,
    next_id: Arc<std::sync::atomic::AtomicU64>,
}

impl SentinelHandle {
    /// Register a file as active. Returns a `FileGuard` that automatically
    /// sends `FileDone` on drop and provides the kill flag for checking.
    pub fn file_start(&self, path: &str) -> FileGuard {
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let kill = Arc::new(AtomicBool::new(false));
        let _ = self.tx.send(SentinelMsg::FileStart {
            id,
            path: path.to_string(),
            kill: kill.clone(),
        });
        FileGuard {
            id,
            kill,
            tx: self.tx.clone(),
        }
    }

    /// Signal the sentinel to shut down.
    pub fn shutdown(&self) {
        let _ = self.tx.send(SentinelMsg::Shutdown);
    }
}

/// RAII guard for a file being processed. Sends `FileDone` on drop
/// and provides the kill flag for cooperative cancellation.
pub struct FileGuard {
    id: u64,
    kill: Arc<AtomicBool>,
    tx: crossbeam_channel::Sender<SentinelMsg>,
}

impl FileGuard {
    /// Check if the sentinel has killed this file's processing.
    #[inline]
    pub fn is_killed(&self) -> bool {
        self.kill.load(Ordering::Relaxed)
    }

    /// Get a clone of the kill flag for passing to other structs.
    pub fn kill_flag(&self) -> Arc<AtomicBool> {
        self.kill.clone()
    }
}

impl Drop for FileGuard {
    fn drop(&mut self) {
        let _ = self.tx.send(SentinelMsg::FileDone { id: self.id });
    }
}

struct ActiveFile {
    _path: String,
    started_at: Instant,
    kill: Arc<AtomicBool>,
}

/// Spawn the sentinel thread. Returns a handle for workers and a
/// `JoinHandle` for the caller to join on shutdown.
pub fn spawn_sentinel(timeout: Duration) -> Option<(SentinelHandle, std::thread::JoinHandle<()>)> {
    let (tx, rx) = crossbeam_channel::unbounded();
    let handle = SentinelHandle {
        tx,
        next_id: Arc::new(std::sync::atomic::AtomicU64::new(0)),
    };

    let join = std::thread::Builder::new()
        .name("sentinel".into())
        .spawn(move || {
            let mut active: Vec<(u64, ActiveFile)> = Vec::new();
            let poll_interval = Duration::from_millis(10);

            loop {
                match rx.recv_timeout(poll_interval) {
                    Ok(SentinelMsg::FileStart { id, path, kill }) => {
                        active.push((
                            id,
                            ActiveFile {
                                _path: path,
                                started_at: Instant::now(),
                                kill,
                            },
                        ));
                    }
                    Ok(SentinelMsg::FileDone { id }) => {
                        active.retain(|(fid, _)| *fid != id);
                    }
                    Ok(SentinelMsg::Shutdown) => break,
                    Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                    Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
                }

                // Check for stalled files
                let now = Instant::now();
                for (_, file) in &active {
                    if now.duration_since(file.started_at) > timeout
                        && !file.kill.load(Ordering::Relaxed)
                    {
                        file.kill.store(true, Ordering::Relaxed);
                    }
                }
            }
        });

    match join {
        Ok(join) => Some((handle, join)),
        Err(e) => {
            tracing::warn!("failed to spawn sentinel thread: {e}, running without file timeouts");
            None
        }
    }
}
