//! Watchdog sentinel for per-file timeout enforcement.
//!
//! When a file exceeds its timeout, the sentinel sets its kill flag, causing
//! the worker to bail out at the next check point via `Result<T, Killed>`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

#[derive(Debug)]
pub struct Killed;

pub enum SentinelMsg {
    /// Includes a kill flag the sentinel can set to abort the file.
    FileStart {
        id: u64,
        path: String,
        kill: Arc<AtomicBool>,
    },
    FileDone {
        id: u64,
    },
    Shutdown,
}

#[derive(Clone)]
pub struct SentinelHandle {
    tx: crossbeam_channel::Sender<SentinelMsg>,
    next_id: Arc<std::sync::atomic::AtomicU64>,
}

impl SentinelHandle {
    /// Returns a `FileGuard` that sends `FileDone` on drop.
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

    pub fn shutdown(&self) {
        let _ = self.tx.send(SentinelMsg::Shutdown);
    }
}

/// Sends `FileDone` on drop.
pub struct FileGuard {
    id: u64,
    kill: Arc<AtomicBool>,
    tx: crossbeam_channel::Sender<SentinelMsg>,
}

impl FileGuard {
    #[inline]
    pub fn is_killed(&self) -> bool {
        self.kill.load(Ordering::Relaxed)
    }

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
