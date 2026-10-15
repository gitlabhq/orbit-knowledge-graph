//! Cancels one code job when the pod's cgroup nears its memory limit, so the
//! kernel's OOM killer does not take the process and every co-scheduled job
//! with it. The victim is the in-flight job with the most repository bytes,
//! the newest on a tie, and the next kill waits until it has unwound.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError, Weak};
use std::time::Duration;

use code_graph::v2::CancellationToken;
use tracing::warn;

const KILL_AT_PERCENT: u128 = 90;
const INTERVAL: Duration = Duration::from_millis(100);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Pressure {
    pub used: u64,
    pub limit: u64,
}

struct Job {
    id: u64,
    bytes: u64,
    cancel: CancellationToken,
    killed: Arc<OnceLock<Pressure>>,
}

pub struct MemoryWatchdog {
    cgroup: PathBuf,
    jobs: Mutex<Vec<Job>>,
    next_id: AtomicU64,
    watching: AtomicBool,
}

/// Keeps the job eligible for cancellation; dropping it withdraws the job.
pub struct Registration {
    watchdog: Arc<MemoryWatchdog>,
    id: u64,
    killed: Arc<OnceLock<Pressure>>,
}

impl Registration {
    pub fn exceeded(&self) -> Option<Pressure> {
        self.killed.get().copied()
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        self.watchdog.jobs().retain(|job| job.id != self.id);
    }
}

impl MemoryWatchdog {
    pub fn for_this_process() -> Arc<Self> {
        Self::new(own_cgroup())
    }

    pub fn new(cgroup: PathBuf) -> Arc<Self> {
        Arc::new(Self {
            cgroup,
            jobs: Mutex::new(Vec::new()),
            next_id: AtomicU64::new(0),
            watching: AtomicBool::new(false),
        })
    }

    pub fn register(self: &Arc<Self>, bytes: u64, cancel: CancellationToken) -> Registration {
        let killed = Arc::new(OnceLock::new());
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.jobs().push(Job {
            id,
            bytes,
            cancel,
            killed: killed.clone(),
        });
        if !self.watching.swap(true, Ordering::AcqRel) {
            tokio::spawn(Self::watch(Arc::downgrade(self)));
        }
        Registration {
            watchdog: self.clone(),
            id,
            killed,
        }
    }

    async fn watch(watchdog: Weak<Self>) {
        loop {
            tokio::time::sleep(INTERVAL).await;
            let Some(watchdog) = watchdog.upgrade() else {
                return;
            };
            if let Some((pressure, bytes)) = watchdog.tick() {
                warn!(
                    used = pressure.used,
                    limit = pressure.limit,
                    repository_bytes = bytes,
                    "cancelling the largest code indexing job under pod memory pressure"
                );
            }
        }
    }

    /// One sample and at most one cancellation; returns the pressure and the
    /// victim's repository bytes when a job was cancelled.
    pub fn tick(&self) -> Option<(Pressure, u64)> {
        let pressure = sample(&self.cgroup)?;
        if u128::from(pressure.used) * 100 < u128::from(pressure.limit) * KILL_AT_PERCENT {
            return None;
        }
        let jobs = self.jobs();
        if jobs.iter().any(|job| job.killed.get().is_some()) {
            return None;
        }
        let victim = jobs.iter().max_by_key(|job| job.bytes)?;
        let _ = victim.killed.set(pressure);
        victim.cancel.cancel();
        Some((pressure, victim.bytes))
    }

    fn jobs(&self) -> std::sync::MutexGuard<'_, Vec<Job>> {
        self.jobs.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// cgroup v2: `memory.max` is a number or `max` (no limit), and `anon` in
/// `memory.stat` is the heap the kernel cannot reclaim.
fn sample(cgroup: &Path) -> Option<Pressure> {
    let limit = std::fs::read_to_string(cgroup.join("memory.max"))
        .ok()?
        .trim()
        .parse()
        .ok()?;
    let stat = std::fs::read_to_string(cgroup.join("memory.stat")).ok()?;
    let used = stat
        .lines()
        .find_map(|line| line.strip_prefix("anon "))?
        .trim()
        .parse()
        .ok()?;
    Some(Pressure { used, limit })
}

fn own_cgroup() -> PathBuf {
    let relative = std::fs::read_to_string("/proc/self/cgroup")
        .ok()
        .and_then(|cgroups| {
            cgroups
                .lines()
                .find_map(|line| line.strip_prefix("0::").map(str::to_owned))
        })
        .unwrap_or_default();
    Path::new("/sys/fs/cgroup").join(relative.trim_start_matches('/'))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cgroup(limit: &str, anon: u64) -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        set(&dir, limit, anon);
        dir
    }

    fn set(dir: &tempfile::TempDir, limit: &str, anon: u64) {
        std::fs::write(dir.path().join("memory.max"), format!("{limit}\n"))
            .unwrap_or_else(|e| panic!("{e}"));
        std::fs::write(
            dir.path().join("memory.stat"),
            format!("file 4096\nanon {anon}\nkernel 128\n"),
        )
        .unwrap_or_else(|e| panic!("{e}"));
    }

    #[tokio::test]
    async fn under_pressure_the_largest_newest_job_is_cancelled_and_then_waited_for() {
        let dir = cgroup("1000", 500);
        let watchdog = MemoryWatchdog::new(dir.path().to_path_buf());
        let (small, big, newest) = (
            CancellationToken::new(),
            CancellationToken::new(),
            CancellationToken::new(),
        );
        let _small = watchdog.register(10, small.clone());
        let _big = watchdog.register(50, big.clone());
        let newest_job = watchdog.register(50, newest.clone());

        assert_eq!(watchdog.tick(), None, "half the limit is not pressure");

        set(&dir, "1000", 900);
        let pressure = Pressure {
            used: 900,
            limit: 1000,
        };
        assert_eq!(watchdog.tick(), Some((pressure, 50)));
        assert!(newest.is_cancelled(), "the newest of the two largest jobs");
        assert!(!big.is_cancelled() && !small.is_cancelled());
        assert_eq!(newest_job.exceeded(), Some(pressure));

        assert_eq!(watchdog.tick(), None, "one cancellation unwinds at a time");
        drop(newest_job);
        assert_eq!(watchdog.tick(), Some((pressure, 50)));
        assert!(big.is_cancelled() && !small.is_cancelled());
    }

    #[tokio::test]
    async fn no_limit_and_no_jobs_never_cancel() {
        let dir = cgroup("max", u64::MAX / 2);
        let watchdog = MemoryWatchdog::new(dir.path().to_path_buf());
        let job = CancellationToken::new();
        let _job = watchdog.register(1, job.clone());
        assert_eq!(watchdog.tick(), None);
        assert!(!job.is_cancelled());

        let dir = cgroup("1000", 1000);
        let watchdog = MemoryWatchdog::new(dir.path().to_path_buf());
        assert_eq!(watchdog.tick(), None);
    }

    #[test]
    fn the_sample_reads_anon_against_the_limit() {
        let dir = cgroup("2048", 1536);
        assert_eq!(
            sample(dir.path()),
            Some(Pressure {
                used: 1536,
                limit: 2048
            })
        );
        assert_eq!(sample(Path::new("/nonexistent")), None);
    }
}
