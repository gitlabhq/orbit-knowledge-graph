//! Cancels one code job when the pod's cgroup nears its memory limit, so the
//! kernel's OOM killer does not take the process and every co-scheduled job
//! with it. The victim is the in-flight job with the most repository bytes,
//! the newest on a tie, and the next kill waits until it has unwound.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError, Weak};
use std::time::Duration;

use code_graph::v2::CancellationToken;
use tracing::{info, warn};

use super::metrics::CodeMetrics;

const KILL_AT_PERCENT: u128 = 90;
const INTERVAL: Duration = Duration::from_millis(100);
const UNREADABLE_SAMPLES_BEFORE_GIVING_UP: u32 = 10;
/// cgroup v1 reports "no limit" as PAGE_COUNTER_MAX pages.
const V1_NO_LIMIT: u64 = 1 << 62;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Pressure {
    pub used: u64,
    pub limit: u64,
}

/// What a cancelled job sees: the pressure at the kill, and whether it was
/// the only job in flight, which decides whether the attempt counts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MemoryKill {
    pub pressure: Pressure,
    pub alone: bool,
}

#[derive(Debug, PartialEq, Eq)]
enum Sample {
    Pressure(Pressure),
    NoLimit,
    Unreadable,
}

/// The files to read: cgroup v2 (`memory.max`, `anon` in `memory.stat`) or
/// v1 (`memory.limit_in_bytes`, `rss` in `memory.stat`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Cgroup {
    dir: PathBuf,
    v1: bool,
}

struct Job {
    id: u64,
    bytes: u64,
    cancel: CancellationToken,
    killed: Arc<OnceLock<MemoryKill>>,
}

pub struct MemoryWatchdog {
    cgroup: Cgroup,
    metrics: CodeMetrics,
    jobs: Mutex<Vec<Job>>,
    next_id: AtomicU64,
    watching: AtomicBool,
}

/// Keeps the job eligible for cancellation; dropping it withdraws the job.
pub struct Registration {
    watchdog: Arc<MemoryWatchdog>,
    id: u64,
    killed: Arc<OnceLock<MemoryKill>>,
}

impl Registration {
    pub fn exceeded(&self) -> Option<MemoryKill> {
        self.killed.get().copied()
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        self.watchdog.jobs().retain(|job| job.id != self.id);
    }
}

impl MemoryWatchdog {
    pub fn for_this_process(metrics: CodeMetrics) -> Arc<Self> {
        Self::new(own_cgroup(), metrics)
    }

    pub fn new(cgroup: Cgroup, metrics: CodeMetrics) -> Arc<Self> {
        Arc::new(Self {
            cgroup,
            metrics,
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
        let mut armed = false;
        let mut unreadable = 0;
        loop {
            let Some(watchdog) = watchdog.upgrade() else {
                return;
            };
            let cgroup = &watchdog.cgroup;
            match sample(cgroup) {
                Sample::Pressure(pressure) => {
                    if !armed {
                        info!(cgroup = %cgroup.dir.display(), limit = pressure.limit, "memory watchdog armed");
                        armed = true;
                    }
                    if let Some((kill, bytes)) = watchdog.tick(pressure) {
                        warn!(
                            used = kill.pressure.used,
                            limit = kill.pressure.limit,
                            repository_bytes = bytes,
                            alone = kill.alone,
                            "cancelling the largest code indexing job under pod memory pressure"
                        );
                    }
                }
                Sample::NoLimit => {
                    warn!(cgroup = %cgroup.dir.display(), "memory watchdog inert: this cgroup has no memory limit");
                    return;
                }
                Sample::Unreadable => {
                    unreadable += 1;
                    if unreadable >= UNREADABLE_SAMPLES_BEFORE_GIVING_UP {
                        warn!(cgroup = %cgroup.dir.display(), "memory watchdog inert: cannot read the cgroup memory files");
                        return;
                    }
                }
            }
            drop(watchdog);
            tokio::time::sleep(INTERVAL).await;
        }
    }

    /// At most one cancellation per sample; returns the kill and the victim's
    /// repository bytes when a job was cancelled.
    fn tick(&self, pressure: Pressure) -> Option<(MemoryKill, u64)> {
        if u128::from(pressure.used) * 100 < u128::from(pressure.limit) * KILL_AT_PERCENT {
            return None;
        }
        let jobs = self.jobs();
        if jobs.iter().any(|job| job.killed.get().is_some()) {
            return None;
        }
        let victim = jobs.iter().max_by_key(|job| job.bytes)?;
        let kill = MemoryKill {
            pressure,
            alone: jobs.len() == 1,
        };
        let _ = victim.killed.set(kill);
        victim.cancel.cancel();
        self.metrics.record_stage_error("memory_pressure");
        Some((kill, victim.bytes))
    }

    fn jobs(&self) -> std::sync::MutexGuard<'_, Vec<Job>> {
        self.jobs.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

fn sample(cgroup: &Cgroup) -> Sample {
    let (limit_file, usage_key) = if cgroup.v1 {
        ("memory.limit_in_bytes", "rss ")
    } else {
        ("memory.max", "anon ")
    };
    let Ok(limit) = std::fs::read_to_string(cgroup.dir.join(limit_file)) else {
        return Sample::Unreadable;
    };
    let limit = match limit.trim().parse::<u64>() {
        Ok(limit) if limit < V1_NO_LIMIT => limit,
        Ok(_) => return Sample::NoLimit,
        Err(_) if limit.trim() == "max" => return Sample::NoLimit,
        Err(_) => return Sample::Unreadable,
    };
    let Ok(stat) = std::fs::read_to_string(cgroup.dir.join("memory.stat")) else {
        return Sample::Unreadable;
    };
    match stat
        .lines()
        .find_map(|line| line.strip_prefix(usage_key))
        .and_then(|value| value.trim().parse().ok())
    {
        Some(used) => Sample::Pressure(Pressure { used, limit }),
        None => Sample::Unreadable,
    }
}

/// The process's own cgroup from `/proc/self/cgroup`: the `0::` line names
/// the v2 path (`/` inside a container); otherwise the line that lists the
/// v1 `memory` controller names the v1 path.
fn own_cgroup() -> Cgroup {
    let cgroups = std::fs::read_to_string("/proc/self/cgroup").unwrap_or_default();
    cgroup_from(&cgroups, Path::new("/sys/fs/cgroup"))
}

fn cgroup_from(cgroups: &str, root: &Path) -> Cgroup {
    let mut v2 = None;
    let mut v1 = None;
    for line in cgroups.lines() {
        let mut fields = line.splitn(3, ':');
        let (Some(_), Some(controllers), Some(path)) =
            (fields.next(), fields.next(), fields.next())
        else {
            continue;
        };
        if controllers.is_empty() {
            v2 = Some(path);
        } else if controllers.split(',').any(|c| c == "memory") {
            v1 = Some(path);
        }
    }
    let relative = |path: &str| path.trim_start_matches('/').to_owned();
    let v2 = v2.map(|path| root.join(relative(path)));
    match (v2, v1) {
        (Some(dir), None) => Cgroup { dir, v1: false },
        (Some(dir), Some(_)) if dir.join("memory.max").exists() => Cgroup { dir, v1: false },
        (_, Some(path)) => Cgroup {
            dir: root.join("memory").join(relative(path)),
            v1: true,
        },
        (None, None) => Cgroup {
            dir: root.to_path_buf(),
            v1: false,
        },
    }
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

    fn v2(dir: &tempfile::TempDir) -> Cgroup {
        Cgroup {
            dir: dir.path().to_path_buf(),
            v1: false,
        }
    }

    fn watchdog(dir: &tempfile::TempDir) -> Arc<MemoryWatchdog> {
        MemoryWatchdog::new(v2(dir), CodeMetrics::new())
    }

    fn pressure(dir: &tempfile::TempDir) -> Pressure {
        match sample(&v2(dir)) {
            Sample::Pressure(pressure) => pressure,
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn under_pressure_the_largest_newest_job_is_cancelled_and_then_waited_for() {
        let dir = cgroup("1000", 500);
        let watchdog = watchdog(&dir);
        let (small, big, newest) = (
            CancellationToken::new(),
            CancellationToken::new(),
            CancellationToken::new(),
        );
        let _small = watchdog.register(10, small.clone());
        let _big = watchdog.register(50, big.clone());
        let newest_job = watchdog.register(50, newest.clone());

        assert_eq!(
            watchdog.tick(pressure(&dir)),
            None,
            "half the limit is not pressure"
        );

        set(&dir, "1000", 900);
        let kill = MemoryKill {
            pressure: Pressure {
                used: 900,
                limit: 1000,
            },
            alone: false,
        };
        assert_eq!(watchdog.tick(pressure(&dir)), Some((kill, 50)));
        assert!(newest.is_cancelled(), "the newest of the two largest jobs");
        assert!(!big.is_cancelled() && !small.is_cancelled());
        assert_eq!(newest_job.exceeded(), Some(kill));

        assert_eq!(
            watchdog.tick(pressure(&dir)),
            None,
            "one cancellation unwinds at a time"
        );
        drop(newest_job);
        assert_eq!(watchdog.tick(pressure(&dir)), Some((kill, 50)));
        assert!(big.is_cancelled() && !small.is_cancelled());
    }

    #[tokio::test]
    async fn a_job_killed_alone_is_told_so() {
        let dir = cgroup("1000", 950);
        let watchdog = watchdog(&dir);
        let only = CancellationToken::new();
        let job = watchdog.register(7, only.clone());
        let (kill, bytes) = watchdog
            .tick(pressure(&dir))
            .unwrap_or_else(|| panic!("a kill"));
        assert!(kill.alone && only.is_cancelled());
        assert_eq!((bytes, job.exceeded()), (7, Some(kill)));
    }

    #[tokio::test]
    async fn no_jobs_means_nothing_to_cancel() {
        let dir = cgroup("1000", 1000);
        assert_eq!(watchdog(&dir).tick(pressure(&dir)), None);
    }

    #[test]
    fn samples_read_v2_and_v1_files_and_tell_no_limit_from_unreadable() {
        let dir = cgroup("2048", 1536);
        assert_eq!(
            sample(&v2(&dir)),
            Sample::Pressure(Pressure {
                used: 1536,
                limit: 2048
            })
        );
        set(&dir, "max", 1);
        assert_eq!(sample(&v2(&dir)), Sample::NoLimit);
        assert_eq!(
            sample(&Cgroup {
                dir: PathBuf::from("/nonexistent"),
                v1: false
            }),
            Sample::Unreadable
        );

        let v1 = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let v1_cgroup = Cgroup {
            dir: v1.path().to_path_buf(),
            v1: true,
        };
        std::fs::write(v1.path().join("memory.limit_in_bytes"), "4096\n")
            .unwrap_or_else(|e| panic!("{e}"));
        std::fs::write(
            v1.path().join("memory.stat"),
            "cache 10\nrss 3000\ntotal_rss 3000\n",
        )
        .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(
            sample(&v1_cgroup),
            Sample::Pressure(Pressure {
                used: 3000,
                limit: 4096
            })
        );
        std::fs::write(
            v1.path().join("memory.limit_in_bytes"),
            "9223372036854771712\n",
        )
        .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(sample(&v1_cgroup), Sample::NoLimit);
    }

    #[test]
    fn the_own_cgroup_follows_the_v2_line_and_falls_back_to_the_v1_memory_controller() {
        let root = tempfile::tempdir().unwrap_or_else(|e| panic!("{e}"));
        let scope = root.path().join("user.slice/run-1.scope");
        std::fs::create_dir_all(&scope).unwrap_or_else(|e| panic!("{e}"));
        std::fs::write(scope.join("memory.max"), "1\n").unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(
            cgroup_from("0::/user.slice/run-1.scope\n", root.path()),
            Cgroup {
                dir: scope,
                v1: false
            }
        );
        assert_eq!(
            cgroup_from("0::/\n", root.path()),
            Cgroup {
                dir: root.path().to_path_buf(),
                v1: false
            }
        );
        assert_eq!(
            cgroup_from(
                "11:cpu,cpuacct:/kubepods/pod1\n10:memory:/kubepods/pod1\n1:name=systemd:/x\n",
                root.path()
            ),
            Cgroup {
                dir: root.path().join("memory/kubepods/pod1"),
                v1: true
            }
        );
    }
}
