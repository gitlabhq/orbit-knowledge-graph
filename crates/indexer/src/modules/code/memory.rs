use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, PoisonError, Weak};
use std::time::Duration;

use code_graph::v2::CancellationToken;
use code_graph_incremental::{Admission, Killed, Sentinel};
use orbit_server_config::resources::{MemoryPressure, cgroup_memory};
use tracing::warn;

use super::metrics::CodeMetrics;

const INFLIGHT_SOURCE_BYTES: u64 = 4 * 1024 * 1024;
const INTERVAL: Duration = Duration::from_millis(100);

pub(super) fn budget() -> Arc<dyn Admission> {
    Arc::new(InflightBudget::new(INFLIGHT_SOURCE_BYTES))
}

struct InflightBudget {
    budget: u64,
    used: Mutex<u64>,
    changed: Condvar,
}

impl InflightBudget {
    fn new(budget: u64) -> Self {
        Self {
            budget,
            used: Mutex::new(0),
            changed: Condvar::new(),
        }
    }

    fn used(&self) -> MutexGuard<'_, u64> {
        self.used.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Admission for InflightBudget {
    fn admit(&self, bytes: u64, run: &Sentinel) -> Result<(), Killed> {
        let mut used = self.used();
        while *used != 0 && *used + bytes > self.budget {
            run.check()?;
            used = self
                .changed
                .wait(used)
                .unwrap_or_else(PoisonError::into_inner);
        }
        *used += bytes;
        Ok(())
    }

    fn release(&self, bytes: u64) {
        *self.used() -= bytes;
        self.changed.notify_all();
    }
}

struct Job {
    bytes: u64,
    cancel: CancellationToken,
    killed: Arc<OnceLock<MemoryPressure>>,
}

pub struct MemoryWatchdog {
    sample: Box<dyn Fn() -> Option<MemoryPressure> + Send + Sync>,
    kill_at_percent: u64,
    metrics: CodeMetrics,
    jobs: Mutex<Vec<Job>>,
    used_at_last_kill: AtomicU64,
    watching: AtomicBool,
}

pub struct Registration {
    watchdog: Arc<MemoryWatchdog>,
    killed: Arc<OnceLock<MemoryPressure>>,
}

impl Registration {
    pub fn exceeded(&self) -> Option<MemoryPressure> {
        self.killed.get().copied()
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        self.watchdog
            .jobs()
            .retain(|job| !Arc::ptr_eq(&job.killed, &self.killed));
    }
}

impl MemoryWatchdog {
    pub fn for_this_process(metrics: CodeMetrics, kill_at_percent: u64) -> Arc<Self> {
        if cgroup_memory().is_none() {
            warn!("memory watchdog inert: this process has no cgroup memory limit");
            return Self::new(Box::new(|| None), metrics, 0);
        }
        Self::new(Box::new(cgroup_memory), metrics, kill_at_percent)
    }

    pub fn new(
        sample: Box<dyn Fn() -> Option<MemoryPressure> + Send + Sync>,
        metrics: CodeMetrics,
        kill_at_percent: u64,
    ) -> Arc<Self> {
        Arc::new(Self {
            sample,
            kill_at_percent: kill_at_percent.min(100),
            metrics,
            jobs: Mutex::new(Vec::new()),
            used_at_last_kill: AtomicU64::new(0),
            watching: AtomicBool::new(false),
        })
    }

    pub fn register(self: &Arc<Self>, bytes: u64, cancel: CancellationToken) -> Registration {
        let killed = Arc::new(OnceLock::new());
        let mut jobs = self.jobs();
        jobs.push(Job {
            bytes,
            cancel,
            killed: killed.clone(),
        });
        if self.kill_at_percent > 0 && !self.watching.swap(true, Ordering::AcqRel) {
            tokio::spawn(Self::watch(Arc::downgrade(self)));
        }
        Registration {
            watchdog: self.clone(),
            killed,
        }
    }

    async fn watch(watchdog: Weak<Self>) {
        while let Some(this) = watchdog.upgrade() {
            if let Some(pressure) = (this.sample)() {
                this.tick(pressure);
            }
            if this.unwatch_when_idle() {
                return;
            }
            drop(this);
            tokio::time::sleep(INTERVAL).await;
        }
    }

    fn unwatch_when_idle(&self) -> bool {
        let jobs = self.jobs();
        let idle = jobs.is_empty();
        if idle {
            self.watching.store(false, Ordering::Release);
        }
        idle
    }

    fn tick(&self, pressure: MemoryPressure) -> Option<u64> {
        if self.kill_at_percent == 0
            || u128::from(pressure.used) * 100
                < u128::from(pressure.limit) * u128::from(self.kill_at_percent)
        {
            self.used_at_last_kill.store(0, Ordering::Relaxed);
            return None;
        }
        let jobs = self.jobs();
        let last_kill = self.used_at_last_kill.load(Ordering::Relaxed);
        if jobs.iter().any(|job| job.killed.get().is_some())
            || (last_kill != 0 && pressure.used <= last_kill)
        {
            return None;
        }
        let victim = jobs.iter().max_by_key(|job| job.bytes)?;
        let _ = victim.killed.set(pressure);
        self.used_at_last_kill
            .store(pressure.used, Ordering::Relaxed);
        victim.cancel.cancel();
        self.metrics.record_stage_error("memory_pressure");
        warn!(
            used = pressure.used,
            limit = pressure.limit,
            repository_bytes = victim.bytes,
            "cancelling the largest code indexing job under pod memory pressure"
        );
        Some(victim.bytes)
    }

    fn jobs(&self) -> MutexGuard<'_, Vec<Job>> {
        self.jobs.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Barrier;

    use super::*;

    const MIB: u64 = 1024 * 1024;

    fn pressure(used: u64) -> MemoryPressure {
        MemoryPressure { used, limit: 1000 }
    }

    fn watchdog(kill_at_percent: u64) -> Arc<MemoryWatchdog> {
        MemoryWatchdog::new(Box::new(|| None), CodeMetrics::new(), kill_at_percent)
    }

    #[test]
    fn a_waiter_leaves_on_cancel_and_an_oversize_item_runs_alone() {
        let budget = InflightBudget::new(2 * MIB);
        budget
            .admit(2 * MIB, &Sentinel::disabled())
            .unwrap_or_else(|e| panic!("{e}"));
        let cancelled = Sentinel::new("run", "", u64::MAX).with_cancellation(Arc::new(|| true));
        assert!(
            budget
                .admit(MIB, &cancelled)
                .is_err_and(|killed| killed.cancelled)
        );
        budget.release(2 * MIB);
        budget
            .admit(3 * MIB, &Sentinel::disabled())
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(*budget.used(), 3 * MIB);
    }

    #[test]
    fn bytes_in_flight_stay_under_the_budget() {
        let budget = InflightBudget::new(2 * MIB);
        let pair = Barrier::new(2);
        let (inflight, peak) = (AtomicU64::new(0), AtomicU64::new(0));
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    budget
                        .admit(MIB, &Sentinel::disabled())
                        .unwrap_or_else(|e| panic!("{e}"));
                    let now = inflight.fetch_add(MIB, Ordering::Relaxed) + MIB;
                    peak.fetch_max(now, Ordering::Relaxed);
                    pair.wait();
                    inflight.fetch_sub(MIB, Ordering::Relaxed);
                    budget.release(MIB);
                });
            }
        });
        assert_eq!(peak.load(Ordering::Relaxed), 2 * MIB);
    }

    #[tokio::test]
    async fn the_largest_newest_job_is_cancelled_and_the_next_kill_waits_for_memory_past_the_kill_level()
     {
        let watchdog = watchdog(90);
        let (small, big, newest) = (
            CancellationToken::new(),
            CancellationToken::new(),
            CancellationToken::new(),
        );
        let _small = watchdog.register(10, small.clone());
        let big_job = watchdog.register(50, big.clone());
        let newest_job = watchdog.register(50, newest.clone());

        assert_eq!(watchdog.tick(pressure(500)), None);
        assert_eq!(watchdog.tick(pressure(900)), Some(50));
        assert!(newest.is_cancelled() && !big.is_cancelled() && !small.is_cancelled());
        assert_eq!(newest_job.exceeded(), Some(pressure(900)));
        assert_eq!(
            watchdog.tick(pressure(950)),
            None,
            "one cancellation unwinds at a time"
        );

        drop(newest_job);
        assert_eq!(
            watchdog.tick(pressure(900)),
            None,
            "the pod sits at the level of the kill"
        );
        assert_eq!(
            watchdog.tick(pressure(960)),
            Some(50),
            "memory that rises past the kill level does not wait"
        );
        assert!(big.is_cancelled() && !small.is_cancelled());

        drop(big_job);
        assert_eq!(watchdog.tick(pressure(500)), None);
        assert_eq!(
            watchdog.tick(pressure(900)),
            Some(10),
            "pressure that dropped below the threshold re-arms the watchdog"
        );
        assert!(small.is_cancelled());
    }

    #[tokio::test]
    async fn zero_percent_is_off_and_more_than_one_hundred_counts_as_one_hundred() {
        let only = CancellationToken::new();
        let off = watchdog(0);
        let _job = off.register(7, only.clone());
        assert_eq!(off.tick(pressure(1000)), None);

        let clamped = watchdog(150);
        let _job = clamped.register(7, only.clone());
        assert_eq!(clamped.tick(pressure(999)), None);
        assert_eq!(clamped.tick(pressure(1000)), Some(7));
        assert!(only.is_cancelled());
    }
}
