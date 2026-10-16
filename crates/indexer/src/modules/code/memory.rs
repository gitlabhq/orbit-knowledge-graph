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
    queue: Mutex<Queue>,
    changed: Condvar,
}

#[derive(Default)]
struct Queue {
    used: u64,
    next: u64,
    serving: u64,
}

impl InflightBudget {
    fn new(budget: u64) -> Self {
        Self {
            budget,
            queue: Mutex::default(),
            changed: Condvar::new(),
        }
    }

    fn queue(&self) -> MutexGuard<'_, Queue> {
        self.queue.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Admission for InflightBudget {
    fn admit(&self, bytes: u64, run: &Sentinel) -> Result<(), Killed> {
        let mut queue = self.queue();
        let ticket = queue.next;
        queue.next += 1;
        loop {
            let killed = run.check().err();
            if queue.serving == ticket {
                if let Some(killed) = killed {
                    queue.serving += 1;
                    self.changed.notify_all();
                    return Err(killed);
                }
                if queue.used == 0 || queue.used + bytes <= self.budget {
                    break;
                }
            }
            queue = self
                .changed
                .wait(queue)
                .unwrap_or_else(PoisonError::into_inner);
        }
        queue.serving += 1;
        queue.used += bytes;
        self.changed.notify_all();
        Ok(())
    }

    fn release(&self, bytes: u64) {
        self.queue().used -= bytes;
        self.changed.notify_all();
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MemoryKill {
    pub pressure: MemoryPressure,
    pub alone: bool,
}

struct Job {
    bytes: u64,
    cancel: CancellationToken,
    killed: Arc<OnceLock<MemoryKill>>,
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
    killed: Arc<OnceLock<MemoryKill>>,
}

impl Registration {
    pub fn exceeded(&self) -> Option<MemoryKill> {
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
        let kill = MemoryKill {
            pressure,
            alone: jobs.len() == 1,
        };
        let _ = victim.killed.set(kill);
        self.used_at_last_kill
            .store(pressure.used, Ordering::Relaxed);
        victim.cancel.cancel();
        self.metrics.record_stage_error("memory_pressure");
        warn!(
            used = pressure.used,
            limit = pressure.limit,
            repository_bytes = victim.bytes,
            alone = kill.alone,
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
    use std::time::Instant;

    use super::*;

    const MIB: u64 = 1024 * 1024;

    fn pressure(used: u64) -> MemoryPressure {
        MemoryPressure { used, limit: 1000 }
    }

    fn kill(used: u64, alone: bool) -> Option<MemoryKill> {
        Some(MemoryKill {
            pressure: pressure(used),
            alone,
        })
    }

    fn watchdog(kill_at_percent: u64) -> Arc<MemoryWatchdog> {
        MemoryWatchdog::new(Box::new(|| None), CodeMetrics::new(), kill_at_percent)
    }

    fn admit(budget: &InflightBudget, bytes: u64) {
        budget
            .admit(bytes, &Sentinel::disabled())
            .unwrap_or_else(|e| panic!("{e}"));
    }

    fn settles(what: &str, condition: impl Fn() -> bool) {
        let started = Instant::now();
        while !condition() {
            assert!(started.elapsed() < Duration::from_secs(10), "{what}");
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    #[test]
    fn a_waiter_leaves_on_cancel_and_an_oversize_item_runs_alone() {
        let budget = InflightBudget::new(2 * MIB);
        admit(&budget, 2 * MIB);
        let cancelled = Sentinel::new("run", "", u64::MAX).with_cancellation(Arc::new(|| true));
        assert!(
            budget
                .admit(MIB, &cancelled)
                .is_err_and(|killed| killed.cancelled)
        );
        budget.release(2 * MIB);
        admit(&budget, 3 * MIB);
        assert_eq!(budget.queue().used, 3 * MIB);
    }

    #[test]
    fn bytes_in_flight_stay_under_the_budget() {
        let budget = InflightBudget::new(2 * MIB);
        let pair = Barrier::new(2);
        let (inflight, peak) = (AtomicU64::new(0), AtomicU64::new(0));
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    admit(&budget, MIB);
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

    #[test]
    fn a_waiting_large_item_is_admitted_before_the_small_items_that_arrive_after_it() {
        let budget = InflightBudget::new(2 * MIB);
        let order = Mutex::new(Vec::new());
        admit(&budget, MIB);
        std::thread::scope(|scope| {
            let (budget, order) = (&budget, &order);
            scope.spawn(move || {
                admit(budget, 3 * MIB);
                order
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push("big");
                budget.release(3 * MIB);
            });
            settles("big has its ticket", || budget.queue().next == 2);
            scope.spawn(move || {
                admit(budget, MIB / 4);
                order
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push("small");
                budget.release(MIB / 4);
            });
            settles("small has its ticket", || budget.queue().next == 3);
            std::thread::sleep(Duration::from_millis(20));
            assert_eq!(budget.queue().used, MIB, "small waits behind big");
            budget.release(MIB);
        });
        assert_eq!(
            *order.lock().unwrap_or_else(PoisonError::into_inner),
            ["big", "small"]
        );
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
        assert_eq!(newest_job.exceeded(), kill(900, false));
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
        let job = clamped.register(7, only.clone());
        assert_eq!(clamped.tick(pressure(999)), None);
        assert_eq!(clamped.tick(pressure(1000)), Some(7));
        assert!(only.is_cancelled());
        assert_eq!(job.exceeded(), kill(1000, true));
    }
}
