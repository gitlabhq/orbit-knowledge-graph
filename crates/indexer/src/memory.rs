use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError, Weak};
use std::time::Duration;

use orbit_server_config::resources::{MemoryPressure, cgroup_memory};
use tokio::runtime::Handle;
use tokio::sync::{Semaphore, SemaphorePermit};
use tracing::warn;

const POLL: Duration = Duration::from_millis(50);
const INTERVAL: Duration = Duration::from_millis(100);

pub type Permit<'a> = SemaphorePermit<'a>;

pub struct Budget {
    permits: Semaphore,
    size: u32,
}

impl Budget {
    pub fn new(size: u32) -> Self {
        Self {
            permits: Semaphore::new(size as usize),
            size,
        }
    }

    fn claim(&self, bytes: u64) -> u32 {
        u32::try_from(bytes).unwrap_or(u32::MAX).min(self.size)
    }

    pub async fn acquire(&self, bytes: u64) -> Option<Permit<'_>> {
        self.permits.acquire_many(self.claim(bytes)).await.ok()
    }

    pub fn acquire_blocking<E>(
        &self,
        bytes: u64,
        runtime: &Handle,
        check: impl Fn() -> Result<(), E>,
    ) -> Result<Option<Permit<'_>>, E> {
        runtime.block_on(async {
            let acquire = self.acquire(bytes);
            tokio::pin!(acquire);
            loop {
                tokio::select! {
                    permit = &mut acquire => return Ok(permit),
                    () = tokio::time::sleep(POLL) => check()?,
                }
            }
        })
    }

    pub fn release(&self, bytes: u64) {
        self.permits.add_permits(self.claim(bytes) as usize);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MemoryKill {
    pub pressure: MemoryPressure,
    pub alone: bool,
}

struct Job {
    claim: u64,
    cancel: Arc<dyn Fn() + Send + Sync>,
    killed: Arc<OnceLock<MemoryKill>>,
}

pub struct MemoryWatchdog {
    sample: Box<dyn Fn() -> Option<MemoryPressure> + Send + Sync>,
    kill_at_percent: u64,
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
    pub fn for_this_process(kill_at_percent: u64) -> Arc<Self> {
        if cgroup_memory().is_none() {
            warn!("memory watchdog inert: this process has no cgroup memory limit");
            return Self::new(Box::new(|| None), 0);
        }
        Self::new(Box::new(cgroup_memory), kill_at_percent)
    }

    pub fn new(
        sample: Box<dyn Fn() -> Option<MemoryPressure> + Send + Sync>,
        kill_at_percent: u64,
    ) -> Arc<Self> {
        Arc::new(Self {
            sample,
            kill_at_percent: kill_at_percent.min(100),
            jobs: Mutex::new(Vec::new()),
            used_at_last_kill: AtomicU64::new(0),
            watching: AtomicBool::new(false),
        })
    }

    pub fn register(
        self: &Arc<Self>,
        claim: u64,
        cancel: Arc<dyn Fn() + Send + Sync>,
    ) -> Registration {
        let killed = Arc::new(OnceLock::new());
        let mut jobs = self.jobs();
        jobs.push(Job {
            claim,
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
            let sampler = this.clone();
            if let Ok(Some(pressure)) =
                tokio::task::spawn_blocking(move || (sampler.sample)()).await
            {
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

    fn tick(&self, pressure: MemoryPressure) {
        if self.kill_at_percent == 0
            || u128::from(pressure.used) * 100
                < u128::from(pressure.limit) * u128::from(self.kill_at_percent)
        {
            self.used_at_last_kill.store(0, Ordering::Relaxed);
            return;
        }
        let jobs = self.jobs();
        let last_kill = self.used_at_last_kill.load(Ordering::Relaxed);
        if jobs.iter().any(|job| job.killed.get().is_some())
            || (last_kill != 0 && pressure.used <= last_kill)
        {
            return;
        }
        let Some(victim) = jobs.iter().max_by_key(|job| job.claim) else {
            return;
        };
        let kill = MemoryKill {
            pressure,
            alone: jobs.len() == 1,
        };
        let _ = victim.killed.set(kill);
        self.used_at_last_kill
            .store(pressure.used, Ordering::Relaxed);
        (victim.cancel)();
        warn!(
            used = pressure.used,
            limit = pressure.limit,
            claim = victim.claim,
            alone = kill.alone,
            "cancelling the largest job under memory pressure"
        );
    }

    fn jobs(&self) -> MutexGuard<'_, Vec<Job>> {
        self.jobs.lock().unwrap_or_else(PoisonError::into_inner)
    }
}
