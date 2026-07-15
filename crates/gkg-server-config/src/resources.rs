use std::collections::HashMap;

use sysinfo::{ProcessRefreshKind, ProcessesToUpdate, System, get_current_pid};
use tracing::info;

use crate::engine::{EngineConfiguration, IndexerModule};

pub(crate) const DEFAULT_MAX_CONCURRENT_WORKERS: usize = 16;

/// Calibrated on prod's hand-tuned pools: code runs 16 workers in 24 GiB and
/// sdlc 20 in 32 GiB — both ~1.5 GiB per worker.
const WORKER_MEMORY_BUDGET_BYTES: u64 = 1536 * 1024 * 1024;

/// Preserves the historical universal-pool split (sdlc 12 / code 4 of 16 workers).
const SDLC_WORKER_SHARE_PERCENT: usize = 75;

/// Half the (CPU-bound) workers index; code pool ran 8 of 16. Fetch stays a static I/O default.
const CODE_INDEXING_LANE_SHARE_PERCENT: usize = 50;

/// Reserve big-repo lanes so a flood of small repos can't starve monorepos (big 2 / small 6).
const CODE_BIG_LANE_SHARE_PERCENT: usize = 25;

/// sysinfo reports an unlimited or unreadable memory ceiling as a near-`u64::MAX` sentinel.
const CGROUP_UNLIMITED_THRESHOLD_BYTES: u64 = 1 << 62;

pub struct CodeIndexingSlots {
    pub small_indexing_slots: usize,
    pub big_indexing_slots: usize,
}

#[derive(Debug, Clone)]
pub struct ContainerResources {
    /// Cgroup-aware on Linux: reflects the pod CPU quota, not the node core count.
    pub available_parallelism: usize,

    /// `None` when no limit is readable (unlimited cgroup, or no cgroups — e.g. macOS).
    pub memory_limit_bytes: Option<u64>,
}

impl ContainerResources {
    pub fn detect() -> Self {
        Self {
            available_parallelism: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1),
            memory_limit_bytes: read_cgroup_memory_limit_bytes(),
        }
    }

    /// The memory limit caps the CPU count so a memory-tight pod cannot derive more workers than it can feed.
    pub fn derive_worker_budget(&self) -> usize {
        let cpu = self.available_parallelism.max(1);
        match self.memory_limit_bytes {
            Some(bytes) => cpu.min(max_workers_for_memory_limit(bytes)),
            None => cpu,
        }
    }
}

impl EngineConfiguration {
    pub fn resolve_runtime_defaults(&mut self, resources: &ContainerResources) {
        if self.max_concurrent_workers.is_none() {
            let workers = resources.derive_worker_budget();
            self.max_concurrent_workers = Some(workers);
            info!(
                available_parallelism = resources.available_parallelism,
                memory_limit_bytes = resources.memory_limit_bytes,
                value = workers,
                "derived engine.max_concurrent_workers"
            );
        }

        if self.concurrency_groups.is_empty() {
            let groups = derive_concurrency_groups(&self.modules, self.max_concurrent_workers());
            info!(?groups, "derived engine.concurrency_groups");
            self.concurrency_groups = groups;
        }
    }
}

pub fn derive_concurrency_groups(
    modules: &[IndexerModule],
    worker_count: usize,
) -> HashMap<String, usize> {
    let sdlc_group = IndexerModule::Sdlc.concurrency_group();
    let code_group = IndexerModule::Code.concurrency_group();

    let has_sdlc = modules.iter().any(|m| m.concurrency_group() == sdlc_group);
    let has_code = modules.iter().any(|m| m.concurrency_group() == code_group);

    let mut groups = HashMap::new();
    match (has_sdlc, has_code) {
        (true, true) => {
            let sdlc_cap = (worker_count * SDLC_WORKER_SHARE_PERCENT / 100).max(1);
            let code_cap = worker_count.saturating_sub(sdlc_cap).max(1);
            groups.insert(sdlc_group.to_string(), sdlc_cap);
            groups.insert(code_group.to_string(), code_cap);
        }
        (true, false) => {
            groups.insert(sdlc_group.to_string(), worker_count.max(1));
        }
        (false, true) => {
            groups.insert(code_group.to_string(), worker_count.max(1));
        }
        (false, false) => {}
    }
    groups
}

pub fn derive_code_indexing_slots(worker_budget: usize) -> CodeIndexingSlots {
    let workers = worker_budget.max(1);
    let indexing = (workers * CODE_INDEXING_LANE_SHARE_PERCENT / 100).max(1);
    let big = (indexing * CODE_BIG_LANE_SHARE_PERCENT / 100).max(1);
    CodeIndexingSlots {
        big_indexing_slots: big,
        small_indexing_slots: indexing.saturating_sub(big).max(1),
    }
}

fn max_workers_for_memory_limit(memory_limit_bytes: u64) -> usize {
    usize::try_from(memory_limit_bytes / WORKER_MEMORY_BUDGET_BYTES)
        .unwrap_or(usize::MAX)
        .max(1)
}

fn memory_limit_if_bounded(total_memory: u64) -> Option<u64> {
    (total_memory < CGROUP_UNLIMITED_THRESHOLD_BYTES).then_some(total_memory)
}

/// sysinfo resolves the process's cgroup via `/proc/self/cgroup`, so this also
/// works without a private cgroup namespace; non-Linux yields `None`.
fn read_cgroup_memory_limit_bytes() -> Option<u64> {
    let pid = get_current_pid().ok()?;
    let mut system = System::new();
    system.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        false,
        ProcessRefreshKind::nothing(),
    );
    let limits = system.process(pid)?.cgroup_limits()?;
    memory_limit_if_bounded(limits.total_memory)
}

#[cfg(test)]
mod tests {
    use super::*;

    const GIB: u64 = 1024 * 1024 * 1024;

    fn resources(
        available_parallelism: usize,
        memory_limit_gib: Option<u64>,
    ) -> ContainerResources {
        ContainerResources {
            available_parallelism,
            memory_limit_bytes: memory_limit_gib.map(|gib| gib * GIB),
        }
    }

    #[test]
    fn worker_budget_is_parallelism_without_a_memory_limit() {
        assert_eq!(resources(20, None).derive_worker_budget(), 20);
        assert_eq!(resources(0, None).derive_worker_budget(), 1);
    }

    #[test]
    fn worker_budget_reproduces_prod_pool_shapes() {
        assert_eq!(resources(16, Some(24)).derive_worker_budget(), 16);
        assert_eq!(resources(8, Some(32)).derive_worker_budget(), 8);
    }

    #[test]
    fn memory_limit_caps_the_worker_budget() {
        assert_eq!(resources(8, Some(4)).derive_worker_budget(), 2);
        assert_eq!(resources(8, Some(1)).derive_worker_budget(), 1);
    }

    #[test]
    fn unlimited_sentinel_reads_as_no_memory_limit() {
        assert_eq!(memory_limit_if_bounded(u64::MAX), None);
        assert_eq!(memory_limit_if_bounded(1 << 62), None);
        assert_eq!(memory_limit_if_bounded(32 * GIB), Some(32 * GIB));
    }

    #[test]
    fn universal_pool_reproduces_historical_split() {
        let groups = derive_concurrency_groups(&IndexerModule::all(), 16);
        assert_eq!(groups.get("sdlc"), Some(&12));
        assert_eq!(groups.get("code"), Some(&4));
    }

    #[test]
    fn single_module_pool_gets_the_whole_cap() {
        let sdlc_only = derive_concurrency_groups(&[IndexerModule::Sdlc], 20);
        assert_eq!(sdlc_only.get("sdlc"), Some(&20));
        assert_eq!(sdlc_only.get("code"), None);

        let code_only = derive_concurrency_groups(&[IndexerModule::Code], 16);
        assert_eq!(code_only.get("code"), Some(&16));
        assert_eq!(code_only.get("sdlc"), None);
    }

    #[test]
    fn namespace_deletion_shares_the_sdlc_group() {
        let groups =
            derive_concurrency_groups(&[IndexerModule::Sdlc, IndexerModule::NamespaceDeletion], 16);
        assert_eq!(groups.get("sdlc"), Some(&16));
        assert_eq!(groups.len(), 1);
    }

    #[test]
    fn resolve_fills_workers_and_groups() {
        let mut cfg = EngineConfiguration::default();

        cfg.resolve_runtime_defaults(&resources(20, None));

        assert_eq!(cfg.max_concurrent_workers(), 20);
        assert_eq!(cfg.concurrency_groups.get("sdlc"), Some(&15));
        assert_eq!(cfg.concurrency_groups.get("code"), Some(&5));
    }

    #[test]
    fn resolve_caps_workers_by_memory() {
        let mut cfg = EngineConfiguration::default();

        cfg.resolve_runtime_defaults(&resources(16, Some(6)));

        assert_eq!(cfg.max_concurrent_workers(), 4);
        assert_eq!(cfg.concurrency_groups.get("sdlc"), Some(&3));
        assert_eq!(cfg.concurrency_groups.get("code"), Some(&1));
    }

    #[test]
    fn code_slots_reproduce_historical_split_on_sixteen_cores() {
        let slots = derive_code_indexing_slots(16);
        assert_eq!(slots.small_indexing_slots, 6);
        assert_eq!(slots.big_indexing_slots, 2);
    }

    #[test]
    fn code_slots_scale_down_on_a_small_pod() {
        let slots = derive_code_indexing_slots(4);
        assert_eq!(slots.small_indexing_slots, 1);
        assert_eq!(slots.big_indexing_slots, 1);
    }

    #[test]
    fn code_slots_floor_every_lane_at_one() {
        for budget in [0, 1] {
            let slots = derive_code_indexing_slots(budget);
            assert_eq!(slots.small_indexing_slots, 1);
            assert_eq!(slots.big_indexing_slots, 1);
        }
    }

    #[test]
    fn explicit_config_beats_derivation() {
        let mut cfg = EngineConfiguration {
            max_concurrent_workers: Some(4),
            concurrency_groups: HashMap::from([("sdlc".to_string(), 3)]),
            ..EngineConfiguration::default()
        };

        cfg.resolve_runtime_defaults(&resources(64, None));

        assert_eq!(cfg.max_concurrent_workers(), 4);
        assert_eq!(
            cfg.concurrency_groups,
            HashMap::from([("sdlc".to_string(), 3)])
        );
    }
}
