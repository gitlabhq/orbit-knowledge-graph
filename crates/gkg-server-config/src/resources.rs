use std::collections::HashMap;

use sysinfo::{ProcessRefreshKind, ProcessesToUpdate, System, get_current_pid};
use tracing::info;

use crate::engine::{EngineConfiguration, IndexerModule};

/// sysinfo reports an unlimited or unreadable memory ceiling as a near-`u64::MAX` sentinel.
const CGROUP_UNLIMITED_THRESHOLD_BYTES: u64 = 1 << 62;

pub(crate) const DEFAULT_MAX_CONCURRENT_WORKERS: usize = 16;

/// Calibration: 65536 is the shipped memory-scarce default; prod's 32 GiB SDLC pods run 262144.
pub(crate) const MEMORY_SCARCE_STREAM_BLOCK_SIZE: u64 = 65_536;
const ROOMY_STREAM_BLOCK_SIZE: u64 = 262_144;

/// Below the 32 GiB prod calibration point so those pods land on the wide tier with margin.
const ROOMY_MEMORY_THRESHOLD_BYTES: u64 = 16 * 1024 * 1024 * 1024;

/// Preserves the historical universal-pool split (sdlc 12 / code 4 of 16 workers).
const SDLC_WORKER_SHARE_PERCENT: usize = 75;

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
}

impl EngineConfiguration {
    pub fn resolve_runtime_defaults(&mut self, resources: &ContainerResources) {
        if self.max_concurrent_workers.is_none() {
            let workers = derive_max_concurrent_workers(resources.available_parallelism);
            self.max_concurrent_workers = Some(workers);
            info!(
                available_parallelism = resources.available_parallelism,
                value = workers,
                "derived engine.max_concurrent_workers"
            );
        }

        if self.handlers.entity_handler.stream_block_size.is_none() {
            let block_size = derive_stream_block_size(resources.memory_limit_bytes);
            self.handlers.entity_handler.stream_block_size = Some(block_size);
            info!(
                memory_limit_bytes = resources.memory_limit_bytes,
                value = block_size,
                "derived engine.handlers.entity_handler.stream_block_size"
            );
        }

        if self.concurrency_groups.is_empty() {
            let groups = derive_concurrency_groups(&self.modules, self.max_concurrent_workers());
            info!(?groups, "derived engine.concurrency_groups");
            self.concurrency_groups = groups;
        }
    }
}

pub fn derive_max_concurrent_workers(available_parallelism: usize) -> usize {
    available_parallelism.max(1)
}

pub fn derive_stream_block_size(memory_limit_bytes: Option<u64>) -> u64 {
    match memory_limit_bytes {
        Some(bytes) if bytes >= ROOMY_MEMORY_THRESHOLD_BYTES => ROOMY_STREAM_BLOCK_SIZE,
        _ => MEMORY_SCARCE_STREAM_BLOCK_SIZE,
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

/// Drops sysinfo's unlimited/unreadable sentinel, leaving a usable byte limit.
pub fn readable_memory_limit_bytes(total_memory: u64) -> Option<u64> {
    (total_memory < CGROUP_UNLIMITED_THRESHOLD_BYTES).then_some(total_memory)
}

fn read_cgroup_memory_limit_bytes() -> Option<u64> {
    let pid = get_current_pid().ok()?;
    let mut system = System::new();
    system.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        false,
        ProcessRefreshKind::nothing(),
    );
    let limits = system.process(pid)?.cgroup_limits()?;
    readable_memory_limit_bytes(limits.total_memory)
}

#[cfg(test)]
mod tests {
    use super::*;

    const GIB: u64 = 1024 * 1024 * 1024;

    #[test]
    fn concrete_limit_passes_through() {
        assert_eq!(
            readable_memory_limit_bytes(34_359_738_368),
            Some(34_359_738_368)
        );
    }

    #[test]
    fn unlimited_sentinel_reads_as_none() {
        assert_eq!(readable_memory_limit_bytes(u64::MAX), None);
    }

    #[test]
    fn max_concurrent_workers_tracks_available_parallelism() {
        assert_eq!(derive_max_concurrent_workers(20), 20);
        assert_eq!(derive_max_concurrent_workers(1), 1);
        assert_eq!(derive_max_concurrent_workers(0), 1);
    }

    #[test]
    fn stream_block_size_calibration_points() {
        assert_eq!(derive_stream_block_size(None), 65_536);
        assert_eq!(derive_stream_block_size(Some(8 * GIB)), 65_536);
        assert_eq!(derive_stream_block_size(Some(32 * GIB)), 262_144);
    }

    #[test]
    fn stream_block_size_switches_at_the_roomy_threshold() {
        assert_eq!(derive_stream_block_size(Some(16 * GIB - 1)), 65_536);
        assert_eq!(derive_stream_block_size(Some(16 * GIB)), 262_144);
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
    fn resolve_fills_every_unset_scale_field() {
        let mut cfg = EngineConfiguration::default();
        let resources = ContainerResources {
            available_parallelism: 20,
            memory_limit_bytes: Some(32 * GIB),
        };

        cfg.resolve_runtime_defaults(&resources);

        assert_eq!(cfg.max_concurrent_workers(), 20);
        assert_eq!(cfg.handlers.entity_handler.stream_block_size(), 262_144);
        assert_eq!(cfg.concurrency_groups.get("sdlc"), Some(&15));
        assert_eq!(cfg.concurrency_groups.get("code"), Some(&5));
    }

    #[test]
    fn explicit_config_beats_derivation() {
        let mut cfg = EngineConfiguration {
            max_concurrent_workers: Some(4),
            concurrency_groups: HashMap::from([("sdlc".to_string(), 3)]),
            ..EngineConfiguration::default()
        };
        cfg.handlers.entity_handler.stream_block_size = Some(1024);

        cfg.resolve_runtime_defaults(&ContainerResources {
            available_parallelism: 64,
            memory_limit_bytes: Some(64 * GIB),
        });

        assert_eq!(cfg.max_concurrent_workers(), 4);
        assert_eq!(cfg.handlers.entity_handler.stream_block_size(), 1024);
        assert_eq!(
            cfg.concurrency_groups,
            HashMap::from([("sdlc".to_string(), 3)])
        );
    }

    #[test]
    fn unreadable_memory_limit_keeps_the_conservative_defaults() {
        let mut cfg = EngineConfiguration::default();

        cfg.resolve_runtime_defaults(&ContainerResources {
            available_parallelism: 8,
            memory_limit_bytes: None,
        });

        assert_eq!(cfg.max_concurrent_workers(), 8);
        assert_eq!(cfg.handlers.entity_handler.stream_block_size(), 65_536);
    }
}
