use std::collections::HashMap;

use tracing::info;

use crate::engine::{EngineConfiguration, IndexerModule};

pub(crate) const DEFAULT_MAX_CONCURRENT_WORKERS: usize = 16;

/// Preserves the historical universal-pool split (sdlc 12 / code 4 of 16 workers).
const SDLC_WORKER_SHARE_PERCENT: usize = 75;

/// Cgroup-aware on Linux: reflects the pod CPU quota, not the node core count.
pub fn detect_available_parallelism() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

impl EngineConfiguration {
    pub fn resolve_runtime_defaults(&mut self, available_parallelism: usize) {
        if self.max_concurrent_workers.is_none() {
            let workers = derive_max_concurrent_workers(available_parallelism);
            self.max_concurrent_workers = Some(workers);
            info!(
                available_parallelism,
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

pub fn derive_max_concurrent_workers(available_parallelism: usize) -> usize {
    available_parallelism.max(1)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn max_concurrent_workers_tracks_available_parallelism() {
        assert_eq!(derive_max_concurrent_workers(20), 20);
        assert_eq!(derive_max_concurrent_workers(1), 1);
        assert_eq!(derive_max_concurrent_workers(0), 1);
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

        cfg.resolve_runtime_defaults(20);

        assert_eq!(cfg.max_concurrent_workers(), 20);
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

        cfg.resolve_runtime_defaults(64);

        assert_eq!(cfg.max_concurrent_workers(), 4);
        assert_eq!(
            cfg.concurrency_groups,
            HashMap::from([("sdlc".to_string(), 3)])
        );
    }
}
