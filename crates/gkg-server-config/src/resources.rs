use sysinfo::{ProcessRefreshKind, ProcessesToUpdate, System, get_current_pid};

/// sysinfo reports an unlimited or unreadable memory ceiling as a near-`u64::MAX` sentinel.
const CGROUP_UNLIMITED_THRESHOLD_BYTES: u64 = 1 << 62;

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

/// Drops sysinfo's unlimited/unreadable sentinel, leaving a usable byte limit.
pub fn readable_memory_limit_bytes(total_memory: u64) -> Option<u64> {
    (total_memory < CGROUP_UNLIMITED_THRESHOLD_BYTES).then_some(total_memory)
}

// `Process::cgroup_limits` resolves /proc/self/cgroup and min-walks ancestors;
// `System::cgroup_limits` reads only fixed root paths and misses sub-cgroup limits.
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
}
