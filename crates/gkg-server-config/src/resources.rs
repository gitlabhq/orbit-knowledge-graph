//! Container resource detection for runtime-derived indexer defaults.
//!
//! Reading the environment (CPU count, cgroup memory limit) is kept here, thin
//! and side-effecting, so the value-mapping functions in [`crate::engine`] stay
//! pure and unit-testable.

const CGROUP_V2_MEMORY_MAX: &str = "/sys/fs/cgroup/memory.max";
const CGROUP_V1_MEMORY_LIMIT: &str = "/sys/fs/cgroup/memory/memory.limit_in_bytes";

/// cgroup v1 encodes "no limit" as a near-`u64::MAX` sentinel (page counter max
/// times the page size). Any reported limit above this is treated as unlimited,
/// which is far larger than any real container allocation.
const CGROUP_UNLIMITED_THRESHOLD_BYTES: u64 = 1 << 62;

/// Observed CPU and memory the container is allowed to use, as the basis for
/// [`crate::engine::EngineConfiguration::resolve_runtime_defaults`].
#[derive(Debug, Clone)]
pub struct ContainerResources {
    /// CPUs the process may run on. `std::thread::available_parallelism` is
    /// cgroup-aware on Linux (it honours the CPU quota), so this reflects the
    /// pod's CPU limit, not the node's core count.
    pub available_parallelism: usize,

    /// Container memory limit in bytes, or `None` when no limit is readable
    /// (unlimited cgroup, or a platform without cgroups such as macOS dev).
    pub memory_limit_bytes: Option<u64>,
}

impl ContainerResources {
    /// Reads CPU and memory limits from the current environment.
    pub fn detect() -> Self {
        Self {
            available_parallelism: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1),
            memory_limit_bytes: read_cgroup_memory_limit_bytes(),
        }
    }
}

fn read_cgroup_memory_limit_bytes() -> Option<u64> {
    parse_memory_limit(&std::fs::read_to_string(CGROUP_V2_MEMORY_MAX).ok()?)
        .or_else(|| parse_memory_limit(&std::fs::read_to_string(CGROUP_V1_MEMORY_LIMIT).ok()?))
}

fn parse_memory_limit(contents: &str) -> Option<u64> {
    let trimmed = contents.trim();
    if trimmed == "max" {
        return None;
    }
    let value: u64 = trimmed.parse().ok()?;
    (value < CGROUP_UNLIMITED_THRESHOLD_BYTES).then_some(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_concrete_byte_limit() {
        assert_eq!(parse_memory_limit("34359738368\n"), Some(34_359_738_368));
    }

    #[test]
    fn cgroup_v2_unlimited_reads_as_none() {
        assert_eq!(parse_memory_limit("max\n"), None);
    }

    #[test]
    fn cgroup_v1_sentinel_reads_as_none() {
        assert_eq!(parse_memory_limit("9223372036854771712"), None);
    }

    #[test]
    fn unparseable_limit_reads_as_none() {
        assert_eq!(parse_memory_limit(""), None);
        assert_eq!(parse_memory_limit("not-a-number"), None);
    }
}
