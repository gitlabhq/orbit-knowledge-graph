const CGROUP_V2_MEMORY_MAX: &str = "/sys/fs/cgroup/memory.max";
const CGROUP_V1_MEMORY_LIMIT: &str = "/sys/fs/cgroup/memory/memory.limit_in_bytes";

/// cgroup v1 reports "no limit" as a near-`u64::MAX` page-counter sentinel, not an absent file.
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
