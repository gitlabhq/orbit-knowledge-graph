//! Resource caps. Every domain wants them, so the store enforces them and
//! the passes never count.

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("{metric} cap exceeded ({count} > {cap})")]
pub struct CapExceeded {
    pub metric: &'static str,
    pub count: u64,
    pub cap: u64,
}

/// Resource caps. Every domain wants them, so the store enforces them and the
/// passes never count. Per store: a process running N loads at once divides
/// its budget by N.
#[derive(Debug, Clone, Copy, Default)]
pub struct Limits {
    /// Over → `List("oversize")`.
    pub file_bytes: Option<u64>,
    /// Over → `SourceError::Cap`.
    pub total_bytes: Option<u64>,
    /// Over → `SourceError::Cap`.
    pub files: Option<usize>,
    /// Over → bytes spill to the scratch file. `Some(0)` spills everything.
    pub resident_bytes: Option<u64>,
    /// Over → `SourceError::Cap`, before the disk says `ENOSPC`.
    pub spilled_bytes: Option<u64>,
}

/// Add to a running total; the first add to pass the cap is an error. Returns
/// the total before the add, which is the offset for an append.
pub(super) fn add_capped(
    total: &AtomicU64,
    metric: &'static str,
    n: u64,
    cap: Option<u64>,
) -> Result<u64, CapExceeded> {
    let before = total.fetch_add(n, Relaxed);
    let count = before.saturating_add(n);
    match cap.filter(|&cap| count > cap) {
        Some(cap) => Err(CapExceeded { metric, count, cap }),
        None => Ok(before),
    }
}
