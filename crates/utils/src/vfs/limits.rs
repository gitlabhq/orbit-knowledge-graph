use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
#[strum(serialize_all = "snake_case")]
pub enum LimitKind {
    Files,
    TotalBytes,
    ResidentBytes,
    SpilledBytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("{metric} cap exceeded ({count} > {cap})")]
pub struct CapExceeded {
    pub metric: LimitKind,
    pub count: u64,
    pub cap: u64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Limits {
    pub file_bytes: Option<u64>,
    pub total_bytes: Option<u64>,
    pub files: Option<usize>,
    pub resident_bytes: Option<u64>,
    pub spilled_bytes: Option<u64>,
}

pub(super) fn add_capped(
    total: &AtomicU64,
    metric: LimitKind,
    n: u64,
    cap: Option<u64>,
) -> Result<u64, CapExceeded> {
    let cap = cap.unwrap_or(u64::MAX);
    total
        .try_update(Relaxed, Relaxed, |before| {
            before.checked_add(n).filter(|&count| count <= cap)
        })
        .map_err(|before| CapExceeded {
            metric,
            count: before.saturating_add(n),
            cap,
        })
}
