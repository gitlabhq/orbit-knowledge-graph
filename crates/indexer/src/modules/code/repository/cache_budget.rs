//! In-memory cache index: sizes, pins, and two-pass LRU eviction ordering.
//! No filesystem access — callers execute eviction decisions on disk.

use std::collections::HashMap;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

use parking_lot::RwLock;

use crate::modules::code::metrics::CodeMetrics;

pub type CacheKey = (i64, String);
pub type EvictionReadGuard<'a> = tokio::sync::RwLockReadGuard<'a, ()>;
pub type EvictionWriteGuard<'a> = tokio::sync::RwLockWriteGuard<'a, ()>;

pub fn cache_key(project_id: i64, branch: &str) -> CacheKey {
    (project_id, branch.to_string())
}

#[derive(Debug, thiserror::Error)]
#[error(
    "cache budget exhausted: {remaining_bytes} bytes in use, target was {target_bytes} bytes — all unpinned entries already evicted"
)]
pub struct CacheBudgetExhausted {
    pub remaining_bytes: u64,
    pub target_bytes: u64,
}

/// RAII guard that keeps a cached repository pinned (safe from eviction).
/// Derefs to `&Path` for convenience.
#[derive(Debug)]
pub struct RepositoryLease {
    path: PathBuf,
    key: CacheKey,
    pin_counts: Arc<RwLock<HashMap<CacheKey, usize>>>,
}

impl RepositoryLease {
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Deref for RepositoryLease {
    type Target = Path;

    fn deref(&self) -> &Path {
        &self.path
    }
}

impl Drop for RepositoryLease {
    fn drop(&mut self) {
        let mut pins = self.pin_counts.write();
        if let Some(count) = pins.get_mut(&self.key) {
            *count -= 1;
            if *count == 0 {
                pins.remove(&self.key);
            }
        }
    }
}

#[derive(Debug, Clone)]
struct IndexEntry {
    size_bytes: u64,
    last_accessed: SystemTime,
}

/// # Lock ordering
///
/// `eviction_lock` → `index` → `pin_counts`. Always acquire in this order.
///
/// - `eviction_lock` (tokio `RwLock`): serializes eviction (write) with
///   `get`+`pin` sequences (read) so an entry cannot be deleted between a
///   cache hit and its pin.
/// - `index` (parking_lot `RwLock`): protects the in-memory size/LRU index.
///   Held briefly — never across await points.
/// - `pin_counts` (parking_lot `RwLock`): tracks active `RepositoryLease` refs.
///   Read under `index` read in `entries_to_evict`; written independently in `pin`/`Drop`.
pub struct CacheBudget {
    usable_budget: u64,
    large_repo_threshold: u64,
    index: Arc<RwLock<HashMap<CacheKey, IndexEntry>>>,
    pin_counts: Arc<RwLock<HashMap<CacheKey, usize>>>,
    total_bytes: Arc<AtomicU64>,
    eviction_lock: Arc<tokio::sync::RwLock<()>>,
    metrics: CodeMetrics,
}

impl CacheBudget {
    pub fn new(usable_budget: u64, large_repo_threshold: u64, metrics: CodeMetrics) -> Self {
        Self {
            usable_budget,
            large_repo_threshold,
            index: Arc::new(RwLock::new(HashMap::new())),
            pin_counts: Arc::new(RwLock::new(HashMap::new())),
            total_bytes: Arc::new(AtomicU64::new(0)),
            eviction_lock: Arc::new(tokio::sync::RwLock::new(())),
            metrics,
        }
    }

    pub fn pin(&self, path: PathBuf, project_id: i64, branch: &str) -> RepositoryLease {
        let key = cache_key(project_id, branch);
        {
            let mut pins = self.pin_counts.write();
            *pins.entry(key.clone()).or_insert(0) += 1;
        }

        RepositoryLease {
            path,
            key,
            pin_counts: Arc::clone(&self.pin_counts),
        }
    }

    pub fn record_size(&self, project_id: i64, branch: &str, size_bytes: u64) {
        let key = cache_key(project_id, branch);
        let old_size = self
            .index
            .write()
            .insert(
                key,
                IndexEntry {
                    size_bytes,
                    last_accessed: SystemTime::now(),
                },
            )
            .map_or(0, |e| e.size_bytes);

        if size_bytes >= old_size {
            self.total_bytes
                .fetch_add(size_bytes - old_size, Ordering::Relaxed);
        } else {
            self.total_bytes
                .fetch_sub(old_size - size_bytes, Ordering::Relaxed);
        }
        self.report_state();
    }

    pub fn touch(&self, project_id: i64, branch: &str) {
        let key = cache_key(project_id, branch);
        let mut index = self.index.write();
        if let Some(entry) = index.get_mut(&key) {
            entry.last_accessed = SystemTime::now();
        }
    }

    /// Removes an entry from the index and returns its tracked size (0 if absent).
    pub fn remove(&self, project_id: i64, branch: &str) -> u64 {
        let key = cache_key(project_id, branch);
        let size = self.index.write().remove(&key).map_or(0, |e| e.size_bytes);
        if size > 0 {
            self.total_bytes.fetch_sub(size, Ordering::Relaxed);
        }
        self.report_state();
        size
    }

    pub async fn eviction_read_lock(&self) -> EvictionReadGuard<'_> {
        self.eviction_lock.read().await
    }

    pub async fn eviction_write_lock(&self) -> EvictionWriteGuard<'_> {
        self.eviction_lock.write().await
    }

    /// Returns the keys that should be evicted to make room for `needed_bytes`.
    /// Pure computation — does not modify the index. Must be called under the
    /// eviction write lock.
    ///
    /// Two-pass LRU: evicts small repos (oldest first) before large ones,
    /// preserving expensive-to-rebuild repositories as long as possible.
    pub fn entries_to_evict(
        &self,
        needed_bytes: u64,
    ) -> Result<Vec<CacheKey>, CacheBudgetExhausted> {
        if needed_bytes > self.usable_budget {
            return Err(CacheBudgetExhausted {
                remaining_bytes: needed_bytes,
                target_bytes: self.usable_budget,
            });
        }

        let target = self.usable_budget - needed_bytes;
        let total = self.total_bytes.load(Ordering::Relaxed);

        if total <= target {
            return Ok(Vec::new());
        }

        let index = self.index.read();
        let pins = self.pin_counts.read();

        let mut small_repos: Vec<(CacheKey, u64, SystemTime)> = Vec::new();
        let mut large_repos: Vec<(CacheKey, u64, SystemTime)> = Vec::new();

        for (key, entry) in index.iter() {
            if pins.get(key).is_some_and(|&count| count > 0) {
                continue;
            }
            let bucket = if entry.size_bytes < self.large_repo_threshold {
                &mut small_repos
            } else {
                &mut large_repos
            };
            bucket.push((key.clone(), entry.size_bytes, entry.last_accessed));
        }

        small_repos.sort_by_key(|(_, _, ts)| *ts);
        large_repos.sort_by_key(|(_, _, ts)| *ts);

        let mut remaining = total;
        let mut keys_to_evict = Vec::new();

        for (key, size_bytes, _) in small_repos.into_iter().chain(large_repos) {
            if remaining <= target {
                break;
            }
            remaining = remaining.saturating_sub(size_bytes);
            keys_to_evict.push(key);
        }

        if remaining > target {
            return Err(CacheBudgetExhausted {
                remaining_bytes: remaining,
                target_bytes: target,
            });
        }

        Ok(keys_to_evict)
    }

    /// Resets all bookkeeping. Used after a full cache purge.
    pub fn clear(&self) {
        self.index.write().clear();
        self.pin_counts.write().clear();
        self.total_bytes.store(0, Ordering::Relaxed);
        self.report_state();
    }

    #[cfg(test)]
    fn is_pinned(&self, key: &CacheKey) -> bool {
        self.pin_counts
            .read()
            .get(key)
            .is_some_and(|&count| count > 0)
    }

    fn report_state(&self) {
        let total_bytes = self.total_bytes.load(Ordering::Relaxed);
        let entry_count = self.index.read().len() as u64;
        self.metrics.record_cache_state(total_bytes, entry_count);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_metrics() -> CodeMetrics {
        CodeMetrics::default()
    }

    fn create_budget(usable_budget: u64, large_threshold: u64) -> CacheBudget {
        CacheBudget::new(usable_budget, large_threshold, test_metrics())
    }

    fn index_entry_timestamp(budget: &CacheBudget, project_id: i64, branch: &str) -> SystemTime {
        budget
            .index
            .read()
            .get(&cache_key(project_id, branch))
            .unwrap()
            .last_accessed
    }

    #[tokio::test]
    async fn entries_to_evict_skips_pinned_entries() {
        let budget = create_budget(500, 400);
        budget.record_size(1, "main", 600);
        let _guard = budget.pin(PathBuf::from("/tmp/fake"), 1, "main");

        let result = budget.entries_to_evict(0);

        assert!(result.is_err(), "should fail when all entries are pinned");
    }

    #[tokio::test]
    async fn entries_to_evict_prefers_small_repos() {
        let budget = create_budget(1200, 400);
        budget.record_size(1, "main", 500);
        budget.record_size(2, "main", 300);
        budget.record_size(3, "main", 350);

        // Total: 1150. Budget: 1200. Evicting for 400 → target = 800.
        // Should evict small repos (300 + 350) before the large one (500).
        let keys = budget.entries_to_evict(400).unwrap();

        let evicted_projects: Vec<i64> = keys.iter().map(|(id, _)| *id).collect();
        assert!(
            evicted_projects.contains(&2) && evicted_projects.contains(&3),
            "small repos should be evicted"
        );
        assert!(!evicted_projects.contains(&1), "large repo should survive");
    }

    #[tokio::test]
    async fn entries_to_evict_prefers_oldest_within_size_class() {
        let budget = create_budget(600, 1000);
        budget.record_size(1, "main", 300);
        std::thread::sleep(std::time::Duration::from_millis(10));
        budget.record_size(2, "main", 300);

        // Total: 600. Budget: 600. Evicting for 100 → target = 500.
        // Project 1 is older, should be evicted first.
        let keys = budget.entries_to_evict(100).unwrap();

        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].0, 1, "older entry should be evicted first");
    }

    #[tokio::test]
    async fn entries_to_evict_returns_empty_when_under_budget() {
        let budget = create_budget(10_000, 500);
        budget.record_size(1, "main", 100);

        let keys = budget.entries_to_evict(100).unwrap();

        assert!(keys.is_empty());
    }

    #[tokio::test]
    async fn entries_to_evict_rejects_entry_larger_than_budget() {
        let budget = create_budget(500, 400);

        let result = budget.entries_to_evict(600);

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn entries_to_evict_does_not_modify_index() {
        let budget = create_budget(500, 400);
        budget.record_size(1, "main", 300);
        budget.record_size(2, "main", 300);

        let _ = budget.entries_to_evict(200);

        assert_eq!(budget.index.read().len(), 2, "index should be unchanged");
    }

    #[tokio::test]
    async fn pin_count_drops_to_zero_after_guard_dropped() {
        let budget = create_budget(1000, 500);

        let guard = budget.pin(PathBuf::from("/tmp/fake"), 42, "main");
        assert!(budget.is_pinned(&cache_key(42, "main")));

        drop(guard);
        assert!(!budget.is_pinned(&cache_key(42, "main")));
    }

    #[tokio::test]
    async fn remove_deletes_from_index_and_returns_size() {
        let budget = create_budget(10_000, 500);
        budget.record_size(1, "main", 100);

        let removed = budget.remove(1, "main");

        assert_eq!(removed, 100);
        assert!(budget.index.read().is_empty());
    }

    #[tokio::test]
    async fn remove_returns_zero_when_absent() {
        let budget = create_budget(10_000, 500);

        let removed = budget.remove(1, "main");

        assert_eq!(removed, 0);
    }

    #[tokio::test]
    async fn touch_updates_last_accessed() {
        let budget = create_budget(10_000, 500);
        budget.record_size(1, "main", 100);
        let timestamp_after_record = index_entry_timestamp(&budget, 1, "main");

        std::thread::sleep(std::time::Duration::from_millis(10));
        budget.touch(1, "main");

        let timestamp_after_touch = index_entry_timestamp(&budget, 1, "main");
        assert!(
            timestamp_after_touch > timestamp_after_record,
            "touch should advance the timestamp"
        );
    }

    #[tokio::test]
    async fn clear_resets_all_state() {
        let budget = create_budget(10_000, 500);
        budget.record_size(1, "main", 100);
        budget.record_size(2, "main", 200);

        budget.clear();

        assert!(budget.index.read().is_empty());
        assert_eq!(budget.total_bytes.load(Ordering::Relaxed), 0);
    }
}
