//! Bounded disk-cache budget management with LRU eviction.
//!
//! This module tracks cache entry sizes, manages pin counts to protect active
//! entries from eviction, and enforces a disk budget using two-pass LRU:
//! small repositories are evicted first, large repositories only when necessary.

use std::collections::HashMap;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

use parking_lot::RwLock;
use tracing::{error, info, warn};

use super::disk::hashed_branch_name;

use crate::modules::code::metrics::CodeMetrics;

/// Read guard on the eviction lock. While held, eviction cannot run.
pub type EvictionReadGuard<'a> = tokio::sync::RwLockReadGuard<'a, ()>;

/// Returned when eviction cannot free enough space because all remaining
/// entries are pinned by active workers.
#[derive(Debug, thiserror::Error)]
#[error(
    "cache budget exhausted: {remaining_bytes} bytes in use, target was {target_bytes} bytes — all unpinned entries already evicted"
)]
pub struct CacheBudgetExhausted {
    pub remaining_bytes: u64,
    pub target_bytes: u64,
}

pub type CacheKey = (i64, String);

pub fn cache_key(project_id: i64, branch: &str) -> CacheKey {
    (project_id, branch.to_string())
}

/// A lease on a cached repository directory, preventing eviction while held.
///
/// Dereferences to `&Path` so it can be passed directly to code that expects a path.
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

struct EvictionCandidate {
    key: CacheKey,
    size_bytes: u64,
}

/// Unpinned entries eligible for eviction, sorted oldest-first within each tier.
struct EvictionPlan {
    small_repos: Vec<EvictionCandidate>,
    large_repos: Vec<EvictionCandidate>,
}

/// Tracks cache entry sizes, pin counts, and enforces a disk budget via LRU eviction.
///
/// # Lock ordering
///
/// `eviction_lock` → `index` → `pin_counts`. Always acquire in this order.
///
/// - `eviction_lock` (tokio `RwLock`): serializes `make_room` (write) with
///   `get`+`pin` sequences (read) so an entry cannot be deleted between a
///   cache hit and its pin.
/// - `index` (parking_lot `RwLock`): protects the in-memory size/LRU index.
///   Held briefly — never across await points.
/// - `pin_counts` (parking_lot `RwLock`): tracks active `RepositoryLease` refs.
///   Read under `index` read in `make_room`; written independently in `pin`/`Drop`.
pub struct CacheBudget {
    base_dir: PathBuf,
    usable_budget: u64,
    disk_budget: u64,
    large_repo_threshold: u64,
    index: Arc<RwLock<HashMap<CacheKey, IndexEntry>>>,
    pin_counts: Arc<RwLock<HashMap<CacheKey, usize>>>,
    total_bytes: Arc<AtomicU64>,
    phantom_bytes: Arc<AtomicU64>,
    eviction_lock: Arc<tokio::sync::RwLock<()>>,
    metrics: CodeMetrics,
}

impl CacheBudget {
    pub fn new(
        base_dir: PathBuf,
        usable_budget: u64,
        disk_budget: u64,
        large_repo_threshold: u64,
        metrics: CodeMetrics,
    ) -> Self {
        Self {
            base_dir,
            usable_budget,
            disk_budget,
            large_repo_threshold,
            index: Arc::new(RwLock::new(HashMap::new())),
            pin_counts: Arc::new(RwLock::new(HashMap::new())),
            total_bytes: Arc::new(AtomicU64::new(0)),
            phantom_bytes: Arc::new(AtomicU64::new(0)),
            eviction_lock: Arc::new(tokio::sync::RwLock::new(())),
            metrics,
        }
    }

    fn branch_dir(&self, project_id: i64, branch: &str) -> PathBuf {
        self.base_dir
            .join(project_id.to_string())
            .join(hashed_branch_name(branch))
    }

    /// Pin an entry so it cannot be evicted. Returns a guard that unpins on drop.
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

    pub fn large_repo_threshold(&self) -> u64 {
        self.large_repo_threshold
    }

    /// Record an entry's size in the index after measuring it on disk.
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

    /// Update the last-accessed timestamp for an entry.
    pub fn touch(&self, project_id: i64, branch: &str) {
        let key = cache_key(project_id, branch);
        let mut index = self.index.write();
        if let Some(entry) = index.get_mut(&key) {
            entry.last_accessed = SystemTime::now();
        }
    }

    /// Adjust a recorded entry's size by a signed delta without re-measuring disk.
    /// Used after incremental updates to keep the budget estimate roughly accurate.
    pub fn adjust_size(&self, project_id: i64, branch: &str, delta: i64) {
        let key = cache_key(project_id, branch);
        let mut index = self.index.write();
        if let Some(entry) = index.get_mut(&key) {
            let new_size = (entry.size_bytes as i64).saturating_add(delta).max(0) as u64;
            let old_size = entry.size_bytes;
            entry.size_bytes = new_size;

            if new_size >= old_size {
                self.total_bytes
                    .fetch_add(new_size - old_size, Ordering::Relaxed);
            } else {
                self.total_bytes
                    .fetch_sub(old_size - new_size, Ordering::Relaxed);
            }
        }
        drop(index);
        self.report_state();
    }

    /// Remove an entry from the index (e.g. on invalidation).
    pub fn remove(&self, project_id: i64, branch: &str) {
        let key = cache_key(project_id, branch);
        if let Some(entry) = self.index.write().remove(&key) {
            self.total_bytes
                .fetch_sub(entry.size_bytes, Ordering::Relaxed);
        }
        self.report_state();
    }

    /// Acquire a read lock on eviction. Hold this across `get` → `pin` sequences
    /// to prevent an in-flight eviction from deleting the entry between the two calls.
    pub async fn eviction_read_lock(&self) -> EvictionReadGuard<'_> {
        self.eviction_lock.read().await
    }

    /// Evict unpinned entries until the cache has room for `new_entry_bytes`.
    ///
    /// Call this *before* `record_size` so the new entry's bytes are not yet
    /// counted in `total_bytes`.
    ///
    /// Holds the eviction write lock for its duration, blocking new `get`+`pin`
    /// sequences until eviction is complete.
    pub async fn make_room(&self, new_entry_bytes: u64) -> Result<(), CacheBudgetExhausted> {
        let _eviction_guard = self.eviction_lock.write().await;
        let target = self.usable_budget.saturating_sub(new_entry_bytes);
        let total = self.total_bytes.load(Ordering::Relaxed);

        if total <= target {
            return Ok(());
        }

        let plan = self.eviction_candidates();
        self.evict_until_under(target, total, plan).await
    }

    /// Snapshot the index, partition into small/large tiers, and sort each by LRU.
    fn eviction_candidates(&self) -> EvictionPlan {
        let index = self.index.read();
        let pins = self.pin_counts.read();

        let mut small_repos = Vec::new();
        let mut large_repos = Vec::new();

        for (key, entry) in index.iter() {
            if pins.get(key).is_some_and(|&count| count > 0) {
                continue;
            }
            let candidate = EvictionCandidate {
                key: key.clone(),
                size_bytes: entry.size_bytes,
            };
            if entry.size_bytes < self.large_repo_threshold {
                small_repos.push((candidate, entry.last_accessed));
            } else {
                large_repos.push((candidate, entry.last_accessed));
            }
        }

        small_repos.sort_by_key(|(_, ts)| *ts);
        large_repos.sort_by_key(|(_, ts)| *ts);

        EvictionPlan {
            small_repos: small_repos.into_iter().map(|(c, _)| c).collect(),
            large_repos: large_repos.into_iter().map(|(c, _)| c).collect(),
        }
    }

    /// Walk candidates oldest-first (small repos first, then large), deleting
    /// from disk and removing from the index until we're under `target` bytes.
    ///
    /// If all unpinned entries are evicted and we're still over budget, either
    /// purges the entire cache (when phantom bytes exceed headroom) or returns
    /// an error so the caller can retry when workers release their pins.
    async fn evict_until_under(
        &self,
        target: u64,
        total_bytes: u64,
        plan: EvictionPlan,
    ) -> Result<(), CacheBudgetExhausted> {
        let mut remaining = total_bytes;
        let mut evicted_bytes = 0u64;
        let mut evicted_keys = Vec::new();

        let candidates = plan.small_repos.into_iter().chain(plan.large_repos);

        for candidate in candidates {
            if remaining <= target {
                break;
            }

            let entry_dir = self.branch_dir(candidate.key.0, &candidate.key.1);

            if let Err(e) = tokio::fs::remove_dir_all(&entry_dir).await {
                self.phantom_bytes
                    .fetch_add(candidate.size_bytes, Ordering::Relaxed);
                warn!(
                    project_id = candidate.key.0,
                    branch = %candidate.key.1,
                    error = %e,
                    phantom_bytes = self.phantom_bytes.load(Ordering::Relaxed),
                    "failed to delete evicted repository from disk, removing from index anyway"
                );
            } else {
                info!(
                    project_id = candidate.key.0,
                    branch = %candidate.key.1,
                    size_bytes = candidate.size_bytes,
                    "evicted cached repository"
                );
            }

            evicted_keys.push(candidate.key);
            remaining = remaining.saturating_sub(candidate.size_bytes);
            evicted_bytes += candidate.size_bytes;
        }

        if !evicted_keys.is_empty() {
            let mut index = self.index.write();
            let mut confirmed_bytes = 0u64;
            for key in &evicted_keys {
                if let Some(entry) = index.remove(key) {
                    confirmed_bytes += entry.size_bytes;
                }
            }
            self.total_bytes
                .fetch_sub(confirmed_bytes, Ordering::Relaxed);
            evicted_bytes = confirmed_bytes;
        }

        if evicted_bytes > 0 {
            self.metrics.record_cache_eviction(evicted_bytes);
        }

        self.report_state();

        if remaining > target {
            let phantom = self.phantom_bytes.load(Ordering::Relaxed);
            let headroom = self.disk_budget.saturating_sub(self.usable_budget);

            if phantom > headroom {
                self.purge_entire_cache().await;
                return Ok(());
            }

            return Err(CacheBudgetExhausted {
                remaining_bytes: remaining,
                target_bytes: target,
            });
        }

        Ok(())
    }

    /// Last-resort wipe of the entire cache directory. Called when phantom bytes
    /// from failed deletions push total estimated disk usage beyond the full disk
    /// budget (usable + headroom). Active workers holding pinned entries will get
    /// I/O errors and their messages will be retried.
    async fn purge_entire_cache(&self) {
        error!(
            phantom_bytes = self.phantom_bytes.load(Ordering::Relaxed),
            disk_budget = self.disk_budget,
            "phantom bytes exceeded disk budget, purging entire cache"
        );

        if let Err(e) = tokio::fs::remove_dir_all(&self.base_dir).await {
            error!(error = %e, "failed to purge cache directory");
        }
        let _ = tokio::fs::create_dir_all(&self.base_dir).await;

        self.index.write().clear();
        self.pin_counts.write().clear();
        self.total_bytes.store(0, Ordering::Relaxed);
        self.phantom_bytes.store(0, Ordering::Relaxed);
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
    use super::super::disk::hashed_branch_name;
    use super::*;
    use tempfile::TempDir;

    fn test_metrics() -> CodeMetrics {
        CodeMetrics::default()
    }

    fn create_budget(temp_dir: &Path, usable_budget: u64, large_threshold: u64) -> CacheBudget {
        CacheBudget::new(
            temp_dir.to_path_buf(),
            usable_budget,
            usable_budget * 2,
            large_threshold,
            test_metrics(),
        )
    }

    fn create_budget_with_headroom(
        temp_dir: &Path,
        usable_budget: u64,
        disk_budget: u64,
        large_threshold: u64,
    ) -> CacheBudget {
        CacheBudget::new(
            temp_dir.to_path_buf(),
            usable_budget,
            disk_budget,
            large_threshold,
            test_metrics(),
        )
    }

    async fn write_entry(base_dir: &Path, project_id: i64, branch_hash: &str, size: usize) {
        let branch_dir = base_dir.join(project_id.to_string()).join(branch_hash);
        let meta_dir = branch_dir.join("meta");
        let repo_dir = branch_dir.join("repository");
        tokio::fs::create_dir_all(&meta_dir).await.unwrap();
        tokio::fs::create_dir_all(&repo_dir).await.unwrap();
        tokio::fs::write(meta_dir.join(".commit"), "abc123")
            .await
            .unwrap();
        tokio::fs::write(repo_dir.join("data.bin"), vec![0u8; size])
            .await
            .unwrap();
    }

    fn register_entry(budget: &CacheBudget, project_id: i64, branch: &str, size: u64) {
        budget.record_size(project_id, branch, size);
    }

    async fn register_entry_on_disk(
        budget: &CacheBudget,
        base_dir: &Path,
        project_id: i64,
        branch: &str,
        size: usize,
    ) {
        let branch_hash = hashed_branch_name(branch);
        write_entry(base_dir, project_id, &branch_hash, size).await;
        budget.record_size(project_id, branch, size as u64);
    }

    fn entry_dir(base_dir: &Path, project_id: i64, branch: &str) -> PathBuf {
        base_dir
            .join(project_id.to_string())
            .join(hashed_branch_name(branch))
    }

    fn pin_entry(
        budget: &CacheBudget,
        base_dir: &Path,
        project_id: i64,
        branch: &str,
    ) -> RepositoryLease {
        budget.pin(entry_dir(base_dir, project_id, branch), project_id, branch)
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
    async fn pin_prevents_eviction() {
        let dir = TempDir::new().unwrap();
        let budget = create_budget(dir.path(), 500, 400);
        register_entry_on_disk(&budget, dir.path(), 1, "main", 600).await;

        let _guard = pin_entry(&budget, dir.path(), 1, "main");

        let result = budget.make_room(0).await;

        assert!(result.is_err(), "should fail when all entries are pinned");
        assert!(
            entry_dir(dir.path(), 1, "main").exists(),
            "pinned entry should not be evicted"
        );
    }

    #[tokio::test]
    async fn pin_count_drops_to_zero_after_guard_dropped() {
        let dir = TempDir::new().unwrap();
        let budget = create_budget(dir.path(), 1000, 500);

        let guard = budget.pin(PathBuf::from("/tmp/fake"), 42, "main");
        assert!(budget.is_pinned(&cache_key(42, "main")));

        drop(guard);
        assert!(!budget.is_pinned(&cache_key(42, "main")));
    }

    #[tokio::test]
    async fn eviction_removes_small_repos_before_large() {
        let dir = TempDir::new().unwrap();
        let budget = create_budget(dir.path(), 1200, 400);
        register_entry_on_disk(&budget, dir.path(), 1, "main", 500).await;
        register_entry_on_disk(&budget, dir.path(), 2, "main", 300).await;
        register_entry_on_disk(&budget, dir.path(), 3, "main", 350).await;

        // Total: 1150. Budget: 1200. Making room for 400 → target = 800.
        // Evicts small repos (300 + 350) before the large one.
        budget.make_room(400).await.unwrap();

        assert!(
            entry_dir(dir.path(), 1, "main").exists(),
            "large repo should survive"
        );
        assert!(
            !entry_dir(dir.path(), 2, "main").exists(),
            "small repo should be evicted"
        );
        assert!(
            !entry_dir(dir.path(), 3, "main").exists(),
            "small repo should be evicted"
        );
    }

    #[tokio::test]
    async fn eviction_prefers_oldest_entry_within_same_size_class() {
        let dir = TempDir::new().unwrap();
        let budget = create_budget(dir.path(), 600, 1000);

        register_entry_on_disk(&budget, dir.path(), 1, "main", 300).await;
        std::thread::sleep(std::time::Duration::from_millis(10));
        register_entry_on_disk(&budget, dir.path(), 2, "main", 300).await;

        // Total: 600. Budget: 600. Making room for 100 → target = 500.
        // Project 1 is older, should be evicted first.
        budget.make_room(100).await.unwrap();

        assert!(
            !entry_dir(dir.path(), 1, "main").exists(),
            "older entry should be evicted"
        );
        assert!(
            entry_dir(dir.path(), 2, "main").exists(),
            "newer entry should survive"
        );
    }

    #[tokio::test]
    async fn make_room_is_noop_when_under_budget() {
        let dir = TempDir::new().unwrap();
        let budget = create_budget(dir.path(), 10_000, 500);
        register_entry_on_disk(&budget, dir.path(), 1, "main", 100).await;

        budget.make_room(100).await.unwrap();

        assert!(entry_dir(dir.path(), 1, "main").exists());
    }

    #[tokio::test]
    async fn remove_deletes_from_index() {
        let dir = TempDir::new().unwrap();
        let budget = create_budget(dir.path(), 10_000, 500);
        register_entry(&budget, 1, "main", 100);

        budget.remove(1, "main");

        assert!(budget.index.read().is_empty());
    }

    #[tokio::test]
    async fn touch_updates_last_accessed() {
        let dir = TempDir::new().unwrap();
        let budget = create_budget(dir.path(), 10_000, 500);
        register_entry(&budget, 1, "main", 100);
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
    async fn purges_entire_cache_when_phantom_bytes_exceed_headroom() {
        let dir = TempDir::new().unwrap();
        // usable_budget=500, disk_budget=600 → headroom=100
        let budget = create_budget_with_headroom(dir.path(), 500, 600, 400);
        register_entry_on_disk(&budget, dir.path(), 1, "main", 550).await;
        let _guard = pin_entry(&budget, dir.path(), 1, "main");

        // Phantom bytes (101) exceed headroom (100) → triggers purge
        budget.phantom_bytes.store(101, Ordering::Relaxed);

        let result = budget.make_room(0).await;

        assert!(
            result.is_ok(),
            "purge should recover instead of returning an error"
        );
        assert!(budget.index.read().is_empty(), "index should be cleared");
        assert_eq!(budget.phantom_bytes.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn does_not_purge_when_phantom_bytes_within_headroom() {
        let dir = TempDir::new().unwrap();
        // usable_budget=500, disk_budget=600 → headroom=100
        let budget = create_budget_with_headroom(dir.path(), 500, 600, 400);
        register_entry_on_disk(&budget, dir.path(), 1, "main", 550).await;
        let _guard = pin_entry(&budget, dir.path(), 1, "main");

        // Phantom bytes (50) within headroom (100) → no purge, returns error
        budget.phantom_bytes.store(50, Ordering::Relaxed);

        let result = budget.make_room(0).await;

        assert!(result.is_err(), "should return budget exhausted, not purge");
        assert!(!budget.index.read().is_empty(), "index should be intact");
    }

    #[tokio::test]
    async fn concurrent_eviction_and_pin_do_not_race() {
        let dir = TempDir::new().unwrap();
        let budget = Arc::new(CacheBudget::new(
            dir.path().to_path_buf(),
            200,
            400,
            500,
            test_metrics(),
        ));
        let hash = hashed_branch_name("main");
        write_entry(dir.path(), 1, &hash, 200).await;
        budget.record_size(1, "main", 200);

        let entry_path = dir.path().join("1").join(&hash);

        let mut readers = Vec::new();
        for _ in 0..10 {
            let budget = Arc::clone(&budget);
            let path = entry_path.clone();
            readers.push(tokio::spawn(async move {
                let _read_guard = budget.eviction_read_lock().await;
                let guard = budget.pin(path, 1, "main");
                tokio::task::yield_now().await;
                drop(guard);
            }));
        }

        let evictor = {
            let budget = Arc::clone(&budget);
            tokio::spawn(async move {
                let _ = budget.make_room(200).await;
            })
        };

        for reader in readers {
            reader.await.unwrap();
        }
        evictor.await.unwrap();
    }
}
