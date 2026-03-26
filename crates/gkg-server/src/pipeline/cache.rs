//! Query result cache for cursor pagination.
//!
//! Caches the full authorized result (post-redaction, post-hydration,
//! pre-cursor-slicing) so that subsequent pages of the same query skip
//! ClickHouse execution, authorization, redaction, and hydration.
//!
//! Keyed by `(user_id, canonical_query_hash)` where the hash is computed
//! from the query JSON with the `cursor` field stripped and keys
//! canonicalized per RFC 8785. TTL-based expiry ensures authorization
//! changes propagate within the configured window.
//!
//! Uses `moka` for lock-free concurrent caching with automatic TTL eviction.

use std::hash::{DefaultHasher, Hash, Hasher};
use std::time::Duration;

use moka::sync::Cache;
use query_engine::compiler::input::InputCursor;
use query_engine::shared::PipelineOutput;
use tracing::{debug, info, warn};

/// Maximum number of cached query results. At ~5 KB per entry (typical
/// search with 30 hydrated rows), 16 384 entries ≈ 80 MB worst case.
const MAX_CACHE_ENTRIES: u64 = 16_384;

/// Maximum cached entries per user to prevent a single user from
/// flooding the cache.
const MAX_ENTRIES_PER_USER: usize = 2;

/// Cache TTL in seconds. Short enough that authorization changes
/// propagate quickly, long enough for multi-page browsing.
const CACHE_TTL_SECS: u64 = 60;

/// Cache key: (user_id, hash of canonicalized query JSON without cursor).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CacheKey {
    user_id: u64,
    query_hash: u64,
}

/// Errors that can occur during cache key computation.
#[derive(Debug, thiserror::Error)]
pub enum CacheError {
    #[error("failed to compute cache key: {0}")]
    Key(#[from] serde_json::Error),
}

pub struct QueryResultCache {
    cache: Cache<CacheKey, PipelineOutput>,
}

impl Default for QueryResultCache {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryResultCache {
    pub fn new() -> Self {
        Self {
            cache: Cache::builder()
                .time_to_live(Duration::from_secs(CACHE_TTL_SECS))
                .max_capacity(MAX_CACHE_ENTRIES)
                .build(),
        }
    }

    /// Look up a cached result for this user and query.
    /// Returns `None` on miss. Logs a warning and returns `None` on key errors.
    pub fn get(&self, user_id: u64, query_json: &str) -> Option<PipelineOutput> {
        let key = match Self::make_key(user_id, query_json) {
            Ok(k) => k,
            Err(e) => {
                warn!(user_id, error = %e, "cache key error on get, skipping cache");
                return None;
            }
        };
        let result = self.cache.get(&key);
        if result.is_some() {
            debug!(user_id, "query result cache hit");
        } else {
            debug!(user_id, "query result cache miss");
        }
        result
    }

    /// Store a result in the cache. Enforces per-user entry limit by
    /// evicting the user's oldest entries when the limit is exceeded.
    /// Logs a warning and skips caching on key errors.
    pub fn put(&self, user_id: u64, query_json: &str, output: PipelineOutput) {
        let key = match Self::make_key(user_id, query_json) {
            Ok(k) => k,
            Err(e) => {
                warn!(user_id, error = %e, "cache key error on put, skipping cache");
                return;
            }
        };

        // Count existing entries for this user and evict oldest if over limit.
        let user_entries: Vec<CacheKey> = self
            .cache
            .iter()
            .filter(|(k, _)| k.user_id == user_id)
            .map(|(k, _)| (*k).clone())
            .collect();

        if user_entries.len() >= MAX_ENTRIES_PER_USER {
            let to_evict = user_entries.len() - (MAX_ENTRIES_PER_USER - 1);
            for evict_key in user_entries.into_iter().take(to_evict) {
                self.cache.invalidate(&evict_key);
            }
            info!(
                user_id,
                evicted = to_evict,
                "evicted cached queries to enforce per-user limit"
            );
        }

        self.cache.insert(key, output);
    }

    fn make_key(user_id: u64, query_json: &str) -> Result<CacheKey, CacheError> {
        Ok(CacheKey {
            user_id,
            query_hash: Self::hash_query(query_json)?,
        })
    }

    /// Hash the query JSON with the cursor field stripped and keys
    /// canonicalized per RFC 8785 (JCS). This ensures semantically
    /// equivalent queries with different key ordering or whitespace
    /// produce the same hash.
    fn hash_query(query_json: &str) -> Result<u64, CacheError> {
        let mut v: serde_json::Value = serde_json::from_str(query_json)?;
        if let Some(obj) = v.as_object_mut() {
            obj.remove("cursor");
        }
        let canonical = json_canon::to_string(&v)?;
        let mut hasher = DefaultHasher::new();
        canonical.hash(&mut hasher);
        Ok(hasher.finish())
    }
}

/// Parse the cursor from raw query JSON without going through the full
/// compilation pipeline. Returns `None` if the cursor field is absent
/// or malformed.
pub fn parse_cursor_from_json(query_json: &str) -> Option<InputCursor> {
    let v: serde_json::Value = serde_json::from_str(query_json).ok()?;
    let cursor_val = v.get("cursor")?;
    serde_json::from_value(cursor_val.clone()).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_query_different_cursor_shares_hash() {
        let q1 = r#"{"query_type":"search","node":{"id":"u","entity":"User"},"limit":100,"cursor":{"offset":0,"page_size":20}}"#;
        let q2 = r#"{"query_type":"search","node":{"id":"u","entity":"User"},"limit":100,"cursor":{"offset":20,"page_size":20}}"#;
        let q3 = r#"{"query_type":"search","node":{"id":"u","entity":"User"},"limit":100}"#;

        assert_eq!(
            QueryResultCache::hash_query(q1).unwrap(),
            QueryResultCache::hash_query(q2).unwrap(),
            "different cursors should produce the same hash"
        );
        assert_eq!(
            QueryResultCache::hash_query(q1).unwrap(),
            QueryResultCache::hash_query(q3).unwrap(),
            "cursor vs no-cursor should produce the same hash"
        );
    }

    #[test]
    fn different_queries_have_different_hashes() {
        let q1 = r#"{"query_type":"search","node":{"id":"u","entity":"User"},"limit":100}"#;
        let q2 = r#"{"query_type":"search","node":{"id":"p","entity":"Project"},"limit":100}"#;

        assert_ne!(
            QueryResultCache::hash_query(q1).unwrap(),
            QueryResultCache::hash_query(q2).unwrap(),
        );
    }

    #[test]
    fn different_key_order_same_hash() {
        let q1 = r#"{"query_type":"search","node":{"id":"u","entity":"User"},"limit":100}"#;
        let q2 = r#"{"limit":100,"node":{"entity":"User","id":"u"},"query_type":"search"}"#;

        assert_eq!(
            QueryResultCache::hash_query(q1).unwrap(),
            QueryResultCache::hash_query(q2).unwrap(),
            "different key order should produce the same hash"
        );
    }

    #[test]
    fn invalid_json_returns_error() {
        assert!(QueryResultCache::hash_query("not json").is_err());
    }

    #[test]
    fn parse_cursor_extracts_offset_and_page_size() {
        let json = r#"{"query_type":"search","node":{"id":"u","entity":"User"},"limit":100,"cursor":{"offset":20,"page_size":10}}"#;
        let cursor = parse_cursor_from_json(json).unwrap();
        assert_eq!(cursor.offset, 20);
        assert_eq!(cursor.page_size, 10);
    }

    #[test]
    fn parse_cursor_returns_none_without_cursor() {
        let json = r#"{"query_type":"search","node":{"id":"u","entity":"User"},"limit":100}"#;
        assert!(parse_cursor_from_json(json).is_none());
    }
}
