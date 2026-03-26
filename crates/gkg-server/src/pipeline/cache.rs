//! Query result cache for cursor pagination.
//!
//! Caches the full authorized result (post-redaction, post-hydration,
//! pre-cursor-slicing) so that subsequent pages of the same query skip
//! ClickHouse execution, authorization, redaction, and hydration.
//!
//! Keyed by `(user_id, canonical_query_hash)` where the hash is computed
//! from the query JSON with the `cursor` field stripped and keys sorted
//! for canonical ordering. TTL-based expiry ensures authorization changes
//! propagate within the configured window.
//!
//! Uses `moka` for lock-free concurrent caching with automatic TTL eviction.

use std::hash::{DefaultHasher, Hash, Hasher};
use std::time::Duration;

use moka::sync::Cache;
use query_engine::compiler::input::InputCursor;
use query_engine::shared::PipelineOutput;
use tracing::{debug, info};

/// Maximum number of cached query results. At ~5 KB per entry (typical
/// search with 30 hydrated rows), 16 384 entries ≈ 80 MB worst case.
const MAX_CACHE_ENTRIES: u64 = 16_384;

/// Maximum cached entries per user to prevent a single user from
/// flooding the cache.
const MAX_ENTRIES_PER_USER: usize = 2;

/// Cache key: (user_id, hash of canonicalized query JSON without cursor).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CacheKey {
    user_id: u64,
    query_hash: u64,
}

pub struct QueryResultCache {
    cache: Cache<CacheKey, PipelineOutput>,
}

impl QueryResultCache {
    pub fn new(ttl: Duration) -> Self {
        Self {
            cache: Cache::builder()
                .time_to_live(ttl)
                .max_capacity(MAX_CACHE_ENTRIES)
                .build(),
        }
    }

    /// Look up a cached result for this user and query.
    pub fn get(&self, user_id: u64, query_json: &str) -> Option<PipelineOutput> {
        let key = Self::make_key(user_id, query_json);
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
    pub fn put(&self, user_id: u64, query_json: &str, output: PipelineOutput) {
        let key = Self::make_key(user_id, query_json);

        // Count existing entries for this user and evict oldest if over limit.
        let user_entries: Vec<CacheKey> = self
            .cache
            .iter()
            .filter(|(k, _)| k.user_id == user_id)
            .map(|(k, _)| (*k).clone())
            .collect();

        if user_entries.len() >= MAX_ENTRIES_PER_USER {
            // Evict all but (MAX - 1) to make room for the new entry.
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

    fn make_key(user_id: u64, query_json: &str) -> CacheKey {
        CacheKey {
            user_id,
            query_hash: Self::hash_query(query_json),
        }
    }

    /// Hash the query JSON with the cursor field stripped and keys sorted
    /// for canonical ordering. This ensures semantically equivalent queries
    /// with different key ordering or whitespace produce the same hash.
    fn hash_query(query_json: &str) -> u64 {
        let normalized = match serde_json::from_str::<serde_json::Value>(query_json) {
            Ok(mut v) => {
                if let Some(obj) = v.as_object_mut() {
                    obj.remove("cursor");
                }
                canonical_json(&v)
            }
            Err(_) => query_json.to_string(),
        };
        let mut hasher = DefaultHasher::new();
        normalized.hash(&mut hasher);
        hasher.finish()
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

/// Produce a canonical JSON string with sorted object keys at all levels.
/// This ensures `{"a":1,"b":2}` and `{"b":2,"a":1}` hash identically.
fn canonical_json(value: &serde_json::Value) -> String {
    use serde_json::Value;
    match value {
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let entries: Vec<String> = keys
                .iter()
                .map(|k| {
                    format!(
                        "{}:{}",
                        serde_json::to_string(k).unwrap(),
                        canonical_json(&map[*k])
                    )
                })
                .collect();
            format!("{{{}}}", entries.join(","))
        }
        Value::Array(arr) => {
            let items: Vec<String> = arr.iter().map(canonical_json).collect();
            format!("[{}]", items.join(","))
        }
        other => serde_json::to_string(other).unwrap_or_default(),
    }
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
            QueryResultCache::hash_query(q1),
            QueryResultCache::hash_query(q2),
            "different cursors should produce the same hash"
        );
        assert_eq!(
            QueryResultCache::hash_query(q1),
            QueryResultCache::hash_query(q3),
            "cursor vs no-cursor should produce the same hash"
        );
    }

    #[test]
    fn different_queries_have_different_hashes() {
        let q1 = r#"{"query_type":"search","node":{"id":"u","entity":"User"},"limit":100}"#;
        let q2 = r#"{"query_type":"search","node":{"id":"p","entity":"Project"},"limit":100}"#;

        assert_ne!(
            QueryResultCache::hash_query(q1),
            QueryResultCache::hash_query(q2),
        );
    }

    #[test]
    fn different_key_order_same_hash() {
        let q1 = r#"{"query_type":"search","node":{"id":"u","entity":"User"},"limit":100}"#;
        let q2 = r#"{"limit":100,"node":{"entity":"User","id":"u"},"query_type":"search"}"#;

        assert_eq!(
            QueryResultCache::hash_query(q1),
            QueryResultCache::hash_query(q2),
            "different key order should produce the same hash"
        );
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
