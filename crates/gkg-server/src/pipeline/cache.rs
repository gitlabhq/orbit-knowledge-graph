//! Query result cache for cursor pagination.
//!
//! Caches the full authorized result (post-redaction, post-hydration,
//! pre-cursor-slicing) so that subsequent pages of the same query skip
//! ClickHouse execution, authorization, redaction, and hydration.
//!
//! Keyed by `(user_id, query_json_hash)` where the hash is computed
//! from the query JSON with the `cursor` field stripped. TTL-based
//! expiry ensures authorization changes propagate within the configured
//! window.

use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use query_engine::shared::PipelineOutput;

/// Cache key: (user_id, hash of query JSON without cursor).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CacheKey {
    user_id: u64,
    query_hash: u64,
}

struct CacheEntry {
    output: PipelineOutput,
    inserted_at: Instant,
}

pub struct QueryResultCache {
    entries: Mutex<HashMap<CacheKey, CacheEntry>>,
    ttl: Duration,
}

impl QueryResultCache {
    pub fn new(ttl: Duration) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            ttl,
        }
    }

    /// Look up a cached result for this user and query.
    /// Returns `None` on miss or if the entry has expired.
    pub fn get(&self, user_id: u64, query_json: &str) -> Option<PipelineOutput> {
        let key = Self::make_key(user_id, query_json);
        let entries = self.entries.lock().unwrap();
        let entry = entries.get(&key)?;
        if entry.inserted_at.elapsed() > self.ttl {
            return None;
        }
        Some(entry.output.clone())
    }

    /// Store a result in the cache. Evicts expired entries opportunistically.
    pub fn put(&self, user_id: u64, query_json: &str, output: PipelineOutput) {
        let key = Self::make_key(user_id, query_json);
        let mut entries = self.entries.lock().unwrap();

        // Opportunistic eviction of expired entries.
        let ttl = self.ttl;
        entries.retain(|_, e| e.inserted_at.elapsed() <= ttl);

        entries.insert(
            key,
            CacheEntry {
                output,
                inserted_at: Instant::now(),
            },
        );
    }

    fn make_key(user_id: u64, query_json: &str) -> CacheKey {
        CacheKey {
            user_id,
            query_hash: Self::hash_query(query_json),
        }
    }

    /// Hash the query JSON with the cursor field stripped so that
    /// different pages of the same query share a cache entry.
    fn hash_query(query_json: &str) -> u64 {
        let normalized = match serde_json::from_str::<serde_json::Value>(query_json) {
            Ok(mut v) => {
                if let Some(obj) = v.as_object_mut() {
                    obj.remove("cursor");
                }
                v.to_string()
            }
            Err(_) => query_json.to_string(),
        };
        let mut hasher = DefaultHasher::new();
        normalized.hash(&mut hasher);
        hasher.finish()
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
}
