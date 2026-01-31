//! Distributed locking for indexing jobs using NATS KV store.
//!
//! Locks prevent duplicate indexing jobs from running concurrently.
//! The dispatcher acquires locks before publishing requests, and handlers
//! release locks upon successful completion.

use std::time::Duration;

pub const INDEXING_LOCKS_BUCKET: &str = "indexing_locks";
pub const LOCK_TTL: Duration = Duration::from_secs(300);

pub fn global_lock_key() -> &'static str {
    "global"
}

pub fn namespace_lock_key(namespace_id: i64) -> String {
    format!("namespace.{namespace_id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn global_lock_key_returns_expected_value() {
        assert_eq!(global_lock_key(), "global");
    }

    #[test]
    fn namespace_lock_key_formats_correctly() {
        assert_eq!(namespace_lock_key(123), "namespace.123");
        assert_eq!(namespace_lock_key(456), "namespace.456");
    }
}
