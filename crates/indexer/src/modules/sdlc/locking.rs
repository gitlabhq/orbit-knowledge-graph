//! Distributed locking for indexing jobs using NATS KV store.
//!
//! Locks prevent duplicate indexing jobs from running concurrently.
//! The dispatcher acquires locks before publishing requests, and handlers
//! release locks upon successful completion.

use std::time::Duration;

pub const INDEXING_LOCKS_BUCKET: &str = "indexing_locks";
pub const SDLC_LOCK_TTL: Duration = Duration::from_secs(300);

pub fn global_lock_key() -> &'static str {
    "global"
}

pub fn namespace_lock_key(organization_id: i64, namespace_id: i64) -> String {
    format!("namespace.{organization_id}.{namespace_id}")
}

pub fn project_lock_key(project_id: i64, branch: &str) -> String {
    use base64::Engine;
    let encoded_branch = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(branch);
    format!("project.{project_id}.{encoded_branch}")
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
        assert_eq!(namespace_lock_key(1, 123), "namespace.1.123");
        assert_eq!(namespace_lock_key(2, 456), "namespace.2.456");
    }

    #[test]
    fn project_lock_key_formats_correctly() {
        assert_eq!(
            project_lock_key(42, "refs/heads/main"),
            "project.42.cmVmcy9oZWFkcy9tYWlu"
        );
    }
}
