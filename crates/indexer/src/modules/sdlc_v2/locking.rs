use std::time::Duration;

pub const INDEXING_LOCKS_BUCKET: &str = "indexing_locks";
pub const SDLC_LOCK_TTL: Duration = Duration::from_secs(300);

pub fn global_lock_key() -> &'static str {
    "global"
}

pub fn namespace_lock_key(organization_id: i64, namespace_id: i64) -> String {
    format!("namespace.{organization_id}.{namespace_id}")
}
