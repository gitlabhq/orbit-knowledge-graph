use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use serde_json::Value;

pub(super) const GRAPH_MARKER: &str = "graph";

const TTL: Duration = Duration::from_secs(2 * 24 * 60 * 60);

pub(super) struct Session {
    dir: PathBuf,
}

impl Session {
    pub(super) fn for_call(call: &Value, now: SystemTime) -> Option<Self> {
        let base = dirs::runtime_dir()
            .or_else(dirs::cache_dir)
            .unwrap_or_else(std::env::temp_dir)
            .join("orbit-hook-sessions");
        Self::open(&base, &id(call)?, now)
    }

    pub(super) fn open(base: &Path, id: &str, now: SystemTime) -> Option<Self> {
        std::fs::create_dir_all(base).ok()?;
        let dir = base.join(id);
        match std::fs::create_dir(&dir) {
            Ok(()) => prune(base, &dir, now),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(_) => return None,
        }
        Some(Self { dir })
    }

    pub(super) fn has(&self, name: &str) -> bool {
        self.dir.join(name).exists()
    }

    pub(super) fn claim(&self, name: &str) -> bool {
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(self.dir.join(name))
            .is_ok()
    }

    pub(super) fn swap(&self, name: &str, value: &str) -> Option<String> {
        let path = self.dir.join(name);
        let previous = std::fs::read_to_string(&path).ok();
        std::fs::write(&path, value).ok()?;
        previous
    }
}

pub(super) fn marker(key: &str) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hasher);
    format!("m-{:016x}", hasher.finish())
}

fn id(call: &Value) -> Option<String> {
    let id: String = call
        .get("session_id")
        .and_then(Value::as_str)?
        .chars()
        .map(
            |c| match c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                true => c,
                false => '_',
            },
        )
        .take(64)
        .collect();
    (!id.is_empty()).then_some(id)
}

fn prune(base: &Path, current: &Path, now: SystemTime) {
    let Ok(entries) = std::fs::read_dir(base) else {
        return;
    };
    for entry in entries.flatten().filter(|entry| entry.path() != current) {
        let Ok(meta) = entry.metadata() else {
            continue;
        };
        let stale = meta
            .modified()
            .ok()
            .and_then(|modified| now.duration_since(modified).ok())
            .is_some_and(|age| age > TTL);
        if stale {
            let _ = match meta.is_dir() {
                true => std::fs::remove_dir_all(entry.path()),
                false => std::fs::remove_file(entry.path()),
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prunes_stale_sessions_and_sanitizes_ids() {
        let base = tempfile::tempdir().unwrap();
        let now = SystemTime::now();
        Session::open(base.path(), "old", now).unwrap();
        Session::open(base.path(), "new", now + TTL * 2).unwrap();
        assert!(!base.path().join("old").exists() && base.path().join("new").exists());
        let id = id(&serde_json::json!({"session_id": "../x"}));
        assert_eq!(id.as_deref(), Some("___x"));
    }
}
