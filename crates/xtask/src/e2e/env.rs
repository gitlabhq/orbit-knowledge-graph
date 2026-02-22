//! Environment and path utilities.

use std::env;
use std::path::PathBuf;

/// Read an env var or return the default.
pub fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

/// Read a required env var. Panics with a clear message if not set.
pub fn require(key: &str) -> String {
    env::var(key).unwrap_or_else(|_| {
        panic!("{key} environment variable is required but not set");
    })
}

/// Resolve `~` at the start of a path to `$HOME`.
pub fn expand_home(path: &str) -> PathBuf {
    if let Some(rest) = path.strip_prefix("~/")
        && let Ok(home) = env::var("HOME")
    {
        return PathBuf::from(home).join(rest);
    }
    PathBuf::from(path)
}

/// Find the workspace root by walking up from the xtask binary location.
/// Falls back to `CARGO_MANIFEST_DIR` at compile time.
pub fn workspace_root() -> PathBuf {
    // At runtime: the binary is at <root>/target/debug/xtask.
    // Walk up from current exe to find Cargo.toml with [workspace].
    if let Ok(exe) = env::current_exe() {
        let mut dir = exe.as_path();
        while let Some(parent) = dir.parent() {
            if parent.join("Cargo.toml").exists() && parent.join("crates").exists() {
                return parent.to_path_buf();
            }
            dir = parent;
        }
    }

    // Compile-time fallback: xtask's Cargo.toml is at crates/xtask/
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .map(|p| p.to_path_buf())
        .expect("could not determine workspace root")
}
