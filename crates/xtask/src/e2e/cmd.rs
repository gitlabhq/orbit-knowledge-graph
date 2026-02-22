//! Shell command helpers built on top of [`xshell`].
//!
//! Thin convenience wrappers so callers don't have to repeat `.quiet()` /
//! `.ignore_status()` / `.ignore_stdout()` chains everywhere.

use xshell::{Shell, cmd};

/// Check if a command exists on PATH.
pub fn exists(sh: &Shell, program: &str) -> bool {
    cmd!(sh, "which {program}")
        .quiet()
        .ignore_status()
        .ignore_stdout()
        .ignore_stderr()
        .run()
        .is_ok()
}

/// Run a command silently, returning `true` if it exits 0.
pub fn succeeds(sh: &Shell, program: &str, args: &[&str]) -> bool {
    cmd!(sh, "{program}")
        .args(args)
        .quiet()
        .ignore_status()
        .ignore_stdout()
        .ignore_stderr()
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Run a command and capture its stdout as a trimmed string.
/// Returns `None` if the command fails.
pub fn capture(sh: &Shell, program: &str, args: &[&str]) -> Option<String> {
    cmd!(sh, "{program}")
        .args(args)
        .quiet()
        .ignore_status()
        .ignore_stderr()
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
}
