//! Shared git helpers: resolve_tree_oid, hex_sha256, generate_packfile.

use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};

use crate::BenchError;

pub fn resolve_tree_oid(repo: &Path, commit: &str) -> Result<String, BenchError> {
    let o = Command::new("git")
        .args(["rev-parse", &format!("{commit}^{{tree}}")])
        .current_dir(repo)
        .output()
        .map_err(|e| BenchError::Git(format!("rev-parse: {e}")))?;
    if !o.status.success() {
        return Err(BenchError::Git("rev-parse failed".into()));
    }
    Ok(String::from_utf8_lossy(&o.stdout).trim().to_string())
}

pub fn hex_sha256(data: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(data);
    format!("{:x}", h.finalize())
}

/// Run `rev-list --objects | pack-objects --stdout` and return (pack_bytes, duration, root_tree_oid).
pub fn generate_packfile_stdout(
    repo: &Path, commit: &str, extra_flags: &[&str],
) -> Result<(Vec<u8>, Duration, String), BenchError> {
    let root = resolve_tree_oid(repo, commit)?;
    let t = Instant::now();

    let mut rev_list = Command::new("git")
        .args(["rev-list", "--objects", "--stdin"])
        .current_dir(repo)
        .stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped())
        .spawn().map_err(|e| BenchError::Git(format!("rev-list: {e}")))?;

    {
        let mut s = rev_list.stdin.take().unwrap();
        writeln!(s, "{commit}^{{tree}}").ok();
    }

    let mut args = vec!["pack-objects", "--stdout", "-q", "--delta-base-offset"];
    args.extend_from_slice(extra_flags);

    let out = Command::new("git").args(&args)
        .current_dir(repo)
        .stdin(rev_list.stdout.take().unwrap())
        .stdout(Stdio::piped()).stderr(Stdio::piped())
        .output().map_err(|e| BenchError::Git(format!("pack-objects: {e}")))?;
    let _ = rev_list.wait();

    if !out.status.success() {
        return Err(BenchError::Git(format!(
            "pack-objects: {}", String::from_utf8_lossy(&out.stderr)
        )));
    }
    Ok((out.stdout, t.elapsed(), root))
}
