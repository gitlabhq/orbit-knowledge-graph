fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    emit_build_version();
}

/// Emit `GKG_BUILD_VERSION` so the binary has a meaningful compiled-in
/// fallback when the `GKG_VERSION` env var is unset.
///
/// Priority: `git describe --tags --match 'v*'` (stripped of the leading `v`)
/// → `0.0.0-dev` when git metadata is unavailable (tarballs, shallow clones
/// without tags).
fn emit_build_version() {
    let git_dir = std::path::Path::new("../../.git");

    // In a git worktree, `.git` is a file (not a directory) pointing at the
    // real gitdir, so `join("HEAD")` below won't resolve. The emitted version
    // is still correct (git-describe works regardless), but the
    // `rerun-if-changed` directives are skipped, meaning cargo may serve a
    // stale version until an unrelated rebuild forces a re-run. Acceptable
    // for a dev-only scenario.
    if git_dir.is_dir() {
        println!("cargo:rerun-if-changed=../../.git/HEAD");
        if let Ok(head) = std::fs::read_to_string(git_dir.join("HEAD"))
            && let Some(refpath) = head.strip_prefix("ref: ")
        {
            let refpath = refpath.trim();
            println!("cargo:rerun-if-changed=../../.git/{refpath}");
        }
        println!("cargo:rerun-if-changed=../../.git/packed-refs");
    }

    let version = git_describe().unwrap_or_else(|| "0.0.0-dev".to_string());
    println!("cargo:rustc-env=GKG_BUILD_VERSION={version}");
}

fn git_describe() -> Option<String> {
    let output = std::process::Command::new("git")
        .args(["describe", "--tags", "--match", "v*", "--always"])
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let raw = String::from_utf8(output.stdout).ok()?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }

    Some(trimmed.strip_prefix('v').unwrap_or(trimmed).to_string())
}
