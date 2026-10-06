fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    emit_build_version();
    generate_vfs_tests();
}

fn generate_vfs_tests() {
    let root = std::path::Path::new("tests/vfs/cases");
    println!("cargo:rerun-if-changed={}", root.display());
    let mut paths: Vec<_> = std::fs::read_dir(root)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "yaml")
        })
        .collect();
    paths.sort();
    assert!(!paths.is_empty(), "VFS suite must contain scenarios");
    let mut code = String::new();
    for path in paths {
        let name = path.file_stem().unwrap().to_str().unwrap();
        assert!(
            name.starts_with(|c: char| c.is_ascii_alphabetic())
                && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'),
            "invalid scenario name: {name}"
        );
        let path = path.canonicalize().unwrap();
        code.push_str(&format!(
            "#[test]\nfn {name}() {{ runner::run(include_str!({path:?}), std::path::Path::new({path:?})); }}\n"
        ));
    }
    std::fs::write(
        std::path::Path::new(&std::env::var("OUT_DIR").unwrap()).join("vfs_cases.rs"),
        code,
    )
    .unwrap();
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
