fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    emit_build_version();
    #[cfg(feature = "regenerate-protos")]
    regenerate_protos();
}

/// Emit `GKG_BUILD_VERSION` so the binary has a meaningful compiled-in
/// fallback when the `GKG_VERSION` env var is unset.
///
/// Priority: `git describe --tags --match 'v*'` (stripped of the leading `v`)
/// → `0.0.0-dev` when git metadata is unavailable (tarballs, shallow clones
/// without tags).
fn emit_build_version() {
    let git_dir = std::path::Path::new("../../.git");
    if git_dir.exists() {
        // Re-run when HEAD moves (checkout / commit) or tags change.
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

#[cfg(feature = "regenerate-protos")]
fn regenerate_protos() {
    use std::path::PathBuf;
    use std::process::Command;

    println!("cargo:rerun-if-changed=proto/gkg.proto");

    let proto_path = PathBuf::from("proto/gkg.proto");
    if !proto_path.exists() {
        println!("cargo:warning=proto/gkg.proto not found, skipping proto regeneration");
        return;
    }

    if Command::new("protoc").arg("--version").output().is_err() {
        println!("cargo:warning=protoc not found, skipping proto regeneration");
        return;
    }

    let out_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap()).join("src/proto");

    std::fs::create_dir_all(&out_dir).expect("Failed to create src/proto directory");

    tonic_prost_build::configure()
        .out_dir(&out_dir)
        .compile_protos(&["proto/gkg.proto"], &["proto"])
        .expect("Failed to compile gkg protos");

    println!("cargo:warning=Regenerated protos to {}", out_dir.display());
}
