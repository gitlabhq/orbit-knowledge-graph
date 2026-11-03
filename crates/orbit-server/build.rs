fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    export_current_ontology_archive_path();
    emit_build_version();
    #[cfg(feature = "regenerate-protos")]
    regenerate_protos();
}

/// Points `ONTOLOGY_ARCHIVE_PATH` at the bundled archive for the current
/// schema version.
fn export_current_ontology_archive_path() {
    let directory = std::path::Path::new(env!("CONFIG_DIR")).join("ontology-archives");
    println!("cargo:rerun-if-changed={}", directory.display());
    // Keep in sync with `OntologyArchive::path`.
    let current_path = directory.join(format!("v{}.tar.gz", orbit_versions::VERSIONS.schema));
    let resolved = current_path.canonicalize().unwrap_or_else(|_| {
        panic!(
            "{} is missing; run `mise schema:snapshot` to seed the current archive.",
            current_path.display()
        )
    });
    println!(
        "cargo:rustc-env=ONTOLOGY_ARCHIVE_PATH={}",
        resolved.display()
    );
}

fn emit_build_version() {
    let git_dir = std::path::Path::new("../../.git");

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

    if let Some(version) = git_describe() {
        println!("cargo:rustc-env=GKG_BUILD_VERSION={version}");
    }
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

    println!("cargo:rerun-if-changed=proto/orbit.proto");

    let proto_path = PathBuf::from("proto/orbit.proto");
    if !proto_path.exists() {
        println!("cargo:warning=proto/orbit.proto not found, skipping proto regeneration");
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
        .compile_protos(&["proto/orbit.proto"], &["proto"])
        .expect("Failed to compile protos");

    println!("cargo:warning=Regenerated protos to {}", out_dir.display());
}
