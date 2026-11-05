fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    export_current_archive_paths();
    #[cfg(feature = "regenerate-protos")]
    regenerate_protos();
}

fn export_current_archive_paths() {
    for (variable, directory) in [
        ("ONTOLOGY_ARCHIVE_PATH", "ontology-archives"),
        ("MODEL_ARCHIVE_PATH", "model-archives"),
    ] {
        let directory = std::path::Path::new(env!("ORBIT_CONFIG_DIR")).join(directory);
        println!("cargo:rerun-if-changed={}", directory.display());
        let current_path = directory.join(format!("v{}.tar.gz", orbit_versions::VERSIONS.schema));
        let resolved = current_path.canonicalize().unwrap_or_else(|_| {
            panic!(
                "{} is missing; run `mise schema:snapshot` to seed the current archive.",
                current_path.display()
            )
        });
        println!("cargo:rustc-env={variable}={}", resolved.display());
    }
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
