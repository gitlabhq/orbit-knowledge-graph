use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, bail};

const PACKAGE: &str = "orbit-server-config";
const SCHEMA: &str = "proto/orbit/config/v1/config.proto";
const CRATE: &str = "prost-protovalidate-types";

/// Regenerates the prost types and descriptor set for the server
/// configuration schema into `crates/orbit-server-config/src/proto`.
///
/// The descriptor set is what carries the `buf.validate` rules to the loader,
/// so a stale one stops enforcing them without any error. CI regenerates and
/// fails on a diff.
///
/// This binary links `orbit-server-config`, so the committed output has to be
/// present before it can be rebuilt.
pub fn run() -> Result<()> {
    let crate_dir = repo_root().join("crates").join(PACKAGE);
    let out_dir = crate_dir.join("src/proto");
    std::fs::create_dir_all(&out_dir)
        .with_context(|| format!("failed to create {}", out_dir.display()))?;

    // `validate.proto` comes from the crate the loader validates with, so the
    // rules compiled into the descriptor are the ones the runtime understands.
    let protovalidate = protovalidate_proto_dir()?;

    prost_build::Config::new()
        .out_dir(&out_dir)
        .file_descriptor_set_path(out_dir.join("config_descriptor.bin"))
        .enable_type_names()
        // Keeps prost-build from emitting Rust for the imported package; only
        // the descriptor needs it, and the loader brings its own types.
        .extern_path(".buf.validate", "::prost_protovalidate_types")
        .compile_protos(
            &[&crate_dir.join(SCHEMA)],
            &[&crate_dir.join("proto"), &protovalidate],
        )
        .context("failed to compile the configuration protos")?;

    println!("wrote {}", out_dir.display());
    Ok(())
}

fn repo_root() -> PathBuf {
    Path::new(env!("ORBIT_CONFIG_DIR"))
        .parent()
        .expect("ORBIT_CONFIG_DIR is a directory inside the repository")
        .to_path_buf()
}

fn protovalidate_proto_dir() -> Result<PathBuf> {
    let output = Command::new(std::env::var("CARGO").as_deref().unwrap_or("cargo"))
        .args(["metadata", "--format-version", "1"])
        .output()
        .context("failed to run cargo metadata")?;
    if !output.status.success() {
        bail!(
            "cargo metadata failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let metadata: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    let found: Vec<&serde_json::Value> = metadata["packages"]
        .as_array()
        .context("cargo metadata has no packages")?
        .iter()
        .filter(|package| package["name"] == CRATE)
        .collect();

    // Two versions in the graph means the generated rules and the validating
    // ones could come from different schemas, which fails by quietly skipping
    // a rule rather than by erroring.
    let [package] = found.as_slice() else {
        bail!(
            "expected exactly one {CRATE} in the dependency graph, found {}",
            found.len()
        );
    };

    let manifest = package["manifest_path"]
        .as_str()
        .context("package has no manifest_path")?;

    Ok(Path::new(manifest)
        .parent()
        .expect("a manifest path has a parent")
        .join("proto"))
}
