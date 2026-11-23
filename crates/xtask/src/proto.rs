use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, bail};

const PACKAGE: &str = "orbit-server-config";
const SCHEMA: &str = "proto/orbit/config/v1/config.proto";
const CRATE: &str = "prost-protovalidate-types";

pub fn run() -> Result<()> {
    let crate_dir = repo_root().join("crates").join(PACKAGE);
    let out_dir = crate_dir.join("src/proto");
    std::fs::create_dir_all(&out_dir)
        .with_context(|| format!("failed to create {}", out_dir.display()))?;

    let protovalidate = protovalidate_proto_dir()?;

    prost_build::Config::new()
        .out_dir(&out_dir)
        .file_descriptor_set_path(out_dir.join("config_descriptor.bin"))
        .enable_type_names()
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
