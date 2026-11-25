use std::path::{Path, PathBuf};

include!("build_support/crate_map_drift.rs");

fn main() {
    let root = workspace_root();
    let manifest = root.join("Cargo.toml");
    let crate_map = root.join("docs/pages/engineering/development/agents-crate-map.md");

    println!("cargo:rerun-if-changed={}", manifest.display());
    println!("cargo:rerun-if-changed={}", crate_map.display());

    let manifest_src = std::fs::read_to_string(&manifest)
        .unwrap_or_else(|e| panic!("read {}: {e}", manifest.display()));
    let crate_map_src = std::fs::read_to_string(&crate_map)
        .unwrap_or_else(|e| panic!("read {}: {e}", crate_map.display()));

    if let Some(report) = drift_report(&manifest_src, &crate_map_src) {
        panic!("{report}");
    }
}

fn workspace_root() -> PathBuf {
    let manifest_dir =
        std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR set by cargo");
    Path::new(&manifest_dir)
        .ancestors()
        .nth(2)
        .expect("crates/xtask has a workspace-root ancestor")
        .to_path_buf()
}
