//! Build-time drift check for the crate map.
//!
//! `docs/dev/agents-crate-map.md` is hand-maintained and `AGENTS.md`
//! requires updating it in the same MR that adds, removes, or renames a
//! crate. Nothing enforced that, so it silently drifted. This build script
//! parses the `[workspace].members` list from the root `Cargo.toml`, derives
//! each crate's workspace-relative path, and asserts it appears as a row in
//! the crate map (and that the map has no rows for crates that no longer
//! exist). It `panic!`s with the offending entries on drift, so it fails
//! locally and in CI (any `cargo build`/`clippy` over the workspace builds
//! `xtask`).
//!
//! Name-presence only: descriptions are not validated. The parsing/diffing
//! logic lives in `build_support/crate_map_drift.rs` so the test target can
//! `include!` and exercise it too.

use std::path::{Path, PathBuf};

include!("build_support/crate_map_drift.rs");

fn main() {
    let root = workspace_root();
    let manifest = root.join("Cargo.toml");
    let crate_map = root.join("docs/dev/agents-crate-map.md");

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

/// `build.rs` runs with `CARGO_MANIFEST_DIR` pointing at `crates/xtask`; the
/// workspace root is two levels up.
fn workspace_root() -> PathBuf {
    let manifest_dir =
        std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR set by cargo");
    Path::new(&manifest_dir)
        .ancestors()
        .nth(2)
        .expect("crates/xtask has a workspace-root ancestor")
        .to_path_buf()
}
