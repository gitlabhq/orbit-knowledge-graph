use std::env;
use std::fs;
use std::path::Path;

/// When `ORBIT_BUNDLED_FTS` points at a gzipped DuckDB fts extension
/// artifact (and `ORBIT_BUNDLED_FTS_VERSION` names its DuckDB version),
/// embed it and enable the `bundled_fts` cfg so release binaries load the
/// extension from disk instead of downloading it. Release CI sets both via
/// scripts/ci/fetch-duckdb-fts.sh; dev builds leave them unset.
fn main() {
    println!("cargo::rustc-check-cfg=cfg(bundled_fts)");
    println!("cargo:rerun-if-env-changed=ORBIT_BUNDLED_FTS");
    println!("cargo:rerun-if-env-changed=ORBIT_BUNDLED_FTS_VERSION");

    let path = env::var("ORBIT_BUNDLED_FTS").ok().filter(|s| !s.is_empty());
    let version = env::var("ORBIT_BUNDLED_FTS_VERSION")
        .ok()
        .filter(|s| !s.is_empty());
    let (path, version) = match (path, version) {
        (Some(path), Some(version)) => (path, version),
        (None, None) => return,
        _ => panic!("ORBIT_BUNDLED_FTS and ORBIT_BUNDLED_FTS_VERSION must be set together"),
    };

    let path = Path::new(&path);
    assert!(
        path.is_absolute() && path.is_file(),
        "ORBIT_BUNDLED_FTS must be an absolute path to an existing file, got {}",
        path.display()
    );
    assert!(
        version
            .strip_prefix('v')
            .is_some_and(|v| v.chars().all(|c| c.is_ascii_digit() || c == '.')),
        "ORBIT_BUNDLED_FTS_VERSION must look like v1.5.5, got {version}"
    );

    let generated = format!(
        "pub const EXTENSION_GZ: &[u8] = include_bytes!({path:?});\n\
         pub const DUCKDB_VERSION: &str = {version:?};\n"
    );
    let out_dir = env::var("OUT_DIR").unwrap();
    fs::write(Path::new(&out_dir).join("bundled_fts.rs"), generated).unwrap();
    println!("cargo:rerun-if-changed={}", path.display());
    println!("cargo:rustc-cfg=bundled_fts");
}
