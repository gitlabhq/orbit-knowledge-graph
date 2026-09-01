//! Bundled DuckDB fts extension, embedded at release build time so search
//! never downloads a dylib over the network at runtime. DuckDB's own
//! signature and metadata checks still run on every LOAD, so a stale or
//! wrong-platform artifact fails loudly instead of being re-downloaded.

use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

use crate::error::{DuckDbError, Result};

include!(concat!(env!("OUT_DIR"), "/bundled_fts.rs"));

/// Decompress the embedded extension under `data_dir` and return its path.
/// The directory name embeds the DuckDB version and build target so engine
/// upgrades and cross-arch binaries never reuse each other's artifact.
pub(crate) fn ensure_extension_on_disk(data_dir: &Path) -> Result<PathBuf> {
    let dir = data_dir.join("duckdb-extensions").join(format!(
        "{DUCKDB_VERSION}-{}-{}",
        std::env::consts::ARCH,
        std::env::consts::OS
    ));
    let path = dir.join("fts.duckdb_extension");
    if path.is_file() {
        return Ok(path);
    }
    fs::create_dir_all(&dir).map_err(io_err)?;

    let mut bytes = Vec::new();
    flate2::read::GzDecoder::new(EXTENSION_GZ)
        .read_to_end(&mut bytes)
        .map_err(io_err)?;

    // Concurrent orbit processes may race here; each writes a unique temp
    // file and renames it, so a reader never observes a partial extension.
    let tmp = dir.join(format!(".fts.duckdb_extension.{}", std::process::id()));
    fs::write(&tmp, &bytes).map_err(io_err)?;
    fs::rename(&tmp, &path).map_err(io_err)?;
    Ok(path)
}

fn io_err(e: std::io::Error) -> DuckDbError {
    DuckDbError::Schema(format!("failed to materialize bundled fts extension: {e}"))
}
