//! Materialises Orbit Grafana dashboards from jsonnet sources.
//!
//! Walks `dashboards/orbit/*.dashboard.jsonnet`, runs each through `jsonnet`
//! (provided by the `aqua:google/go-jsonnet` mise tool), and writes the
//! resulting JSON next to the source. With `--check`, compares each
//! regenerated file against its committed copy and fails on drift, mirroring
//! the `metrics-catalog` CI gate.

use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, anyhow, bail};

/// Default source directory relative to the workspace root.
const DEFAULT_DIR: &str = "dashboards/orbit";
/// Glob suffix that identifies dashboard sources (rather than helpers).
const SOURCE_SUFFIX: &str = ".dashboard.jsonnet";

pub fn run(dir: Option<PathBuf>, check: bool) -> Result<()> {
    let dir = dir.unwrap_or_else(|| PathBuf::from(DEFAULT_DIR));
    let sources = collect_sources(&dir)?;
    if sources.is_empty() {
        bail!("no `*{SOURCE_SUFFIX}` files found under {}", dir.display());
    }

    let mut drift = Vec::new();
    for src in &sources {
        let dest = src.with_extension("json");
        let rendered = run_jsonnet(src)?;
        if check {
            let current = fs::read_to_string(&dest)
                .with_context(|| format!("reading existing dashboard at {}", dest.display()))?;
            if normalise(&current) != normalise(&rendered) {
                drift.push(
                    dest.file_name()
                        .and_then(OsStr::to_str)
                        .unwrap_or("?")
                        .to_string(),
                );
            }
        } else {
            fs::write(&dest, &rendered)
                .with_context(|| format!("writing dashboard to {}", dest.display()))?;
            println!("wrote {}", dest.display());
        }
    }

    if check {
        if drift.is_empty() {
            println!(
                "dashboards are up to date ({} files in {})",
                sources.len(),
                dir.display(),
            );
            return Ok(());
        }
        eprintln!("dashboards in {} are stale:", dir.display());
        for name in &drift {
            eprintln!("  - {name}");
        }
        eprintln!("run `mise run dashboards` and commit.");
        return Err(anyhow!("{} dashboard(s) stale", drift.len()));
    }

    println!(
        "generated {} dashboards under {}",
        sources.len(),
        dir.display(),
    );
    Ok(())
}

fn collect_sources(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut sources: Vec<PathBuf> = fs::read_dir(dir)
        .with_context(|| format!("reading {}", dir.display()))?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|p| {
            p.file_name()
                .and_then(OsStr::to_str)
                .is_some_and(|name| name.ends_with(SOURCE_SUFFIX))
        })
        .collect();
    sources.sort();
    Ok(sources)
}

fn run_jsonnet(src: &Path) -> Result<String> {
    // Prefer `jsonnet` already on PATH (e.g. when mise is activated). Fall
    // back to `mise exec -- jsonnet ...` so a user with mise installed but
    // not yet activated still gets a working build.
    let direct = Command::new("jsonnet").arg(src).output();
    let output = match direct {
        Ok(o) => o,
        Err(_) => Command::new("mise")
            .args(["exec", "--", "jsonnet"])
            .arg(src)
            .output()
            .with_context(|| {
                format!(
                    "running jsonnet for {} (need `aqua:google/go-jsonnet` installed via mise)",
                    src.display()
                )
            })?,
    };
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("jsonnet failed for {}:\n{stderr}", src.display());
    }
    let mut json = String::from_utf8(output.stdout)
        .with_context(|| format!("jsonnet output for {} not UTF-8", src.display()))?;
    if !json.ends_with('\n') {
        json.push('\n');
    }
    Ok(json)
}

/// Canonicalise JSON so whitespace-only differences don't count as drift.
fn normalise(raw: &str) -> String {
    match serde_json::from_str::<serde_json::Value>(raw) {
        Ok(value) => serde_json::to_string_pretty(&value).unwrap_or_else(|_| raw.to_string()),
        Err(_) => raw.to_string(),
    }
}
