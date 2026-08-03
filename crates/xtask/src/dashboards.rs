//! Materialises Orbit Grafana dashboards from jsonnet sources.
//!
//! Walks `dashboards/orbit/*.dashboard.jsonnet` and runs each through
//! `jsonnet` (provided by the `aqua:google/go-jsonnet` mise tool) twice,
//! once per flavor: `com` (for dashboards.gitlab.net) lands next to the
//! source, `dedicated` (for GitLab Dedicated tenant Grafanas) lands in the
//! sibling `dashboards/dedicated/` directory. With `--check`, compares each
//! regenerated file against its committed copy and fails on drift, mirroring
//! the `metrics-catalog` CI gate.

use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, anyhow, bail};

const DEFAULT_DIR: &str = "dashboards/orbit";
/// Sibling directory (of the source dir) receiving the `dedicated` flavor.
const DEDICATED_DIR_NAME: &str = "dedicated";
/// Glob suffix that identifies dashboard sources (rather than helpers).
const SOURCE_SUFFIX: &str = ".dashboard.jsonnet";

pub fn run(dir: Option<PathBuf>, check: bool) -> Result<()> {
    let dir = dir.unwrap_or_else(|| PathBuf::from(DEFAULT_DIR));
    let sources = collect_sources(&dir)?;
    if sources.is_empty() {
        bail!("no `*{SOURCE_SUFFIX}` files found under {}", dir.display());
    }

    let dedicated_dir = dir
        .parent()
        .map(|p| p.join(DEDICATED_DIR_NAME))
        .unwrap_or_else(|| PathBuf::from(DEDICATED_DIR_NAME));
    if !check {
        fs::create_dir_all(&dedicated_dir)
            .with_context(|| format!("creating {}", dedicated_dir.display()))?;
    }

    let mut drift = Vec::new();
    for src in &sources {
        let json_name = src.with_extension("json");
        let json_name = json_name
            .file_name()
            .ok_or_else(|| anyhow!("no file name in {}", src.display()))?;
        let flavors = [
            ("com", src.with_extension("json")),
            ("dedicated", dedicated_dir.join(json_name)),
        ];
        for (flavor, dest) in flavors {
            let rendered = run_jsonnet(src, flavor)?;
            if check {
                let current = fs::read_to_string(&dest)
                    .with_context(|| format!("reading existing dashboard at {}", dest.display()))?;
                if normalise(&current) != normalise(&rendered) {
                    drift.push(dest.display().to_string());
                }
            } else {
                fs::write(&dest, &rendered)
                    .with_context(|| format!("writing dashboard to {}", dest.display()))?;
                println!("wrote {}", dest.display());
            }
        }
    }

    if check {
        if drift.is_empty() {
            println!(
                "dashboards are up to date ({} sources in {}, com + dedicated flavors)",
                sources.len(),
                dir.display(),
            );
            return Ok(());
        }
        eprintln!("dashboards are stale:");
        for name in &drift {
            eprintln!("  - {name}");
        }
        eprintln!("run `mise run dashboards` and commit.");
        return Err(anyhow!("{} dashboard(s) stale", drift.len()));
    }

    println!(
        "generated {} dashboards under {} and {}",
        sources.len() * 2,
        dir.display(),
        dedicated_dir.display(),
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

fn run_jsonnet(src: &Path, flavor: &str) -> Result<String> {
    let flavor_arg = format!("flavor={flavor}");
    // Prefer `jsonnet` already on PATH (e.g. when mise is activated). Fall
    // back to `mise exec -- jsonnet ...` so a user with mise installed but
    // not yet activated still gets a working build.
    let direct = Command::new("jsonnet")
        .args(["--ext-str", &flavor_arg])
        .arg(src)
        .output();
    let output = match direct {
        Ok(o) => o,
        Err(_) => Command::new("mise")
            .args(["exec", "--", "jsonnet", "--ext-str", &flavor_arg])
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
