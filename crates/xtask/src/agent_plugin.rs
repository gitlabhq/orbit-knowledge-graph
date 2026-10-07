use std::fs;
use std::io::{Cursor, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail, ensure};
use serde_json::{Value, json};
use zip::write::SimpleFileOptions;
use zip::{CompressionMethod, DateTime, ZipArchive, ZipWriter};

const PLUGIN: &str = "plugins/orbit";
const WRAPPER: &str = "skills/orbit-wrapper/SKILL.md";
const CATALOGS: [&str; 2] = [
    ".claude-plugin/marketplace.json",
    ".agents/plugins/marketplace.json",
];

pub fn package(output: &Path) -> Result<()> {
    fs::write(output, archive()?).with_context(|| format!("writing {}", output.display()))
}

pub fn check() -> Result<()> {
    let packaged = archive()?;
    ensure!(packaged == archive()?, "plugin archive is not reproducible");
    let scratch = tempfile::Builder::new().prefix("orbit plugin ").tempdir()?;
    let root = scratch.path().join("installed");
    ZipArchive::new(Cursor::new(packaged))?.extract(&root)?;
    let unpacked = |path: &PathBuf| fs::read(root.join(path)).ok() == fs::read(path).ok();
    ensure!(files()?.iter().all(unpacked), "archive contents differ");

    let plugin = root.join(PLUGIN);
    let portable = read_json(&plugin.join("plugin.json"))?;
    let claude = read_json(&plugin.join(".claude-plugin/plugin.json"))?;
    let keys = ["name", "version", "description", "author", "repository"];
    ensure!(
        keys.iter().all(|key| portable[key] == claude[key]),
        "plugin manifests differ"
    );
    ensure!(
        fs::read(plugin.join(WRAPPER)).ok() == Some(fs::read(WRAPPER)?),
        "{PLUGIN}/{WRAPPER} differs from {WRAPPER}"
    );
    for catalog in CATALOGS {
        check_catalog(&root, catalog, &portable["name"])?;
    }
    #[cfg(unix)]
    check_hooks(
        scratch.path(),
        &plugin,
        claude["hooks"].as_str().unwrap_or("-"),
    )?;
    println!("Agent plugin manifests, archive, and hooks are valid.");
    Ok(())
}

fn check_catalog(root: &Path, catalog: &str, name: &Value) -> Result<()> {
    let plugin = fs::canonicalize(root.join(PLUGIN))?;
    let market = read_json(&root.join(catalog))?;
    let entry = &market["plugins"][0];
    let source = entry["source"]
        .as_str()
        .or(entry["source"]["path"].as_str())
        .and_then(|source| fs::canonicalize(root.join(source)).ok());
    ensure!(
        market["plugins"].as_array().map(Vec::len) == Some(1)
            && entry["name"] == *name
            && source.as_ref() == Some(&plugin),
        "{catalog} must list only {PLUGIN}"
    );
    Ok(())
}

#[cfg(unix)]
fn check_hooks(scratch: &Path, plugin: &Path, hooks_file: &str) -> Result<()> {
    use std::os::unix::fs::{PermissionsExt, symlink};

    let config = read_json(&plugin.join(hooks_file))?;
    let hooks = config["hooks"]["PreToolUse"]
        .as_array()
        .filter(|hooks| hooks.len() == 1);
    let command = hooks.context("expected one search PreToolUse hook")?[0]["hooks"][0]["command"]
        .as_str()
        .unwrap_or("-");
    let (bin, input) = (scratch.join("bin"), scratch.join("in"));
    fs::create_dir(&bin)?;
    symlink("/bin/sh", bin.join("sh"))?;
    let payload = json!({"tool_input": {"command": "rg function src"}}).to_string();
    fs::write(&input, &payload)?;
    let echo = "printf \"%s\\n\" \"$*\"; /bin/cat";
    let fail = "echo x >&2; exit 1";
    for (case, orbit, glab, expected) in [
        ("missing", None, None, String::new()),
        (
            "orbit",
            Some(echo),
            Some(fail),
            format!("hook-guard search\n{payload}"),
        ),
        (
            "glab",
            None,
            Some(echo),
            format!("orbit hook-guard search\n{payload}"),
        ),
        ("failing", Some(fail), None, String::new()),
    ] {
        for (name, body) in [("orbit", orbit), ("glab", glab)] {
            let path = bin.join(name);
            match body {
                Some(body) => {
                    fs::write(&path, format!("#!/bin/sh\n{body}\n"))?;
                    fs::set_permissions(&path, fs::Permissions::from_mode(0o755))?;
                }
                None if path.exists() => fs::remove_file(&path)?,
                None => {}
            }
        }
        let output = std::process::Command::new("/bin/sh")
            .args(["-c", command])
            .env("PATH", &bin)
            .env("CLAUDE_PLUGIN_ROOT", plugin)
            .stdin(fs::File::open(&input)?)
            .output()?;
        ensure!(
            output.status.success()
                && output.stdout == expected.as_bytes()
                && output.stderr.is_empty(),
            "search hook misbehaves when {case}: {output:?}"
        );
    }
    Ok(())
}

fn files() -> Result<Vec<PathBuf>> {
    let mut paths: Vec<PathBuf> = ["LICENSE.md", CATALOGS[0], CATALOGS[1]]
        .map(PathBuf::from)
        .into();
    for entry in walkdir::WalkDir::new(PLUGIN) {
        let entry = entry?;
        if entry.path_is_symlink() {
            bail!(
                "plugin files must not be symlinks: {}",
                entry.path().display()
            );
        }
        if entry.file_type().is_file() {
            paths.push(entry.into_path());
        }
    }
    paths.sort();
    Ok(paths)
}

fn archive() -> Result<Vec<u8>> {
    let options = SimpleFileOptions::default()
        .compression_method(CompressionMethod::Deflated)
        .unix_permissions(0o644)
        .last_modified_time(DateTime::default());
    let mut zip = ZipWriter::new(Cursor::new(Vec::new()));
    for path in files()? {
        zip.start_file(path.to_string_lossy().replace('\\', "/"), options)?;
        zip.write_all(&fs::read(&path)?)?;
    }
    Ok(zip.finish()?.into_inner())
}

fn read_json(path: &Path) -> Result<Value> {
    let raw = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    serde_json::from_str(&raw).with_context(|| format!("parsing {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_source_errors_identify_the_catalog() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir_all(dir.path().join(PLUGIN)).unwrap();
        let catalog = CATALOGS[0];
        let path = dir.path().join(catalog);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        for source in [Value::Null, json!("missing"), json!({"path": "missing"})] {
            let market = json!({"plugins": [{"name": "orbit", "source": source}]});
            fs::write(&path, market.to_string()).unwrap();
            let error = check_catalog(dir.path(), catalog, &json!("orbit")).unwrap_err();
            assert_eq!(
                error.to_string(),
                format!("{catalog} must list only {PLUGIN}")
            );
        }
    }
}
