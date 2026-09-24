use std::fs;
use std::io::{Cursor, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail, ensure};
use serde_json::{Value, json};
use zip::write::SimpleFileOptions;
use zip::{CompressionMethod, DateTime, ZipArchive, ZipWriter};

const PLUGIN: &str = "plugins/orbit";
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
    let skill = fs::read_to_string(plugin.join("skills/orbit-cli/SKILL.md"))?;
    ensure!(
        skill_version(&skill) == portable["version"].as_str(),
        "SKILL.md version differs"
    );
    for catalog in CATALOGS {
        let market = read_json(&root.join(catalog))?;
        let entry = &market["plugins"][0];
        let source = entry["source"]
            .as_str()
            .or(entry["source"]["path"].as_str());
        ensure!(
            market["plugins"].as_array().map(Vec::len) == Some(1)
                && entry["name"] == portable["name"]
                && fs::canonicalize(root.join(source.unwrap_or("-")))?
                    == fs::canonicalize(&plugin)?,
            "{catalog} must list only {PLUGIN}"
        );
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

#[cfg(unix)]
fn check_hooks(scratch: &Path, plugin: &Path, hooks_file: &str) -> Result<()> {
    use std::os::unix::fs::{PermissionsExt, symlink};

    let config = read_json(&plugin.join(hooks_file))?;
    let hooks = config["hooks"]["PreToolUse"]
        .as_array()
        .filter(|hooks| hooks.len() == 2);
    let hooks = hooks.context("expected search and read PreToolUse hooks")?;
    let (bin, orbit, input) = (
        scratch.join("bin"),
        scratch.join("bin/orbit"),
        scratch.join("in"),
    );
    fs::create_dir(&bin)?;
    symlink("/bin/sh", bin.join("sh"))?;
    let payload = json!({"tool_input": {"command": "rg function src"}}).to_string();
    fs::write(&input, &payload)?;
    let success = "printf \"%s\\n\" \"$*\"; /bin/cat";
    for (case, body) in [
        ("missing", ""),
        ("success", success),
        ("failure", "echo x >&2; exit 1"),
    ] {
        if !body.is_empty() {
            fs::write(&orbit, format!("#!/bin/sh\n{body}\n"))?;
            fs::set_permissions(&orbit, fs::Permissions::from_mode(0o755))?;
        }
        for (hook, kind) in hooks.iter().zip(["search", "read"]) {
            let output = std::process::Command::new("/bin/sh")
                .args(["-c", hook["hooks"][0]["command"].as_str().unwrap_or("-")])
                .env("PATH", &bin)
                .env("CLAUDE_PLUGIN_ROOT", plugin)
                .stdin(fs::File::open(&input)?)
                .output()?;
            let expected = match case {
                "success" => format!("hook-guard {kind}\n{payload}"),
                _ => String::new(),
            };
            ensure!(
                output.status.success()
                    && output.stdout == expected.as_bytes()
                    && output.stderr.is_empty(),
                "{kind} hook misbehaves when orbit is {case}: {output:?}"
            );
        }
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

fn skill_version(skill: &str) -> Option<&str> {
    let frontmatter = skill.strip_prefix("---\n")?.split("\n---").next()?;
    let version = frontmatter
        .lines()
        .find_map(|line| line.strip_prefix("version:"))?;
    Some(version.trim().trim_matches(['"', '\'']))
}

fn read_json(path: &Path) -> Result<Value> {
    let raw = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    serde_json::from_str(&raw).with_context(|| format!("parsing {}", path.display()))
}
