use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use super::{
    Installer, Report, backup_once, file_mentions, remove_file_and_empty_parents, write_file,
};
use super::{hooks, json};
use crate::commands::setup::Target;
use crate::commands::setup::spec::{Agent, PluginSpec};

const MANIFEST: &str = ".orbit-setup.json";

pub(super) struct AgentPlugins;

impl Installer for AgentPlugins {
    fn plan(&self, agent: Agent, target: &Target) -> Result<Vec<String>> {
        let plugin = agent
            .plugin
            .iter()
            .flat_map(|plugin| [&plugin.dir, &plugin.settings]);
        let files = agent.ui_files(true).map(|file| &file.path);
        let registrations = agent.ui_registrations(true).map(|entry| &entry.file);
        plugin
            .chain(files)
            .chain(registrations)
            .map(|scoped| target.resolve(scoped).map(|(_, label)| label))
            .collect()
    }

    fn install(&self, agents: &[Agent], target: &Target, report: &mut Report) -> Result<()> {
        for agent in agents {
            if let Some(plugin) = &agent.plugin {
                install_plugin(*agent, plugin, target, report)?;
            }
            hooks::install_files(
                agent.ui_files(true),
                agent.ui_registrations(true),
                target,
                report,
            )?;
        }
        Ok(())
    }

    fn remove(&self, agents: &[Agent], target: &Target, report: &mut Report) -> Result<()> {
        for agent in agents {
            if let Some(plugin) = &agent.plugin {
                remove_plugin(plugin, target, report)?;
            }
            hooks::remove_files(
                agent.ui_files(true),
                agent.ui_registrations(true),
                target,
                report,
            )?;
        }
        Ok(())
    }

    fn is_installed(&self, agent: Agent, target: &Target) -> bool {
        let plugin = agent.plugin.as_ref().is_some_and(|plugin| {
            target
                .resolve(&plugin.dir)
                .is_ok_and(|(dir, _)| dir.join(MANIFEST).is_file())
        });
        plugin
            || agent.ui_files(true).any(|file| {
                target
                    .resolve(&file.path)
                    .is_ok_and(|(path, _)| path.exists())
            })
    }
}

fn install_plugin(
    agent: Agent,
    plugin: &PluginSpec,
    target: &Target,
    report: &mut Report,
) -> Result<()> {
    let (dir, dir_label) = target.resolve(&plugin.dir)?;
    let found = run(&plugin.version_command, target)
        .ok()
        .and_then(|stdout| stdout.split_whitespace().next().map(str::to_string));
    if !found
        .as_deref()
        .is_some_and(|version| at_least(version, &plugin.min_version))
    {
        report.note(
            &dir_label,
            format!(
                "Orbit UI skipped: needs {} {}+ (you have {})",
                agent.title,
                plugin.min_version,
                found.as_deref().unwrap_or("none")
            ),
        );
        return Ok(());
    }

    refuse_symlinks(target, &dir)?;
    let previous = read_manifest(&dir)?;
    let mut manifest = BTreeMap::new();
    let mut written = 0;
    let name = plugin.marketplace_for(target);
    for (relative, contents) in plugin.files(&name) {
        let path = dir.join(&relative);
        if !inside(&dir, &path) {
            bail!(
                "{} is a link out of {}; remove it and re-run",
                path.display(),
                dir.display()
            );
        }
        let current = std::fs::read(&path).ok();
        if current.as_deref() != Some(contents.as_slice()) {
            let ours = current
                .as_deref()
                .is_some_and(|current| previous.get(&relative) == Some(&hash(current)));
            if !ours {
                backup_once(&path, &format!("{dir_label}/{relative}"), report)?;
            }
            write_file(&path, &contents)?;
            written += 1;
        }
        manifest.insert(relative, hash(&contents));
    }
    for (relative, expected) in &previous {
        let path = dir.join(relative);
        if manifest.contains_key(relative) || !path.exists() || !inside(&dir, &path) {
            continue;
        }
        if std::fs::read(&path).is_ok_and(|current| hash(&current) == *expected) {
            remove_file_and_empty_parents(&path, target)?;
        } else {
            report.note(
                &format!("{dir_label}/{relative}"),
                "kept (edited, no longer shipped; delete it by hand)",
            );
        }
    }
    write_file(&dir.join(MANIFEST), json::render(&json!(manifest))?)?;
    report.note(
        &dir_label,
        match written {
            0 => "unchanged".to_string(),
            count => format!("{count} plugin files written"),
        },
    );

    let (settings, settings_label) = target.resolve(&plugin.settings)?;
    let mut root = json::read_object(&settings)?;
    let id = plugin.id(&name);
    let pruned = prune_stale_entries(plugin, &name, &mut root);
    let registered = json::set_key(&mut root, &marketplace_path(&name), marketplace(&dir))
        .with_context(|| format!("failed to update {}", settings.display()))?;
    let enabled = json::set_key(&mut root, &enabled_path(&id), json!(true))
        .with_context(|| format!("failed to update {}", settings.display()))?;
    let orbit_wrote_it = file_mentions(&settings, &id)
        || agent
            .json_merges
            .iter()
            .any(|merge| file_mentions(&settings, &merge.marker));
    if registered || enabled || pruned {
        if !orbit_wrote_it {
            backup_once(&settings, &settings_label, report)?;
        }
        json::write_object(&settings, &root)?;
        report.note(&settings_label, format!("{id} enabled"));
    } else {
        report.note(&settings_label, "unchanged");
    }
    prune_stale_user_marketplaces(plugin, &name, target);
    let add = [
        program(plugin),
        "plugin",
        "marketplace",
        "add",
        &dir.display().to_string(),
        "--scope",
        scope(target),
    ];
    if let Err(error) = run(&add.map(str::to_string), target) {
        report.note(
            &settings_label,
            format!("restart Claude Code once to load the plugin ({error})"),
        );
    }
    Ok(())
}

fn remove_plugin(plugin: &PluginSpec, target: &Target, report: &mut Report) -> Result<()> {
    let (dir, dir_label) = target.resolve(&plugin.dir)?;
    refuse_symlinks(target, &dir)?;
    if dir.join(MANIFEST).is_file() {
        let mut kept = 0;
        for (relative, expected) in read_manifest(&dir)? {
            let path = dir.join(&relative);
            if !inside(&dir, &path) {
                kept += 1;
                continue;
            }
            match std::fs::read(&path) {
                Ok(current) if hash(&current) == expected => {
                    remove_file_and_empty_parents(&path, target)?;
                }
                Ok(_) => kept += 1,
                Err(_) => {}
            }
        }
        remove_file_and_empty_parents(&dir.join(MANIFEST), target)?;
        report.note(
            &dir_label,
            match kept {
                0 => "removed".to_string(),
                count => format!("removed; kept {count} edited files (delete them by hand)"),
            },
        );
    }

    let name = plugin.marketplace_for(target);
    let (settings, settings_label) = target.resolve(&plugin.settings)?;
    if settings.exists() {
        let mut root = json::read_object(&settings)?;
        let pruned = prune_stale_entries(plugin, &name, &mut root);
        let unregistered =
            json::remove_key_if(&mut root, &marketplace_path(&name), &marketplace(&dir));
        let disabled =
            json::remove_key_if(&mut root, &enabled_path(&plugin.id(&name)), &json!(true));
        if unregistered || disabled || pruned {
            json::write_or_delete_when_empty(&settings, &root, target, &settings_label, report)?;
        }
        if unregistered || disabled {
            let remove = [program(plugin), "plugin", "marketplace", "remove", &name];
            let _ = run(&remove.map(str::to_string), target);
        }
    }
    prune_stale_user_marketplaces(plugin, &name, target);
    Ok(())
}

fn is_stale_orbit_marketplace(plugin: &PluginSpec, current: &str, name: &str, path: &str) -> bool {
    name != current
        && name.starts_with(&format!("{}-", plugin.marketplace))
        && !Path::new(path).exists()
}

/// Drops `enabledPlugins` and `extraKnownMarketplaces` entries of Orbit
/// marketplaces whose directory is gone, such as after a project move.
fn prune_stale_entries(plugin: &PluginSpec, current: &str, root: &mut Value) -> bool {
    let stale: Vec<String> = root["extraKnownMarketplaces"]
        .as_object()
        .into_iter()
        .flatten()
        .filter(|(name, entry)| {
            let source = &entry["source"];
            source["source"] == "directory"
                && source["path"]
                    .as_str()
                    .is_some_and(|path| is_stale_orbit_marketplace(plugin, current, name, path))
        })
        .map(|(name, _)| name.clone())
        .collect();
    for name in &stale {
        for (section, key) in [
            ("extraKnownMarketplaces", name.clone()),
            ("enabledPlugins", plugin.id(name)),
        ] {
            if let Some(map) = root.get_mut(section).and_then(Value::as_object_mut) {
                map.remove(&key);
                if map.is_empty()
                    && let Some(root) = root.as_object_mut()
                {
                    root.remove(section);
                }
            }
        }
    }
    !stale.is_empty()
}

/// Unregisters Orbit marketplaces whose directory is gone from Claude's
/// user-level state, which setup in a since-deleted project left behind.
fn prune_stale_user_marketplaces(plugin: &PluginSpec, current: &str, target: &Target) {
    let list = [program(plugin), "plugin", "marketplace", "list", "--json"];
    let Ok(stdout) = run(&list.map(str::to_string), target) else {
        return;
    };
    let Ok(Value::Array(entries)) = serde_json::from_str::<Value>(&stdout) else {
        return;
    };
    for entry in entries {
        let (Some(name), Some("directory"), Some(path)) = (
            entry["name"].as_str(),
            entry["source"].as_str(),
            entry["path"].as_str(),
        ) else {
            continue;
        };
        if is_stale_orbit_marketplace(plugin, current, name, path) {
            let remove = [program(plugin), "plugin", "marketplace", "remove", name];
            let _ = run(&remove.map(str::to_string), target);
        }
    }
}

fn marketplace(dir: &Path) -> Value {
    json!({ "source": { "source": "directory", "path": dir.display().to_string() } })
}

fn marketplace_path(name: &str) -> [String; 2] {
    ["extraKnownMarketplaces".to_string(), name.to_string()]
}

fn enabled_path(id: &str) -> [String; 2] {
    ["enabledPlugins".to_string(), id.to_string()]
}

fn read_manifest(dir: &Path) -> Result<BTreeMap<String, String>> {
    let path = dir.join(MANIFEST);
    let entries: BTreeMap<String, String> = match std::fs::read_to_string(&path) {
        Ok(raw) => serde_json::from_str(&raw)
            .with_context(|| format!("{} is not valid; remove it and re-run", path.display()))?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
        Err(e) => return Err(e).with_context(|| format!("failed to read {}", path.display())),
    };
    if let Some(bad) = entries.keys().find(|key| !is_plain_relative(key)) {
        bail!(
            "{} lists {bad:?}, which is not a path inside the plugin folder; remove it and re-run",
            path.display()
        );
    }
    Ok(entries)
}

fn refuse_symlinks(target: &Target, dir: &Path) -> Result<()> {
    let Target::Project(root) = target else {
        return Ok(());
    };
    let relative = dir.strip_prefix(root).unwrap_or(Path::new(""));
    let mut path = root.clone();
    let folders = relative.components().map(|part| {
        path.push(part);
        path.clone()
    });
    for link in folders.chain([dir.join(MANIFEST)]) {
        if std::fs::symlink_metadata(&link).is_ok_and(|meta| meta.file_type().is_symlink()) {
            bail!(
                "{} is a symbolic link, so the plugin would be written outside {}; replace it with a real {} and re-run",
                link.display(),
                root.display(),
                if link.ends_with(MANIFEST) {
                    "file"
                } else {
                    "folder"
                },
            );
        }
    }
    Ok(())
}

fn inside(dir: &Path, path: &Path) -> bool {
    if std::fs::symlink_metadata(path).is_ok_and(|meta| meta.file_type().is_symlink()) {
        return false;
    }
    let Ok(dir) = std::fs::canonicalize(dir) else {
        return true;
    };
    let ancestor = path.ancestors().skip(1).find(|ancestor| ancestor.exists());
    ancestor
        .and_then(|ancestor| std::fs::canonicalize(ancestor).ok())
        .is_some_and(|ancestor| ancestor.starts_with(&dir))
}

fn is_plain_relative(key: &str) -> bool {
    !key.is_empty()
        && Path::new(key)
            .components()
            .all(|part| matches!(part, std::path::Component::Normal(_)))
}

fn hash(contents: &[u8]) -> String {
    Sha256::digest(contents)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn program(plugin: &PluginSpec) -> &str {
    plugin
        .version_command
        .first()
        .map_or("claude", String::as_str)
}

fn scope(target: &Target) -> &'static str {
    match target {
        Target::Global => "user",
        Target::Project(_) => "local",
    }
}

#[cfg(test)]
thread_local! {
    pub(in crate::commands::setup) static TEST_VERSION: std::cell::RefCell<Option<String>> =
        std::cell::RefCell::new(Some("2.1.308".to_string()));
    pub(in crate::commands::setup) static TEST_MARKETPLACES: std::cell::RefCell<String> =
        std::cell::RefCell::new("[]".to_string());
    pub(in crate::commands::setup) static TEST_RUNS: std::cell::RefCell<Vec<Vec<String>>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

#[cfg(test)]
fn run(command: &[String], _target: &Target) -> Result<String, String> {
    if command.last().is_some_and(|arg| arg == "--version") {
        return TEST_VERSION
            .with(|version| version.borrow().clone())
            .ok_or_else(|| "not found".to_string());
    }
    if command.last().is_some_and(|arg| arg == "--json") {
        return Ok(TEST_MARKETPLACES.with(|list| list.borrow().clone()));
    }
    TEST_RUNS.with(|runs| runs.borrow_mut().push(command.to_vec()));
    Ok(String::new())
}

#[cfg(not(test))]
fn run(command: &[String], target: &Target) -> Result<String, String> {
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};
    let (program, args) = command.split_first().ok_or("no command")?;
    let scratch = tempfile::tempdir().map_err(|error| error.to_string())?;
    let cwd = match target {
        Target::Project(root) => root.clone(),
        Target::Global => scratch.path().to_path_buf(),
    };
    let mut process = Command::new(program);
    process.current_dir(cwd);
    let mut child = process
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| error.to_string())?;
    let started = Instant::now();
    while child
        .try_wait()
        .map_err(|error| error.to_string())?
        .is_none()
    {
        if started.elapsed() > Duration::from_secs(30) {
            let _ = child.kill();
            return Err(format!("{program} timed out"));
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    let output = child
        .wait_with_output()
        .map_err(|error| error.to_string())?;
    if output.status.success() {
        return Ok(String::from_utf8_lossy(&output.stdout).into_owned());
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    Err(stderr.lines().next().unwrap_or("failed").to_string())
}

fn at_least(found: &str, minimum: &str) -> bool {
    let parse = |version: &str| {
        version
            .split('.')
            .map(|part| part.parse::<u64>().ok())
            .collect::<Option<Vec<u64>>>()
    };
    matches!((parse(found), parse(minimum)), (Some(found), Some(minimum)) if found >= minimum)
}

#[cfg(test)]
mod tests {
    use super::at_least;

    #[test]
    fn version_gate_compares_numerically() {
        assert!(at_least("2.1.287", "2.1.287"));
        assert!(at_least("2.1.308", "2.1.287"));
        assert!(at_least("2.10.0", "2.1.287"));
        assert!(!at_least("2.1.286", "2.1.287"));
        assert!(!at_least("not-a-version", "2.1.287"));
    }
}
