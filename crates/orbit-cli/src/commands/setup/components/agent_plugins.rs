use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, Result};
use ontology::migrations::sha256_hex;
use serde_json::{Value, json};

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
    let found = installed_version(&plugin.version_command);
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

    let previous = read_manifest(&dir)?;
    let mut manifest = BTreeMap::new();
    let mut written = 0;
    let name = plugin.marketplace_for(target);
    for (relative, contents) in plugin.files(&name) {
        let path = dir.join(&relative);
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
    let registered = json::set_key(&mut root, &marketplace_path(&name), marketplace(&dir))
        .with_context(|| format!("failed to update {}", settings.display()))?;
    let enabled = json::set_key(&mut root, &enabled_path(&id), json!(true))
        .with_context(|| format!("failed to update {}", settings.display()))?;
    let orbit_wrote_it = file_mentions(&settings, &id)
        || agent
            .json_merges
            .iter()
            .any(|merge| file_mentions(&settings, &merge.marker));
    if registered || enabled {
        if !orbit_wrote_it {
            backup_once(&settings, &settings_label, report)?;
        }
        json::write_object(&settings, &root)?;
        report.note(&settings_label, format!("{id} enabled"));
    } else {
        report.note(&settings_label, "unchanged");
    }
    let scope = match target {
        Target::Global => "user",
        Target::Project(_) => "local",
    };
    let add = [
        &plugin.version_command[0],
        "plugin",
        "marketplace",
        "add",
        &dir.display().to_string(),
        "--scope",
        scope,
    ];
    if !run(&add.map(str::to_string), target) {
        report.note(
            &settings_label,
            "restart Claude Code once to load the plugin",
        );
    }
    Ok(())
}

fn remove_plugin(plugin: &PluginSpec, target: &Target, report: &mut Report) -> Result<()> {
    let (dir, dir_label) = target.resolve(&plugin.dir)?;
    if dir.join(MANIFEST).is_file() {
        let mut kept = 0;
        for (relative, expected) in read_manifest(&dir)? {
            let path = dir.join(&relative);
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

    let (settings, settings_label) = target.resolve(&plugin.settings)?;
    if settings.exists() {
        let mut root = json::read_object(&settings)?;
        let name = plugin.marketplace_for(target);
        let unregistered =
            json::remove_key_if(&mut root, &marketplace_path(&name), &marketplace(&dir));
        let disabled =
            json::remove_key_if(&mut root, &enabled_path(&plugin.id(&name)), &json!(true));
        if unregistered || disabled {
            json::write_or_delete_when_empty(&settings, &root, target, &settings_label, report)?;
            let remove = [
                &plugin.version_command[0],
                "plugin",
                "marketplace",
                "remove",
                &name,
            ];
            run(&remove.map(str::to_string), target);
        }
    }
    Ok(())
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
    match std::fs::read_to_string(&path) {
        Ok(raw) => serde_json::from_str(&raw)
            .with_context(|| format!("{} is not valid; remove it and re-run", path.display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(BTreeMap::new()),
        Err(e) => Err(e).with_context(|| format!("failed to read {}", path.display())),
    }
}

fn hash(contents: &[u8]) -> String {
    sha256_hex(&String::from_utf8_lossy(contents))
}

#[cfg(test)]
thread_local! {
    pub(in crate::commands::setup) static TEST_VERSION: std::cell::RefCell<Option<String>> =
        std::cell::RefCell::new(Some("2.1.308".to_string()));
}

#[cfg(test)]
fn run(_command: &[String], _target: &Target) -> bool {
    true
}

#[cfg(not(test))]
fn run(command: &[String], target: &Target) -> bool {
    let Some((program, args)) = command.split_first() else {
        return false;
    };
    let mut process = std::process::Command::new(program);
    if let Target::Project(root) = target {
        process.current_dir(root);
    }
    process
        .args(args)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

#[cfg(test)]
fn installed_version(_command: &[String]) -> Option<String> {
    TEST_VERSION.with(|version| version.borrow().clone())
}

#[cfg(not(test))]
fn installed_version(command: &[String]) -> Option<String> {
    let (program, args) = command.split_first()?;
    let output = std::process::Command::new(program)
        .args(args)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8_lossy(&output.stdout)
        .split_whitespace()
        .next()
        .map(str::to_string)
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
