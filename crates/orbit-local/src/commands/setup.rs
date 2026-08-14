//! `orbit setup` — configure AI coding assistants to consult the graph before grepping or
//! reading raw files. Assistants are declared in `config/setup/*.yaml` (see `spec`); this
//! module applies and inverts those declared operations, globally by default or against one
//! project with `--project`/`--dir`. Any pre-existing file gets a one-time `.orbit-backup`
//! sibling before its first modification.

mod json_config;
mod json_ops;
mod markdown;
pub(crate) mod spec;

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde_json::Value;

use spec::{AssistantSpec, Mode, ScopedPath};

pub(crate) fn assistant_value_parser() -> clap::builder::PossibleValuesParser {
    clap::builder::PossibleValuesParser::new(spec::names())
}

pub(crate) enum Target {
    Global,
    Project(PathBuf),
}

impl Target {
    pub(crate) fn project(dir: Option<PathBuf>) -> Result<Target> {
        let root = match dir {
            Some(dir) => dir,
            None => std::env::current_dir().context("failed to read current directory")?,
        };
        let root = dunce::canonicalize(&root)
            .with_context(|| format!("failed to resolve project directory {}", root.display()))?;
        Ok(Target::Project(root))
    }

    fn resolve(&self, scoped: &ScopedPath) -> Result<(PathBuf, String)> {
        match self {
            Target::Project(root) => Ok((root.join(&scoped.project), scoped.project.clone())),
            Target::Global => {
                let home = dirs::home_dir().context("could not determine home directory")?;
                let rest = scoped
                    .global
                    .strip_prefix("~/")
                    .with_context(|| format!("global path {} must start with ~/", scoped.global))?;
                Ok((home.join(rest), scoped.global.clone()))
            }
        }
    }

    fn registration_value(&self, scoped: &ScopedPath) -> Result<String> {
        match self {
            Target::Project(_) => Ok(scoped.project.clone()),
            Target::Global => Ok(self.resolve(scoped)?.0.display().to_string()),
        }
    }
}

pub(crate) fn run(assistants: Vec<String>, remove: bool, mode: Mode, target: Target) -> Result<()> {
    let specs: Vec<&AssistantSpec> = if assistants.is_empty() {
        if !remove {
            bail!(
                "specify at least one assistant to set up: {}",
                spec::names().join(", ")
            );
        }
        spec::all().iter().collect()
    } else {
        let names: BTreeSet<String> = assistants.into_iter().collect();
        names
            .iter()
            .map(|name| spec::get(name).with_context(|| format!("unknown assistant {name:?}")))
            .collect::<Result<_>>()?
    };

    for (path, label) in instruction_files(&specs, &target)? {
        if remove {
            markdown::strip_block_from_file(&path, &label)?;
        } else {
            markdown::upsert_block_in_file(&path, &label, mode)?;
        }
    }

    for spec in &specs {
        if remove {
            remove_extras(spec, &target)?;
        } else {
            install_extras(spec, &target, mode)?;
        }
    }

    Ok(())
}

fn install_extras(spec: &AssistantSpec, target: &Target, mode: Mode) -> Result<()> {
    for merge in &spec.json_merges {
        let (path, label) = target.resolve(&merge.file)?;
        let entries: Vec<Value> = merge
            .entries
            .iter()
            .map(|entry| resolve_mode(entry, mode))
            .collect();
        let mut root = json_config::read_object(&path)?;
        if path.exists() {
            backup_once(&path, &label)?;
        }
        json_ops::merge_owned(&mut root, &merge.path, &merge.marker, &entries)
            .with_context(|| format!("failed to update {}", path.display()))?;
        json_config::write_object(&path, &root)?;
        println!("  {label}  ->  orbit entries installed");
    }

    for template_file in &spec.template_files {
        let (path, label) = target.resolve(&template_file.path)?;
        if path.exists() {
            backup_once(&path, &label)?;
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        std::fs::write(&path, template_file.contents(mode))
            .with_context(|| format!("failed to write {}", path.display()))?;
        println!("  {label}  ->  written");
    }

    for registration in &spec.registrations {
        let (path, label) = target.resolve(&registration.file)?;
        let value = target.registration_value(&registration.value)?;
        let mut root = json_config::read_object(&path)?;
        if json_ops::register(&mut root, &registration.path, &value)
            .with_context(|| format!("failed to update {}", path.display()))?
        {
            if path.exists() {
                backup_once(&path, &label)?;
            }
            json_config::write_object(&path, &root)?;
            println!("  {label}  ->  {value} registered");
        }
    }

    Ok(())
}

fn remove_extras(spec: &AssistantSpec, target: &Target) -> Result<()> {
    for merge in &spec.json_merges {
        let (path, label) = target.resolve(&merge.file)?;
        if !path.exists() {
            continue;
        }
        let mut root = json_config::read_object(&path)?;
        if json_ops::remove_owned(&mut root, &merge.path, &merge.marker) {
            write_or_delete_when_empty(&path, &root, &label)?;
        }
    }

    for template_file in &spec.template_files {
        let (path, label) = target.resolve(&template_file.path)?;
        if !path.exists() {
            continue;
        }
        let current = std::fs::read_to_string(&path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        if !template_file.is_unmodified(&current) {
            println!("  {label}  ->  kept (edited since install; delete it by hand)");
            continue;
        }
        std::fs::remove_file(&path)
            .with_context(|| format!("failed to remove {}", path.display()))?;
        println!("  {label}  ->  removed");
    }

    for registration in &spec.registrations {
        let (path, label) = target.resolve(&registration.file)?;
        if !path.exists() {
            continue;
        }
        let value = target.registration_value(&registration.value)?;
        let mut root = json_config::read_object(&path)?;
        if json_ops::deregister(&mut root, &registration.path, &value) {
            write_or_delete_when_empty(&path, &root, &label)?;
        }
    }

    Ok(())
}

fn backup_once(path: &Path, label: &str) -> Result<()> {
    let backup = backup_path(path);
    if backup.exists() {
        return Ok(());
    }
    std::fs::copy(path, &backup)
        .with_context(|| format!("failed to back up {}", path.display()))?;
    println!("  {label}  ->  backup at {label}.orbit-backup");
    Ok(())
}

fn backup_path(path: &Path) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(".orbit-backup");
    path.with_file_name(name)
}

fn resolve_mode(value: &Value, mode: Mode) -> Value {
    match value {
        Value::String(s) => Value::String(s.replace("{mode}", mode.as_str())),
        Value::Array(items) => Value::Array(items.iter().map(|v| resolve_mode(v, mode)).collect()),
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(k, v)| (k.clone(), resolve_mode(v, mode)))
                .collect(),
        ),
        other => other.clone(),
    }
}

fn write_or_delete_when_empty(path: &Path, root: &Value, label: &str) -> Result<()> {
    if root.as_object().is_some_and(|map| map.is_empty()) {
        std::fs::remove_file(path)
            .with_context(|| format!("failed to remove {}", path.display()))?;
        println!("  {label}  ->  removed (was orbit-only)");
    } else {
        json_config::write_object(path, root)?;
        println!("  {label}  ->  orbit entries removed");
    }
    Ok(())
}

fn instruction_files(specs: &[&AssistantSpec], target: &Target) -> Result<Vec<(PathBuf, String)>> {
    let mut resolved: Vec<(PathBuf, String)> = specs
        .iter()
        .map(|spec| target.resolve(&spec.instruction_file))
        .collect::<Result<_>>()?;
    resolved.sort_by(|a, b| a.0.cmp(&b.0));
    resolved.dedup_by(|a, b| a.0 == b.0);

    let mut files: Vec<(PathBuf, String)> = Vec::new();
    let mut canonicals: Vec<PathBuf> = Vec::new();
    for (path, label) in resolved {
        let canonical = dunce::canonicalize(&path).unwrap_or_else(|_| path.clone());
        if canonicals.contains(&canonical) {
            continue;
        }
        canonicals.push(canonical);
        files.push((path, label));
    }
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn project(dir: &Path) -> Target {
        Target::project(Some(dir.to_path_buf())).unwrap()
    }

    fn specs_for(names: &[&str]) -> Vec<&'static AssistantSpec> {
        names.iter().map(|name| spec::get(name).unwrap()).collect()
    }

    #[cfg(unix)]
    #[test]
    fn instruction_files_dedupes_symlinked_claude_md() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("AGENTS.md"), "# rules\n").unwrap();
        std::os::unix::fs::symlink("AGENTS.md", dir.path().join("CLAUDE.md")).unwrap();

        let files = instruction_files(&specs_for(&["claude", "codex"]), &project(dir.path()));
        let labels: Vec<String> = files.unwrap().into_iter().map(|(_, label)| label).collect();
        assert_eq!(labels, vec!["AGENTS.md"]);
    }

    #[test]
    fn instruction_files_split_when_distinct() {
        let dir = tempfile::tempdir().unwrap();
        let files = instruction_files(&specs_for(&["claude", "pi"]), &project(dir.path()));
        let labels: Vec<String> = files.unwrap().into_iter().map(|(_, label)| label).collect();
        assert_eq!(labels, vec!["AGENTS.md", "CLAUDE.md"]);
    }

    #[test]
    fn global_instruction_files_are_per_assistant() {
        let files = instruction_files(&specs_for(&["claude", "codex", "pi"]), &Target::Global);
        let labels: Vec<String> = files.unwrap().into_iter().map(|(_, label)| label).collect();
        assert_eq!(
            labels,
            vec![
                "~/.claude/CLAUDE.md",
                "~/.codex/AGENTS.md",
                "~/.pi/agent/AGENTS.md"
            ]
        );
    }

    #[test]
    fn global_paths_resolve_to_real_components_under_home() {
        let home = dirs::home_dir().unwrap();
        for (assistant, tail) in [
            ("claude", [".claude", "CLAUDE.md"]),
            ("codex", [".codex", "AGENTS.md"]),
        ] {
            let spec = spec::get(assistant).unwrap();
            let (path, _) = Target::Global.resolve(&spec.instruction_file).unwrap();
            assert!(path.starts_with(&home), "{assistant}: {path:?}");
            assert!(
                path.ends_with(Path::new(tail[0]).join(tail[1])),
                "{assistant}: {path:?}"
            );
        }
    }

    #[test]
    fn install_requires_at_least_one_assistant_and_bare_remove_removes_all() {
        let dir = tempfile::tempdir().unwrap();

        let err = run(vec![], false, Mode::Local, project(dir.path())).unwrap_err();
        assert!(err.to_string().contains("at least one assistant"));

        run(
            vec!["opencode".into()],
            false,
            Mode::Local,
            project(dir.path()),
        )
        .unwrap();
        assert!(dir.path().join(".opencode/plugins/orbit.js").is_file());

        run(vec![], true, Mode::Local, project(dir.path())).unwrap();
        assert!(!dir.path().join(".opencode/plugins/orbit.js").exists());
        assert!(!dir.path().join("AGENTS.md").exists());
    }

    #[test]
    fn setup_and_remove_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("AGENTS.md"), "# My rules\n").unwrap();

        run(
            vec!["codex".into(), "opencode".into()],
            false,
            Mode::Local,
            project(dir.path()),
        )
        .unwrap();

        let agents = std::fs::read_to_string(dir.path().join("AGENTS.md")).unwrap();
        assert!(agents.contains("<!-- orbit:setup:begin -->"));
        assert!(agents.contains("# My rules"));
        assert!(dir.path().join(".opencode/plugins/orbit.js").is_file());
        let config: Value = serde_json::from_str(
            &std::fs::read_to_string(dir.path().join(".opencode/opencode.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(config["plugin"], json!([".opencode/plugins/orbit.js"]));

        run(
            vec!["codex".into(), "opencode".into()],
            true,
            Mode::Local,
            project(dir.path()),
        )
        .unwrap();

        assert_eq!(
            std::fs::read_to_string(dir.path().join("AGENTS.md")).unwrap(),
            "# My rules\n"
        );
        assert!(!dir.path().join(".opencode/plugins/orbit.js").exists());
        assert!(!dir.path().join(".opencode/opencode.json").exists());
    }

    #[test]
    fn preexisting_files_get_a_one_time_backup() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("AGENTS.md"), "# Mine\n").unwrap();
        std::fs::create_dir_all(dir.path().join(".claude")).unwrap();
        std::fs::write(
            dir.path().join(".claude/settings.json"),
            "{\"permissions\": {}}",
        )
        .unwrap();
        std::fs::create_dir_all(dir.path().join(".opencode/plugins")).unwrap();
        std::fs::write(
            dir.path().join(".opencode/plugins/orbit.js"),
            "// my own plugin\n",
        )
        .unwrap();

        run(
            vec!["claude".into(), "codex".into(), "opencode".into()],
            false,
            Mode::Local,
            project(dir.path()),
        )
        .unwrap();
        run(
            vec!["claude".into(), "codex".into(), "opencode".into()],
            false,
            Mode::Remote,
            project(dir.path()),
        )
        .unwrap();

        assert_eq!(
            std::fs::read_to_string(dir.path().join("AGENTS.md.orbit-backup")).unwrap(),
            "# Mine\n"
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join(".claude/settings.json.orbit-backup")).unwrap(),
            "{\"permissions\": {}}"
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join(".opencode/plugins/orbit.js.orbit-backup"))
                .unwrap(),
            "// my own plugin\n"
        );
        assert!(!dir.path().join("CLAUDE.md.orbit-backup").exists());
    }

    #[test]
    fn remove_keeps_a_template_file_the_user_edited() {
        let dir = tempfile::tempdir().unwrap();
        let plugin = dir.path().join(".opencode/plugins/orbit.js");
        run(
            vec!["opencode".into()],
            false,
            Mode::Local,
            project(dir.path()),
        )
        .unwrap();

        let edited = format!(
            "{}\n// my tweak\n",
            std::fs::read_to_string(&plugin).unwrap()
        );
        std::fs::write(&plugin, &edited).unwrap();

        run(
            vec!["opencode".into()],
            true,
            Mode::Local,
            project(dir.path()),
        )
        .unwrap();

        assert_eq!(std::fs::read_to_string(&plugin).unwrap(), edited);
    }

    #[test]
    fn claude_setup_merges_hooks_and_removal_preserves_foreign_settings() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join(".claude")).unwrap();
        std::fs::write(
            dir.path().join(".claude/settings.json"),
            r#"{"permissions": {"allow": ["Bash"]}}"#,
        )
        .unwrap();

        run(
            vec!["claude".into()],
            false,
            Mode::Local,
            project(dir.path()),
        )
        .unwrap();

        let settings: Value = serde_json::from_str(
            &std::fs::read_to_string(dir.path().join(".claude/settings.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(settings["permissions"]["allow"][0], "Bash");
        assert_eq!(settings["hooks"]["PreToolUse"].as_array().unwrap().len(), 2);

        run(
            vec!["claude".into()],
            true,
            Mode::Local,
            project(dir.path()),
        )
        .unwrap();

        let settings: Value = serde_json::from_str(
            &std::fs::read_to_string(dir.path().join(".claude/settings.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(settings, json!({"permissions": {"allow": ["Bash"]}}));
        assert!(!dir.path().join("CLAUDE.md").exists());
    }

    #[test]
    fn mode_tokens_resolve_in_installed_artifacts() {
        let dir = tempfile::tempdir().unwrap();
        run(
            vec!["claude".into(), "opencode".into()],
            false,
            Mode::Remote,
            project(dir.path()),
        )
        .unwrap();

        let settings = std::fs::read_to_string(dir.path().join(".claude/settings.json")).unwrap();
        assert!(settings.contains("orbit hook-guard search --mode remote"));
        assert!(!settings.contains("{mode}"));

        let plugin =
            std::fs::read_to_string(dir.path().join(".opencode/plugins/orbit.js")).unwrap();
        assert!(plugin.contains("glab orbit remote"));
        assert!(plugin.contains("const REQUIRE_LOCAL_GRAPH = false"));
        assert!(!plugin.contains("{{"));
    }

    #[test]
    fn invalid_settings_json_is_never_clobbered() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join(".claude")).unwrap();
        std::fs::write(dir.path().join(".claude/settings.json"), "{not json").unwrap();

        let err = run(
            vec!["claude".into()],
            false,
            Mode::Local,
            project(dir.path()),
        )
        .unwrap_err();
        assert!(format!("{err:#}").contains("not valid JSON"));
        assert_eq!(
            std::fs::read_to_string(dir.path().join(".claude/settings.json")).unwrap(),
            "{not json"
        );
    }
}
