//! `orbit setup` — configure AI coding assistants to consult the graph before grepping or
//! reading raw files. Assistants are declared in `config/setup/*.yaml` (see `spec`); this
//! module applies those declared operations (and `orbit uninstall` inverts them), globally by
//! default or against one project with `--project`/`--dir`. Any pre-existing file gets a
//! one-time `.orbit-backup` sibling before its first modification.

pub(crate) mod detect;
mod json_config;
mod json_ops;
mod markdown;
mod mcp_ops;
mod skills_ops;
pub(crate) mod spec;

use std::collections::BTreeSet;
use std::io::{IsTerminal, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde_json::Value;

use detect::Machine;
use spec::{AssistantSpec, ScopedPath};

pub(crate) fn assistant_value_parser() -> clap::builder::PossibleValuesParser {
    clap::builder::PossibleValuesParser::new(spec::names())
}

pub(crate) struct Options {
    pub(crate) assistants: Vec<String>,
    pub(crate) all: bool,
    pub(crate) yes: bool,
    pub(crate) dry_run: bool,
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

pub(crate) fn install(options: Options, target: Target, machine: &Machine) -> Result<()> {
    let specs = if options.all {
        spec::all().iter().collect()
    } else if !options.assistants.is_empty() {
        named_specs(&options.assistants)?
    } else {
        let installed = machine.installed_assistants();
        if installed.is_empty() {
            println!(
                "No AI coding assistant detected. Name one to configure it: orbit setup <{}>",
                spec::names().join("|")
            );
            return Ok(());
        }
        for (assistant, found) in &installed {
            println!("Detected {}  ({})", assistant.name, found.display());
        }
        installed
            .into_iter()
            .map(|(assistant, _)| assistant)
            .collect()
    };

    print_plan(&specs, &target)?;
    if options.dry_run {
        return Ok(());
    }
    if !confirm(
        options.yes,
        &format!("Configure {} assistant(s)?", specs.len()),
    )? {
        return Ok(());
    }

    for (path, label) in instruction_files(&specs, &target)? {
        markdown::upsert_block_in_file(&path, &label)?;
    }
    for assistant in &specs {
        install_extras(assistant, &target)?;
    }
    for skill in skill_targets(&specs, &target)? {
        skills_ops::install(&skill.root, &skill.label)?;
        if let Some((link_path, link_label)) = &skill.link {
            skills_ops::link(link_path, &skill.root, link_label)?;
        }
    }

    if spec::launcher() == spec::GLAB_LAUNCHER {
        ensure_glab_auto_run();
    }
    Ok(())
}

pub(crate) fn uninstall(options: Options, target: Target) -> Result<()> {
    let specs = if options.assistants.is_empty() {
        spec::all().iter().collect()
    } else {
        named_specs(&options.assistants)?
    };

    print_plan(&specs, &target)?;
    if options.dry_run {
        return Ok(());
    }
    if !confirm(
        options.yes,
        &format!("Remove Orbit from {} assistant(s)?", specs.len()),
    )? {
        return Ok(());
    }

    for (path, label) in instruction_files(&specs, &target)? {
        markdown::strip_block_from_file(&path, &label)?;
    }
    for assistant in &specs {
        remove_extras(assistant, &target)?;
    }
    for skill in skill_targets(&specs, &target)? {
        if let Some((link_path, link_label)) = &skill.link {
            skills_ops::unlink(link_path, link_label)?;
        }
        skills_ops::remove(&skill.root, &skill.label)?;
    }

    println!("Backups (*.orbit-backup) were kept.");
    Ok(())
}

fn named_specs(names: &[String]) -> Result<Vec<&'static AssistantSpec>> {
    let unique: BTreeSet<&String> = names.iter().collect();
    unique
        .into_iter()
        .map(|name| spec::get(name).with_context(|| format!("unknown assistant {name:?}")))
        .collect()
}

fn print_plan(specs: &[&AssistantSpec], target: &Target) -> Result<()> {
    for assistant in specs {
        println!("{}", assistant.name);
        for label in planned_labels(assistant, target)? {
            println!("  {label}");
        }
    }
    Ok(())
}

fn planned_labels(assistant: &AssistantSpec, target: &Target) -> Result<BTreeSet<String>> {
    let mut files: Vec<&ScopedPath> = vec![&assistant.instruction_file];
    files.extend(assistant.json_merges.iter().map(|merge| &merge.file));
    files.extend(assistant.template_files.iter().map(|file| &file.path));
    files.extend(assistant.registrations.iter().map(|entry| &entry.file));
    files.extend(assistant.mcp.iter().map(|entry| &entry.file));

    let mut labels: BTreeSet<String> = files
        .into_iter()
        .map(|scoped| target.resolve(scoped).map(|(_, label)| label))
        .collect::<Result<_>>()?;
    for skill in skill_targets(&[assistant], target)? {
        labels.insert(skill.label);
        labels.extend(skill.link.map(|(_, label)| label));
    }
    Ok(labels)
}

pub(crate) fn confirm(yes: bool, question: &str) -> Result<bool> {
    if yes {
        return Ok(true);
    }
    if !std::io::stdin().is_terminal() {
        bail!("stdin is not a terminal; pass --yes to proceed without a prompt");
    }
    print!("{question} [Y/n] ");
    std::io::stdout().flush()?;
    let mut answer = String::new();
    std::io::stdin().read_line(&mut answer)?;
    Ok(matches!(
        answer.trim().to_ascii_lowercase().as_str(),
        "" | "y" | "yes"
    ))
}

fn ensure_glab_auto_run() {
    let ok = std::process::Command::new("glab")
        .args(["config", "set", "orbit_cli_auto_run", "true"])
        .status()
        .map(|status| status.success())
        .unwrap_or(false);
    if ok {
        println!("  glab config  ->  orbit_cli_auto_run=true (hooks run without prompting)");
    } else {
        eprintln!(
            "warning: failed to set orbit_cli_auto_run; run `glab config set \
             orbit_cli_auto_run true` so the installed hooks never prompt"
        );
    }
}

fn install_extras(assistant: &AssistantSpec, target: &Target) -> Result<()> {
    if let Some(entry) = &assistant.mcp {
        let (path, label) = target.resolve(&entry.file)?;
        mcp_ops::install(&path, &label, entry.format, &spec::mcp_server())?;
    }

    for merge in &assistant.json_merges {
        let (path, label) = target.resolve(&merge.file)?;
        let entries: Vec<Value> = merge.entries.iter().map(resolve_launcher).collect();
        let mut root = json_config::read_object(&path)?;
        if path.exists() {
            backup_once(&path, &label)?;
        }
        json_ops::merge_owned(&mut root, &merge.path, &merge.marker, &entries)
            .with_context(|| format!("failed to update {}", path.display()))?;
        json_config::write_object(&path, &root)?;
        println!("  {label}  ->  orbit entries installed");
    }

    for template_file in &assistant.template_files {
        let (path, label) = target.resolve(&template_file.path)?;
        if path.exists() {
            backup_once(&path, &label)?;
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        std::fs::write(&path, template_file.contents())
            .with_context(|| format!("failed to write {}", path.display()))?;
        println!("  {label}  ->  written");
    }

    for registration in &assistant.registrations {
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

fn remove_extras(assistant: &AssistantSpec, target: &Target) -> Result<()> {
    for merge in &assistant.json_merges {
        let (path, label) = target.resolve(&merge.file)?;
        if !path.exists() {
            continue;
        }
        let mut root = json_config::read_object(&path)?;
        if json_ops::remove_owned(&mut root, &merge.path, &merge.marker) {
            write_or_delete_when_empty(&path, &root, &label)?;
        }
    }

    for template_file in &assistant.template_files {
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

    for registration in &assistant.registrations {
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

    if let Some(entry) = &assistant.mcp {
        let (path, label) = target.resolve(&entry.file)?;
        mcp_ops::remove(&path, &label, entry.format, spec::mcp_server().name)?;
    }

    Ok(())
}

struct SkillTarget {
    root: PathBuf,
    label: String,
    link: Option<(PathBuf, String)>,
}

fn skill_targets(specs: &[&AssistantSpec], target: &Target) -> Result<Vec<SkillTarget>> {
    let mut targets: Vec<SkillTarget> = Vec::new();
    for dirs in specs
        .iter()
        .filter_map(|assistant| assistant.skills.as_ref())
    {
        let (dir, dir_label) = target.resolve(&dirs.dir)?;
        let root = dir.join(crate::skill::INSTALL_DIR_NAME);
        let link = dirs
            .link
            .as_ref()
            .map(|link| target.resolve(link))
            .transpose()?
            .map(|(dir, label)| {
                (
                    dir.join(crate::skill::INSTALL_DIR_NAME),
                    format!("{label}/{}", crate::skill::INSTALL_DIR_NAME),
                )
            });

        match targets.iter_mut().find(|existing| existing.root == root) {
            Some(existing) => existing.link = existing.link.take().or(link),
            None => targets.push(SkillTarget {
                root,
                label: format!("{dir_label}/{}", crate::skill::INSTALL_DIR_NAME),
                link,
            }),
        }
    }
    Ok(targets)
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

fn resolve_launcher(value: &Value) -> Value {
    match value {
        Value::String(s) => Value::String(s.replace("{{orbit}}", spec::launcher())),
        Value::Array(items) => Value::Array(items.iter().map(resolve_launcher).collect()),
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(k, v)| (k.clone(), resolve_launcher(v)))
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
        .map(|assistant| target.resolve(&assistant.instruction_file))
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
    use std::collections::BTreeMap;

    fn project(dir: &Path) -> Target {
        Target::project(Some(dir.to_path_buf())).unwrap()
    }

    fn specs_for(names: &[&str]) -> Vec<&'static AssistantSpec> {
        names.iter().map(|name| spec::get(name).unwrap()).collect()
    }

    fn options(names: &[&str]) -> Options {
        Options {
            assistants: names.iter().map(|name| name.to_string()).collect(),
            all: false,
            yes: true,
            dry_run: false,
        }
    }

    fn bare_machine() -> Machine {
        Machine::new(PathBuf::from("/nonexistent-home"), BTreeMap::new())
    }

    fn setup(names: &[&str], dir: &Path) {
        install(options(names), project(dir), &bare_machine()).unwrap();
    }

    fn teardown(names: &[&str], dir: &Path) {
        uninstall(options(names), project(dir)).unwrap();
    }

    fn read_json(path: &Path) -> Value {
        serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
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
    fn setup_detects_installed_assistants_from_their_config_dirs() {
        let home = tempfile::tempdir().unwrap();
        std::fs::create_dir(home.path().join(".codex")).unwrap();
        let claude_config = tempfile::tempdir().unwrap();
        let env = BTreeMap::from([(
            "CLAUDE_CONFIG_DIR".to_string(),
            claude_config.path().display().to_string(),
        )]);
        let machine = Machine::new(home.path().to_path_buf(), env);

        let dir = tempfile::tempdir().unwrap();
        install(options(&[]), project(dir.path()), &machine).unwrap();

        assert!(dir.path().join("CLAUDE.md").is_file());
        assert!(dir.path().join(".codex/config.toml").is_file());
        assert!(!dir.path().join(".opencode").exists());
    }

    #[test]
    fn setup_with_nothing_detected_writes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        install(options(&[]), project(dir.path()), &bare_machine()).unwrap();
        assert!(std::fs::read_dir(dir.path()).unwrap().next().is_none());
    }

    #[test]
    fn dry_run_prints_the_plan_and_writes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let mut dry = options(&["claude", "codex", "opencode"]);
        dry.dry_run = true;
        install(dry, project(dir.path()), &bare_machine()).unwrap();
        assert!(std::fs::read_dir(dir.path()).unwrap().next().is_none());
    }

    #[test]
    fn bare_uninstall_removes_every_assistant() {
        let dir = tempfile::tempdir().unwrap();
        setup(&["opencode"], dir.path());
        assert!(dir.path().join(".opencode/plugins/orbit.js").is_file());

        teardown(&[], dir.path());

        assert!(!dir.path().join(".opencode/plugins/orbit.js").exists());
        assert!(!dir.path().join("AGENTS.md").exists());
        assert!(!dir.path().join("opencode.json").exists());
    }

    #[test]
    fn setup_and_uninstall_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("AGENTS.md"), "# My rules\n").unwrap();

        setup(&["codex", "opencode"], dir.path());

        let agents = std::fs::read_to_string(dir.path().join("AGENTS.md")).unwrap();
        assert!(agents.contains("<!-- orbit:setup:begin -->"));
        assert!(agents.contains("# My rules"));
        assert!(dir.path().join(".opencode/plugins/orbit.js").is_file());
        assert_eq!(
            read_json(&dir.path().join(".opencode/opencode.json"))["plugin"],
            json!([".opencode/plugins/orbit.js"])
        );
        assert_eq!(
            read_json(&dir.path().join("opencode.json"))["mcp"]["orbit"],
            json!({"type": "local", "command": ["orbit", "mcp", "serve"], "enabled": true})
        );
        let codex = std::fs::read_to_string(dir.path().join(".codex/config.toml")).unwrap();
        assert!(codex.contains("[mcp_servers.orbit]"), "{codex}");
        assert!(codex.contains(r#"args = ["mcp", "serve"]"#), "{codex}");
        assert!(
            dir.path()
                .join(".agents/skills/orbit-cli/SKILL.md")
                .is_file()
        );

        teardown(&["codex", "opencode"], dir.path());

        assert_eq!(
            std::fs::read_to_string(dir.path().join("AGENTS.md")).unwrap(),
            "# My rules\n"
        );
        assert!(!dir.path().join(".opencode/plugins/orbit.js").exists());
        assert!(!dir.path().join(".opencode/opencode.json").exists());
        assert!(!dir.path().join("opencode.json").exists());
        assert!(!dir.path().join(".codex/config.toml").exists());
        assert!(!dir.path().join(".agents").exists());
    }

    #[test]
    fn claude_mcp_entry_joins_existing_servers_and_leaves_them_on_uninstall() {
        let dir = tempfile::tempdir().unwrap();
        let mcp_json = dir.path().join(".mcp.json");
        let theirs = json!({"mcpServers": {"theirs": {"command": "their-server"}}});
        std::fs::write(&mcp_json, theirs.to_string()).unwrap();

        setup(&["claude"], dir.path());
        setup(&["claude"], dir.path());

        let servers = read_json(&mcp_json)["mcpServers"].clone();
        assert_eq!(servers["theirs"], json!({"command": "their-server"}));
        assert_eq!(
            servers["orbit"],
            json!({"type": "stdio", "command": "orbit", "args": ["mcp", "serve"]})
        );

        teardown(&["claude"], dir.path());

        assert_eq!(read_json(&mcp_json), theirs);
    }

    #[test]
    fn codex_mcp_entry_preserves_comments_and_foreign_tables() {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join(".codex/config.toml");
        std::fs::create_dir_all(config.parent().unwrap()).unwrap();
        let theirs = "# my codex settings\nmodel = \"o3\"\n\n[mcp_servers.theirs]\ncommand = \"their-server\"\n";
        std::fs::write(&config, theirs).unwrap();

        setup(&["codex"], dir.path());

        let installed = std::fs::read_to_string(&config).unwrap();
        assert!(
            installed.starts_with("# my codex settings\nmodel = \"o3\""),
            "{installed}"
        );
        assert!(installed.contains("[mcp_servers.theirs]"), "{installed}");
        assert!(installed.contains("[mcp_servers.orbit]"), "{installed}");

        teardown(&["codex"], dir.path());

        assert_eq!(std::fs::read_to_string(&config).unwrap(), theirs);
    }

    #[test]
    fn invalid_codex_toml_is_never_clobbered() {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join(".codex/config.toml");
        std::fs::create_dir_all(config.parent().unwrap()).unwrap();
        std::fs::write(&config, "model = [unclosed\n").unwrap();

        let err = install(options(&["codex"]), project(dir.path()), &bare_machine()).unwrap_err();

        assert!(format!("{err:#}").contains("not valid TOML"), "{err:#}");
        assert_eq!(
            std::fs::read_to_string(&config).unwrap(),
            "model = [unclosed\n"
        );
    }

    #[test]
    fn opencode_jsonc_config_is_refused_not_rewritten() {
        let dir = tempfile::tempdir().unwrap();
        let jsonc = dir.path().join("opencode.jsonc");
        std::fs::write(&jsonc, "{\n  // mine\n  \"theme\": \"dark\"\n}\n").unwrap();

        let err =
            install(options(&["opencode"]), project(dir.path()), &bare_machine()).unwrap_err();

        let message = format!("{err:#}");
        assert!(message.contains("opencode.jsonc"), "{message}");
        assert!(message.contains("\"mcp\""), "{message}");
        assert_eq!(
            std::fs::read_to_string(&jsonc).unwrap(),
            "{\n  // mine\n  \"theme\": \"dark\"\n}\n"
        );
        assert!(!dir.path().join("opencode.json").exists());
    }

    #[cfg(unix)]
    #[test]
    fn skill_is_written_once_and_linked_into_claude() {
        let dir = tempfile::tempdir().unwrap();
        setup(&["claude", "codex"], dir.path());

        let canonical = dir.path().join(".agents/skills/orbit-cli");
        let link = dir.path().join(".claude/skills/orbit-cli");
        assert!(canonical.join("SKILL.md").is_file());
        assert!(std::fs::symlink_metadata(&link).unwrap().is_symlink());
        assert_eq!(
            std::fs::read_link(&link).unwrap(),
            Path::new("../../.agents/skills/orbit-cli")
        );
        assert!(link.join("SKILL.md").is_file());

        let edited = canonical.join("references/local/sql.md");
        std::fs::write(&edited, "my notes\n").unwrap();
        teardown(&["claude", "codex"], dir.path());

        assert!(!link.exists());
        assert!(!canonical.join("SKILL.md").exists());
        assert_eq!(std::fs::read_to_string(&edited).unwrap(), "my notes\n");
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

        setup(&["claude", "codex", "opencode"], dir.path());
        setup(&["claude", "codex", "opencode"], dir.path());

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
    fn uninstall_keeps_a_template_file_the_user_edited() {
        let dir = tempfile::tempdir().unwrap();
        let plugin = dir.path().join(".opencode/plugins/orbit.js");
        setup(&["opencode"], dir.path());

        let edited = format!(
            "{}\n// my tweak\n",
            std::fs::read_to_string(&plugin).unwrap()
        );
        std::fs::write(&plugin, &edited).unwrap();

        teardown(&["opencode"], dir.path());

        assert_eq!(std::fs::read_to_string(&plugin).unwrap(), edited);
    }

    #[test]
    fn claude_setup_merges_hooks_and_uninstall_preserves_foreign_settings() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join(".claude")).unwrap();
        std::fs::write(
            dir.path().join(".claude/settings.json"),
            r#"{"permissions": {"allow": ["Bash"]}}"#,
        )
        .unwrap();

        setup(&["claude"], dir.path());

        let settings = read_json(&dir.path().join(".claude/settings.json"));
        assert_eq!(settings["permissions"]["allow"][0], "Bash");
        assert_eq!(settings["hooks"]["PreToolUse"].as_array().unwrap().len(), 2);

        teardown(&["claude"], dir.path());

        assert_eq!(
            read_json(&dir.path().join(".claude/settings.json")),
            json!({"permissions": {"allow": ["Bash"]}})
        );
        assert!(!dir.path().join("CLAUDE.md").exists());
        assert!(!dir.path().join(".mcp.json").exists());
    }

    #[test]
    fn launcher_tokens_resolve_in_installed_artifacts() {
        let dir = tempfile::tempdir().unwrap();
        setup(&["claude", "opencode"], dir.path());

        let settings = std::fs::read_to_string(dir.path().join(".claude/settings.json")).unwrap();
        assert!(
            settings.contains("\"orbit hook-guard search\""),
            "{settings}"
        );
        assert!(!settings.contains("--mode"));
        assert!(!settings.contains("{{orbit}}"));

        let plugin =
            std::fs::read_to_string(dir.path().join(".opencode/plugins/orbit.js")).unwrap();
        assert!(plugin.contains("run orbit grep"));
        assert!(!plugin.contains("{{"));
    }

    #[test]
    fn invalid_settings_json_is_never_clobbered() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join(".claude")).unwrap();
        std::fs::write(dir.path().join(".claude/settings.json"), "{not json").unwrap();

        let err = install(options(&["claude"]), project(dir.path()), &bare_machine()).unwrap_err();
        assert!(format!("{err:#}").contains("not valid JSON"));
        assert_eq!(
            std::fs::read_to_string(dir.path().join(".claude/settings.json")).unwrap(),
            "{not json"
        );
    }
}
