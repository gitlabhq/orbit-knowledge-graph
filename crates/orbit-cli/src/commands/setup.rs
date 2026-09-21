//! `orbit setup` and `orbit uninstall`. Assistants are declared in `config/setup/*.yaml` (see
//! `spec`) and found on disk by `detect`. `plan` turns a selection into the list of files that
//! change. `changes` holds one `Change` per `Component`, each able to plan, install, and remove
//! itself, and collects a `Report`. `wizard` owns the flow: it asks, shows the plan, then applies.
//! Any pre-existing file gets a one-time `.orbit-backup` sibling before its first modification.

mod changes;
pub(crate) mod detect;
mod json;
mod plan;
pub(crate) mod spec;
pub(crate) mod wizard;

use std::collections::BTreeSet;
use std::path::PathBuf;

use anyhow::{Context, Result};

use spec::ScopedPath;

pub(crate) fn assistant_value_parser() -> clap::builder::PossibleValuesParser {
    clap::builder::PossibleValuesParser::new(spec::names())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, clap::ValueEnum)]
pub(crate) enum Component {
    Instructions,
    Hooks,
    Skill,
    Mcp,
}

impl Component {
    pub(super) const ALL: [Component; 4] = [
        Component::Instructions,
        Component::Hooks,
        Component::Skill,
        Component::Mcp,
    ];

    pub(crate) fn selection(mcp: bool, skip: &[Component]) -> BTreeSet<Component> {
        Component::ALL
            .into_iter()
            .filter(|component| *component != Component::Mcp || mcp)
            .filter(|component| !skip.contains(component))
            .collect()
    }

    fn label(self) -> &'static str {
        match self {
            Component::Instructions => "instructions",
            Component::Hooks => "hooks",
            Component::Skill => "skill",
            Component::Mcp => "mcp server",
        }
    }
}

pub(crate) struct Options {
    pub(crate) assistants: Vec<String>,
    pub(crate) all: bool,
    pub(crate) yes: bool,
    pub(crate) dry_run: bool,
    pub(crate) verbose: bool,
    pub(crate) components: BTreeSet<Component>,
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

    fn describe(&self) -> String {
        match self {
            Target::Global => "your user config".to_string(),
            Target::Project(root) => format!("project {}", root.display()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::Path;

    use serde_json::{Value, json};

    use super::detect::Machine;
    use super::*;

    fn project(dir: &Path) -> Target {
        Target::project(Some(dir.to_path_buf())).unwrap()
    }

    fn options(names: &[&str]) -> Options {
        Options {
            assistants: names.iter().map(|name| name.to_string()).collect(),
            all: false,
            yes: true,
            dry_run: false,
            verbose: false,
            components: Component::selection(false, &[]),
        }
    }

    fn options_with_mcp(names: &[&str]) -> Options {
        Options {
            components: Component::selection(true, &[]),
            ..options(names)
        }
    }

    fn bare_machine() -> Machine {
        Machine::new(PathBuf::from("/nonexistent-home"), BTreeMap::new())
    }

    fn setup(names: &[&str], dir: &Path) {
        wizard::install(options_with_mcp(names), project(dir), &bare_machine()).unwrap();
    }

    fn teardown(names: &[&str], dir: &Path) {
        wizard::uninstall(options(names), project(dir)).unwrap();
    }

    fn read_json(path: &Path) -> Value {
        serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
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
        wizard::install(options(&[]), project(dir.path()), &machine).unwrap();

        assert!(dir.path().join("CLAUDE.md").is_file());
        assert!(dir.path().join("AGENTS.md").is_file());
        assert!(dir.path().join(".claude/settings.json").is_file());
        assert!(!dir.path().join(".opencode").exists());
    }

    #[test]
    fn setup_with_nothing_detected_writes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        wizard::install(options(&[]), project(dir.path()), &bare_machine()).unwrap();
        assert!(std::fs::read_dir(dir.path()).unwrap().next().is_none());
    }

    #[test]
    fn dry_run_prints_the_plan_and_writes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let mut dry = options_with_mcp(&["claude", "codex", "opencode"]);
        dry.dry_run = true;
        wizard::install(dry, project(dir.path()), &bare_machine()).unwrap();
        assert!(std::fs::read_dir(dir.path()).unwrap().next().is_none());
    }

    #[test]
    fn mcp_server_is_opt_in() {
        let dir = tempfile::tempdir().unwrap();
        wizard::install(
            options(&["claude", "codex", "opencode"]),
            project(dir.path()),
            &bare_machine(),
        )
        .unwrap();

        assert!(dir.path().join("CLAUDE.md").is_file());
        assert!(!dir.path().join(".mcp.json").exists());
        assert!(!dir.path().join(".codex").exists());
        assert!(!dir.path().join("opencode.json").exists());

        setup(&["claude"], dir.path());
        assert!(dir.path().join(".mcp.json").is_file());
    }

    #[test]
    fn skipped_components_are_left_alone() {
        let dir = tempfile::tempdir().unwrap();
        let mut only_instructions = options(&["claude", "opencode"]);
        only_instructions.components =
            Component::selection(false, &[Component::Hooks, Component::Skill]);
        wizard::install(only_instructions, project(dir.path()), &bare_machine()).unwrap();

        assert!(dir.path().join("CLAUDE.md").is_file());
        assert!(dir.path().join("AGENTS.md").is_file());
        assert!(!dir.path().join(".claude").exists());
        assert!(!dir.path().join(".opencode").exists());
        assert!(!dir.path().join(".agents").exists());
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

        let err = wizard::install(
            options_with_mcp(&["codex"]),
            project(dir.path()),
            &bare_machine(),
        )
        .unwrap_err();

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

        let err = wizard::install(
            options_with_mcp(&["opencode"]),
            project(dir.path()),
            &bare_machine(),
        )
        .unwrap_err();

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

        let err = wizard::install(options(&["claude"]), project(dir.path()), &bare_machine())
            .unwrap_err();
        assert!(format!("{err:#}").contains("not valid JSON"));
        assert_eq!(
            std::fs::read_to_string(dir.path().join(".claude/settings.json")).unwrap(),
            "{not json"
        );
    }
}
