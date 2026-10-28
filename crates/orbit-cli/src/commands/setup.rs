//! `orbit setup` and `orbit uninstall`. `spec` declares each agent from `config/setup/agents/`,
//! `detect` finds the installed ones, `plan` lists the files a selection touches, and
//! `components` installs or removes one `Component` at a time into a `Report`. `index_repo`
//! indexes the current repository afterwards. `summary` turns all of it into text and `tui`
//! draws it. Any pre-existing file gets a one-time `.orbit-backup` sibling before its first
//! modification.

mod components;
pub(crate) mod detect;
mod index_repo;
mod plan;
pub(crate) mod spec;
mod summary;

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use anyhow::{Context, Result};

use components::Report;
use detect::Machine;
use index_repo::IndexOutcome;
use plan::{Plan, Selection};
use spec::ScopedPath;

use crate::tui;

#[derive(Debug)]
pub(crate) struct SetupRun {
    pub(crate) detected_agents: Vec<String>,
    pub(crate) selected_agents: Vec<String>,
    pub(crate) components: BTreeSet<Component>,
    pub(crate) global: bool,
    pub(crate) interactive: bool,
    pub(crate) graph_first: Option<bool>,
    pub(crate) index: Option<bool>,
    pub(crate) outcome: Outcome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Outcome {
    Applied,
    DryRun,
    NoAgentSelected,
}

impl SetupRun {
    fn new(
        detected_agents: &[(spec::Agent, PathBuf)],
        selection: &Selection,
        target: &Target,
        interactive: bool,
        outcome: Outcome,
    ) -> SetupRun {
        SetupRun {
            detected_agents: detected_agents
                .iter()
                .map(|(agent, _)| agent.name.clone())
                .collect(),
            selected_agents: selection.selected_agent_names(),
            components: selection.components.clone(),
            global: matches!(target, Target::Global),
            interactive,
            graph_first: None,
            index: None,
            outcome,
        }
    }
}

pub(crate) async fn install(
    options: Options,
    target: Target,
    machine: &Machine,
) -> Result<SetupRun> {
    let interactive = tui::can_prompt(options.yes)?;
    let detected_agents = machine.installed_agents();
    let finish = |selection: &Selection, outcome, indexed: bool| SetupRun {
        index: Some(indexed),
        ..SetupRun::new(&detected_agents, selection, &target, interactive, outcome)
    };
    let mut selection = Selection::from_setup_options(&options, &detected_agents)?;
    tui::intro(format!(
        "Orbit setup ({})",
        summary::join_component_labels(&selection.components)
    ))?;
    crate::skill::warn_inside_setup_frame();

    if interactive {
        let location_hints = summary::detected_location_hints(&detected_agents, machine);
        let every_agent: Vec<_> = spec::agents().collect();
        selection = ask_which_agents(
            selection,
            "Which agents should use Orbit?",
            &every_agent,
            &location_hints,
        )?;
    }
    let ui_agents: Vec<&str> = selection
        .agents
        .iter()
        .filter(|agent| agent.has_ui())
        .map(|agent| agent.title.as_str())
        .collect();
    if interactive
        && options.ask_agent_plugins
        && !ui_agents.is_empty()
        && !tui::confirm(
            &format!(
                "Show Orbit calls in {} (Orbit UI)?",
                ui_agents.join(" and ")
            ),
            true,
        )?
    {
        selection.components.remove(&Component::AgentPlugins);
    }
    if selection.agents.is_empty() {
        tui::outro_cancel(format!(
            "No agent selected. Name one to configure it: orbit setup <{}>",
            spec::agent_names().join("|")
        ))?;
        return Ok(finish(&selection, Outcome::NoAgentSelected, false));
    }

    let plan = plan::for_selection(&selection, &target)?;
    if options.dry_run {
        show_dry_run(&plan, "Dry run: nothing written.")?;
        return Ok(finish(&selection, Outcome::DryRun, false));
    }

    if selection.components.contains(&Component::Skill)
        && needs_instance_skill(&selection.skill, &selection.agents, &target)
    {
        selection.skill.full_tree = Some(crate::skill::full_tree().await?);
    }

    apply_and_report(&options, |report| {
        components::install(&selection, &target, report)
    })?;
    tui::card("Configured", summary::format_components_per_agent(&plan))?;
    if selection.components.contains(&Component::Instructions) && !spec::reaches_gitlab() {
        tui::card(
            "GitLab",
            format!(
                "Not signed in, so agent instructions cover local code only. Run `glab auth \
                 login`, then `{} setup` again to add GitLab search.",
                spec::launcher()
            ),
        )?;
    }

    let index_outcome = match index_repo::repository_root(&target) {
        None => IndexOutcome::OutsideRepository,
        Some(repo_root) => {
            let index_path = index_repo::index_path_for(&target, &repo_root);
            if options.index {
                index_repo::index_repository(repo_root, index_path)?
            } else {
                IndexOutcome::NotIndexed { index_path }
            }
        }
    };
    if let Some(command) = summary::format_try_it_command(&index_outcome) {
        tui::card("Try it", command)?;
    }
    tui::outro(summary::format_closing_line(&index_outcome))?;
    let indexed = matches!(index_outcome, IndexOutcome::Indexed { .. });
    Ok(finish(&selection, Outcome::Applied, indexed))
}

pub(crate) fn uninstall(options: Options, target: Target, machine: &Machine) -> Result<SetupRun> {
    let interactive = tui::can_prompt(options.yes)?;
    let detected_agents = machine.installed_agents();
    let finish = |selection: &Selection, outcome| {
        SetupRun::new(&detected_agents, selection, &target, interactive, outcome)
    };
    let mut selection = Selection::from_uninstall_options(&options, &target, &detected_agents)?;
    tui::intro(format!(
        "Orbit uninstall ({})",
        summary::join_component_labels(&selection.components)
    ))?;
    if selection.agents.is_empty() {
        tui::outro("Orbit is not set up for any agent. Nothing to remove.")?;
        return Ok(finish(&selection, Outcome::NoAgentSelected));
    }

    if interactive {
        let location_hints = summary::detected_location_hints(&detected_agents, machine);
        let installed_agents = selection.agents.clone();
        selection = ask_which_agents(
            selection,
            "Remove Orbit from which agents?",
            &installed_agents,
            &location_hints,
        )?;
    }
    if selection.agents.is_empty() {
        tui::outro_cancel("No agent selected.")?;
        return Ok(finish(&selection, Outcome::NoAgentSelected));
    }

    let plan = plan::for_selection(&selection, &target)?;
    if options.dry_run {
        show_dry_run(&plan, "Dry run: nothing removed.")?;
        return Ok(finish(&selection, Outcome::DryRun));
    }

    let report = apply_and_report(&options, |report| {
        components::remove(&selection, &target, report)
    })?;
    tui::card(
        "Removed",
        summary::format_removed_files_per_component(&report),
    )?;
    tui::outro("Done. Backups stay only for files you edited after setup.")?;
    Ok(finish(&selection, Outcome::Applied))
}

fn ask_which_agents(
    selection: Selection,
    question: &str,
    offered_agents: &[spec::Agent],
    location_hints: &BTreeMap<String, String>,
) -> Result<Selection> {
    let choices = summary::agent_picker_choices(offered_agents, location_hints);
    let preselected = selection.selected_agent_names();
    let chosen = tui::multiselect(question, &choices, &preselected)?;
    selection.with_agents_named(&chosen)
}

fn apply_and_report(
    options: &Options,
    apply: impl FnOnce(&mut Report) -> Result<()>,
) -> Result<Report> {
    let mut report = Report::default();
    let outcome = apply(&mut report);
    if options.verbose || outcome.is_err() {
        show_outcomes_per_component(&report)?;
    }
    outcome?;
    Ok(report)
}

fn show_dry_run(plan: &Plan, closing: &str) -> Result<()> {
    tui::card("Plan", summary::format_components_per_agent(plan))?;
    tui::card(
        format!("Files in {}", plan.scope),
        summary::format_files_per_component(plan),
    )?;
    tui::outro(closing)
}

fn show_outcomes_per_component(report: &Report) -> Result<()> {
    for (group, body) in summary::format_outcomes_per_component(report) {
        tui::card(group, body)?;
    }
    Ok(())
}

pub(crate) fn agent_name_parser() -> clap::builder::PossibleValuesParser {
    clap::builder::PossibleValuesParser::new(spec::agent_names())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, clap::ValueEnum)]
pub(crate) enum Component {
    Instructions,
    Hooks,
    Skill,
    Mcp,
    AgentPlugins,
}

impl Component {
    pub(super) const ALL: [Component; 5] = [
        Component::Instructions,
        Component::Hooks,
        Component::Skill,
        Component::Mcp,
        Component::AgentPlugins,
    ];

    pub(crate) fn from_flags(
        install: &[Component],
        skip: &[Component],
    ) -> Result<BTreeSet<Component>> {
        if let Some(both) = install.iter().find(|component| skip.contains(component)) {
            let name =
                clap::ValueEnum::to_possible_value(both).map(|value| value.get_name().to_string());
            anyhow::bail!(
                "--install and --skip both name {}",
                name.unwrap_or_default()
            );
        }
        Ok(Component::ALL
            .into_iter()
            .filter(|component| *component != Component::Mcp || install.contains(component))
            .filter(|component| !skip.contains(component))
            .collect())
    }

    fn label(self) -> &'static str {
        match self {
            Component::Instructions => "instructions",
            Component::Hooks => "hooks",
            Component::Skill => "skill",
            Component::Mcp => "mcp server",
            Component::AgentPlugins => "agent plugins",
        }
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    clap::ValueEnum,
    serde::Serialize,
    serde::Deserialize,
)]
#[serde(rename_all = "lowercase")]
pub(crate) enum SkillKind {
    /// A thin skill that loads the real one when the agent reads it.
    #[default]
    Wrapper,
    /// The skill tree itself, for agents that cannot run commands when they load a skill.
    Full,
}

/// What `--skill` asked for, and the instance's full tree when one may be needed. Which kind
/// each skills directory gets is decided at install time from the agents that share it.
#[derive(Clone, Default)]
pub(crate) struct SkillRequest {
    pub(crate) kind: Option<SkillKind>,
    pub(crate) full_tree: Option<BTreeMap<String, Vec<u8>>>,
    pub(crate) search_path: Option<std::ffi::OsString>,
}

fn needs_instance_skill(request: &SkillRequest, selected: &[spec::Agent], target: &Target) -> bool {
    if let Some(kind) = request.kind {
        return kind == SkillKind::Full;
    }
    let needs_full = selected
        .iter()
        .any(|agent| agent.skills.as_ref().is_some_and(|dirs| dirs.requires_full));
    // A rerun keeps `full`, so it needs the instance's tree again, not the embedded one.
    needs_full || components::records_full_skill(selected, target)
}

pub(crate) struct Options {
    pub(crate) agents: Vec<String>,
    pub(crate) all: bool,
    pub(crate) yes: bool,
    pub(crate) dry_run: bool,
    pub(crate) verbose: bool,
    pub(crate) index: bool,
    pub(crate) components: BTreeSet<Component>,
    pub(crate) ask_agent_plugins: bool,
    pub(crate) skill: SkillRequest,
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

    fn root(&self) -> Result<PathBuf> {
        match self {
            Target::Project(root) => Ok(root.clone()),
            Target::Global => dirs::home_dir().context("could not determine home directory"),
        }
    }

    fn resolve(&self, scoped: &ScopedPath) -> Result<(PathBuf, String)> {
        match self {
            Target::Project(root) => Ok((root.join(&scoped.project), scoped.project.clone())),
            Target::Global => {
                let rest = scoped
                    .global
                    .strip_prefix("~/")
                    .with_context(|| format!("global path {} must start with ~/", scoped.global))?;
                Ok((self.root()?.join(rest), scoped.global.clone()))
            }
        }
    }

    fn registration_value(&self, scoped: &ScopedPath) -> Result<String> {
        match self {
            Target::Project(_) => Ok(scoped.project.clone()),
            Target::Global => Ok(self.resolve(scoped)?.0.display().to_string()),
        }
    }

    fn scope_label(&self) -> String {
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

    use super::*;

    fn install(options: Options, target: Target, machine: &Machine) -> Result<SetupRun> {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(super::install(options, target, machine))
    }

    fn project(dir: &Path) -> Target {
        Target::project(Some(dir.to_path_buf())).unwrap()
    }

    #[test]
    fn the_instance_skill_is_needed_when_duo_is_in_the_final_selection() {
        let dir = tempfile::tempdir().unwrap();
        let target = project(dir.path());
        let request = SkillRequest::default();
        let duo = plan::agents_named(&["duo".to_string()]).unwrap();
        let claude = plan::agents_named(&["claude".to_string()]).unwrap();
        assert!(needs_instance_skill(&request, &duo, &target));
        assert!(!needs_instance_skill(&request, &claude, &target));
        let wrapper = SkillRequest {
            kind: Some(SkillKind::Wrapper),
            full_tree: None,
            search_path: None,
        };
        assert!(!needs_instance_skill(&wrapper, &duo, &target));
    }

    fn options_for(names: &[&str]) -> Options {
        Options {
            agents: names.iter().map(|name| name.to_string()).collect(),
            all: false,
            yes: true,
            dry_run: false,
            verbose: false,
            index: false,
            components: Component::from_flags(&[], &[]).unwrap(),
            ask_agent_plugins: false,
            skill: SkillRequest::default(),
        }
    }

    fn options_with_mcp(names: &[&str]) -> Options {
        Options {
            components: Component::from_flags(&[Component::Mcp], &[]).unwrap(),
            ..options_for(names)
        }
    }

    fn install_default(names: &[&str], dir: &Path) {
        install(options_for(names), project(dir), &bare_machine()).unwrap();
    }

    fn bare_machine() -> Machine {
        Machine::new(PathBuf::from("/nonexistent-home"), BTreeMap::new())
    }

    fn install_with_mcp(names: &[&str], dir: &Path) {
        install(options_with_mcp(names), project(dir), &bare_machine()).unwrap();
    }

    fn uninstall_named(names: &[&str], dir: &Path) -> SetupRun {
        uninstall(options_for(names), project(dir), &bare_machine()).unwrap()
    }

    fn read_json(path: &Path) -> Value {
        serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
    }

    #[test]
    fn setup_detects_installed_agents_from_their_config_dirs() {
        let home = tempfile::tempdir().unwrap();
        std::fs::create_dir(home.path().join(".codex")).unwrap();
        let claude_config = tempfile::tempdir().unwrap();
        let env = BTreeMap::from([(
            "CLAUDE_CONFIG_DIR".to_string(),
            claude_config.path().display().to_string(),
        )]);
        let machine = Machine::new(home.path().to_path_buf(), env);

        let dir = tempfile::tempdir().unwrap();
        let run = install(options_for(&[]), project(dir.path()), &machine).unwrap();

        assert_eq!(run.detected_agents, ["claude", "codex"]);
        assert_eq!(run.selected_agents, ["claude", "codex"]);
        assert_eq!(run.outcome, Outcome::Applied);
        assert!(!run.global);
        assert!(dir.path().join("CLAUDE.md").is_file());
        assert!(dir.path().join("AGENTS.md").is_file());
        assert!(dir.path().join(".claude/settings.json").is_file());
        assert!(!dir.path().join(".opencode").exists());
    }

    #[test]
    fn setup_with_nothing_detected_writes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let run = install(options_for(&[]), project(dir.path()), &bare_machine()).unwrap();
        assert_eq!(run.outcome, Outcome::NoAgentSelected);
        assert!(std::fs::read_dir(dir.path()).unwrap().next().is_none());
    }

    #[test]
    fn dry_run_prints_the_plan_and_writes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let mut dry = options_with_mcp(&["claude", "codex", "opencode"]);
        dry.dry_run = true;
        dry.index = true;
        let run = install(dry, project(dir.path()), &bare_machine()).unwrap();
        assert_eq!(run.outcome, Outcome::DryRun);
        assert_eq!(run.index, Some(false));
        assert_eq!(run.graph_first, None);
        assert!(run.components.contains(&Component::Mcp));
        assert!(std::fs::read_dir(dir.path()).unwrap().next().is_none());
    }

    #[test]
    fn mcp_server_is_opt_in() {
        let dir = tempfile::tempdir().unwrap();
        install(
            options_for(&["claude", "codex", "opencode"]),
            project(dir.path()),
            &bare_machine(),
        )
        .unwrap();

        assert!(dir.path().join("CLAUDE.md").is_file());
        assert!(!dir.path().join(".mcp.json").exists());
        assert!(!dir.path().join(".codex").exists());
        assert!(!dir.path().join("opencode.json").exists());

        install_with_mcp(&["claude"], dir.path());
        assert!(dir.path().join(".mcp.json").is_file());
    }

    #[test]
    fn skipped_components_are_left_alone() {
        let dir = tempfile::tempdir().unwrap();
        let mut only_instructions = options_for(&["claude", "opencode"]);
        only_instructions.components = Component::from_flags(
            &[],
            &[Component::Hooks, Component::Skill, Component::AgentPlugins],
        )
        .unwrap();
        install(only_instructions, project(dir.path()), &bare_machine()).unwrap();

        assert!(dir.path().join("CLAUDE.md").is_file());
        assert!(dir.path().join("AGENTS.md").is_file());
        assert!(!dir.path().join(".claude").exists());
        assert!(!dir.path().join(".opencode").exists());
        assert!(!dir.path().join(".agents").exists());
    }

    #[test]
    fn bare_uninstall_covers_the_installed_and_detected_agents() {
        let home = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(home.path().join(".config/opencode")).unwrap();
        std::fs::create_dir_all(home.path().join(".codex")).unwrap();
        let machine = Machine::new(home.path().to_path_buf(), BTreeMap::new());
        let dir = tempfile::tempdir().unwrap();
        install_with_mcp(&["opencode"], dir.path());
        assert!(dir.path().join(".opencode/plugins/orbit.js").is_file());

        uninstall(options_for(&[]), project(dir.path()), &machine).unwrap();

        assert!(!dir.path().join(".opencode").exists());
        assert!(!dir.path().join("AGENTS.md").exists());
        assert!(!dir.path().join("opencode.json").exists());
    }

    #[test]
    fn setup_and_uninstall_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("AGENTS.md"), "# My rules\n").unwrap();
        let opencode_config = dir.path().join(".opencode/opencode.json");
        std::fs::create_dir_all(opencode_config.parent().unwrap()).unwrap();
        std::fs::write(&opencode_config, r#"{"plugin": ["other.js"]}"#).unwrap();

        install_with_mcp(&["codex", "opencode"], dir.path());

        let agents = std::fs::read_to_string(dir.path().join("AGENTS.md")).unwrap();
        assert!(agents.contains("<!-- orbit:setup:begin -->"));
        assert!(agents.contains("# My rules"));
        assert!(dir.path().join(".opencode/plugins/orbit.js").is_file());
        assert_eq!(
            read_json(&opencode_config)["plugin"],
            json!([
                "other.js",
                ".opencode/plugins/orbit.js",
                ".opencode/plugins/orbit-ui.js"
            ])
        );
        assert_eq!(
            read_json(&dir.path().join("opencode.json"))["mcp"]["orbit"],
            json!({
                "type": "local",
                "command": ["orbit", "workspace", "mcp", "serve"],
                "enabled": true,
                "environment": {"AI_AGENT": "opencode"}
            })
        );
        let codex = std::fs::read_to_string(dir.path().join(".codex/config.toml")).unwrap();
        assert!(codex.contains("[mcp_servers.orbit]"), "{codex}");
        assert!(
            codex.contains(r#"args = ["workspace", "mcp", "serve"]"#),
            "{codex}"
        );
        assert!(codex.contains(r#"env = { AI_AGENT = "codex" }"#), "{codex}");
        assert!(dir.path().join(".agents/skills/orbit/SKILL.md").is_file());

        let removed = uninstall_named(&["codex", "opencode"], dir.path());

        assert_eq!(removed.selected_agents, ["codex", "opencode"]);
        assert_eq!(removed.outcome, Outcome::Applied);
        assert_eq!(removed.index, None);
        assert_eq!(
            std::fs::read_to_string(dir.path().join("AGENTS.md")).unwrap(),
            "# My rules\n"
        );
        assert!(!dir.path().join("AGENTS.md.orbit-backup").exists());
        assert_eq!(read_json(&opencode_config), json!({"plugin": ["other.js"]}));
        assert!(!dir.path().join(".opencode/plugins").exists());
        assert!(!dir.path().join("opencode.json").exists());
        assert!(!dir.path().join(".codex").exists());
        assert!(!dir.path().join(".agents").exists());
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_claude_md_gets_one_orbit_block() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("AGENTS.md"), "# rules\n").unwrap();
        std::os::unix::fs::symlink("AGENTS.md", dir.path().join("CLAUDE.md")).unwrap();

        install_with_mcp(&["claude", "codex"], dir.path());

        let agents = std::fs::read_to_string(dir.path().join("AGENTS.md")).unwrap();
        assert_eq!(agents.matches("<!-- orbit:setup:begin -->").count(), 1);
        let claude = std::fs::symlink_metadata(dir.path().join("CLAUDE.md")).unwrap();
        assert!(claude.is_symlink());
    }

    #[test]
    fn claude_mcp_entry_joins_existing_servers_and_leaves_them_on_uninstall() {
        let dir = tempfile::tempdir().unwrap();
        let mcp_json = dir.path().join(".mcp.json");
        let theirs = json!({"mcpServers": {"theirs": {"command": "their-server"}}});
        std::fs::write(&mcp_json, theirs.to_string()).unwrap();

        install_with_mcp(&["claude"], dir.path());
        install_with_mcp(&["claude"], dir.path());

        let servers = read_json(&mcp_json)["mcpServers"].clone();
        assert_eq!(servers["theirs"], json!({"command": "their-server"}));
        assert_eq!(
            servers["orbit"],
            json!({
                "type": "stdio",
                "command": "orbit",
                "args": ["workspace", "mcp", "serve"],
                "env": {"AI_AGENT": "claude-code"}
            })
        );

        uninstall_named(&["claude"], dir.path());

        assert_eq!(read_json(&mcp_json), theirs);
    }

    #[test]
    fn codex_mcp_entry_preserves_comments_and_foreign_tables() {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join(".codex/config.toml");
        std::fs::create_dir_all(config.parent().unwrap()).unwrap();
        let theirs = "# my codex settings\nmodel = \"o3\"\n\n[mcp_servers.theirs]\ncommand = \"their-server\"\n";
        std::fs::write(&config, theirs).unwrap();

        install_with_mcp(&["codex"], dir.path());

        let installed = std::fs::read_to_string(&config).unwrap();
        assert!(
            installed.starts_with("# my codex settings\nmodel = \"o3\""),
            "{installed}"
        );
        assert!(installed.contains("[mcp_servers.theirs]"), "{installed}");
        assert!(installed.contains("[mcp_servers.orbit]"), "{installed}");

        uninstall_named(&["codex"], dir.path());

        assert_eq!(std::fs::read_to_string(&config).unwrap(), theirs);
    }

    #[test]
    fn uninstall_removes_mcp_entries_written_by_either_distribution() {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join(".codex/config.toml");
        std::fs::create_dir_all(config.parent().unwrap()).unwrap();
        std::fs::write(
            &config,
            "[mcp_servers.orbit]\ncommand = \"glab\"\nargs = [\"orbit\", \"mcp\", \"serve\"]\n",
        )
        .unwrap();
        let mcp_json = dir.path().join(".mcp.json");
        std::fs::write(
            &mcp_json,
            r#"{"mcpServers": {"orbit": {"type": "stdio", "command": "glab", "args": ["orbit", "mcp", "serve"]}}}"#,
        )
        .unwrap();

        uninstall_named(&["claude", "codex"], dir.path());

        assert!(!config.exists());
        assert!(!mcp_json.exists());
    }

    #[test]
    fn broken_config_files_are_never_clobbered() {
        for (agent, file, contents, complaint) in [
            (
                "claude",
                ".claude/settings.json",
                "{not json",
                "not valid JSON",
            ),
            (
                "claude",
                ".claude/settings.json",
                r#"{"hooks": "a-string"}"#,
                "expected an object",
            ),
            (
                "codex",
                ".codex/config.toml",
                "model = [unclosed\n",
                "not valid TOML",
            ),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join(file);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, contents).unwrap();

            let err = install(
                options_with_mcp(&[agent]),
                project(dir.path()),
                &bare_machine(),
            )
            .unwrap_err();

            assert!(format!("{err:#}").contains(complaint), "{file}: {err:#}");
            assert_eq!(std::fs::read_to_string(&path).unwrap(), contents, "{file}");
        }
    }

    #[test]
    fn opencode_jsonc_config_is_refused_not_rewritten() {
        let dir = tempfile::tempdir().unwrap();
        let jsonc = dir.path().join("opencode.jsonc");
        std::fs::write(&jsonc, "{\n  // mine\n  \"theme\": \"dark\"\n}\n").unwrap();

        let err = install(
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

    fn write_legacy_skill(legacy: &Path) {
        for (relative, contents) in crate::skill::embedded_files() {
            let file = legacy.join(relative);
            std::fs::create_dir_all(file.parent().unwrap()).unwrap();
            std::fs::write(file, contents).unwrap();
        }
    }

    fn skill_manifest_name(skill_dir: &Path) -> String {
        let manifest = std::fs::read_to_string(skill_dir.join("SKILL.md")).unwrap();
        manifest
            .lines()
            .find_map(|line| line.strip_prefix("name: "))
            .unwrap()
            .to_string()
    }

    #[cfg(unix)]
    #[test]
    fn skill_is_written_once_and_linked_into_claude() {
        let dir = tempfile::tempdir().unwrap();
        install_with_mcp(&["claude", "codex"], dir.path());

        let canonical = dir.path().join(".agents/skills/orbit");
        let link = dir.path().join(".claude/skills/orbit");
        assert!(std::fs::symlink_metadata(&link).unwrap().is_symlink());
        assert_eq!(
            std::fs::read_link(&link).unwrap(),
            Path::new("../../.agents/skills/orbit")
        );
        assert_eq!(skill_manifest_name(&link), "orbit");
        assert!(dir.path().join(".agents/skills/.orbit-managed").is_file());
        assert!(!dir.path().join(".agents/skills/orbit-cli").exists());

        uninstall_named(&["claude", "codex"], dir.path());

        assert!(!link.exists());
        assert!(!canonical.exists());
        assert!(!dir.path().join(".agents").exists());
    }

    #[test]
    fn the_default_skill_is_the_wrapper_named_orbit() {
        let dir = tempfile::tempdir().unwrap();
        install_with_mcp(&["codex"], dir.path());

        let installed = dir.path().join(".agents/skills/orbit");
        assert_eq!(skill_manifest_name(&installed), "orbit");
        let manifest = std::fs::read_to_string(installed.join("SKILL.md")).unwrap();
        assert!(manifest.contains("!`orbit skills get orbit`"), "{manifest}");
        let files: Vec<_> = std::fs::read_dir(&installed)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        assert_eq!(files, ["SKILL.md"]);
    }

    #[test]
    fn a_full_skill_replaces_the_wrapper_and_keeps_the_name() {
        let dir = tempfile::tempdir().unwrap();
        install_with_mcp(&["codex"], dir.path());
        let installed = dir.path().join(".agents/skills/orbit");

        let options = Options {
            skill: SkillRequest {
                kind: Some(SkillKind::Full),
                full_tree: Some(crate::skill::embedded_full_tree().unwrap()),
                search_path: None,
            },
            ..options_for(&["codex"])
        };
        install(options, project(dir.path()), &bare_machine()).unwrap();

        assert_eq!(skill_manifest_name(&installed), "orbit");
        assert!(installed.join("references/local/cli.md").is_file());

        uninstall_named(&["codex"], dir.path());
        assert!(!installed.exists());
    }

    #[test]
    fn setup_moves_an_unmarked_skill_aside_and_uninstall_keeps_one() {
        let dir = tempfile::tempdir().unwrap();
        let skills = dir.path().join(".agents/skills");
        let installed = skills.join("orbit");
        std::fs::create_dir_all(&installed).unwrap();
        std::fs::write(
            installed.join("SKILL.md"),
            "---\nname: orbit\n---\nfrom me\n",
        )
        .unwrap();
        std::fs::write(installed.join("extra.md"), "mine\n").unwrap();

        install_with_mcp(&["codex"], dir.path());

        assert!(skills.join(".orbit-managed").is_file());
        assert!(!installed.join("extra.md").exists());
        let backup = skills.join("orbit.orbit-backup");
        assert_eq!(
            std::fs::read_to_string(backup.join("extra.md")).unwrap(),
            "mine\n"
        );
        assert!(!backup.join("SKILL.md").exists());
        assert_eq!(
            std::fs::read_to_string(backup.join("SKILL.md.orbit-backup")).unwrap(),
            "---\nname: orbit\n---\nfrom me\n"
        );

        std::fs::remove_file(skills.join(".orbit-managed")).unwrap();
        uninstall_named(&["codex"], dir.path());
        assert!(installed.join("SKILL.md").is_file());
    }

    fn write_glab_wrapper(skills: &Path) {
        let tree = crate::skill::wrapper_tree_for(spec::GLAB_LAUNCHER).unwrap();
        std::fs::create_dir_all(skills.join("orbit")).unwrap();
        std::fs::write(skills.join("orbit/SKILL.md"), &tree["SKILL.md"]).unwrap();
    }

    fn search_path_with(program: Option<&str>) -> (tempfile::TempDir, std::ffi::OsString) {
        let bin = tempfile::tempdir().unwrap();
        if let Some(program) = program {
            std::fs::write(bin.path().join(program), "").unwrap();
        }
        let path = bin.path().as_os_str().to_owned();
        (bin, path)
    }

    fn install_with_search_path(names: &[&str], dir: &Path, search_path: std::ffi::OsString) {
        let mut options = options_with_mcp(names);
        options.skill.search_path = Some(search_path);
        install(options, project(dir), &bare_machine()).unwrap();
    }

    #[test]
    fn a_glab_wrapper_without_a_record_is_adopted_when_glab_runs() {
        let dir = tempfile::tempdir().unwrap();
        let skills = dir.path().join(".agents/skills");
        write_glab_wrapper(&skills);
        let (_bin, path) = search_path_with(Some("glab"));

        install_with_search_path(&["codex"], dir.path(), path);

        let manifest = std::fs::read_to_string(skills.join("orbit/SKILL.md")).unwrap();
        assert!(
            manifest.contains("!`glab orbit skills get orbit`"),
            "{manifest}"
        );
        assert!(skills.join(".orbit-managed").is_file());
        assert!(!skills.join("orbit.orbit-backup").exists());
    }

    #[test]
    fn a_glab_wrapper_is_replaced_when_glab_cannot_run() {
        let dir = tempfile::tempdir().unwrap();
        let skills = dir.path().join(".agents/skills");
        write_glab_wrapper(&skills);
        let (_bin, path) = search_path_with(None);

        install_with_search_path(&["claude"], dir.path(), path);

        let manifest = std::fs::read_to_string(skills.join("orbit/SKILL.md")).unwrap();
        assert!(manifest.contains("!`orbit skills get orbit`"), "{manifest}");
        assert!(!skills.join("orbit.orbit-backup").exists());
        let settings = std::fs::read_to_string(dir.path().join(".claude/settings.json")).unwrap();
        assert!(!settings.contains("glab orbit"), "{settings}");
    }

    #[test]
    fn a_glab_wrapper_survives_setup_and_gets_its_permission_rule() {
        let dir = tempfile::tempdir().unwrap();
        let skills = dir.path().join(".agents/skills");
        write_glab_wrapper(&skills);
        let (_bin, path) = search_path_with(Some("glab"));

        install_with_search_path(&["claude"], dir.path(), path.clone());
        install_with_search_path(&["claude"], dir.path(), path);

        let manifest = std::fs::read_to_string(skills.join("orbit/SKILL.md")).unwrap();
        assert!(
            manifest.contains("!`glab orbit skills get orbit`"),
            "{manifest}"
        );
        let settings = std::fs::read_to_string(dir.path().join(".claude/settings.json")).unwrap();
        for rule in [
            "Bash(glab orbit skills get orbit)",
            "Bash(orbit skills get orbit)",
        ] {
            assert!(settings.contains(rule), "{settings}");
        }
    }

    #[test]
    fn a_full_skill_replaces_an_unrecorded_wrapper_without_a_backup() {
        let dir = tempfile::tempdir().unwrap();
        let skills = dir.path().join(".agents/skills");
        write_glab_wrapper(&skills);

        let mut options = options_with_mcp(&["duo"]);
        options.skill = SkillRequest {
            kind: Some(SkillKind::Full),
            full_tree: Some(crate::skill::embedded_full_tree().unwrap()),
            search_path: None,
        };
        install(options, project(dir.path()), &bare_machine()).unwrap();

        assert!(!skills.join("orbit.orbit-backup").exists());
        assert!(
            !std::fs::read_to_string(skills.join("orbit/SKILL.md"))
                .unwrap()
                .contains("!`glab orbit")
        );
    }

    #[test]
    fn a_second_foreign_skill_gets_its_own_backup() {
        let dir = tempfile::tempdir().unwrap();
        let skills = dir.path().join(".agents/skills");
        std::fs::create_dir_all(skills.join("orbit.orbit-backup")).unwrap();
        std::fs::create_dir_all(skills.join("orbit")).unwrap();
        std::fs::write(skills.join("orbit/extra.md"), "newer\n").unwrap();

        install_with_mcp(&["codex"], dir.path());

        assert_eq!(
            std::fs::read_to_string(skills.join("orbit.orbit-backup.1/extra.md")).unwrap(),
            "newer\n"
        );
    }

    #[test]
    fn uninstalling_one_agent_keeps_the_skill_the_others_share() {
        let dir = tempfile::tempdir().unwrap();
        install_with_mcp(&["claude", "codex", "duo"], dir.path());
        let shared = dir.path().join(".agents/skills/orbit");

        uninstall_named(&["codex"], dir.path());

        assert!(shared.join("SKILL.md").is_file());
        #[cfg(unix)]
        assert!(dir.path().join(".claude/skills/orbit/SKILL.md").is_file());

        uninstall_named(&["claude"], dir.path());
        assert!(shared.join("SKILL.md").is_file());
        #[cfg(unix)]
        assert!(!dir.path().join(".claude/skills/orbit").exists());

        uninstall_named(&["duo"], dir.path());
        assert!(!shared.exists());
        assert!(!dir.path().join(".agents/skills/.orbit-managed").exists());
    }

    #[test]
    fn duo_gets_the_full_skill_and_a_later_setup_for_another_agent_keeps_it() {
        let dir = tempfile::tempdir().unwrap();
        let installed = dir.path().join(".agents/skills/orbit");

        install_with_mcp(&["duo"], dir.path());
        assert!(installed.join("references/local/cli.md").is_file());

        install_with_mcp(&["claude"], dir.path());
        assert!(installed.join("references/local/cli.md").is_file());

        let wrapper = Options {
            skill: SkillRequest {
                kind: Some(SkillKind::Wrapper),
                full_tree: None,
                search_path: None,
            },
            ..options_with_mcp(&["claude"])
        };
        install(wrapper, project(dir.path()), &bare_machine()).unwrap();
        assert!(!installed.join("references").exists());
    }

    #[test]
    fn a_marker_left_behind_does_not_hide_a_recreated_skill_from_the_backup() {
        let dir = tempfile::tempdir().unwrap();
        let skills = dir.path().join(".agents/skills");
        install_with_mcp(&["codex"], dir.path());
        std::fs::remove_dir_all(skills.join("orbit")).unwrap();

        uninstall_named(&["codex"], dir.path());
        assert!(!skills.join(".orbit-managed").exists());

        std::fs::create_dir_all(skills.join("orbit")).unwrap();
        std::fs::write(skills.join("orbit/notes.md"), "mine\n").unwrap();
        install_with_mcp(&["codex"], dir.path());

        assert_eq!(
            std::fs::read_to_string(skills.join("orbit.orbit-backup/notes.md")).unwrap(),
            "mine\n"
        );
    }

    #[test]
    fn a_skill_recreated_by_hand_is_backed_up_even_when_the_record_is_stale() {
        let dir = tempfile::tempdir().unwrap();
        let skills = dir.path().join(".agents/skills");
        install_with_mcp(&["codex"], dir.path());
        std::fs::remove_dir_all(skills.join("orbit")).unwrap();
        std::fs::create_dir_all(skills.join("orbit")).unwrap();
        std::fs::write(skills.join("orbit/notes.md"), "mine\n").unwrap();

        uninstall_named(&["codex"], dir.path());
        assert!(skills.join("orbit/notes.md").is_file());

        install_with_mcp(&["codex"], dir.path());
        assert_eq!(
            std::fs::read_to_string(skills.join("orbit.orbit-backup/notes.md")).unwrap(),
            "mine\n"
        );
        assert!(skills.join("orbit/SKILL.md").is_file());
    }

    #[test]
    fn a_skill_edited_after_setup_is_backed_up_not_overwritten() {
        let dir = tempfile::tempdir().unwrap();
        let skills = dir.path().join(".agents/skills");
        install_with_mcp(&["codex"], dir.path());
        std::fs::write(skills.join("orbit/notes.md"), "mine\n").unwrap();

        install_with_mcp(&["codex"], dir.path());

        assert!(skills.join("orbit.orbit-backup/notes.md").is_file());
        assert!(!skills.join("orbit/notes.md").exists());
    }

    #[test]
    fn uninstalling_an_edited_shared_skill_still_releases_the_agents() {
        let dir = tempfile::tempdir().unwrap();
        let skills = dir.path().join(".agents/skills");
        install_with_mcp(&["claude", "codex"], dir.path());
        let notes = skills.join("orbit/notes.md");
        std::fs::write(&notes, "mine\n").unwrap();

        uninstall_named(&["codex"], dir.path());
        uninstall_named(&["claude"], dir.path());
        assert!(notes.is_file());
        assert!(!skills.join(".orbit-managed").exists());

        install_with_mcp(&["codex"], dir.path());
        assert!(skills.join("orbit.orbit-backup").is_dir());
        uninstall_named(&["codex"], dir.path());
        assert!(!skills.join("orbit").exists());
    }

    #[test]
    fn a_wrapper_from_another_release_is_replaced_not_backed_up() {
        let dir = tempfile::tempdir().unwrap();
        let skills = dir.path().join(".agents/skills");
        install_with_mcp(&["codex"], dir.path());
        let manifest = skills.join("orbit/SKILL.md");
        let current = std::fs::read_to_string(&manifest).unwrap();
        let version_line = current
            .lines()
            .find(|line| line.starts_with("version:"))
            .unwrap();
        let other_release = current.replace(version_line, "version: 9.9.9");
        assert_ne!(other_release, current);
        std::fs::write(&manifest, other_release).unwrap();

        install_with_mcp(&["codex"], dir.path());

        assert!(!skills.join("orbit.orbit-backup").exists());
        assert_eq!(std::fs::read_to_string(&manifest).unwrap(), current);

        std::fs::write(&manifest, "---\nname: orbit\n---\nmine\n").unwrap();
        install_with_mcp(&["codex"], dir.path());
        assert!(skills.join("orbit.orbit-backup").is_dir());
    }

    fn install_then_edit_wrapper(dir: &Path) -> (PathBuf, String) {
        let manifest = dir.join(".agents/skills/orbit/SKILL.md");
        install_with_mcp(&["codex"], dir);
        let edited = format!(
            "{}Always use project X.\n",
            std::fs::read_to_string(&manifest).unwrap()
        );
        std::fs::write(&manifest, &edited).unwrap();
        (manifest, edited)
    }

    #[test]
    fn an_edited_wrapper_is_backed_up_on_setup() {
        let dir = tempfile::tempdir().unwrap();
        let (_, edited) = install_then_edit_wrapper(dir.path());

        install_with_mcp(&["codex"], dir.path());

        let backup = dir
            .path()
            .join(".agents/skills/orbit.orbit-backup/SKILL.md.orbit-backup");
        assert_eq!(std::fs::read_to_string(backup).unwrap(), edited);
    }

    #[test]
    fn an_edited_wrapper_is_kept_on_uninstall() {
        let dir = tempfile::tempdir().unwrap();
        let (manifest, edited) = install_then_edit_wrapper(dir.path());

        uninstall_named(&["codex"], dir.path());

        assert_eq!(std::fs::read_to_string(manifest).unwrap(), edited);
    }

    #[test]
    fn a_finder_file_does_not_count_as_a_change() {
        let dir = tempfile::tempdir().unwrap();
        let skills = dir.path().join(".agents/skills");
        install_with_mcp(&["codex"], dir.path());
        std::fs::write(skills.join("orbit/.DS_Store"), "x").unwrap();

        install_with_mcp(&["codex"], dir.path());

        assert!(!skills.join("orbit.orbit-backup").exists());
    }

    #[test]
    fn the_wrapper_stays_when_no_agent_needs_the_full_skill() {
        let dir = tempfile::tempdir().unwrap();
        install_with_mcp(&["claude", "codex"], dir.path());
        install_with_mcp(&["codex"], dir.path());
        assert!(!dir.path().join(".agents/skills/orbit/references").exists());
    }

    #[cfg(unix)]
    #[test]
    fn a_legacy_orbit_cli_skill_and_its_link_are_migrated() {
        let dir = tempfile::tempdir().unwrap();
        let legacy = dir.path().join(".agents/skills/orbit-cli");
        write_legacy_skill(&legacy);
        let mine = dir.path().join(".agents/skills/mine");
        std::fs::create_dir_all(&mine).unwrap();
        std::fs::create_dir_all(dir.path().join(".claude/skills")).unwrap();
        let legacy_link = dir.path().join(".claude/skills/orbit-cli");
        std::os::unix::fs::symlink("../../.agents/skills/orbit-cli", &legacy_link).unwrap();

        install_with_mcp(&["claude"], dir.path());

        assert!(!legacy.exists());
        assert!(std::fs::symlink_metadata(&legacy_link).is_err());
        assert!(dir.path().join(".agents/skills/orbit/SKILL.md").is_file());
        assert!(mine.is_dir());
    }

    #[test]
    fn a_legacy_orbit_cli_copy_in_the_claude_dir_is_migrated_and_uninstalled() {
        let dir = tempfile::tempdir().unwrap();
        let legacy_copy = dir.path().join(".claude/skills/orbit-cli");
        write_legacy_skill(&legacy_copy);

        install_with_mcp(&["claude"], dir.path());

        assert!(!legacy_copy.exists());
        assert!(dir.path().join(".claude/skills/orbit/SKILL.md").is_file());

        write_legacy_skill(&legacy_copy);
        uninstall_named(&["claude"], dir.path());
        assert!(!legacy_copy.exists());
    }

    #[test]
    fn uninstall_removes_a_legacy_skill_and_keeps_one_the_user_owns() {
        let dir = tempfile::tempdir().unwrap();
        let legacy = dir.path().join(".agents/skills/orbit-cli");
        write_legacy_skill(&legacy);
        uninstall_named(&["codex"], dir.path());
        assert!(!legacy.exists());

        std::fs::create_dir_all(&legacy).unwrap();
        std::fs::write(legacy.join("SKILL.md"), "---\nname: something-else\n---\n").unwrap();
        uninstall_named(&["codex"], dir.path());
        assert!(legacy.join("SKILL.md").is_file());
    }

    #[cfg(unix)]
    #[test]
    fn setup_replaces_a_foreign_claude_skill_with_a_link() {
        let dir = tempfile::tempdir().unwrap();
        let foreign = dir.path().join(".claude/skills/orbit");
        std::fs::create_dir_all(&foreign).unwrap();
        std::fs::write(foreign.join("SKILL.md"), "---\nname: orbit\n---\nforeign\n").unwrap();
        let elsewhere = dir.path().join(".claude/skills/other");
        std::fs::create_dir_all(&elsewhere).unwrap();

        install_with_mcp(&["claude"], dir.path());

        let expected = Path::new("../../.agents/skills/orbit");
        assert_eq!(std::fs::read_link(&foreign).unwrap(), expected);
        assert!(
            std::fs::read_to_string(foreign.join("SKILL.md"))
                .unwrap()
                .contains("skills get orbit")
        );

        std::fs::remove_file(&foreign).unwrap();
        std::os::unix::fs::symlink(&elsewhere, &foreign).unwrap();
        install_with_mcp(&["claude"], dir.path());
        assert_eq!(std::fs::read_link(&foreign).unwrap(), expected);
        assert!(elsewhere.is_dir());
    }

    #[test]
    fn migration_keeps_files_the_user_added_or_edited_in_an_unmarked_legacy_skill() {
        let dir = tempfile::tempdir().unwrap();
        let legacy = dir.path().join(".agents/skills/orbit-cli");
        write_legacy_skill(&legacy);
        std::fs::write(legacy.join("my-notes.md"), "mine\n").unwrap();
        std::fs::write(legacy.join("references/local/sql.md"), "my edit\n").unwrap();

        install_with_mcp(&["codex"], dir.path());

        assert_eq!(
            std::fs::read_to_string(legacy.join("my-notes.md")).unwrap(),
            "mine\n"
        );
        assert_eq!(
            std::fs::read_to_string(legacy.join("references/local/sql.md")).unwrap(),
            "my edit\n"
        );
        assert!(!legacy.join("SKILL.md").exists());
        assert!(!legacy.join("references/local/cli.md").exists());
        assert!(dir.path().join(".agents/skills/orbit/SKILL.md").is_file());
    }

    #[test]
    fn an_edited_legacy_manifest_is_backed_up_so_only_one_skill_remains() {
        let dir = tempfile::tempdir().unwrap();
        let legacy = dir.path().join(".agents/skills/orbit-cli");
        write_legacy_skill(&legacy);
        std::fs::write(
            legacy.join("SKILL.md"),
            "---\nname: orbit-cli\n---\nold release\n",
        )
        .unwrap();

        install_with_mcp(&["codex"], dir.path());

        assert!(!legacy.join("SKILL.md").exists());
        assert_eq!(
            std::fs::read_to_string(legacy.join("SKILL.md.orbit-backup")).unwrap(),
            "---\nname: orbit-cli\n---\nold release\n"
        );
        assert_eq!(
            skill_manifest_name(&dir.path().join(".agents/skills/orbit")),
            "orbit"
        );
    }

    #[test]
    fn a_retired_legacy_manifest_never_overwrites_an_earlier_backup() {
        let dir = tempfile::tempdir().unwrap();
        let legacy = dir.path().join(".agents/skills/orbit-cli");
        write_legacy_skill(&legacy);
        std::fs::write(
            legacy.join("SKILL.md"),
            "---\nname: orbit-cli\n---\nsecond\n",
        )
        .unwrap();
        std::fs::write(legacy.join("SKILL.md.orbit-backup"), "first\n").unwrap();

        install_with_mcp(&["codex"], dir.path());

        assert!(!legacy.join("SKILL.md").exists());
        assert_eq!(
            std::fs::read_to_string(legacy.join("SKILL.md.orbit-backup")).unwrap(),
            "first\n"
        );
        assert!(
            std::fs::read_to_string(legacy.join("SKILL.md.orbit-backup.1"))
                .unwrap()
                .contains("second")
        );
    }

    #[test]
    fn uninstall_removes_allow_rules_written_under_either_launcher() {
        let dir = tempfile::tempdir().unwrap();
        let settings = dir.path().join(".claude/settings.json");
        std::fs::create_dir_all(settings.parent().unwrap()).unwrap();
        std::fs::write(
            &settings,
            r#"{"permissions":{"allow":["Bash(glab orbit skills get orbit)","Bash(glab orbit skills get orbit *)","Bash(ls)"]}}"#,
        )
        .unwrap();

        uninstall_named(&["claude"], dir.path());

        assert_eq!(
            read_json(&settings)["permissions"]["allow"],
            json!(["Bash(ls)"])
        );
    }

    #[test]
    fn claude_may_run_the_wrapper_command_without_asking() {
        let dir = tempfile::tempdir().unwrap();
        let settings = dir.path().join(".claude/settings.json");
        std::fs::create_dir_all(settings.parent().unwrap()).unwrap();
        std::fs::write(&settings, r#"{"permissions":{"allow":["Bash(ls)"]}}"#).unwrap();

        install_with_mcp(&["claude"], dir.path());

        let allow = read_json(&settings)["permissions"]["allow"].clone();
        assert_eq!(
            allow,
            json!([
                "Bash(ls)",
                "Bash(orbit skills get orbit)",
                "Bash(orbit skills get orbit *)"
            ])
        );

        install_with_mcp(&["claude"], dir.path());
        assert_eq!(read_json(&settings)["permissions"]["allow"], allow);

        uninstall_named(&["claude"], dir.path());
        assert_eq!(
            read_json(&settings)["permissions"]["allow"],
            json!(["Bash(ls)"])
        );
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

        install_with_mcp(&["claude", "codex", "opencode"], dir.path());
        install_with_mcp(&["claude", "codex", "opencode"], dir.path());

        let agents = std::fs::read_to_string(dir.path().join("AGENTS.md")).unwrap();
        assert_eq!(agents.matches("<!-- orbit:setup:begin -->").count(), 1);
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
    fn rerunning_setup_leaves_nothing_behind_after_uninstall() {
        let dir = tempfile::tempdir().unwrap();
        let every_agent = ["claude", "codex", "duo", "opencode", "pi"];

        install_with_mcp(&every_agent, dir.path());
        install_with_mcp(&every_agent, dir.path());
        uninstall_named(&every_agent, dir.path());

        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        assert!(leftovers.is_empty(), "{leftovers:#?}");
    }

    #[test]
    fn opencode_plugin_looks_for_the_graph_where_orbit_writes_it() {
        let dir = tempfile::tempdir().unwrap();
        install_with_mcp(&["opencode"], dir.path());

        let plugin =
            std::fs::read_to_string(dir.path().join(".opencode/plugins/orbit.js")).unwrap();
        assert!(
            plugin.contains(r#"join(homedir(), ".gitlab", "orbit")"#),
            "{plugin}"
        );
    }

    #[test]
    fn opencode_plugin_shows_a_toast_for_orbit_calls() {
        let dir = tempfile::tempdir().unwrap();
        install_with_mcp(&["opencode"], dir.path());

        let plugin =
            std::fs::read_to_string(dir.path().join(".opencode/plugins/orbit-ui.js")).unwrap();
        assert!(plugin.contains(r#""tool.execute.before""#), "{plugin}");
        assert!(plugin.contains("client.tui.showToast"), "{plugin}");
    }

    #[test]
    fn uninstall_keeps_a_template_file_the_user_edited() {
        let dir = tempfile::tempdir().unwrap();
        let plugin = dir.path().join(".opencode/plugins/orbit.js");
        install_with_mcp(&["opencode"], dir.path());

        let edited = format!(
            "{}\n// my tweak\n",
            std::fs::read_to_string(&plugin).unwrap()
        );
        std::fs::write(&plugin, &edited).unwrap();

        uninstall_named(&["opencode"], dir.path());

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

        install_with_mcp(&["claude"], dir.path());

        let settings = read_json(&dir.path().join(".claude/settings.json"));
        assert_eq!(settings["permissions"]["allow"][0], "Bash");
        assert_eq!(settings["hooks"]["PreToolUse"].as_array().unwrap().len(), 1);

        uninstall_named(&["claude"], dir.path());

        assert_eq!(
            read_json(&dir.path().join(".claude/settings.json")),
            json!({"permissions": {"allow": ["Bash"]}})
        );
        assert!(
            dir.path()
                .join(".claude/settings.json.orbit-backup")
                .is_file()
        );
        assert!(!dir.path().join("CLAUDE.md").exists());
        assert!(!dir.path().join(".mcp.json").exists());
    }

    #[test]
    fn the_claude_plugin_installs_by_default_and_skips_when_asked() {
        let dir = tempfile::tempdir().unwrap();
        let mut skipped = options_for(&["claude"]);
        skipped.components = Component::from_flags(&[], &[Component::AgentPlugins]).unwrap();
        install(skipped, project(dir.path()), &bare_machine()).unwrap();
        assert!(!dir.path().join(".claude/orbit").exists());
        assert!(!dir.path().join(".claude/settings.local.json").exists());

        install_default(&["claude", "codex"], dir.path());

        let marketplace = dunce::canonicalize(dir.path())
            .unwrap()
            .join(".claude/orbit/claude_plugin");
        assert!(
            marketplace
                .join(".claude-plugin/marketplace.json")
                .is_file()
        );
        assert!(marketplace.join("gitlab-orbit/hooks/register.ts").is_file());
        assert!(!marketplace.join("gitlab-orbit/tests").exists());
        assert!(!marketplace.join("gitlab-orbit/tsconfig.json").exists());
        let settings = read_json(&dir.path().join(".claude/settings.local.json"));
        let name = read_json(&marketplace.join(".claude-plugin/marketplace.json"))["name"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(name.starts_with("gitlab-orbit-"), "{name}");
        assert_eq!(
            settings["enabledPlugins"][format!("gitlab-orbit@{name}")],
            true
        );
        assert_eq!(
            settings["extraKnownMarketplaces"][&name]["source"],
            json!({"source": "directory", "path": marketplace.display().to_string()})
        );
        assert!(!dir.path().join(".codex/orbit").exists());
    }

    #[test]
    fn install_and_skip_of_one_component_is_an_error() {
        assert!(Component::from_flags(&[Component::Mcp], &[Component::Mcp]).is_err());
        let defaults = Component::from_flags(&[], &[]).unwrap();
        assert!(defaults.contains(&Component::AgentPlugins) && !defaults.contains(&Component::Mcp));
    }

    #[test]
    fn each_project_gets_its_own_claude_plugin_marketplace() {
        let claude = spec::agent_named("claude").unwrap();
        let plugin = claude.plugin.as_ref().unwrap();
        let a = plugin.marketplace_for(&Target::Project("/repo/a".into()));
        let b = plugin.marketplace_for(&Target::Project("/repo/b".into()));
        assert_ne!(a, b);
        assert_eq!(plugin.marketplace_for(&Target::Global), "gitlab-orbit");
    }

    #[test]
    fn the_claude_plugin_is_skipped_on_an_old_or_missing_claude() {
        for version in [Some("2.1.286"), None] {
            components::agent_plugins::TEST_VERSION
                .with(|fake| *fake.borrow_mut() = version.map(str::to_string));
            let dir = tempfile::tempdir().unwrap();
            install_default(&["claude"], dir.path());
            assert!(!dir.path().join(".claude/orbit").exists(), "{version:?}");
            assert!(
                !dir.path().join(".claude/settings.local.json").exists(),
                "{version:?}"
            );
        }
        components::agent_plugins::TEST_VERSION
            .with(|fake| *fake.borrow_mut() = Some("2.1.308".to_string()));
    }

    #[test]
    fn uninstall_removes_the_claude_plugin_and_keeps_foreign_settings() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join(".claude")).unwrap();
        std::fs::write(
            dir.path().join(".claude/settings.local.json"),
            r#"{"enabledPlugins": {"other@team": true}}"#,
        )
        .unwrap();

        install_default(&["claude"], dir.path());
        install_default(&["claude"], dir.path());
        uninstall_named(&["claude"], dir.path());

        assert_eq!(
            read_json(&dir.path().join(".claude/settings.local.json")),
            json!({"enabledPlugins": {"other@team": true}})
        );
        assert!(!dir.path().join(".claude/orbit").exists());
    }

    #[test]
    fn uninstall_keeps_a_claude_plugin_file_the_user_edited() {
        let dir = tempfile::tempdir().unwrap();
        install_default(&["claude"], dir.path());
        let register = dir
            .path()
            .join(".claude/orbit/claude_plugin/gitlab-orbit/hooks/register.ts");
        let edited = format!(
            "{}\n// my tweak\n",
            std::fs::read_to_string(&register).unwrap()
        );
        std::fs::write(&register, &edited).unwrap();

        uninstall_named(&["claude"], dir.path());

        assert_eq!(std::fs::read_to_string(&register).unwrap(), edited);
        assert!(
            !dir.path()
                .join(".claude/orbit/claude_plugin/.claude-plugin/marketplace.json")
                .exists()
        );
        assert!(!dir.path().join(".claude/settings.local.json").exists());
    }

    #[test]
    fn launcher_tokens_resolve_in_installed_artifacts() {
        let dir = tempfile::tempdir().unwrap();
        install_with_mcp(&["claude", "opencode"], dir.path());

        let settings = std::fs::read_to_string(dir.path().join(".claude/settings.json")).unwrap();
        assert!(
            settings.contains("\"orbit hook-guard search\"")
                || settings.contains("\"orbit hook-guard search --gitlab\""),
            "{settings}"
        );
        assert!(!settings.contains("--mode"));
        assert!(!settings.contains("{{orbit}}"));

        let plugin =
            std::fs::read_to_string(dir.path().join(".opencode/plugins/orbit.js")).unwrap();
        assert!(plugin.contains("run orbit grep"));
        assert!(!plugin.contains("{{"));
    }
}
