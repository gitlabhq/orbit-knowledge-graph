//! Declarative agent specs embedded from `config/setup/agents/`. Each YAML file describes
//! one agent as generic operations (detection paths, instruction file, marker-owned JSON
//! merges, templated files, string registrations, MCP entry, skill directories), so adding an
//! agent means adding a YAML file, not Rust. The instruction block, hook nudges, MCP server,
//! and template values live in `config/setup/setup.yaml`.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::LazyLock;

use ontology::migrations::sha256_hex;
use rust_embed::Embed;
use serde::Deserialize;
use serde_json::Value;

#[derive(Embed)]
#[folder = "$CONFIG_DIR/setup"]
struct SetupAssets;

#[derive(Embed)]
#[folder = "$CARGO_MANIFEST_DIR/../../packages/agent-plugins"]
#[include = ".claude-plugin/*"]
#[include = "hooks/*"]
#[include = "core/*"]
#[exclude = "*.test.ts"]
struct AgentPluginAssets;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SetupTexts {
    mcp_server: McpServerText,
    instructions: String,
    #[serde(default)]
    remote: BTreeMap<String, String>,
    nudge_search: String,
    nudge_gitlab: String,
    #[serde(default)]
    template_vars: BTreeMap<String, String>,
}

static TEXTS: LazyLock<SetupTexts> = LazyLock::new(|| {
    let file = SetupAssets::get("setup.yaml").expect("config/setup/setup.yaml must be embedded");
    orbit_utils::yaml::from_slice(&file.data)
        .unwrap_or_else(|e| panic!("config/setup/setup.yaml is invalid: {e}"))
});

pub(crate) const DIRECT_LAUNCHER: &str = "orbit";
pub(crate) const GLAB_LAUNCHER: &str = "glab orbit";

pub(crate) fn launcher() -> &'static str {
    static LAUNCHER: LazyLock<&'static str> = LazyLock::new(|| {
        if crate::telemetry::detect_distribution(|key| std::env::var(key).ok()).is_some() {
            GLAB_LAUNCHER
        } else {
            DIRECT_LAUNCHER
        }
    });
    &LAUNCHER
}

fn substitute_launcher(text: &str, launcher: &str) -> String {
    text.replace("{{orbit}}", launcher)
}

const QUERY_LINE: &str = "remote_query";

fn render_instructions(launcher: &str, remote: bool, query_syntax: Option<&str>) -> String {
    let mut text = TEXTS.instructions.trim_end().to_string();
    for (name, value) in &TEXTS.remote {
        let value = match (remote, query_syntax) {
            (false, _) => "",
            (true, None) if name == QUERY_LINE => "",
            (true, _) => value.as_str(),
        };
        text = text.replace(&format!("{{{{{name}}}}}"), value);
    }
    let text = text.replace("{{query_syntax}}", query_syntax.unwrap_or_default());
    substitute_launcher(&text, launcher)
}

fn query_syntax(served: Option<&str>) -> Option<&str> {
    match served {
        Some(description) if description.contains("MATCH") => Some(description),
        Some(_) => None,
        None => Some(crate::descriptions::gql_syntax()),
    }
}

static REACHES_GITLAB: LazyLock<bool> =
    LazyLock::new(|| crate::remote::client::gitlab_usable(Path::new(".")));

static RENDERED_INSTRUCTIONS: LazyLock<String> = LazyLock::new(|| {
    let remote = reaches_gitlab();
    let served = remote
        .then(|| crate::remote::client::query_graph_description(Path::new(".")))
        .flatten();
    render_instructions(launcher(), remote, query_syntax(served.as_deref()))
});

pub(crate) fn reaches_gitlab() -> bool {
    *REACHES_GITLAB
}

static RENDERED_NUDGE_SEARCH: LazyLock<String> =
    LazyLock::new(|| substitute_launcher(TEXTS.nudge_search.trim_end(), launcher()));

pub(crate) fn instructions_text() -> &'static str {
    &RENDERED_INSTRUCTIONS
}

pub(crate) fn search_nudge_text() -> &'static str {
    &RENDERED_NUDGE_SEARCH
}

static RENDERED_NUDGE_GITLAB: LazyLock<String> =
    LazyLock::new(|| substitute_launcher(TEXTS.nudge_gitlab.trim_end(), launcher()));

pub(crate) fn gitlab_nudge_text() -> &'static str {
    &RENDERED_NUDGE_GITLAB
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct McpServerText {
    name: String,
    command: String,
}

pub(super) struct McpServer {
    pub(super) name: &'static str,
    pub(super) command: String,
    pub(super) args: Vec<String>,
}

pub(super) fn mcp_server() -> McpServer {
    let rendered = substitute_launcher(&TEXTS.mcp_server.command, launcher());
    let mut words = rendered.split_whitespace().map(str::to_string);
    McpServer {
        name: &TEXTS.mcp_server.name,
        command: words.next().expect("mcp_server.command is non-empty"),
        args: words.collect(),
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AgentSpec {
    pub(super) name: String,
    pub(super) title: String,
    pub(super) detect: Vec<String>,
    pub(super) instruction_file: ScopedPath,
    #[serde(default)]
    pub(super) json_merges: Vec<JsonMerge>,
    #[serde(default)]
    pub(super) template_files: Vec<TemplateFile>,
    #[serde(default)]
    pub(super) registrations: Vec<Registration>,
    pub(super) mcp: Option<McpEntry>,
    pub(super) skills: Option<SkillDirs>,
    pub(super) plugin: Option<PluginSpec>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PluginSpec {
    pub(super) dir: ScopedPath,
    pub(super) settings: ScopedPath,
    pub(super) marketplace: String,
    pub(super) name: String,
    pub(super) min_version: String,
    pub(super) version_command: Vec<String>,
}

impl PluginSpec {
    pub(super) fn id(&self) -> String {
        format!("{}@{}", self.name, self.marketplace)
    }

    pub(super) fn files(&self) -> Vec<(String, Vec<u8>)> {
        let marketplace = serde_json::json!({
            "name": self.marketplace,
            "owner": { "name": "GitLab" },
            "plugins": [{ "name": self.name, "source": format!("./{}", self.name) }],
        });
        let plugin = AgentPluginAssets::iter().map(|path| {
            let file = AgentPluginAssets::get(&path).expect("embedded file must be readable");
            (format!("{}/{path}", self.name), file.data.into_owned())
        });
        std::iter::once((
            ".claude-plugin/marketplace.json".to_string(),
            serde_json::to_vec_pretty(&marketplace).unwrap(),
        ))
        .chain(plugin)
        .collect()
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct McpEntry {
    pub(super) file: ScopedPath,
    pub(super) format: McpFormat,
    pub(super) coding_agent: String,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(super) enum McpFormat {
    Claude,
    Codex,
    Opencode,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SkillDirs {
    pub(super) dir: ScopedPath,
    pub(super) link: Option<ScopedPath>,
    pub(super) permissions: Option<SkillPermissions>,
    /// The assistant cannot run commands when it loads a skill, so it needs the real tree.
    #[serde(default)]
    pub(super) requires_full: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SkillPermissions {
    pub(super) file: ScopedPath,
    pub(super) rules: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ScopedPath {
    pub(super) project: String,
    pub(super) global: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct JsonMerge {
    pub(super) file: ScopedPath,
    pub(super) path: Vec<String>,
    pub(super) marker: String,
    pub(super) entries: Vec<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TemplateFile {
    pub(super) path: ScopedPath,
    pub(super) template: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Registration {
    pub(super) file: ScopedPath,
    pub(super) path: Vec<String>,
    pub(super) value: ScopedPath,
}

static AGENT_SPECS: LazyLock<Vec<AgentSpec>> = LazyLock::new(|| {
    let mut specs: Vec<AgentSpec> = SetupAssets::iter()
        .filter_map(|path| {
            let stem = path.strip_prefix("agents/")?.strip_suffix(".yaml")?;
            let file = SetupAssets::get(&path).expect("embedded file must be readable");
            let spec: AgentSpec = orbit_utils::yaml::from_slice(&file.data)
                .unwrap_or_else(|e| panic!("config/setup/{path} is invalid: {e}"));
            assert_eq!(
                spec.name, stem,
                "config/setup/{path}: name must match the file stem"
            );
            Some(spec)
        })
        .collect();
    specs.sort_by(|a, b| a.name.cmp(&b.name));
    assert!(!specs.is_empty(), "no agent specs embedded");
    specs
});

#[derive(Clone, Copy)]
pub(super) struct Agent(&'static AgentSpec);

impl std::ops::Deref for Agent {
    type Target = AgentSpec;

    fn deref(&self) -> &AgentSpec {
        self.0
    }
}

pub(super) fn agents() -> impl Iterator<Item = Agent> {
    AGENT_SPECS.iter().map(Agent)
}

pub(super) fn agent_named(name: &str) -> Option<Agent> {
    agents().find(|agent| agent.name == name)
}

pub(crate) fn agent_names() -> Vec<&'static str> {
    AGENT_SPECS.iter().map(|spec| spec.name.as_str()).collect()
}

const TEMPLATE_CHECKSUM_PREFIX: &str = "// orbit setup checksum: ";

impl TemplateFile {
    pub(super) fn render(&self) -> String {
        let mut body = read_embedded_text(&self.template);
        for (name, value) in &TEXTS.template_vars {
            body = body.replace(&format!("{{{{{name}}}}}"), value);
        }
        let body = substitute_launcher(&body, launcher());
        format!("{TEMPLATE_CHECKSUM_PREFIX}{}\n{body}", sha256_hex(&body))
    }

    pub(super) fn is_unmodified(contents: &str) -> bool {
        contents.split_once('\n').is_some_and(|(stamp, body)| {
            stamp.strip_prefix(TEMPLATE_CHECKSUM_PREFIX) == Some(sha256_hex(body).as_str())
        })
    }
}

fn read_embedded_text(name: &str) -> String {
    let file =
        SetupAssets::get(name).unwrap_or_else(|| panic!("config/setup/{name} is not embedded"));
    String::from_utf8(file.data.into_owned())
        .unwrap_or_else(|e| panic!("config/setup/{name} is not UTF-8: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::setup::components::json;

    #[test]
    fn all_specs_parse_and_expected_agents_exist() {
        for name in ["duo", "claude", "codex", "opencode", "pi"] {
            assert!(agent_named(name).is_some(), "missing spec for {name}");
        }
        assert_eq!(agent_names().len(), agents().count());
    }

    #[test]
    fn merge_entries_contain_their_marker() {
        for spec in agents() {
            for merge in &spec.json_merges {
                assert!(!merge.marker.is_empty(), "{}: empty marker", spec.name);
                for entry in &merge.entries {
                    assert!(
                        json::contains_marker(entry, &merge.marker),
                        "{}: entry {entry} does not contain marker {:?}",
                        spec.name,
                        merge.marker
                    );
                }
            }
        }
    }

    #[test]
    fn launcher_substitution_renders_both_distributions() {
        for (launcher, expected) in [
            (DIRECT_LAUNCHER, "`orbit grep"),
            (GLAB_LAUNCHER, "`glab orbit grep"),
        ] {
            for remote in [true, false] {
                let rendered = render_instructions(launcher, remote, query_syntax(None));
                assert!(rendered.contains(expected), "{launcher}: {rendered}");
                assert!(!rendered.contains("{{"), "{launcher}: {rendered}");
                for phrase in [
                    " grep` must be used for search",
                    "Ignores case, `_`, `-`",
                    "`a|b` OR",
                    "Batch all reads and searches you can name",
                ] {
                    assert!(rendered.contains(phrase), "{launcher}: {phrase}");
                }
                assert_eq!(
                    rendered.contains("GitLab"),
                    remote,
                    "{launcher}: {rendered}"
                );
                assert!(
                    rendered.contains("instead of glab and glab api") == remote,
                    "{launcher}"
                );
                assert_eq!(rendered.contains("CALL db.schema()"), remote, "{launcher}");
                let budget = if remote { 250 } else { 75 };
                assert!(rendered.split_whitespace().count() <= budget, "{launcher}");
            }
        }
        let local = render_instructions(DIRECT_LAUNCHER, false, query_syntax(None));
        assert_eq!(
            local.lines().filter(|line| line.starts_with("- ")).count(),
            2,
            "{local}"
        );
        assert!(search_nudge_text().contains("MANDATORY"));
        let glab = agent_named("claude").unwrap().json_merges[0].entries[0].to_string();
        assert!(glab.contains("{{orbit}} hook-guard"), "{glab}");
    }

    #[test]
    fn query_line_follows_the_served_query_language() {
        let served_gql = "Read-only GQL. MATCH (n) RETURN n.";
        assert_eq!(query_syntax(Some(served_gql)), Some(served_gql));
        let gql = render_instructions(DIRECT_LAUNCHER, true, query_syntax(Some(served_gql)));
        assert!(gql.contains("`orbit query` must be used"), "{gql}");
        assert!(gql.contains(served_gql), "{gql}");

        let json = render_instructions(
            DIRECT_LAUNCHER,
            true,
            query_syntax(Some("Send a JSON query object.")),
        );
        assert!(!json.contains("orbit query"), "{json}");
        assert!(json.contains("instead of glab and glab api"), "{json}");
        assert!(!json.contains("{{"), "{json}");
    }

    const UNLABELED: &[&str] = &[
        "hook-guard",
        "send-event",
        "refresh",
        "mcp serve",
        "workspace mcp serve",
    ];

    #[derive(serde::Deserialize)]
    struct CallLabels {
        commands: BTreeMap<String, Value>,
        tools: BTreeMap<String, Value>,
    }

    fn call_labels() -> CallLabels {
        let source = include_str!("../../../../../packages/agent-plugins/core/labels.ts");
        serde_json::from_str(source.trim_start_matches("export default ")).unwrap()
    }

    struct CliCommand {
        path: String,
        aliases: Vec<String>,
        ancestors: Vec<String>,
        has_subcommands: bool,
    }

    fn cli_commands(command: &clap::Command, ancestors: &[String], out: &mut Vec<CliCommand>) {
        let prefix = ancestors
            .last()
            .map(|p| format!("{p} "))
            .unwrap_or_default();
        for sub in command.get_subcommands() {
            let path = format!("{prefix}{}", sub.get_name());
            let mut lineage = ancestors.to_vec();
            lineage.push(path.clone());
            cli_commands(sub, &lineage, out);
            out.push(CliCommand {
                aliases: sub
                    .get_all_aliases()
                    .map(|a| format!("{prefix}{a}"))
                    .collect(),
                ancestors: ancestors.to_vec(),
                has_subcommands: sub.has_subcommands(),
                path,
            });
        }
    }

    #[test]
    fn call_labels_name_real_commands_and_cover_every_command() {
        use clap::CommandFactory;
        let labels = call_labels().commands;
        let mut commands = Vec::new();
        cli_commands(&crate::Cli::command(), &[], &mut commands);
        let known: Vec<&str> = commands
            .iter()
            .flat_map(|c| std::iter::once(&c.path).chain(&c.aliases))
            .map(String::as_str)
            .collect();

        for key in labels.keys() {
            assert!(
                known.contains(&key.as_str()),
                "labels.ts: `{key}` is not an orbit command"
            );
        }
        for command in &commands {
            if labels.contains_key(&command.path) {
                for alias in &command.aliases {
                    assert!(
                        labels.contains_key(alias),
                        "labels.ts: label `{alias}` like `{}`",
                        command.path
                    );
                }
            }
            let labeled = labels.contains_key(&command.path)
                || command.ancestors.iter().any(|a| labels.contains_key(a));
            assert!(
                labeled || command.has_subcommands || UNLABELED.contains(&command.path.as_str()),
                "labels.ts: label `{}`, or add it to UNLABELED",
                command.path
            );
        }
    }

    #[test]
    fn call_labels_name_real_orbit_tools() {
        let prompts = orbit_prompts::Prompts::load_embedded("remote").unwrap();
        let tools: Vec<&str> = prompts.iter().map(|(_, prompt)| prompt.name()).collect();
        for tool in call_labels().tools.keys() {
            assert!(
                tools.contains(&tool.as_str()),
                "labels.ts: `{tool}` is not an Orbit tool"
            );
        }
    }

    #[test]
    fn the_claude_plugin_embeds_its_marketplace_and_plugin() {
        let claude = agent_named("claude").unwrap();
        let plugin = claude.plugin.as_ref().unwrap();
        let mut files: Vec<String> = plugin.files().into_iter().map(|(path, _)| path).collect();
        files.sort();
        let shipped = [
            "core/calls.ts",
            "core/command.ts",
            "core/cypher.ts",
            "core/labels.ts",
            "core/stats.ts",
            "hooks/hooks.json",
            "hooks/orbit-mark.ts",
            "hooks/register.ts",
            "hooks/rows.ts",
            "hooks/theme.ts",
        ];
        let mut expected = vec![
            ".claude-plugin/marketplace.json".to_string(),
            "gitlab-orbit/.claude-plugin/plugin.json".to_string(),
        ];
        expected.extend(shipped.iter().map(|path| format!("gitlab-orbit/{path}")));
        assert_eq!(files, expected);
        let marketplace: Value = serde_json::from_slice(
            &plugin
                .files()
                .into_iter()
                .find(|(path, _)| path == ".claude-plugin/marketplace.json")
                .unwrap()
                .1,
        )
        .unwrap();
        assert_eq!(marketplace["name"], plugin.marketplace.as_str());
        assert_eq!(marketplace["plugins"][0]["name"], plugin.name.as_str());
        assert!(
            marketplace["plugins"][0]["source"]
                .as_str()
                .is_some_and(|source| source.starts_with("./")),
            "the plugin needs a relative source so it loads without an install step"
        );
    }

    #[test]
    fn opencode_plugins_are_shell_safe() {
        let template = read_embedded_text("opencode_plugin.js");
        assert_eq!(
            template.matches("{{").count(),
            1,
            "the reminder is the only placeholder"
        );
        assert!(template.contains(r#""{{reminder}}""#));

        for (name, text) in &TEXTS.template_vars {
            assert!(
                !text.contains(['"', '`']) && !text.contains("$("),
                "{name} is not shell-safe"
            );
        }
    }

    #[test]
    fn instruction_files_are_known_names_and_globals_are_home_anchored() {
        for spec in agents() {
            assert!(
                ["AGENTS.md", "CLAUDE.md"].contains(&spec.instruction_file.project.as_str()),
                "{}: unexpected instruction file {}",
                spec.name,
                spec.instruction_file.project
            );
            for global in std::iter::once(&spec.instruction_file.global)
                .chain(spec.json_merges.iter().map(|m| &m.file.global))
                .chain(spec.template_files.iter().map(|t| &t.path.global))
                .chain(
                    spec.registrations
                        .iter()
                        .flat_map(|r| [&r.file.global, &r.value.global]),
                )
                .chain(spec.mcp.iter().map(|entry| &entry.file.global))
                .chain(
                    spec.plugin
                        .iter()
                        .flat_map(|plugin| [&plugin.dir.global, &plugin.settings.global]),
                )
                .chain(spec.skills.iter().flat_map(|dirs| {
                    std::iter::once(&dirs.dir.global).chain(dirs.link.iter().map(|l| &l.global))
                }))
            {
                assert!(global.starts_with("~/"), "{}: {global}", spec.name);
            }
        }
    }
}
