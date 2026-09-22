//! Declarative agent specs embedded from `config/setup/agents/`. Each YAML file describes
//! one agent as generic operations (detection paths, instruction file, marker-owned JSON
//! merges, templated files, string registrations, MCP entry, skill directories), so adding an
//! agent means adding a YAML file, not Rust. The instruction block, hook nudges, MCP server,
//! and template values live in `config/setup/setup.yaml`.

use std::collections::BTreeMap;
use std::sync::LazyLock;

use rust_embed::Embed;
use serde::Deserialize;
use serde_json::Value;

#[derive(Embed)]
#[folder = "$CONFIG_DIR/setup"]
struct SetupAssets;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SetupTexts {
    mcp_server: McpServerText,
    instructions: String,
    nudge_search: String,
    nudge_read: String,
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
        if std::env::var("GITLAB_ORBIT_DISTRIBUTION").as_deref() == Ok("glab") {
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

fn render_instructions(launcher: &str) -> String {
    substitute_launcher(
        &TEXTS
            .instructions
            .trim_end()
            .replace("{{graph_contents}}", &describe_graph_contents()),
        launcher,
    )
}

static RENDERED_INSTRUCTIONS: LazyLock<String> = LazyLock::new(|| render_instructions(launcher()));

static RENDERED_NUDGE_SEARCH: LazyLock<String> =
    LazyLock::new(|| substitute_launcher(TEXTS.nudge_search.trim_end(), launcher()));

static RENDERED_NUDGE_READ: LazyLock<String> =
    LazyLock::new(|| substitute_launcher(TEXTS.nudge_read.trim_end(), launcher()));

fn describe_graph_contents() -> String {
    use strum::IntoEnumIterator;

    use code_graph::v2::types::{EdgeKind, NodeKind};

    let ontology = ontology::Ontology::load_embedded().expect("embedded ontology must load");
    let nodes = NodeKind::iter()
        .map(|kind| {
            let node = ontology
                .get_node(kind.as_ref())
                .unwrap_or_else(|| panic!("ontology must declare node {}", kind.as_ref()));
            if matches!(kind, NodeKind::Definition) {
                let def_types = node
                    .fields
                    .iter()
                    .find(|field| field.name == "definition_type")
                    .and_then(|field| field.description.as_deref())
                    .expect("ontology Definition must describe definition_type")
                    .trim_end_matches('.');
                format!(
                    "`{}` (`definition_type`: {def_types}; not an exhaustive list)",
                    node.destination_table
                )
            } else {
                format!("`{}`", node.destination_table)
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let edges = EdgeKind::iter()
        .map(|kind| format!("`{}`", kind.as_ref()))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "{nodes}; typed edges in `{}` (`relationship_kind`: {edges})",
        ontology.edge_table()
    )
}

pub(crate) fn instructions_text() -> &'static str {
    &RENDERED_INSTRUCTIONS
}

pub(crate) fn search_nudge_text() -> &'static str {
    &RENDERED_NUDGE_SEARCH
}

pub(crate) fn read_nudge_text() -> &'static str {
    &RENDERED_NUDGE_READ
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
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct McpEntry {
    pub(super) file: ScopedPath,
    pub(super) format: McpFormat,
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

impl TemplateFile {
    pub(super) fn render(&self) -> String {
        self.render_with_launcher(launcher())
    }

    fn render_with_launcher(&self, launcher: &str) -> String {
        let mut rendered = read_embedded_text(&self.template);
        for (name, value) in &TEXTS.template_vars {
            rendered = rendered.replace(&format!("{{{{{name}}}}}"), value);
        }
        substitute_launcher(&rendered, launcher)
    }

    pub(super) fn is_unmodified(&self, contents: &str) -> bool {
        [DIRECT_LAUNCHER, GLAB_LAUNCHER]
            .iter()
            .any(|launcher| self.render_with_launcher(launcher) == contents)
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
            let rendered = render_instructions(launcher);
            assert!(rendered.contains(expected), "{launcher}: {rendered}");
            assert!(!rendered.contains("{{orbit}}"), "{launcher}");
        }
        let glab = agent_named("claude").unwrap().json_merges[0].entries[0].to_string();
        assert!(glab.contains("{{orbit}} hook-guard"), "{glab}");
    }

    #[test]
    fn opencode_plugins_are_shell_safe() {
        let contents = agent_named("opencode").unwrap().template_files[0].render();
        assert!(!contents.contains('`'));
        assert!(!contents.contains("$("));

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
                .chain(spec.skills.iter().flat_map(|dirs| {
                    std::iter::once(&dirs.dir.global).chain(dirs.link.iter().map(|l| &l.global))
                }))
            {
                assert!(global.starts_with("~/"), "{}: {global}", spec.name);
            }
        }
    }
}
