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
#[include = ".claude-plugin/plugin.json"]
#[include = "hooks/*"]
#[include = "core/*"]
#[exclude = "*.test.ts"]
#[exclude = "*.d.ts"]
struct AgentPluginAssets;

#[derive(Embed)]
#[folder = "$CARGO_MANIFEST_DIR/../../packages/agent-plugins-nightly"]
#[exclude = "tests/*"]
#[exclude = "node_modules/*"]
#[exclude = "package.json"]
#[exclude = "README.md"]
struct NightlyAssets;

/// A feature in `packages/agent-plugins-nightly/features.yaml`, on or off for each channel.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Feature {
    nightly: bool,
    stable: bool,
    claude: Option<ClaudeFeature>,
    pi: Option<FeatureFiles>,
}

/// Files a feature installs into an assistant's `feature_dir`, as destination name to source file.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FeatureFiles {
    files: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClaudeFeature {
    /// Hooks merged into the plugin's `hooks/hooks.json`.
    hooks: String,
    /// Files copied into the plugin under `features/`.
    files: Vec<String>,
}

pub(super) type Features = BTreeMap<String, Feature>;

fn parse_features(yaml: &[u8]) -> Features {
    orbit_utils::yaml::from_slice(yaml)
        .expect("packages/agent-plugins-nightly/features.yaml is invalid")
}

static FEATURES: LazyLock<Features> = LazyLock::new(|| {
    let file = NightlyAssets::get("features.yaml").expect("features.yaml is embedded");
    parse_features(&file.data)
});

fn feature_on(features: &Features, name: &str, nightly: bool) -> bool {
    let feature = features
        .get(name)
        .unwrap_or_else(|| panic!("feature {name} is not in features.yaml"));
    if nightly {
        feature.nightly
    } else {
        feature.stable
    }
}

/// `ORBIT_NIGHTLY=1 orbit setup` installs the nightly plugins instead of the stable ones.
pub(super) fn nightly_enabled() -> bool {
    std::env::var("ORBIT_NIGHTLY").is_ok_and(|value| {
        !matches!(
            value.trim().to_lowercase().as_str(),
            "" | "0" | "false" | "off" | "no"
        )
    })
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SetupTexts {
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

fn render_instructions(launcher: &str, remote: bool) -> String {
    let mut text = TEXTS.instructions.trim_end().to_string();
    for (name, value) in &TEXTS.remote {
        let value = if remote { value.as_str() } else { "" };
        text = text.replace(&format!("{{{{{name}}}}}"), value);
    }
    let text = text.replace("{{query_syntax}}", crate::descriptions::gql_syntax());
    substitute_launcher(&text, launcher)
}

static REACHES_GITLAB: LazyLock<bool> =
    LazyLock::new(|| crate::remote::client::gitlab_usable(Path::new(".")));

static RENDERED_INSTRUCTIONS: LazyLock<String> =
    LazyLock::new(|| render_instructions(launcher(), reaches_gitlab()));

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
    #[serde(default)]
    pub(super) retired: Retired,
    pub(super) skills: Option<SkillDirs>,
    pub(super) feature_dir: Option<ScopedPath>,
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
    /// Whether the features that are on for the channel add their files and hooks to this plugin.
    #[serde(default)]
    pub(super) features: bool,
}

impl PluginSpec {
    pub(super) fn marketplace_for(&self, target: &super::Target) -> String {
        match target {
            super::Target::Global(_) => self.marketplace.clone(),
            super::Target::Project(root) => {
                format!(
                    "{}-{}",
                    self.marketplace,
                    &sha256_hex(&root.display().to_string())[..8]
                )
            }
        }
    }

    pub(super) fn id(&self, marketplace: &str) -> String {
        format!("{}@{marketplace}", self.name)
    }

    pub(super) fn files(&self, marketplace: &str, nightly: bool) -> Vec<(String, Vec<u8>)> {
        self.files_with(&FEATURES, marketplace, nightly)
    }

    fn files_with(
        &self,
        features: &Features,
        marketplace: &str,
        nightly: bool,
    ) -> Vec<(String, Vec<u8>)> {
        let marketplace = serde_json::json!({
            "name": marketplace,
            "owner": { "name": "GitLab" },
            "plugins": [{ "name": self.name, "source": format!("./{}", self.name) }],
        });
        let plugin = AgentPluginAssets::iter().map(|path| {
            let file = AgentPluginAssets::get(&path).expect("embedded file must be readable");
            let mut data = file.data.into_owned();
            if path == ".claude-plugin/plugin.json" {
                let mut manifest: Value =
                    serde_json::from_slice(&data).expect("plugin.json is JSON");
                manifest["version"] = Value::String(env!("ORBIT_VERSION").to_string());
                data = serde_json::to_vec_pretty(&manifest).unwrap();
            }
            (format!("{}/{path}", self.name), data)
        });
        let mut files: Vec<_> = [(".gitignore".to_string(), b"*\n".to_vec())]
            .into_iter()
            .chain(std::iter::once((
                ".claude-plugin/marketplace.json".to_string(),
                serde_json::to_vec_pretty(&marketplace).unwrap(),
            )))
            .chain(plugin)
            .collect();
        if self.features {
            self.add_features(&mut files, features, nightly);
        }
        files
    }

    /// Merges the hooks of the features that are on for the channel into the stable plugin's
    /// hooks.json and ships their files.
    fn add_features(&self, files: &mut Vec<(String, Vec<u8>)>, features: &Features, nightly: bool) {
        let hooks_path = format!("{}/hooks/hooks.json", self.name);
        let mut merged = serde_json::Map::new();
        for (name, claude) in features
            .iter()
            .filter(|(name, _)| feature_on(features, name, nightly))
            .filter_map(|(name, feature)| Some((name, feature.claude.as_ref()?)))
        {
            let hooks = NightlyAssets::get(&claude.hooks)
                .unwrap_or_else(|| panic!("{} is not embedded", claude.hooks));
            let hooks: serde_json::Map<String, Value> = serde_json::from_slice(&hooks.data)
                .unwrap_or_else(|e| panic!("{} is invalid: {e}", claude.hooks));
            for (event, entries) in hooks {
                let Value::Array(entries) = entries else {
                    panic!("{name}: hooks for {event} must be an array");
                };
                let slot = merged
                    .entry(event)
                    .or_insert_with(|| Value::Array(Vec::new()));
                slot.as_array_mut()
                    .expect("merged hooks are arrays")
                    .extend(entries);
            }
            for path in &claude.files {
                let file =
                    NightlyAssets::get(path).unwrap_or_else(|| panic!("{path} is not embedded"));
                files.push((
                    format!("{}/features/{path}", self.name),
                    file.data.into_owned(),
                ));
            }
        }
        if merged.is_empty() {
            return;
        }
        let (_, stable) = files
            .iter_mut()
            .find(|(path, _)| *path == hooks_path)
            .expect("the stable plugin ships hooks/hooks.json");
        let mut manifest: Value = serde_json::from_slice(stable).expect("hooks.json is JSON");
        manifest["hooks"] = Value::Object(merged);
        *stable = serde_json::to_vec_pretty(&manifest).unwrap();
    }
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
    #[serde(default)]
    pub(super) ui: bool,
    /// Installed only while this feature is on for the channel; setup for the other channel removes it.
    pub(super) feature: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Registration {
    pub(super) file: ScopedPath,
    pub(super) path: Vec<String>,
    pub(super) value: ScopedPath,
    #[serde(default)]
    pub(super) ui: bool,
}

/// What an earlier release installed and this one no longer does. Install and uninstall remove
/// these: a file only while its checksum shows it is unedited, and a registration by its value.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Retired {
    #[serde(default)]
    pub(super) template_files: Vec<RetiredFile>,
    #[serde(default)]
    pub(super) registrations: Vec<Registration>,
    #[serde(default)]
    pub(super) mcp: Option<McpEntry>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RetiredFile {
    pub(super) path: ScopedPath,
    #[serde(default)]
    pub(super) ui: bool,
}

/// The template files of the features that have files for this assistant, whichever channel they are on.
fn feature_template_files(spec: &AgentSpec, features: &Features) -> Vec<TemplateFile> {
    let Some(dir) = &spec.feature_dir else {
        return Vec::new();
    };
    let mut files = Vec::new();
    for (name, feature) in features {
        let section = match spec.name.as_str() {
            "pi" => feature.pi.as_ref(),
            _ => None,
        };
        for (destination, source) in section.iter().flat_map(|section| &section.files) {
            files.push(TemplateFile {
                path: ScopedPath {
                    project: format!("{}/{destination}", dir.project),
                    global: format!("{}/{destination}", dir.global),
                },
                template: source.clone(),
                ui: true,
                feature: Some(name.clone()),
            });
        }
    }
    files
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
        .map(|mut spec| {
            spec.template_files
                .extend(feature_template_files(&spec, &FEATURES));
            spec
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

impl Agent {
    pub(super) fn ui_files(self, ui: bool) -> impl Iterator<Item = &'static TemplateFile> {
        self.ui_files_in(ui, nightly_enabled())
    }

    pub(super) fn ui_files_in(
        self,
        ui: bool,
        nightly: bool,
    ) -> impl Iterator<Item = &'static TemplateFile> {
        self.ui_files_with(&FEATURES, ui, nightly)
    }

    fn ui_files_with(
        self,
        features: &'static Features,
        ui: bool,
        nightly: bool,
    ) -> impl Iterator<Item = &'static TemplateFile> {
        self.0.template_files.iter().filter(move |file| {
            file.ui == ui
                && file
                    .feature
                    .as_deref()
                    .is_none_or(|name| feature_on(features, name, nightly))
        })
    }

    pub(super) fn ui_registrations(self, ui: bool) -> impl Iterator<Item = &'static Registration> {
        self.0
            .registrations
            .iter()
            .filter(move |entry| entry.ui == ui)
    }

    pub(super) fn retired_files(self, ui: bool) -> impl Iterator<Item = &'static ScopedPath> {
        self.0
            .retired
            .template_files
            .iter()
            .filter(move |file| file.ui == ui)
            .map(|file| &file.path)
    }

    /// Feature files that setup for the current channel removes again.
    pub(super) fn switched_off_files(self, ui: bool) -> impl Iterator<Item = &'static ScopedPath> {
        self.switched_off_files_in(ui, nightly_enabled())
    }

    pub(super) fn switched_off_files_in(
        self,
        ui: bool,
        nightly: bool,
    ) -> impl Iterator<Item = &'static ScopedPath> {
        self.switched_off_files_with(&FEATURES, ui, nightly)
    }

    fn switched_off_files_with(
        self,
        features: &'static Features,
        ui: bool,
        nightly: bool,
    ) -> impl Iterator<Item = &'static ScopedPath> {
        self.feature_files(ui)
            .filter(move |file| {
                file.feature
                    .as_deref()
                    .is_some_and(|name| !feature_on(features, name, nightly))
            })
            .map(|file| &file.path)
    }

    /// Every feature file, whatever the channel.
    pub(super) fn feature_files(self, ui: bool) -> impl Iterator<Item = &'static TemplateFile> {
        self.0
            .template_files
            .iter()
            .filter(move |file| file.ui == ui && file.feature.is_some())
    }

    pub(super) fn retired_registrations(
        self,
        ui: bool,
    ) -> impl Iterator<Item = &'static Registration> {
        self.0
            .retired
            .registrations
            .iter()
            .filter(move |entry| entry.ui == ui)
    }

    pub(super) fn has_ui(self) -> bool {
        self.plugin.is_some() || self.ui_files(true).next().is_some()
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
        let mut body = read_embedded_text(&self.template, self.feature.is_some());
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

fn read_embedded_text(name: &str, nightly: bool) -> String {
    let file = if nightly {
        NightlyAssets::get(name)
    } else {
        SetupAssets::get(name)
    }
    .unwrap_or_else(|| panic!("{name} is not embedded"));
    String::from_utf8(file.data.into_owned()).unwrap_or_else(|e| panic!("{name} is not UTF-8: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::setup::components::json;
    use crate::commands::setup::detect::Machine;

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
                let rendered = render_instructions(launcher, remote);
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
        let local = render_instructions(DIRECT_LAUNCHER, false);
        assert_eq!(
            local.lines().filter(|line| line.starts_with("- ")).count(),
            2,
            "{local}"
        );
        assert!(search_nudge_text().contains("MANDATORY"));
        let glab = agent_named("claude").unwrap().json_merges[0].entries[0].to_string();
        assert!(glab.contains("{{orbit}} hook-guard"), "{glab}");
    }

    const UNLABELED: &[&str] = &["hook-guard", "send-event", "refresh"];

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
        let mut files: Vec<String> = plugin
            .files(&plugin.marketplace, false)
            .into_iter()
            .map(|(path, _)| path)
            .collect();
        files.sort();
        let shipped = [
            "core/calls.ts",
            "core/command.ts",
            "core/cypher.ts",
            "core/labels.ts",
            "core/lib.js",
            "core/quiet-filters.ts",
            "core/shell-lexer.ts",
            "core/stats.ts",
            "hooks/highlight.ts",
            "hooks/hooks.json",
            "hooks/orbit-mark.ts",
            "hooks/register.ts",
            "hooks/rows.ts",
            "hooks/theme.ts",
        ];
        let mut expected = vec![
            ".claude-plugin/marketplace.json".to_string(),
            ".gitignore".to_string(),
            "gitlab-orbit/.claude-plugin/plugin.json".to_string(),
        ];
        expected.extend(shipped.iter().map(|path| format!("gitlab-orbit/{path}")));
        assert_eq!(files, expected);
        let manifest = plugin
            .files(&plugin.marketplace, false)
            .into_iter()
            .find(|(path, _)| path.ends_with("plugin.json"))
            .unwrap()
            .1;
        let manifest: Value = serde_json::from_slice(&manifest).unwrap();
        assert_eq!(manifest["version"], env!("ORBIT_VERSION"));
        let marketplace: Value = serde_json::from_slice(
            &plugin
                .files(&plugin.marketplace, false)
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

    fn registry(stable: bool) -> &'static Features {
        let yaml = String::from_utf8(
            NightlyAssets::get("features.yaml")
                .unwrap()
                .data
                .into_owned(),
        )
        .unwrap()
        .replace("stable: false", &format!("stable: {stable}"));
        Box::leak(Box::new(parse_features(yaml.as_bytes())))
    }

    fn claude_paths(features: &Features, nightly: bool) -> Vec<String> {
        let claude = agent_named("claude").unwrap();
        let plugin = claude.plugin.as_ref().unwrap();
        plugin
            .files_with(features, &plugin.marketplace, nightly)
            .into_iter()
            .map(|(path, _)| path)
            .filter(|path| path.contains("/features/"))
            .collect()
    }

    #[test]
    fn the_registry_decides_which_channel_gets_the_claude_hooks() {
        let guard = [
            "gitlab-orbit/features/output-guard/claude.mjs",
            "gitlab-orbit/features/output-guard/core.mjs",
        ];
        assert!(claude_paths(registry(false), false).is_empty());
        assert_eq!(claude_paths(registry(false), true), guard);
        assert_eq!(claude_paths(registry(true), false), guard);

        let claude = agent_named("claude").unwrap();
        let plugin = claude.plugin.as_ref().unwrap();
        let hooks = |features: &Features, nightly: bool| -> Value {
            let files = plugin.files_with(features, &plugin.marketplace, nightly);
            let (_, data) = files
                .iter()
                .find(|(path, _)| path == "gitlab-orbit/hooks/hooks.json")
                .unwrap();
            serde_json::from_slice(data).unwrap()
        };
        let stable = hooks(registry(false), false);
        let nightly = hooks(registry(false), true);
        assert!(stable.get("hooks").is_none());
        assert_eq!(nightly["modules"], stable["modules"]);
        let command = nightly["hooks"]["PostToolUse"][0]["hooks"][0]["command"]
            .as_str()
            .unwrap();
        assert!(command.contains("${CLAUDE_PLUGIN_ROOT}/features/output-guard/claude.mjs"));
        assert_eq!(hooks(registry(true), false), nightly);
    }

    #[test]
    fn the_registry_decides_which_channel_gets_the_pi_extension() {
        let pi = agent_named("pi").unwrap();
        let paths = |features: &'static Features, nightly: bool| -> Vec<String> {
            pi.ui_files_with(features, true, nightly)
                .map(|file| file.path.project.clone())
                .collect()
        };
        let extension = [
            ".pi/extensions/orbit-features/core.mjs",
            ".pi/extensions/orbit-features/index.js",
        ];
        assert!(paths(registry(false), false).is_empty());
        assert_eq!(paths(registry(false), true), extension);
        assert_eq!(paths(registry(true), false), extension);
        assert_eq!(
            pi.switched_off_files_with(registry(false), true, false)
                .count(),
            2
        );
        assert_eq!(
            pi.switched_off_files_with(registry(false), true, true)
                .count(),
            0
        );
        assert_eq!(
            pi.switched_off_files_with(registry(true), true, false)
                .count(),
            0
        );
        for file in pi.ui_files_with(registry(false), true, true) {
            let rendered = file.render();
            assert!(rendered.starts_with(TEMPLATE_CHECKSUM_PREFIX));
            assert!(TemplateFile::is_unmodified(&rendered));
        }
    }

    #[test]
    fn the_shipped_registry_matches_the_plan() {
        let guard = &FEATURES["output-guard"];
        assert!(guard.nightly && !guard.stable);
        for spec in agents() {
            for file in &spec.template_files {
                if let Some(name) = &file.feature {
                    assert!(
                        FEATURES.contains_key(name),
                        "{name} is not in features.yaml"
                    );
                }
            }
        }
    }

    #[test]
    fn opencode_plugins_are_shell_safe() {
        let template = read_embedded_text("opencode_plugin.js", false);
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
    fn opencode_plugins_use_the_opencode_2_shape() {
        let opencode = agent_named("opencode").unwrap();
        for file in &opencode.template_files {
            let rendered = file.render();
            assert!(!rendered.contains("{{"), "{}: unfilled", file.template);
            assert!(TemplateFile::is_unmodified(&rendered), "{}", file.template);
        }
        for template in [
            "opencode_plugin.js",
            "agents/opencode_ui/index.js",
            "agents/opencode_ui/tui.tsx",
        ] {
            let text = read_embedded_text(template, false);
            assert!(
                text.contains("export default") || text.contains(" as default"),
                "{template}"
            );
        }
        assert!(opencode.registrations.is_empty());
        let ui_dir = |file: &TemplateFile| {
            std::path::Path::new(&file.path.global)
                .parent()
                .map(std::path::Path::to_path_buf)
        };
        let ui: Vec<_> = opencode.ui_files(true).collect();
        assert!(
            ui.iter().all(|file| ui_dir(file) == ui_dir(ui[0])),
            "the UI parts form one plugin directory"
        );
    }

    #[test]
    fn retired_paths_and_registrations_never_overlap_the_active_ones() {
        let same = |a: &ScopedPath, b: &ScopedPath| a.project == b.project || a.global == b.global;
        for spec in agents() {
            for retired in &spec.retired.template_files {
                assert!(
                    !spec
                        .template_files
                        .iter()
                        .any(|active| same(&active.path, &retired.path)),
                    "{}: {} is both active and retired",
                    spec.name,
                    retired.path.project
                );
            }
            for retired in &spec.retired.registrations {
                assert!(
                    !spec.registrations.iter().any(|active| {
                        same(&active.file, &retired.file) && same(&active.value, &retired.value)
                    }),
                    "{}: {} is both registered and retired",
                    spec.name,
                    retired.value.project
                );
            }
        }
    }

    #[test]
    fn instruction_files_are_known_names_and_globals_expand_under_home_or_an_absolute_config_dir() {
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
                        .chain(&spec.retired.registrations)
                        .flat_map(|r| [&r.file.global, &r.value.global]),
                )
                .chain(spec.retired.template_files.iter().map(|f| &f.path.global))
                .chain(spec.retired.mcp.iter().map(|entry| &entry.file.global))
                .chain(
                    spec.plugin
                        .iter()
                        .flat_map(|plugin| [&plugin.dir.global, &plugin.settings.global]),
                )
                .chain(spec.skills.iter().flat_map(|dirs| {
                    std::iter::once(&dirs.dir.global).chain(dirs.link.iter().map(|l| &l.global))
                }))
            {
                let home = Path::new("/home/test");
                let bare = Machine::new(home.to_path_buf(), BTreeMap::new());
                let path = bare.expand_global_path(global).unwrap();
                assert!(path.starts_with(home), "{}: {global}", spec.name);
                let env = BTreeMap::from([("CLAUDE_CONFIG_DIR".to_string(), "/cfg".to_string())]);
                let configured = Machine::new(home.to_path_buf(), env);
                assert!(
                    configured.expand_global_path(global).is_ok(),
                    "{}: {global}",
                    spec.name
                );
            }
        }
    }
}
