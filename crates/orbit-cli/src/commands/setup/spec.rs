//! Declarative assistant specs embedded from `config/setup/agents/`. Each YAML file describes
//! one assistant as four generic operations (instruction file, marker-owned JSON merges,
//! templated files, string registrations), so adding an assistant means adding a YAML file, not
//! Rust. The instruction block, hook nudges, and template values live in
//! `config/setup/setup.yaml`.

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
    instructions: String,
    nudge_search: String,
    nudge_usage: String,
    nudge_in_file: String,
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

fn render_launcher(text: &str, launcher: &str) -> String {
    text.replace("{{orbit}}", launcher)
}

fn render_instructions(launcher: &str) -> String {
    render_launcher(TEXTS.instructions.trim_end(), launcher)
}

static RENDERED_INSTRUCTIONS: LazyLock<String> = LazyLock::new(|| render_instructions(launcher()));

static RENDERED_NUDGE_SEARCH: LazyLock<String> =
    LazyLock::new(|| render_launcher(TEXTS.nudge_search.trim_end(), launcher()));

static RENDERED_NUDGE_USAGE: LazyLock<String> =
    LazyLock::new(|| render_launcher(TEXTS.nudge_usage.trim_end(), launcher()));

static RENDERED_NUDGE_IN_FILE: LazyLock<String> =
    LazyLock::new(|| render_launcher(TEXTS.nudge_in_file.trim_end(), launcher()));

static RENDERED_NUDGE_READ: LazyLock<String> =
    LazyLock::new(|| render_launcher(TEXTS.nudge_read.trim_end(), launcher()));

pub(crate) fn instructions() -> &'static str {
    &RENDERED_INSTRUCTIONS
}

pub(crate) fn nudge_search() -> &'static str {
    &RENDERED_NUDGE_SEARCH
}

pub(crate) fn nudge_usage() -> &'static str {
    &RENDERED_NUDGE_USAGE
}

pub(crate) fn nudge_in_file() -> &'static str {
    &RENDERED_NUDGE_IN_FILE
}

pub(crate) fn nudge_read() -> &'static str {
    &RENDERED_NUDGE_READ
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AssistantSpec {
    pub(super) name: String,
    pub(super) instruction_file: ScopedPath,
    #[serde(default)]
    pub(super) json_merges: Vec<JsonMerge>,
    #[serde(default)]
    pub(super) template_files: Vec<TemplateFile>,
    #[serde(default)]
    pub(super) registrations: Vec<Registration>,
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

static SPECS: LazyLock<Vec<AssistantSpec>> = LazyLock::new(|| {
    let mut specs: Vec<AssistantSpec> = SetupAssets::iter()
        .filter_map(|path| {
            let stem = path.strip_prefix("agents/")?.strip_suffix(".yaml")?;
            let file = SetupAssets::get(&path).expect("embedded file must be readable");
            let spec: AssistantSpec = orbit_utils::yaml::from_slice(&file.data)
                .unwrap_or_else(|e| panic!("config/setup/{path} is invalid: {e}"));
            assert_eq!(
                spec.name, stem,
                "config/setup/{path}: name must match the file stem"
            );
            Some(spec)
        })
        .collect();
    specs.sort_by(|a, b| a.name.cmp(&b.name));
    assert!(!specs.is_empty(), "no assistant specs embedded");
    specs
});

pub(super) fn all() -> &'static [AssistantSpec] {
    &SPECS
}

pub(super) fn get(name: &str) -> Option<&'static AssistantSpec> {
    SPECS.iter().find(|spec| spec.name == name)
}

pub(crate) fn names() -> Vec<&'static str> {
    SPECS.iter().map(|spec| spec.name.as_str()).collect()
}

impl TemplateFile {
    pub(super) fn contents(&self) -> String {
        self.contents_with(launcher())
    }

    fn contents_with(&self, launcher: &str) -> String {
        let mut rendered = embedded_text(&self.template);
        for (name, value) in &TEXTS.template_vars {
            rendered = rendered.replace(&format!("{{{{{name}}}}}"), value);
        }
        render_launcher(&rendered, launcher)
    }

    pub(super) fn is_unmodified(&self, contents: &str) -> bool {
        [DIRECT_LAUNCHER, GLAB_LAUNCHER]
            .iter()
            .any(|launcher| self.contents_with(launcher) == contents)
    }
}

fn embedded_text(name: &str) -> String {
    let file =
        SetupAssets::get(name).unwrap_or_else(|| panic!("config/setup/{name} is not embedded"));
    String::from_utf8(file.data.into_owned())
        .unwrap_or_else(|e| panic!("config/setup/{name} is not UTF-8: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::setup::json_ops;

    #[test]
    fn all_specs_parse_and_expected_assistants_exist() {
        for name in ["claude", "codex", "opencode", "pi"] {
            assert!(get(name).is_some(), "missing spec for {name}");
        }
        assert_eq!(names().len(), all().len());
    }

    #[test]
    fn merge_entries_contain_their_marker() {
        for spec in all() {
            for merge in &spec.json_merges {
                assert!(!merge.marker.is_empty(), "{}: empty marker", spec.name);
                for entry in &merge.entries {
                    assert!(
                        json_ops::contains_marker(entry, &merge.marker),
                        "{}: entry {entry} does not contain marker {:?}",
                        spec.name,
                        merge.marker
                    );
                }
            }
        }
    }

    #[test]
    fn template_and_text_assets_resolve() {
        for spec in all() {
            for template_file in &spec.template_files {
                let rendered = template_file.contents();
                assert!(!rendered.is_empty());
                assert!(
                    !rendered.contains("{{"),
                    "{}: unresolved placeholder in {}",
                    spec.name,
                    template_file.template
                );
            }
        }
        for text in [instructions(), nudge_search(), nudge_read()] {
            assert!(!text.trim().is_empty());
            assert!(!text.contains("{{"), "unresolved placeholder: {text}");
        }
        for text in [nudge_usage(), nudge_in_file()] {
            assert!(!text.contains("{{orbit}}"), "unresolved launcher: {text}");
            assert!(
                text.contains("{{term}}"),
                "missing term placeholder: {text}"
            );
        }
        assert!(nudge_in_file().contains("{{path}}"));
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
        let glab = get("claude").unwrap().json_merges[0].entries[0].to_string();
        assert!(glab.contains("{{orbit}} hook-guard"), "{glab}");
    }

    #[test]
    fn opencode_plugins_are_shell_safe() {
        let contents = get("opencode").unwrap().template_files[0].contents();
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
        for spec in all() {
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
            {
                assert!(global.starts_with("~/"), "{}: {global}", spec.name);
            }
        }
    }
}
