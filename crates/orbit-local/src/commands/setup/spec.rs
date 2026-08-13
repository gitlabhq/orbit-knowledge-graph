//! Declarative assistant specs embedded from `config/setup/agents/`. Each YAML file
//! describes one assistant in terms of four generic operations (instruction
//! file, marker-owned JSON merges, templated files, string registrations);
//! adding an assistant means adding a YAML file, not Rust.

use std::sync::LazyLock;

use rust_embed::Embed;
use serde::Deserialize;
use serde_json::Value;

#[derive(Embed)]
#[folder = "$CONFIG_DIR/setup"]
struct SetupAssets;

/// Which graph mechanism the written guidance points at. The rules are the
/// same either way; only the commands differ, so the two variants replace
/// each other rather than combining. `{mode}` tokens in specs resolve to the
/// matching `config/setup/modes/<mode>/` assets and hook-guard flags.
#[derive(clap::ValueEnum, Clone, Copy, PartialEq, Eq, Debug, Default)]
pub(crate) enum Mode {
    Local,
    #[default]
    Remote,
}

impl Mode {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Mode::Local => "local",
            Mode::Remote => "remote",
        }
    }
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

/// A file location that differs by install scope: `project` is relative to
/// the project root, `global` is a `~/`-anchored user-config path.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ScopedPath {
    pub(super) project: String,
    pub(super) global: String,
}

/// Idempotent merge into a JSON array at `path`: entries containing `marker`
/// in any nested string are owned by orbit and replaced wholesale, everything
/// else is preserved.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct JsonMerge {
    pub(super) file: ScopedPath,
    pub(super) path: Vec<String>,
    pub(super) marker: String,
    pub(super) entries: Vec<Value>,
}

/// A file written verbatim from an embedded `config/setup/` template.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TemplateFile {
    pub(super) path: ScopedPath,
    pub(super) template: String,
}

/// A string appended to a JSON array at `path` (e.g. a plugin list).
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
            let spec: AssistantSpec = serde_yaml::from_slice(&file.data)
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
    pub(super) fn contents(&self, mode: Mode) -> String {
        embedded_text(&self.template.replace("{mode}", mode.as_str()))
    }
}

/// Agent-facing text shipped next to the specs (instruction block, hook
/// nudges), embedded so every install method carries version-matched content.
pub(crate) fn embedded_text(name: &str) -> String {
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
        // An entry without its marker would not be recognized as orbit-owned,
        // so re-running setup would duplicate it.
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
    fn template_and_text_assets_resolve_in_both_modes() {
        for mode in [Mode::Local, Mode::Remote] {
            for spec in all() {
                for template_file in &spec.template_files {
                    assert!(!template_file.contents(mode).is_empty());
                }
            }
            for name in ["instructions.md", "nudge_search.md", "nudge_read.md"] {
                let text = embedded_text(&format!("modes/{}/{name}", mode.as_str()));
                assert!(!text.trim().is_empty());
            }
        }
    }

    // The reminder echo lands inside bash double quotes, so backticks and $(
    // would substitute there, corrupting output and executing what we only
    // suggest.
    #[test]
    fn opencode_plugins_are_shell_safe() {
        for mode in [Mode::Local, Mode::Remote] {
            let contents = get("opencode").unwrap().template_files[0].contents(mode);
            assert!(!contents.contains('`'), "{}", mode.as_str());
            assert!(!contents.contains("$("), "{}", mode.as_str());
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
