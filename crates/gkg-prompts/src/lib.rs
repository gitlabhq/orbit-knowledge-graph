//! Agent-facing prompt registry.
//!
//! Everything that ends up in a model's context window — tool and command
//! descriptions today, schema/DSL prose later — is authored as a versioned
//! YAML file under `config/prompts/` instead of a Rust string literal.
//! Consuming crates call [`load_dir`] + [`render_modules`] from their build
//! script to compile each prompt into a `&'static str` constant, so a
//! malformed prompt file fails the build rather than shipping.

use std::fmt::Write as _;
use std::path::Path;

use serde::Deserialize;

/// One prompt file. `name` must match the file stem and `version` must be
/// semver; bump the version whenever the wording changes so downstream
/// eval tooling can key off it.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Prompt {
    pub name: String,
    pub version: String,
    /// One-line form, e.g. a command summary listed by `list_commands`.
    pub summary: Option<String>,
    /// CLI `--help` one-liner. When `description` is also present it must
    /// start with this text, so the two surfaces cannot diverge.
    pub short: Option<String>,
    /// Full text inserted into the tool or command definition.
    pub description: Option<String>,
}

impl Prompt {
    fn fields(&self) -> [(&'static str, Option<&str>); 3] {
        [
            ("summary", self.summary.as_deref()),
            ("short", self.short.as_deref()),
            ("description", self.description.as_deref()),
        ]
    }

    fn validate(&self, stem: &str) -> Result<(), String> {
        if self.name != stem {
            return Err(format!(
                "prompt `{stem}.yml` declares name `{}`; the name must match the file stem",
                self.name
            ));
        }
        let semver = self.version.split('.').collect::<Vec<_>>();
        if semver.len() != 3 || semver.iter().any(|part| part.parse::<u32>().is_err()) {
            return Err(format!(
                "prompt `{stem}` has version `{}`; expected MAJOR.MINOR.PATCH",
                self.version
            ));
        }
        if self.fields().iter().all(|(_, value)| value.is_none()) {
            return Err(format!(
                "prompt `{stem}` must declare at least one of summary, short, or description"
            ));
        }
        for (field, value) in self.fields() {
            if let Some(value) = value
                && value.trim().is_empty()
            {
                return Err(format!("prompt `{stem}` has an empty `{field}`"));
            }
        }
        if let (Some(short), Some(description)) = (&self.short, &self.description)
            && !description.starts_with(short.as_str())
        {
            return Err(format!(
                "prompt `{stem}`: description must start with the short form so the CLI \
                 help and the agent-facing text cannot diverge.\nshort: {short:?}"
            ));
        }
        Ok(())
    }
}

/// Loads and validates every `*.yml` prompt in `dir`, sorted by name.
pub fn load_dir(dir: &Path) -> Result<Vec<Prompt>, String> {
    let entries = std::fs::read_dir(dir).map_err(|e| format!("reading {}: {e}", dir.display()))?;

    let mut prompts = Vec::new();
    for entry in entries {
        let path = entry
            .map_err(|e| format!("reading {}: {e}", dir.display()))?
            .path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("yml") {
            continue;
        }
        let stem = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .ok_or_else(|| format!("non-UTF-8 prompt file name: {}", path.display()))?;
        let raw = std::fs::read_to_string(&path)
            .map_err(|e| format!("reading {}: {e}", path.display()))?;
        let mut prompt: Prompt =
            serde_yaml::from_str(&raw).map_err(|e| format!("parsing {}: {e}", path.display()))?;
        // Block scalars make a trailing newline depend on `|` vs `|-`;
        // normalize so that YAML style choice never changes the prompt.
        prompt.summary = prompt.summary.map(|s| s.trim_end().to_string());
        prompt.short = prompt.short.map(|s| s.trim_end().to_string());
        prompt.description = prompt.description.map(|s| s.trim_end().to_string());
        prompt.validate(stem)?;
        prompts.push(prompt);
    }

    if prompts.is_empty() {
        return Err(format!("no prompt files found in {}", dir.display()));
    }
    prompts.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(prompts)
}

/// Renders one `pub mod <name>` per prompt, each exposing the prompt's
/// present fields as `&str` constants. Write the result to `OUT_DIR` and
/// `include!` it.
pub fn render_modules(prompts: &[Prompt]) -> String {
    let mut out = String::from("// @generated from config/prompts - do not edit\n");
    for prompt in prompts {
        let _ = write!(out, "\npub mod {} {{\n", prompt.name);
        for (field, value) in prompt.fields() {
            if let Some(value) = value {
                let _ = writeln!(
                    out,
                    "    pub const {}: &str = {value:?};",
                    field.to_uppercase()
                );
            }
        }
        out.push_str("}\n");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn prompt(yaml: &str) -> Prompt {
        serde_yaml::from_str(yaml).expect("test prompt should parse")
    }

    #[test]
    fn validate_accepts_a_full_prompt() {
        let p = prompt(
            "name: index\nversion: 1.0.0\nshort: Index a repo\ndescription: Index a repo\n\n  Slowly.",
        );
        assert_eq!(p.validate("index"), Ok(()));
    }

    #[test]
    fn validate_rejects_name_stem_mismatch() {
        let p = prompt("name: index\nversion: 1.0.0\ndescription: text");
        assert!(p.validate("other").is_err());
    }

    #[test]
    fn validate_rejects_non_semver_version() {
        for version in ["1.0", "v1.0.0", "1.0.x"] {
            let p = prompt(&format!("name: a\nversion: '{version}'\ndescription: text"));
            assert!(p.validate("a").is_err(), "{version} should be rejected");
        }
    }

    #[test]
    fn validate_rejects_prompt_without_text() {
        let p = prompt("name: a\nversion: 1.0.0");
        assert!(p.validate("a").is_err());
    }

    #[test]
    fn validate_rejects_description_that_drops_the_short_prefix() {
        let p = prompt("name: a\nversion: 1.0.0\nshort: One thing\ndescription: Another thing");
        assert!(p.validate("a").is_err());
    }

    #[test]
    fn render_emits_a_module_per_prompt_with_present_fields_only() {
        let prompts = vec![
            prompt("name: a\nversion: 1.0.0\nsummary: Sum\ndescription: \"Line\\nbreak\""),
            prompt("name: b\nversion: 1.0.0\nshort: Short only"),
        ];
        let out = render_modules(&prompts);
        assert!(out.contains("pub mod a {"));
        assert!(out.contains("pub const SUMMARY: &str = \"Sum\";"));
        assert!(out.contains("pub const DESCRIPTION: &str = \"Line\\nbreak\";"));
        assert!(out.contains("pub mod b {"));
        assert!(out.contains("pub const SHORT: &str = \"Short only\";"));
        assert!(!out.contains("pub const DESCRIPTION: &str = \"Short only\";"));
    }
}
