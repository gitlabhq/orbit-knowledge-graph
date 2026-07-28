//! Agent-facing prompt registry: [`embed!`] compiles the versioned YAML
//! prompt files under `config/prompts/` into modules of string constants,
//! so a malformed prompt fails the build rather than shipping.

use std::fmt::Write as _;
use std::path::Path;

use proc_macro::TokenStream;
use serde::Deserialize;

/// Embeds a subdirectory of `PROMPTS_DIR` (`.cargo/config.toml` env) as one
/// `pub mod <name>` per prompt file, mirroring nested directories as nested
/// modules: `gkg_prompts::embed!("remote");`.
///
/// Each module exposes the prompt's present fields as `SUMMARY`, `SHORT`,
/// and `DESCRIPTION` constants — or `DESCRIPTION_TEMPLATE` when the prompt
/// declares `variables:`, whose names are checked against the MiniJinja
/// template's placeholders at expansion time.
#[proc_macro]
pub fn embed(input: TokenStream) -> TokenStream {
    let input = input.to_string();
    let subdir = parse_string_literal(&input)
        .unwrap_or_else(|| panic!("expected a string literal, e.g. embed!(\"remote\")"));
    let prompts_dir =
        std::env::var("PROMPTS_DIR").expect("PROMPTS_DIR must be set via .cargo/config.toml [env]");
    let dir = Path::new(&prompts_dir).join(subdir);

    let source = render_dir(&dir).unwrap_or_else(|e| panic!("{e}"));
    source
        .parse()
        .expect("generated prompt modules should be valid Rust")
}

fn parse_string_literal(input: &str) -> Option<&str> {
    let input = input.trim();
    let inner = input.strip_prefix('"')?.strip_suffix('"')?;
    (!inner.contains('"')).then_some(inner)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Prompt {
    name: String,
    version: String,
    summary: Option<String>,
    short: Option<String>,
    description: Option<String>,
    #[serde(default)]
    variables: Vec<String>,
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
        semver::Version::parse(&self.version)
            .map_err(|e| format!("prompt `{stem}` has version `{}`: {e}", self.version))?;
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
        self.validate_template()
    }

    fn validate_template(&self) -> Result<(), String> {
        let stem = &self.name;
        if !self.variables.is_empty() && self.description.is_none() {
            return Err(format!(
                "prompt `{stem}` declares variables but no description to use them in"
            ));
        }
        let placeholders = match &self.description {
            Some(description) => template_placeholders(description)
                .map_err(|e| format!("prompt `{stem}` has an invalid template: {e}"))?,
            None => Vec::new(),
        };
        let declared: Vec<&str> = self.variables.iter().map(String::as_str).collect();
        let mut used: Vec<&str> = placeholders.iter().map(String::as_str).collect();
        used.sort_unstable();
        let mut expected = declared.clone();
        expected.sort_unstable();
        if used != expected {
            return Err(format!(
                "prompt `{stem}`: template placeholders {used:?} must exactly match the \
                 declared variables {declared:?}"
            ));
        }
        for variable in &self.variables {
            if !is_rust_ident(variable) {
                return Err(format!(
                    "prompt `{stem}`: variable `{variable}` is not a valid identifier"
                ));
            }
        }
        Ok(())
    }
}

fn template_placeholders(template: &str) -> Result<Vec<String>, minijinja::Error> {
    let mut environment = minijinja::Environment::new();
    environment.set_undefined_behavior(minijinja::UndefinedBehavior::Strict);
    Ok(environment
        .template_from_str(template)?
        .undeclared_variables(false)
        .into_iter()
        .collect())
}

const RUST_KEYWORDS: &[&str] = &[
    "abstract", "as", "async", "await", "become", "box", "break", "const", "continue", "crate",
    "do", "dyn", "else", "enum", "extern", "false", "final", "fn", "for", "gen", "if", "impl",
    "in", "let", "loop", "macro", "match", "mod", "move", "mut", "override", "priv", "pub", "ref",
    "return", "self", "Self", "static", "struct", "super", "trait", "true", "try", "type",
    "typeof", "unsafe", "unsized", "use", "virtual", "where", "while", "yield",
];

fn is_rust_ident(s: &str) -> bool {
    if RUST_KEYWORDS.contains(&s) {
        return false;
    }
    let mut chars = s.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn render_dir(dir: &Path) -> Result<String, String> {
    let mut entries: Vec<_> = std::fs::read_dir(dir)
        .map_err(|e| format!("reading {}: {e}", dir.display()))?
        .collect::<Result<_, _>>()
        .map_err(|e| format!("reading {}: {e}", dir.display()))?;
    entries.sort_by_key(std::fs::DirEntry::file_name);

    let mut out = String::new();
    for entry in entries {
        let path = entry.path();
        if path.is_dir() {
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .filter(|name| is_rust_ident(name))
                .ok_or_else(|| {
                    format!("prompt directory {} is not an identifier", path.display())
                })?;
            let _ = write!(out, "pub mod {name} {{\n{}}}\n", render_dir(&path)?);
            continue;
        }
        if path.extension().and_then(|ext| ext.to_str()) != Some("yml") {
            continue;
        }
        out.push_str(&render_prompt_module(&path)?);
    }

    if out.is_empty() {
        return Err(format!("no prompt files found in {}", dir.display()));
    }
    Ok(out)
}

fn render_prompt_module(path: &Path) -> Result<String, String> {
    let stem = path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .filter(|stem| is_rust_ident(stem))
        .ok_or_else(|| format!("prompt file name {} is not an identifier", path.display()))?;
    let raw =
        std::fs::read_to_string(path).map_err(|e| format!("reading {}: {e}", path.display()))?;
    let mut prompt: Prompt =
        serde_yaml::from_str(&raw).map_err(|e| format!("parsing {}: {e}", path.display()))?;
    prompt.summary = prompt.summary.map(|s| s.trim_end().to_string());
    prompt.short = prompt.short.map(|s| s.trim_end().to_string());
    prompt.description = prompt.description.map(|s| s.trim_end().to_string());
    prompt.validate(stem)?;
    Ok(render_module(&prompt, path))
}

fn render_module(prompt: &Prompt, source: &Path) -> String {
    let mut out = format!("pub mod {} {{\n", prompt.name);
    // include_str! puts the file in rustc's dep-info, so editing a prompt
    // recompiles the consuming crate.
    let _ = writeln!(
        out,
        "    const _: &str = include_str!({:?});",
        source.display()
    );
    for (field, value) in prompt.fields() {
        if let Some(value) = value {
            let name = if field == "description" && !prompt.variables.is_empty() {
                "DESCRIPTION_TEMPLATE".to_string()
            } else {
                field.to_uppercase()
            };
            let _ = writeln!(out, "    pub const {name}: &str = {value:?};");
        }
    }
    out.push_str("}\n");
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
    fn validate_rejects_non_semver_versions() {
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
    fn validate_rejects_undeclared_template_placeholders() {
        let p = prompt("name: a\nversion: 1.0.0\ndescription: \"Hello {{ who }}\"");
        assert!(p.validate("a").is_err());
    }

    #[test]
    fn validate_rejects_unused_declared_variables() {
        let p = prompt("name: a\nversion: 1.0.0\nvariables: [who]\ndescription: Hello");
        assert!(p.validate("a").is_err());
    }

    #[test]
    fn validate_rejects_rust_keyword_variables() {
        let p = prompt("name: a\nversion: 1.0.0\nvariables: [type]\ndescription: \"{{ type }}\"");
        assert!(p.validate("a").is_err());
        assert!(!is_rust_ident("mod"));
        assert!(is_rust_ident("query_graph"));
    }

    #[test]
    fn validate_accepts_matching_template_variables() {
        let p =
            prompt("name: a\nversion: 1.0.0\nvariables: [who]\ndescription: \"Hello {{ who }}\"");
        assert_eq!(p.validate("a"), Ok(()));
    }

    #[test]
    fn render_emits_present_fields_and_template_const() {
        let source = Path::new("/tmp/a.yml");
        let plain = render_module(
            &prompt("name: a\nversion: 1.0.0\nsummary: Sum\ndescription: \"Line\\nbreak\""),
            source,
        );
        assert!(plain.contains("pub mod a {"));
        assert!(plain.contains("pub const SUMMARY: &str = \"Sum\";"));
        assert!(plain.contains("pub const DESCRIPTION: &str = \"Line\\nbreak\";"));
        assert!(plain.contains("include_str!(\"/tmp/a.yml\")"));

        let templated = render_module(
            &prompt("name: a\nversion: 1.0.0\nvariables: [who]\ndescription: \"Hi {{ who }}\""),
            source,
        );
        assert!(templated.contains("pub const DESCRIPTION_TEMPLATE: &str = \"Hi {{ who }}\";"));
    }
}
