use std::collections::BTreeMap;
use std::path::Path;

use rust_embed::Embed;
use serde::Deserialize;

#[derive(Embed)]
#[folder = "$PROMPTS_DIR"]
struct PromptFiles;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Prompt {
    name: String,
    version: String,
    summary: Option<String>,
    short: Option<String>,
    description: Option<String>,
    #[serde(default)]
    variables: Vec<String>,
}

impl Prompt {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn version(&self) -> &str {
        &self.version
    }

    pub fn summary(&self) -> &str {
        self.summary
            .as_deref()
            .unwrap_or_else(|| panic!("prompt `{}` has no summary", self.name))
    }

    pub fn short(&self) -> &str {
        self.short
            .as_deref()
            .unwrap_or_else(|| panic!("prompt `{}` has no short form", self.name))
    }

    pub fn description(&self) -> &str {
        self.description
            .as_deref()
            .unwrap_or_else(|| panic!("prompt `{}` has no description", self.name))
    }

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

fn parse_prompt(key: &str, raw: &str) -> Result<Prompt, String> {
    let mut prompt: Prompt =
        orbit_utils::yaml::from_str(raw).map_err(|e| format!("parsing prompt `{key}`: {e}"))?;
    prompt.summary = prompt.summary.map(|s| s.trim_end().to_string());
    prompt.short = prompt.short.map(|s| s.trim_end().to_string());
    prompt.description = prompt.description.map(|s| s.trim_end().to_string());
    let stem = key.rsplit('/').next().unwrap_or(key);
    prompt.validate(stem)?;
    Ok(prompt)
}

pub struct Prompts(BTreeMap<String, Prompt>);

impl Prompts {
    pub fn load_embedded(scope: &str) -> Result<Self, String> {
        let prefix = format!("{scope}/");
        let mut prompts = BTreeMap::new();
        for path in PromptFiles::iter() {
            let Some(key) = path
                .strip_prefix(&prefix)
                .and_then(|rest| rest.strip_suffix(".yml"))
            else {
                continue;
            };
            let file = PromptFiles::get(&path)
                .ok_or_else(|| format!("embedded prompt `{path}` unreadable"))?;
            let raw = std::str::from_utf8(&file.data)
                .map_err(|e| format!("prompt `{path}` is not UTF-8: {e}"))?;
            prompts.insert(key.to_string(), parse_prompt(key, raw)?);
        }
        if prompts.is_empty() {
            return Err(format!("no prompt files embedded for scope `{scope}`"));
        }
        Ok(Self(prompts))
    }

    pub fn load_dir(dir: &Path) -> Result<Self, String> {
        let mut prompts = BTreeMap::new();
        load_dir_into(dir, "", &mut prompts)?;
        if prompts.is_empty() {
            return Err(format!("no prompt files found in {}", dir.display()));
        }
        Ok(Self(prompts))
    }

    pub fn get(&self, key: &str) -> Option<&Prompt> {
        self.0.get(key)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &Prompt)> {
        self.0.iter().map(|(key, prompt)| (key.as_str(), prompt))
    }
}

fn load_dir_into(
    dir: &Path,
    prefix: &str,
    prompts: &mut BTreeMap<String, Prompt>,
) -> Result<(), String> {
    let entries = std::fs::read_dir(dir).map_err(|e| format!("reading {}: {e}", dir.display()))?;
    for entry in entries {
        let path = entry
            .map_err(|e| format!("reading {}: {e}", dir.display()))?
            .path();
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| format!("non-UTF-8 file name: {}", path.display()))?;
        if path.is_dir() {
            load_dir_into(&path, &format!("{prefix}{name}/"), prompts)?;
            continue;
        }
        let Some(stem) = name.strip_suffix(".yml") else {
            continue;
        };
        let key = format!("{prefix}{stem}");
        let raw = std::fs::read_to_string(&path)
            .map_err(|e| format!("reading {}: {e}", path.display()))?;
        prompts.insert(key.clone(), parse_prompt(&key, &raw)?);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn prompt(yaml: &str) -> Prompt {
        orbit_utils::yaml::from_str(yaml).expect("test prompt should parse")
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
    fn validate_accepts_matching_template_variables() {
        let p =
            prompt("name: a\nversion: 1.0.0\nvariables: [who]\ndescription: \"Hello {{ who }}\"");
        assert_eq!(p.validate("a"), Ok(()));
    }

    #[test]
    fn parse_prompt_normalizes_trailing_block_scalar_newlines() {
        let p = parse_prompt("a", "name: a\nversion: 1.0.0\ndescription: |\n  text\n").unwrap();
        assert_eq!(p.description(), "text");
    }

    #[test]
    fn embedded_scopes_load_and_contain_expected_prompts() {
        let remote = Prompts::load_embedded("remote").expect("remote prompts load");
        assert!(remote.get("list_commands").is_some());
        assert!(remote.get("invoke_command").is_some());
        assert!(remote.get("tools/query_graph").is_some());

        let local = Prompts::load_embedded("local").expect("local prompts load");
        assert!(local.get("index").is_some());
        assert_eq!(local.get("index").unwrap().name(), "index");
    }

    #[test]
    fn load_dir_matches_embedded_keys() {
        let dir = Path::new(env!("PROMPTS_DIR")).join("remote");
        let from_fs = Prompts::load_dir(&dir).expect("remote prompts load from fs");
        let embedded = Prompts::load_embedded("remote").expect("remote prompts load embedded");
        let fs_keys: Vec<&str> = from_fs.iter().map(|(key, _)| key).collect();
        let embedded_keys: Vec<&str> = embedded.iter().map(|(key, _)| key).collect();
        assert_eq!(fs_keys, embedded_keys);
    }
}
