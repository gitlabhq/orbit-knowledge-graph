use std::collections::BTreeMap;
use std::fmt;

use serde::de::{self, Visitor};

const MANIFEST: &str = "SKILL.md";

#[derive(Debug)]
struct StrictString;

impl<'de> serde::Deserialize<'de> for StrictString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct StrictStringVisitor;

        impl<'de> Visitor<'de> for StrictStringVisitor {
            type Value = StrictString;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a string")
            }

            fn visit_str<E>(self, _value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(StrictString)
            }

            fn visit_string<E>(self, _value: String) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(StrictString)
            }
        }

        deserializer.deserialize_any(StrictStringVisitor)
    }
}

#[derive(Debug)]
pub struct SkillFrontmatter {
    pub name: String,
    pub version: semver::Version,
    pub description: String,
    pub compatibility: String,
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RawSkillFrontmatter {
    name: String,
    version: semver::Version,
    description: String,
    #[serde(default, rename = "license")]
    _license: Option<String>,
    compatibility: String,
    metadata: BTreeMap<String, StrictString>,
    #[serde(default, rename = "allowed-tools")]
    _allowed_tools: Option<String>,
}

pub fn parse_skill_frontmatter(
    manifest: &str,
    expected_name: &str,
) -> Result<SkillFrontmatter, String> {
    let normalized = manifest.replace("\r\n", "\n");
    let content = normalized
        .strip_prefix("---\n")
        .ok_or_else(|| format!("{MANIFEST} has no YAML frontmatter"))?;
    let (frontmatter, _) = content
        .split_once("\n---\n")
        .ok_or_else(|| format!("{MANIFEST} has unterminated YAML frontmatter"))?;
    let parsed: RawSkillFrontmatter = orbit_utils::yaml::from_str(frontmatter)
        .map_err(|error| format!("parsing {MANIFEST} frontmatter: {error}"))?;
    if parsed.name != expected_name {
        return Err(format!(
            "skill name {:?} does not match {expected_name:?}",
            parsed.name
        ));
    }
    if parsed.description.trim().is_empty() {
        return Err("skill description must not be empty".to_string());
    }
    if parsed.compatibility.trim().is_empty() {
        return Err("skill compatibility must not be empty".to_string());
    }
    if parsed.metadata.contains_key("version") {
        return Err("skill version must be a top-level frontmatter field".to_string());
    }

    Ok(SkillFrontmatter {
        name: parsed.name,
        version: parsed.version,
        description: parsed.description,
        compatibility: parsed.compatibility,
    })
}
