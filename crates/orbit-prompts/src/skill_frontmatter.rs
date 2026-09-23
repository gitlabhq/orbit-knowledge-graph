use std::collections::BTreeMap;
use std::fmt;

use serde::de::{self, Visitor};

const MANIFEST: &str = "SKILL.md";

#[derive(Debug)]
struct StrictString(String);

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

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(StrictString(value.to_string()))
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(StrictString(value))
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
    let mut parsed: RawSkillFrontmatter = orbit_utils::yaml::from_str(frontmatter)
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
    let version = parsed
        .metadata
        .remove("version")
        .ok_or_else(|| "skill metadata.version is required".to_string())?
        .0
        .parse()
        .map_err(|error| format!("skill metadata.version is not valid semver: {error}"))?;

    Ok(SkillFrontmatter {
        name: parsed.name,
        version,
        description: parsed.description,
        compatibility: parsed.compatibility,
    })
}
