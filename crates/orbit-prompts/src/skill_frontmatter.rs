use std::collections::BTreeMap;

const MANIFEST: &str = "SKILL.md";

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
    version: Option<semver::Version>,
    description: String,
    #[serde(default, rename = "license")]
    _license: Option<String>,
    compatibility: String,
    #[serde(default)]
    metadata: BTreeMap<String, serde_json::Value>,
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
    if parsed.metadata.contains_key("version") {
        return Err("skill version must be a top-level frontmatter field".to_string());
    }
    let version = parsed.version.ok_or("missing field `version`")?;
    for (key, value) in &parsed.metadata {
        if !value.is_string() {
            return Err(format!(
                "skill metadata key {key:?} must have a string value"
            ));
        }
    }
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
    Ok(SkillFrontmatter {
        name: parsed.name,
        version,
        description: parsed.description,
        compatibility: parsed.compatibility,
    })
}
