use serde::{Deserialize, Deserializer, Serialize};

/// One traversal path the user holds in their scope, paired with the exact
/// effective access levels they hold on that path. Mirrors the `(path, roles)`
/// tuples discussed with Security: a single user can hold Reporter on one group
/// and Developer on another, and the compiler security pass needs to see both
/// so it can drop lower-role paths from an entity's predicate.
///
/// `access_levels` contains raw `Gitlab::Access` integers (Reporter=20,
/// SecurityManager=25, Developer=30, ...) so comparisons against
/// `required_role` in the ontology remain direct numeric checks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraversalPathClaim {
    pub path: String,
    pub access_levels: Vec<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,
    pub iss: String,
    pub aud: String,
    pub iat: i64,
    pub exp: i64,
    pub user_id: u64,
    pub username: String,
    #[serde(default)]
    pub admin: bool,
    #[serde(default)]
    pub organization_id: Option<u64>,
    #[serde(default)]
    pub min_access_level: Option<u32>,
    /// Traversal paths the user can query, each paired with the exact
    /// access-level set on that path. Rails derives this from
    /// `Search::GroupsFinder`. The compiler security pass consumes it to filter
    /// paths per-entity: for example, a user with
    /// `[("1/2/", [Reporter]), ("1/3/", [Security Manager])]` sees Project rows
    /// from both paths but only Vulnerability rows from `1/3/`.
    #[serde(default)]
    pub group_traversal_ids: Vec<TraversalPathClaim>,
    #[serde(deserialize_with = "deserialize_source_type")]
    pub source_type: SourceType,
    #[serde(default, rename = "session_id")]
    pub ai_session_id: Option<String>,
    #[serde(default)]
    pub instance_id: Option<String>,
    #[serde(default)]
    pub unique_instance_id: Option<String>,
    #[serde(default)]
    pub instance_version: Option<String>,
    #[serde(default)]
    pub global_user_id: Option<String>,
    #[serde(default)]
    pub host_name: Option<String>,
    #[serde(default)]
    pub root_namespace_id: Option<i64>,
    #[serde(default)]
    pub deployment_type: Option<String>,
    #[serde(default)]
    pub realm: Option<String>,
    /// Whether the user is a GitLab team member. Set by Rails via
    /// `Gitlab::Com.gitlab_com_group_member?(user)`. Always false on
    /// self-managed / Dedicated instances.
    #[serde(default)]
    pub is_gitlab_team_member: Option<bool>,
}

/// Source type of the request, matching the Iglu `orbit_query` enum.
/// Unknown JWT values deserialize to `Rest` (the catch-all).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum SourceType {
    Frontend,
    Dws,
    Mcp,
    Core,
    Rest,
    CodeIntelligence,
}

fn deserialize_source_type<'de, D: Deserializer<'de>>(d: D) -> Result<SourceType, D::Error> {
    let s = String::deserialize(d)?;
    Ok(match s.as_str() {
        "frontend" => SourceType::Frontend,
        "dws" => SourceType::Dws,
        "mcp" => SourceType::Mcp,
        "core" => SourceType::Core,
        "code_intelligence" => SourceType::CodeIntelligence,
        _ => SourceType::Rest,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    fn parse(raw: &str) -> SourceType {
        deserialize_source_type(Value::String(raw.into())).unwrap()
    }

    #[test]
    fn code_intelligence_round_trips() {
        assert_eq!(parse("code_intelligence"), SourceType::CodeIntelligence);
        assert_eq!(
            <&str>::from(SourceType::CodeIntelligence),
            "code_intelligence"
        );
    }

    #[test]
    fn unknown_source_type_falls_back_to_rest() {
        assert_eq!(parse("something_else"), SourceType::Rest);
    }
}
