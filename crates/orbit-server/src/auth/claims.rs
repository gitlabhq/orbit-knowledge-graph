use orbit_utils::traversal_path::TraversalPath;
use secrecy::SecretString;
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
    pub path: TraversalPath,
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
    pub request_id: Option<String>,
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
    /// SHA-256 of the instance's online cloud license, sent by Rails on self-managed
    /// and Dedicated only. Authenticates the quota gate to CustomersDot as
    /// `X-License-Token`. Anyone holding it can query that subscription's CustomersDot
    /// quota verdicts, so it is never serialized or logged.
    #[serde(
        default,
        deserialize_with = "deserialize_license_checksum",
        skip_serializing
    )]
    pub license_checksum: Option<SecretString>,
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

const LICENSE_CHECKSUM_LEN: usize = 64;

// A malformed checksum drops to `None` instead of failing: serde errors can echo the
// offending value, and `grpc::auth` puts JWT validation errors in logs and the status.
fn deserialize_license_checksum<'de, D: Deserializer<'de>>(
    d: D,
) -> Result<Option<SecretString>, D::Error> {
    match Option::<serde_json::Value>::deserialize(d)? {
        None => Ok(None),
        Some(serde_json::Value::String(s)) if is_license_checksum(&s) => Ok(Some(s.into())),
        Some(_) => {
            tracing::warn!("license_checksum claim malformed; ignoring");
            Ok(None)
        }
    }
}

fn is_license_checksum(s: &str) -> bool {
    s.len() == LICENSE_CHECKSUM_LEN && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

#[cfg(test)]
mod tests {
    use super::*;
    use secrecy::ExposeSecret;
    use serde_json::{Value, json};

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

    const CHECKSUM: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    fn claims_json(license_checksum: Option<Value>) -> Value {
        let mut v = json!({
            "sub": "user:1",
            "iss": "gitlab",
            "aud": "gitlab-knowledge-graph",
            "iat": 0,
            "exp": 0,
            "user_id": 1,
            "username": "u",
            "source_type": "mcp",
        });
        if let Some(checksum) = license_checksum {
            v["license_checksum"] = checksum;
        }
        v
    }

    fn parse_claims(license_checksum: Option<Value>) -> Claims {
        serde_json::from_value(claims_json(license_checksum)).unwrap()
    }

    #[test]
    fn valid_license_checksum_is_kept() {
        let claims = parse_claims(Some(json!(CHECKSUM)));
        assert_eq!(
            claims.license_checksum.as_ref().map(|s| s.expose_secret()),
            Some(CHECKSUM)
        );
    }

    #[test]
    fn absent_or_null_license_checksum_is_none() {
        assert!(parse_claims(None).license_checksum.is_none());
        assert!(parse_claims(Some(Value::Null)).license_checksum.is_none());
    }

    #[test]
    fn malformed_license_checksum_is_dropped_without_failing() {
        let malformed = [
            json!(&CHECKSUM[..63]),
            json!(format!("{CHECKSUM}0")),
            json!(CHECKSUM.to_uppercase()),
            json!(CHECKSUM.replacen('0', "g", 1)),
            json!(42),
            json!({ "value": CHECKSUM }),
        ];
        for value in malformed {
            let claims = parse_claims(Some(value.clone()));
            assert!(claims.license_checksum.is_none(), "accepted {value}");
        }
    }

    #[test]
    fn license_checksum_is_never_serialized_or_debug_printed() {
        let claims = parse_claims(Some(json!(CHECKSUM)));
        assert!(!format!("{claims:?}").contains(CHECKSUM));
        let serialized = serde_json::to_string(&claims).unwrap();
        assert!(!serialized.contains(CHECKSUM));
        assert!(!serialized.contains("license_checksum"));
    }
}
