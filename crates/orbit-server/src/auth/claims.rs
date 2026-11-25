use orbit_utils::traversal_path::TraversalPath;
use secrecy::SecretString;
use serde::{Deserialize, Deserializer, Serialize};

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
    #[serde(default)]
    pub is_gitlab_team_member: Option<bool>,
    #[serde(default)]
    pub orbit_quota_exempt: Option<bool>,
    #[serde(default)]
    pub user_type: Option<String>,
    #[serde(default)]
    pub token_type: Option<String>,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, strum::IntoStaticStr)]
#[cfg_attr(test, derive(strum::EnumIter))]
#[serde(rename_all = "snake_case")]
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

fn deserialize_license_checksum<'de, D: Deserializer<'de>>(
    d: D,
) -> Result<Option<SecretString>, D::Error> {
    Ok(Option::<String>::deserialize(d)?.map(SecretString::from))
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
    fn every_source_type_serializes_to_the_name_the_deserializer_accepts() {
        use strum::IntoEnumIterator;
        for variant in SourceType::iter() {
            let wire = serde_json::to_value(variant).unwrap();
            assert_eq!(wire, Value::String(<&str>::from(variant).into()));
            let parsed = deserialize_source_type(wire).unwrap();
            assert_eq!(parsed, variant);
        }
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
    fn caller_credentials_are_read_when_rails_sends_them() {
        let mut json = claims_json(None);
        json["user_type"] = json!("service_account");
        json["token_type"] = json!("oauth_access_token");
        let claims: Claims = serde_json::from_value(json).unwrap();
        assert_eq!(claims.user_type.as_deref(), Some("service_account"));
        assert_eq!(claims.token_type.as_deref(), Some("oauth_access_token"));
    }

    #[test]
    fn caller_credentials_are_optional() {
        let claims = parse_claims(None);
        assert_eq!(claims.user_type, None);
        assert_eq!(claims.token_type, None);
    }

    #[test]
    fn orbit_quota_exempt_is_optional() {
        assert_eq!(parse_claims(None).orbit_quota_exempt, None);

        let mut json = claims_json(None);
        json["orbit_quota_exempt"] = json!(true);
        let claims: Claims = serde_json::from_value(json).unwrap();
        assert_eq!(claims.orbit_quota_exempt, Some(true));
    }

    #[test]
    fn unknown_claims_are_ignored() {
        let mut json = claims_json(None);
        json["some_future_claim"] = json!(true);
        assert!(serde_json::from_value::<Claims>(json).is_ok());
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
    fn license_checksum_is_never_serialized_or_debug_printed() {
        let claims = parse_claims(Some(json!(CHECKSUM)));
        assert!(!format!("{claims:?}").contains(CHECKSUM));
        let serialized = serde_json::to_string(&claims).unwrap();
        assert!(!serialized.contains(CHECKSUM));
        assert!(!serialized.contains("license_checksum"));
    }
}
