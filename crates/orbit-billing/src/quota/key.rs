use secrecy::SecretString;

use super::inputs::QuotaCheckInputs;
use crate::constants;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct CacheKey {
    pub realm: String,
    pub user_id: String,
    pub root_namespace_id: String,
    pub instance_id: String,
    pub unique_instance_id: String,
    pub event_type: String,
    pub feature_qualified_name: String,
}

// `license_checksum` stays out of `CacheKey`: key fields are logged, and one
// self-managed instance holds one license, so it adds no cache discrimination.
// `correlation_id` differs on every request, so keying on it would defeat the cache.
pub(crate) struct CdotRequest {
    pub key: CacheKey,
    pub global_user_id: String,
    pub instance_version: String,
    pub license_checksum: Option<SecretString>,
    pub correlation_id: Option<String>,
}

impl CdotRequest {
    pub(crate) fn from_inputs(inputs: &QuotaCheckInputs, correlation_id: &str) -> Option<Self> {
        let realm = inputs.realm.clone()?;

        if constants::normalize_realm(&realm) == Some(constants::REALM_SAAS)
            && inputs.root_namespace_id.is_none()
        {
            return None;
        }

        Some(Self {
            key: CacheKey {
                realm,
                user_id: inputs.user_id.to_string(),
                root_namespace_id: inputs
                    .root_namespace_id
                    .map(|n| n.to_string())
                    .unwrap_or_default(),
                instance_id: inputs.instance_id.clone().unwrap_or_default(),
                unique_instance_id: inputs.unique_instance_id.clone().unwrap_or_default(),
                event_type: constants::EVENT_TYPE.to_string(),
                feature_qualified_name: constants::feature_qualified_name(&inputs.source_type),
            },
            global_user_id: inputs.global_user_id.clone().unwrap_or_default(),
            instance_version: inputs.instance_version.clone().unwrap_or_default(),
            license_checksum: inputs.license_checksum.clone(),
            correlation_id: (!correlation_id.is_empty()).then(|| correlation_id.to_string()),
        })
    }

    pub(crate) fn as_query_params(&self) -> Vec<(&'static str, &str)> {
        let mut params: Vec<(&'static str, &str)> = vec![
            ("realm", &self.key.realm),
            ("user_id", &self.key.user_id),
            ("global_user_id", &self.global_user_id),
            ("root_namespace_id", &self.key.root_namespace_id),
            ("instance_id", &self.key.instance_id),
            ("unique_instance_id", &self.key.unique_instance_id),
            ("instance_version", &self.instance_version),
            ("event_type", &self.key.event_type),
            ("feature_qualified_name", &self.key.feature_qualified_name),
        ];
        // Matches the AI Gateway's parameter so CustomersDot request logs can be joined to
        // Orbit's logs without a CustomersDot change.
        if let Some(id) = self.correlation_id.as_deref() {
            params.push(("correlation_id", id));
        }
        params
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn inputs_with(
        realm: Option<&str>,
        guid: Option<&str>,
        uiid: Option<&str>,
        rnid: Option<i64>,
    ) -> QuotaCheckInputs {
        QuotaCheckInputs {
            source_type: "mcp".into(),
            user_id: 42,
            realm: realm.map(Into::into),
            global_user_id: guid.map(Into::into),
            root_namespace_id: rnid,
            instance_id: None,
            unique_instance_id: uiid.map(Into::into),
            instance_version: None,
            license_checksum: None,
        }
    }

    #[test]
    fn builds_request_when_required_fields_present() {
        let inputs = inputs_with(Some("SaaS"), Some("guid-1"), Some("uid-1"), Some(9970));
        let req = CdotRequest::from_inputs(&inputs, "").unwrap();

        assert_eq!(req.key.user_id, "42");
        assert_eq!(req.key.root_namespace_id, "9970");
        assert_eq!(req.global_user_id, "guid-1");
    }

    #[test]
    fn feature_qualified_name_is_generated_from_source_type() {
        let inputs = inputs_with(Some("SaaS"), None, None, Some(1));
        let req = CdotRequest::from_inputs(&inputs, "").unwrap();
        assert_eq!(req.key.feature_qualified_name, "orbit_mcp");
    }

    #[test]
    fn event_type_is_constant() {
        let inputs = inputs_with(Some("SaaS"), None, None, Some(1));
        let req = CdotRequest::from_inputs(&inputs, "").unwrap();
        assert_eq!(req.key.event_type, constants::EVENT_TYPE);
    }

    #[test]
    fn returns_none_when_realm_missing() {
        let inputs = inputs_with(None, None, None, None);
        assert!(CdotRequest::from_inputs(&inputs, "").is_none());
    }

    #[test]
    fn returns_none_for_saas_without_root_namespace_id() {
        let inputs = inputs_with(Some("SaaS"), None, None, None);
        assert!(CdotRequest::from_inputs(&inputs, "").is_none());
    }

    #[test]
    fn query_params_map_to_cdot_field_names() {
        let mut inputs = inputs_with(Some("SaaS"), Some("guid-1"), Some("uid-1"), Some(9970));
        inputs.instance_id = Some("inst-1".into());
        inputs.instance_version = Some("19.5.0".into());
        let req = CdotRequest::from_inputs(&inputs, "").unwrap();
        let params = req.as_query_params();

        let get = |key: &str| params.iter().find(|(k, _)| *k == key).map(|(_, v)| *v);

        assert_eq!(get("realm"), Some("SaaS"));
        assert_eq!(get("user_id"), Some("42"));
        assert_eq!(get("global_user_id"), Some("guid-1"));
        assert_eq!(get("root_namespace_id"), Some("9970"));
        assert_eq!(get("instance_id"), Some("inst-1"));
        assert_eq!(get("unique_instance_id"), Some("uid-1"));
        assert_eq!(get("instance_version"), Some("19.5.0"));
        assert_eq!(get("event_type"), Some(constants::EVENT_TYPE));
        assert_eq!(get("feature_qualified_name"), Some("orbit_mcp"));
    }

    // The quota check sends the realm claim exactly as Rails signed it (`self-managed`), which
    // is the value CustomersDot expects. It must never be replaced by `constants::REALM_SM`
    // ("SM"), the realm Orbit writes into billing events.
    #[test]
    fn self_managed_realm_claim_is_sent_verbatim() {
        let inputs = inputs_with(Some("self-managed"), Some("guid-1"), Some("uid-1"), None);
        let req = CdotRequest::from_inputs(&inputs, "").unwrap();
        let params = req.as_query_params();
        let realm = params.iter().find(|(k, _)| *k == "realm").map(|(_, v)| *v);
        assert_eq!(realm, Some("self-managed"));
    }

    #[test]
    fn license_checksum_is_excluded_from_cache_key_and_debug_output() {
        let checksum = "a".repeat(64);
        let mut inputs = inputs_with(Some("self-managed"), None, Some("uid-1"), None);
        inputs.license_checksum = Some(checksum.clone().into());
        let req = CdotRequest::from_inputs(&inputs, "").unwrap();

        assert!(req.license_checksum.is_some());
        assert!(!format!("{:?}", req.key).contains(&checksum));
        assert!(!format!("{inputs:?}").contains(&checksum));
        assert!(req.as_query_params().iter().all(|(_, v)| *v != checksum));
    }

    #[test]
    fn correlation_id_is_sent_as_query_param_when_present() {
        let inputs = inputs_with(Some("SaaS"), None, None, Some(1));
        let req = CdotRequest::from_inputs(&inputs, "req-123").unwrap();
        let params = req.as_query_params();
        let correlation_id = params
            .iter()
            .find(|(k, _)| *k == "correlation_id")
            .map(|(_, v)| *v);
        assert_eq!(correlation_id, Some("req-123"));
    }

    #[test]
    fn correlation_id_param_is_omitted_when_absent() {
        let inputs = inputs_with(Some("SaaS"), None, None, Some(1));
        let req = CdotRequest::from_inputs(&inputs, "").unwrap();
        assert!(req.correlation_id.is_none());
        assert!(
            req.as_query_params()
                .iter()
                .all(|(k, _)| *k != "correlation_id")
        );
    }
}
