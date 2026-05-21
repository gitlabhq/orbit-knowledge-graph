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

pub(crate) struct CdotRequest {
    pub key: CacheKey,
    pub global_user_id: String,
}

impl CdotRequest {
    pub(crate) fn from_inputs(inputs: &QuotaCheckInputs) -> Option<Self> {
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
        })
    }

    pub(crate) fn as_query_params(&self) -> Vec<(&'static str, &str)> {
        vec![
            ("realm", &self.key.realm),
            ("user_id", &self.key.user_id),
            ("global_user_id", &self.global_user_id),
            ("root_namespace_id", &self.key.root_namespace_id),
            ("instance_id", &self.key.instance_id),
            ("unique_instance_id", &self.key.unique_instance_id),
            ("event_type", &self.key.event_type),
            ("feature_qualified_name", &self.key.feature_qualified_name),
        ]
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
        }
    }

    #[test]
    fn builds_request_when_required_fields_present() {
        let inputs = inputs_with(Some("SaaS"), Some("guid-1"), Some("uid-1"), Some(9970));
        let req = CdotRequest::from_inputs(&inputs).unwrap();

        assert_eq!(req.key.user_id, "42");
        assert_eq!(req.key.root_namespace_id, "9970");
        assert_eq!(req.global_user_id, "guid-1");
    }

    #[test]
    fn feature_qualified_name_is_generated_from_source_type() {
        let inputs = inputs_with(Some("SaaS"), None, None, Some(1));
        let req = CdotRequest::from_inputs(&inputs).unwrap();
        assert_eq!(req.key.feature_qualified_name, "orbit-mcp");
    }

    #[test]
    fn event_type_is_constant() {
        let inputs = inputs_with(Some("SaaS"), None, None, Some(1));
        let req = CdotRequest::from_inputs(&inputs).unwrap();
        assert_eq!(req.key.event_type, constants::EVENT_TYPE);
    }

    #[test]
    fn returns_none_when_realm_missing() {
        let inputs = inputs_with(None, None, None, None);
        assert!(CdotRequest::from_inputs(&inputs).is_none());
    }

    #[test]
    fn returns_none_for_saas_without_root_namespace_id() {
        let inputs = inputs_with(Some("SaaS"), None, None, None);
        assert!(CdotRequest::from_inputs(&inputs).is_none());
    }

    #[test]
    fn query_params_map_to_cdot_field_names() {
        let mut inputs = inputs_with(Some("SaaS"), Some("guid-1"), Some("uid-1"), Some(9970));
        inputs.instance_id = Some("inst-1".into());
        let req = CdotRequest::from_inputs(&inputs).unwrap();
        let params = req.as_query_params();

        let get = |key: &str| params.iter().find(|(k, _)| *k == key).map(|(_, v)| *v);

        assert_eq!(get("realm"), Some("SaaS"));
        assert_eq!(get("user_id"), Some("42"));
        assert_eq!(get("global_user_id"), Some("guid-1"));
        assert_eq!(get("root_namespace_id"), Some("9970"));
        assert_eq!(get("instance_id"), Some("inst-1"));
        assert_eq!(get("unique_instance_id"), Some("uid-1"));
        assert_eq!(get("event_type"), Some(constants::EVENT_TYPE));
        assert_eq!(get("feature_qualified_name"), Some("orbit-mcp"));
    }
}
