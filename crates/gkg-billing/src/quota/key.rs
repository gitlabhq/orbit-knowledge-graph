use super::inputs::QuotaInputs;

// Cache-key shape mirrors AIGW's CACHE_KEY_FIELDS (lib/billing_events/context.py).
// Any divergence silently fragments or merges cache entries across services that
// share the same CustomersDot backend, so keep the field list in sync.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct CacheKey {
    pub environment: String,
    pub realm: String,
    pub user_id: String,
    pub global_user_id: String,
    pub root_namespace_id: String,
    pub unique_instance_id: String,
    pub feature_enablement_type: String,
    pub feature_qualified_name: String,
}

impl CacheKey {
    pub(crate) fn from_inputs(inputs: &QuotaInputs, environment: &str) -> Option<Self> {
        // Required fields. Missing ones are not defaulted to empty strings —
        // that would silently collapse distinct consumers onto the same key.
        // Caller falls back to uncached fail-open when this returns None.
        let realm = inputs.realm.clone()?;
        let feature_qualified_name = inputs.feature_qualified_name.clone()?;
        let feature_enablement_type = inputs.feature_enablement_type.clone()?;

        Some(Self {
            environment: environment.to_string(),
            realm,
            user_id: inputs.user_id.to_string(),
            global_user_id: inputs.global_user_id.clone().unwrap_or_default(),
            root_namespace_id: inputs
                .root_namespace_id
                .map(|n| n.to_string())
                .unwrap_or_default(),
            unique_instance_id: inputs.unique_instance_id.clone().unwrap_or_default(),
            feature_enablement_type,
            feature_qualified_name,
        })
    }

    pub(crate) fn as_query_params(&self) -> Vec<(&'static str, &str)> {
        vec![
            ("environment", &self.environment),
            ("realm", &self.realm),
            ("user_id", &self.user_id),
            ("global_user_id", &self.global_user_id),
            ("root_namespace_id", &self.root_namespace_id),
            ("unique_instance_id", &self.unique_instance_id),
            ("feature_enablement_type", &self.feature_enablement_type),
            ("feature_qualified_name", &self.feature_qualified_name),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn inputs_with(
        realm: Option<&str>,
        fqn: Option<&str>,
        fet: Option<&str>,
        guid: Option<&str>,
        uiid: Option<&str>,
        rnid: Option<i64>,
    ) -> QuotaInputs {
        QuotaInputs {
            source_type: "mcp".into(),
            user_id: 42,
            realm: realm.map(Into::into),
            global_user_id: guid.map(Into::into),
            root_namespace_id: rnid,
            unique_instance_id: uiid.map(Into::into),
            feature_qualified_name: fqn.map(Into::into),
            feature_enablement_type: fet.map(Into::into),
        }
    }

    #[test]
    fn builds_key_when_required_fields_present() {
        let inputs = inputs_with(
            Some("SaaS"),
            Some("orbit_query"),
            Some("duo_enterprise"),
            Some("guid-1"),
            Some("uid-1"),
            Some(9970),
        );
        let key = CacheKey::from_inputs(&inputs, "production").unwrap();

        assert_eq!(key.environment, "production");
        assert_eq!(key.user_id, "42");
        assert_eq!(key.global_user_id, "guid-1");
        assert_eq!(key.root_namespace_id, "9970");
    }

    #[test]
    fn returns_none_when_realm_missing() {
        let inputs = inputs_with(
            None,
            Some("orbit_query"),
            Some("duo_enterprise"),
            None,
            None,
            None,
        );
        assert!(CacheKey::from_inputs(&inputs, "production").is_none());
    }

    #[test]
    fn returns_none_when_feature_qualified_name_missing() {
        let inputs = inputs_with(Some("SaaS"), None, Some("duo_enterprise"), None, None, None);
        assert!(CacheKey::from_inputs(&inputs, "production").is_none());
    }

    #[test]
    fn returns_none_when_feature_enablement_type_missing() {
        let inputs = inputs_with(Some("SaaS"), Some("orbit_query"), None, None, None, None);
        assert!(CacheKey::from_inputs(&inputs, "production").is_none());
    }

    #[test]
    fn empty_string_for_optional_inputs() {
        let inputs = inputs_with(
            Some("SaaS"),
            Some("orbit_query"),
            Some("duo_enterprise"),
            None,
            None,
            None,
        );
        let key = CacheKey::from_inputs(&inputs, "production").unwrap();
        assert_eq!(key.global_user_id, "");
        assert_eq!(key.root_namespace_id, "");
        assert_eq!(key.unique_instance_id, "");
    }

    #[test]
    fn distinct_keys_hash_differently() {
        use std::collections::HashSet;
        let a = CacheKey::from_inputs(
            &inputs_with(
                Some("SaaS"),
                Some("orbit_query"),
                Some("duo_enterprise"),
                None,
                None,
                Some(1),
            ),
            "production",
        )
        .unwrap();
        let b = CacheKey::from_inputs(
            &inputs_with(
                Some("SaaS"),
                Some("orbit_query"),
                Some("duo_enterprise"),
                None,
                None,
                Some(2),
            ),
            "production",
        )
        .unwrap();
        let set: HashSet<_> = [a, b].into_iter().collect();
        assert_eq!(set.len(), 2);
    }
}
