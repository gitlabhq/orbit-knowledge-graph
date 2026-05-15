use super::inputs::QuotaInputs;
use crate::constants;

// Cache-key shape and outgoing CDot query params. The first six fields
// identify the consumer (Rails-populated via JWT). `event_type` and
// `feature_qualified_name` are GKG-owned identifiers for the consumed
// surface — same strings used by the billing event constructor so the
// quota and billing observability paths can't disagree per request.
//
// `event_type` is included because CDot's BillingEligibility branches on
// it (`Billing::Usage::BillingEligibility::BaseService#track_consumption?`)
// and `Resolver#skip_cutoff?` early-returns when it is blank — without it,
// the resolve endpoint can never apply consumption tracking for our traffic.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct CacheKey {
    pub environment: String,
    pub realm: String,
    pub user_id: String,
    pub global_user_id: String,
    pub root_namespace_id: String,
    pub unique_instance_id: String,
    pub event_type: String,
    pub feature_qualified_name: String,
}

impl CacheKey {
    pub(crate) fn from_inputs(inputs: &QuotaInputs, environment: &str) -> Option<Self> {
        // `realm` is the only JWT-derived required cache-key field. Missing →
        // None so the caller can fail open uncached rather than collapsing
        // distinct consumers onto an empty-string slot.
        let realm = inputs.realm.clone()?;

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
            event_type: constants::EVENT_TYPE.to_string(),
            feature_qualified_name: constants::feature_qualified_name(&inputs.source_type),
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
            ("event_type", &self.event_type),
            ("feature_qualified_name", &self.feature_qualified_name),
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
    ) -> QuotaInputs {
        QuotaInputs {
            source_type: "mcp".into(),
            user_id: 42,
            realm: realm.map(Into::into),
            global_user_id: guid.map(Into::into),
            root_namespace_id: rnid,
            unique_instance_id: uiid.map(Into::into),
        }
    }

    #[test]
    fn builds_key_when_required_fields_present() {
        let inputs = inputs_with(Some("SaaS"), Some("guid-1"), Some("uid-1"), Some(9970));
        let key = CacheKey::from_inputs(&inputs, "production").unwrap();

        assert_eq!(key.environment, "production");
        assert_eq!(key.user_id, "42");
        assert_eq!(key.global_user_id, "guid-1");
        assert_eq!(key.root_namespace_id, "9970");
    }

    #[test]
    fn feature_qualified_name_is_generated_from_source_type() {
        let inputs = inputs_with(Some("SaaS"), None, None, None);
        let key = CacheKey::from_inputs(&inputs, "production").unwrap();
        assert_eq!(key.feature_qualified_name, "orbit-mcp");
    }

    #[test]
    fn event_type_is_constant() {
        let inputs = inputs_with(Some("SaaS"), None, None, None);
        let key = CacheKey::from_inputs(&inputs, "production").unwrap();
        assert_eq!(key.event_type, constants::EVENT_TYPE);
    }

    #[test]
    fn returns_none_when_realm_missing() {
        let inputs = inputs_with(None, None, None, None);
        assert!(CacheKey::from_inputs(&inputs, "production").is_none());
    }

    #[test]
    fn empty_string_for_optional_inputs() {
        let inputs = inputs_with(Some("SaaS"), None, None, None);
        let key = CacheKey::from_inputs(&inputs, "production").unwrap();
        assert_eq!(key.global_user_id, "");
        assert_eq!(key.root_namespace_id, "");
        assert_eq!(key.unique_instance_id, "");
    }

    #[test]
    fn distinct_keys_hash_differently() {
        use std::collections::HashSet;
        let a = CacheKey::from_inputs(
            &inputs_with(Some("SaaS"), None, None, Some(1)),
            "production",
        )
        .unwrap();
        let b = CacheKey::from_inputs(
            &inputs_with(Some("SaaS"), None, None, Some(2)),
            "production",
        )
        .unwrap();
        let set: HashSet<_> = [a, b].into_iter().collect();
        assert_eq!(set.len(), 2);
    }
}
