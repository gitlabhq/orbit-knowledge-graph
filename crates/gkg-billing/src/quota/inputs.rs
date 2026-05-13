//! Input shape for `QuotaService`.
//!
//! Mirrors the `BillingInputs` pattern: `gkg-billing` does not know about
//! `auth::Claims`. The single conversion point — `impl From<&Claims> for
//! QuotaInputs` — lives in `crates/gkg-server/src/billing_adapter.rs`. The
//! field set is the 8-field cache-key contract AIGW uses
//! (`lib/billing_events/context.py::CACHE_KEY_FIELDS`); divergence silently
//! fragments or merges cache entries across services that share the same
//! CustomersDot backend.

#[derive(Clone, Debug)]
pub struct QuotaInputs {
    pub source_type: String,
    pub user_id: i64,
    pub realm: Option<String>,
    pub global_user_id: Option<String>,
    pub root_namespace_id: Option<i64>,
    pub unique_instance_id: Option<String>,
    pub feature_qualified_name: Option<String>,
    pub feature_enablement_type: Option<String>,
}
