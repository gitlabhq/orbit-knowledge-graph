//! Input shape for `QuotaService`.
//!
//! Mirrors the `BillingInputs` pattern: `gkg-billing` does not know about
//! `auth::Claims`. The single conversion point — `impl From<&Claims> for
//! QuotaInputs` — lives in `crates/gkg-server/src/billing_adapter.rs`. The
//! field set covers the AIGW cache-key contract
//! (`lib/billing_events/context.py::CACHE_KEY_FIELDS`); divergence silently
//! fragments or merges cache entries across services that share the same
//! CustomersDot backend.
//!
//! `feature_qualified_name` is intentionally absent here — it is GKG-owned
//! and derived from `source_type` via `constants::feature_qualified_name`.
//! The same generated string lands in both the quota cache key and the
//! billing event metadata, so the two observability surfaces never drift.

#[derive(Clone, Debug)]
pub struct QuotaInputs {
    pub source_type: String,
    pub user_id: i64,
    pub realm: Option<String>,
    pub global_user_id: Option<String>,
    pub root_namespace_id: Option<i64>,
    pub unique_instance_id: Option<String>,
}
