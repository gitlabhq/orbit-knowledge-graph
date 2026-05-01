//! Input shape for `BillingObserver`.
//!
//! `gkg-billing` does not know about `auth::Claims`. The single conversion
//! point — `impl From<&Claims> for BillingInputs` — lives in
//! `crates/gkg-server/src/billing_adapter.rs`. This struct is the entire
//! contract between auth and billing: only these fields cross the boundary.

#[derive(Clone, Debug)]
pub struct BillingInputs {
    pub realm: Option<String>,
    pub user_id: i64,
    pub source_type: String,
    pub organization_id: Option<i64>,
    pub instance_id: Option<String>,
    pub unique_instance_id: Option<String>,
    pub instance_version: Option<String>,
    pub global_user_id: Option<String>,
    pub host_name: Option<String>,
    pub root_namespace_id: Option<i64>,
    pub deployment_type: Option<String>,
}
