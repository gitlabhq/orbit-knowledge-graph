//! Violation of SOX rule R3: wires billing-relevant data (fields that
//! populate `BillingInputs`) through a non-adapter call site.
//!
//! See `docs/dev/sox-billing-boundary.md`.

#[expect(
    dead_code,
    reason = "synthetic SOX violation for Duo coverage testing — do not merge"
)]
pub struct UsageRouteInputs {
    pub user_id: i64,
    pub realm: Option<String>,
    pub organization_id: Option<i64>,
    pub root_namespace_id: Option<i64>,
    pub instance_id: Option<String>,
    pub global_user_id: Option<String>,
}

#[expect(
    dead_code,
    reason = "synthetic SOX violation for Duo coverage testing — do not merge"
)]
pub fn route_usage(inputs: UsageRouteInputs) -> i64 {
    inputs.user_id
}
