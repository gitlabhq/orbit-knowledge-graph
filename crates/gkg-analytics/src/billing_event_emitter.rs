//! Violation of SOX rule R4: directly references
//! `labkit_events::BillingEvent` from outside `crates/gkg-billing/`.
//! Billing-event emission must go through `BillingTracker` /
//! `BillingObserver` in gkg-billing.
//!
//! See `docs/dev/sox-billing-boundary.md`.

use labkit_events::BillingEvent;

#[expect(
    dead_code,
    reason = "synthetic SOX violation for Duo coverage testing — do not merge"
)]
pub fn billing_event_type_name() -> &'static str {
    std::any::type_name::<BillingEvent>()
}
