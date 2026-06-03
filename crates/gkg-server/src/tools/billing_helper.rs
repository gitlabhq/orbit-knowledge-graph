//! Violation of SOX rule R1: introduces a new `use gkg_billing` import
//! in a file that did not previously import gkg-billing.
//!
//! See `docs/dev/sox-billing-boundary.md`.

use gkg_billing::BillingObserver;

#[expect(
    dead_code,
    reason = "synthetic SOX violation for Duo coverage testing — do not merge"
)]
pub fn observer_type_name() -> &'static str {
    std::any::type_name::<BillingObserver>()
}
