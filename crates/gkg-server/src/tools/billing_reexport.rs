//! Violation of SOX rule R2: re-exports gkg-billing types so they're
//! reachable through gkg-server's public surface.
//!
//! See `docs/dev/sox-billing-boundary.md`.

#![expect(
    unused_imports,
    reason = "synthetic SOX violation for Duo coverage testing — do not merge"
)]

pub use gkg_billing::BillingTracker;
pub use gkg_billing::QuotaService;
