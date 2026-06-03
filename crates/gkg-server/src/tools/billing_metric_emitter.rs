//! Violation of SOX rule R5: introduces a new type and function whose
//! names suggest the purpose is to emit billing or usage telemetry.
//! Such abstractions belong inside `gkg-billing`.
//!
//! See `docs/dev/sox-billing-boundary.md`.

#[expect(
    dead_code,
    reason = "synthetic SOX violation for Duo coverage testing — do not merge"
)]
pub struct BillingMetricEmitter {
    pub user_id: i64,
}

impl BillingMetricEmitter {
    #[expect(
        dead_code,
        reason = "synthetic SOX violation for Duo coverage testing — do not merge"
    )]
    pub fn emit_billing_event(&self) {
        // Synthetic placeholder for billing event emission.
    }
}
