//! Billing domain logic for the GKG service.
//!
//! All billing-specific logic lives here: Snowplow event construction, the
//! observer that hooks into the query pipeline, and the constants that
//! identify GKG to CustomersDot. Server config (`BillingConfig`) lives in
//! `gkg-server-config`; the auth → billing seam is the single
//! `crates/gkg-server/src/billing_adapter.rs` file in `gkg-server`.

pub mod constants;
pub mod inputs;
mod observer;
mod tracker;

pub use inputs::BillingInputs;
pub use observer::BillingObserver;
pub use tracker::{BillingTracker, SnowplowBillingTracker};
