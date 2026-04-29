pub(crate) mod constants;
pub(crate) mod metrics;
mod observer;
mod tracker;

pub use metrics::register as register_metrics;
pub(crate) use observer::BillingObserver;
pub use tracker::{BillingTracker, SnowplowBillingTracker};

pub(crate) const REASON_REALM_MISSING: &str = "realm_missing";
pub(crate) const REASON_REALM_UNRECOGNIZED: &str = "realm_unrecognized";
pub(crate) const REASON_EVENT_BUILD_FAILED: &str = "event_build_failed";
