//! Billing event pipeline metrics.
//!
//! Counters cover the three observable outcomes of the per-query billing path:
//! a successful handoff to the Snowplow tracker, a drop because the event
//! could not be built, and a tracker error after handoff.

use crate::MetricSpec;

pub mod labels {
    pub const REASON: &str = "reason";
}

const DOMAIN: &str = "billing.events";

pub const EVENTS_EMITTED: MetricSpec = MetricSpec::counter(
    "gkg.billing.events.emitted",
    "Billing events handed to the Snowplow tracker after a successful query.",
    None,
    &[],
    DOMAIN,
);

pub const EVENTS_DROPPED: MetricSpec = MetricSpec::counter(
    "gkg.billing.events.dropped",
    "Billing events not emitted because event construction failed (realm missing, realm unrecognized, or builder error).",
    None,
    &[labels::REASON],
    DOMAIN,
);

pub const TRACK_ERRORS: MetricSpec = MetricSpec::counter(
    "gkg.billing.track.errors",
    "Errors returned by the Snowplow tracker when handing off a billing event.",
    None,
    &[],
    DOMAIN,
);

pub const CATALOG: &[&MetricSpec] = &[&EVENTS_EMITTED, &EVENTS_DROPPED, &TRACK_ERRORS];
