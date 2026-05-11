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

pub const EVENTS_REJECTED: MetricSpec = MetricSpec::counter(
    "gkg.billing.events.rejected",
    "Billing events refused by the labkit tracker at enqueue (queue full or \
     tracker shutdown). HTTP delivery failures to the Snowplow collector \
     occur in labkit's background loop and are not surfaced through this \
     counter.",
    None,
    &[],
    DOMAIN,
);

pub const CATALOG: &[&MetricSpec] = &[&EVENTS_EMITTED, &EVENTS_DROPPED, &EVENTS_REJECTED];
