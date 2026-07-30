//! NATS-level reconciliation metrics — housekeeping that acts on JetStream's
//! own advisories rather than on indexed domain data.

use crate::MetricSpec;

pub mod labels {
    pub const STREAM: &str = "stream";
    pub const CONSUMER: &str = "consumer";
}

const DOMAIN: &str = "indexer.nats";

pub const MAX_DELIVERIES_EXHAUSTED: MetricSpec = MetricSpec::counter(
    "gkg.indexer.max_deliveries.exhausted",
    "Messages deleted after JetStream exhausted max_deliver, labelled by stream and consumer.",
    None,
    &[labels::STREAM, labels::CONSUMER],
    DOMAIN,
);

pub const CATALOG: &[&MetricSpec] = &[&MAX_DELIVERIES_EXHAUSTED];
