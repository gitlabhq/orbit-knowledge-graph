//! Webserver schema-watcher readiness state.

use crate::MetricSpec;

pub mod labels {
    pub const STATE: &str = "state";
}

const DOMAIN: &str = "server.schema_watcher";

pub const STATE: MetricSpec = MetricSpec::observable_gauge(
    "gkg.webserver.schema.state",
    "Webserver readiness gate state; 1 indicates the active state per `state` label.",
    None,
    &[labels::STATE],
    DOMAIN,
);

pub const CATALOG: &[&MetricSpec] = &[&STATE];
