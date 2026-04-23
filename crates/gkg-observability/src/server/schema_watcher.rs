//! Webserver schema-watcher readiness state.

use crate::{MetricKind, MetricSpec, Stability};

pub mod labels {
    pub const STATE: &str = "state";
}

const DOMAIN: &str = "server.schema_watcher";

pub const STATE: MetricSpec = MetricSpec {
    otel_name: "gkg.webserver.schema.state",
    description: "Webserver readiness gate state; 1 indicates the active state per `state` label.",
    kind: MetricKind::ObservableGauge,
    unit: None,
    labels: &[labels::STATE],
    buckets: None,
    stability: Stability::Stable,
    domain: DOMAIN,
};

pub const CATALOG: &[&MetricSpec] = &[&STATE];
