use crate::MetricSpec;

pub mod labels {
    pub const STATE: &str = "state";
}

const DOMAIN: &str = "server.schema_watcher";

pub const STATE: MetricSpec = MetricSpec::observable_gauge(
    "gkg.webserver.schema.state",
    "Webserver serving snapshot state (pending|ready); 1 indicates the current state per `state` label.",
    None,
    &[labels::STATE],
    DOMAIN,
);

pub const CATALOG: &[&MetricSpec] = &[&STATE];
