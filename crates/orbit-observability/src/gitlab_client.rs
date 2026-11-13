use crate::MetricSpec;

pub mod labels {
    pub const METHOD: &str = "method";
}

const DOMAIN: &str = "gitlab_client";

pub const INSECURE_CREDENTIAL_REQUESTS: MetricSpec = MetricSpec::counter(
    "gkg.gitlab_client.insecure_credential.requests",
    "Credentialed requests to GitLab over a channel that is neither TLS nor private.",
    None,
    &[labels::METHOD],
    DOMAIN,
);

pub const CATALOG: &[&MetricSpec] = &[&INSECURE_CREDENTIAL_REQUESTS];
