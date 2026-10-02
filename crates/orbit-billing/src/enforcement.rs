//! Startup validation for builds compiled with `GKG_BILLING_ENFORCED=true`.
//!
//! The switch is a build-time cfg flag, so the deployed config cannot turn it off.

use orbit_server_config::{BillingAuthMode, BillingConfig, QuotaAuthMode};
use reqwest::Url;

pub const ENFORCED: bool = cfg!(gkg_billing_enforced);

struct Environment {
    name: &'static str,
    customers_dot_url: &'static str,
    collector_url: &'static str,
}

const ENVIRONMENTS: [Environment; 2] = [
    Environment {
        name: "production",
        customers_dot_url: "https://customers.gitlab.com",
        collector_url: "https://billing.prdsub.gitlab.net",
    },
    Environment {
        name: "staging",
        customers_dot_url: "https://customers.staging.gitlab.com",
        collector_url: "https://billing.stgsub.gitlab.net",
    },
];

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum EnforcementError {
    #[error("this build requires billing.enabled=true")]
    BillingDisabled,

    #[error("this build requires billing.quota.enabled=true")]
    QuotaDisabled,

    #[error(
        "billing.quota.customers_dot_url and billing.collector_url must both belong to one \
         environment: {allowed}",
        allowed = allowed_environments()
    )]
    UnknownEnvironment,

    #[error(
        "billing.quota.auth_mode={quota:?} cannot be combined with billing.auth_mode={billing:?}; \
         allowed pairs are (admin_token, oidc) and (license_checksum, cloud_connector)"
    )]
    MixedAuthFamilies {
        quota: QuotaAuthMode,
        billing: BillingAuthMode,
    },
}

pub fn validate(config: &BillingConfig) -> Result<(), EnforcementError> {
    validate_with(ENFORCED, config)
}

fn validate_with(enforced: bool, config: &BillingConfig) -> Result<(), EnforcementError> {
    if !enforced {
        return Ok(());
    }
    if !config.enabled {
        return Err(EnforcementError::BillingDisabled);
    }
    if !config.quota.enabled {
        return Err(EnforcementError::QuotaDisabled);
    }
    let known_environment = ENVIRONMENTS.iter().any(|env| {
        same_base_url(&config.quota.customers_dot_url, env.customers_dot_url)
            && same_base_url(&config.collector_url, env.collector_url)
    });
    if !known_environment {
        return Err(EnforcementError::UnknownEnvironment);
    }
    match (config.quota.auth_mode, config.auth_mode) {
        (QuotaAuthMode::AdminToken, BillingAuthMode::Oidc)
        | (QuotaAuthMode::LicenseChecksum, BillingAuthMode::CloudConnector) => Ok(()),
        (quota, billing) => Err(EnforcementError::MixedAuthFamilies { quota, billing }),
    }
}

// The clients append their request path to the configured URL, so a path prefix would
// send requests to an unknown route on the real host.
fn same_base_url(actual: &str, expected: &str) -> bool {
    let (Ok(actual), Ok(expected)) = (Url::parse(actual), Url::parse(expected)) else {
        return false;
    };
    actual.scheme() == expected.scheme()
        && actual.host_str() == expected.host_str()
        && actual.port_or_known_default() == expected.port_or_known_default()
        && actual.path() == "/"
        && actual.username().is_empty()
        && actual.password().is_none()
        && actual.query().is_none()
        && actual.fragment().is_none()
}

fn allowed_environments() -> String {
    ENVIRONMENTS
        .iter()
        .map(|env| {
            format!(
                "{} ({}, {})",
                env.name, env.customers_dot_url, env.collector_url
            )
        })
        .collect::<Vec<_>>()
        .join(" or ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use orbit_server_config::AppConfig;

    const PROD_CDOT: &str = "https://customers.gitlab.com";
    const PROD_COLLECTOR: &str = "https://billing.prdsub.gitlab.net";
    const STG_CDOT: &str = "https://customers.staging.gitlab.com";
    const STG_COLLECTOR: &str = "https://billing.stgsub.gitlab.net";

    fn config(
        cdot: &str,
        collector: &str,
        quota_auth: QuotaAuthMode,
        billing_auth: BillingAuthMode,
    ) -> BillingConfig {
        let mut billing = AppConfig::embedded_defaults().billing;
        billing.enabled = true;
        billing.collector_url = collector.into();
        billing.auth_mode = billing_auth;
        billing.quota.enabled = true;
        billing.quota.customers_dot_url = cdot.into();
        billing.quota.auth_mode = quota_auth;
        billing
    }

    fn production() -> BillingConfig {
        config(
            PROD_CDOT,
            PROD_COLLECTOR,
            QuotaAuthMode::AdminToken,
            BillingAuthMode::Oidc,
        )
    }

    #[test]
    fn not_enforced_accepts_any_config() {
        let mut billing = AppConfig::embedded_defaults().billing;
        billing.enabled = false;
        billing.quota.enabled = false;
        billing.quota.auth_mode = QuotaAuthMode::LicenseChecksum;
        billing.auth_mode = BillingAuthMode::Oidc;
        assert_eq!(validate_with(false, &billing), Ok(()));
    }

    #[test]
    fn enforced_rejects_disabled_billing() {
        let mut billing = production();
        billing.enabled = false;
        assert_eq!(
            validate_with(true, &billing),
            Err(EnforcementError::BillingDisabled)
        );
    }

    #[test]
    fn enforced_rejects_disabled_quota() {
        let mut billing = production();
        billing.quota.enabled = false;
        assert_eq!(
            validate_with(true, &billing),
            Err(EnforcementError::QuotaDisabled)
        );
    }

    #[test]
    fn enforced_accepts_production_with_either_family() {
        assert_eq!(validate_with(true, &production()), Ok(()));
        let self_managed = config(
            PROD_CDOT,
            PROD_COLLECTOR,
            QuotaAuthMode::LicenseChecksum,
            BillingAuthMode::CloudConnector,
        );
        assert_eq!(validate_with(true, &self_managed), Ok(()));
    }

    #[test]
    fn enforced_accepts_staging_pair() {
        let billing = config(
            STG_CDOT,
            STG_COLLECTOR,
            QuotaAuthMode::AdminToken,
            BillingAuthMode::Oidc,
        );
        assert_eq!(validate_with(true, &billing), Ok(()));
    }

    #[test]
    fn enforced_rejects_mixed_environment_pair() {
        for (cdot, collector) in [(PROD_CDOT, STG_COLLECTOR), (STG_CDOT, PROD_COLLECTOR)] {
            let billing = config(
                cdot,
                collector,
                QuotaAuthMode::AdminToken,
                BillingAuthMode::Oidc,
            );
            assert_eq!(
                validate_with(true, &billing),
                Err(EnforcementError::UnknownEnvironment)
            );
        }
    }

    #[test]
    fn enforced_rejects_lookalike_hosts() {
        for cdot in [
            "https://customers.gitlab.com.evil.io",
            "https://evilcustomers.gitlab.com",
            "http://customers.gitlab.com",
            "https://customers.gitlab.com:8443",
            "https://customers.gitlab.com@evil.io",
            "https://user:pass@customers.gitlab.com",
            "https://customers.gitlab.com/stub",
            "https://customers.gitlab.com/?x=1",
            "https://customers.gitlab.com/#frag",
        ] {
            let billing = config(
                cdot,
                PROD_COLLECTOR,
                QuotaAuthMode::AdminToken,
                BillingAuthMode::Oidc,
            );
            assert_eq!(
                validate_with(true, &billing),
                Err(EnforcementError::UnknownEnvironment),
                "{cdot}"
            );
        }
    }

    #[test]
    fn enforced_ignores_trailing_slash_host_case_and_default_port() {
        let billing = config(
            "https://Customers.GitLab.com:443/",
            "https://BILLING.prdsub.gitlab.net/",
            QuotaAuthMode::AdminToken,
            BillingAuthMode::Oidc,
        );
        assert_eq!(validate_with(true, &billing), Ok(()));
    }

    #[test]
    fn enforced_rejects_mixed_auth_families() {
        for (quota, billing_auth) in [
            (QuotaAuthMode::AdminToken, BillingAuthMode::CloudConnector),
            (QuotaAuthMode::LicenseChecksum, BillingAuthMode::Oidc),
        ] {
            let billing = config(PROD_CDOT, PROD_COLLECTOR, quota, billing_auth);
            assert_eq!(
                validate_with(true, &billing),
                Err(EnforcementError::MixedAuthFamilies {
                    quota,
                    billing: billing_auth
                })
            );
        }
    }

    #[test]
    fn enforced_rejects_unparseable_urls() {
        for (cdot, collector) in [("not a url", PROD_COLLECTOR), (PROD_CDOT, "")] {
            let billing = config(
                cdot,
                collector,
                QuotaAuthMode::AdminToken,
                BillingAuthMode::Oidc,
            );
            assert_eq!(
                validate_with(true, &billing),
                Err(EnforcementError::UnknownEnvironment)
            );
        }
    }

    #[test]
    fn error_message_names_allowed_environments() {
        let message = EnforcementError::UnknownEnvironment.to_string();
        assert!(message.contains("https://customers.gitlab.com"));
        assert!(message.contains("https://billing.stgsub.gitlab.net"));
    }
}
