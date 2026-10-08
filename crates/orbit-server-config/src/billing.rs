use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum BillingAuthMode {
    /// GCP workload-identity OIDC. Only works on GitLab.com (SaaS).
    #[default]
    Oidc,
    /// Cloud Connector instance token, pulled from Rails and cached in memory.
    /// Used on Self-Managed / Dedicated.
    CloudConnector,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct BillingConfig {
    pub enabled: bool,
    pub collector_url: String,
    pub auth_mode: BillingAuthMode,
    pub quota: QuotaConfig,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct QuotaConfig {
    pub enabled: bool,
    pub customers_dot_url: String,
    pub auth_mode: QuotaAuthMode,
    pub api_user: Option<String>,
    pub api_token: Option<String>,
    pub request_timeout_ms: u64,
    pub fallback_cache_ttl_secs: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum QuotaAuthMode {
    /// `X-Admin-Email` / `X-Admin-Token` from `api_user` / `api_token`. GitLab.com only.
    AdminToken,
    /// `X-License-Token` from the `license_checksum` JWT claim. Self-managed and Dedicated,
    /// where the CustomersDot admin token must not be deployed.
    LicenseChecksum,
}

impl QuotaConfig {
    /// Startup checks for an enabled quota gate; a disabled gate always passes.
    pub fn validate(&self) -> Result<(), QuotaConfigError> {
        if !self.enabled {
            return Ok(());
        }
        if self.customers_dot_url.trim().is_empty() {
            return Err(QuotaConfigError::MissingCustomersDotUrl);
        }
        if self.auth_mode == QuotaAuthMode::AdminToken
            && (self.api_user.is_none() || self.api_token.is_none())
        {
            return Err(QuotaConfigError::MissingAdminCredentials);
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum QuotaConfigError {
    #[error("billing.quota.enabled=true but billing.quota.customers_dot_url is empty")]
    MissingCustomersDotUrl,

    #[error(
        "billing.quota.auth_mode=admin_token but billing.quota.api_user or api_token is not set \
         (mount them at /etc/secrets/billing/quota/)"
    )]
    MissingAdminCredentials,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AppConfig;

    fn enabled(auth_mode: QuotaAuthMode) -> QuotaConfig {
        QuotaConfig {
            enabled: true,
            auth_mode,
            api_user: None,
            api_token: None,
            ..AppConfig::embedded_defaults().billing.quota
        }
    }

    #[test]
    fn license_checksum_mode_needs_no_admin_credentials() {
        assert_eq!(enabled(QuotaAuthMode::LicenseChecksum).validate(), Ok(()));
    }

    #[test]
    fn admin_token_mode_requires_admin_credentials() {
        assert_eq!(
            enabled(QuotaAuthMode::AdminToken).validate(),
            Err(QuotaConfigError::MissingAdminCredentials)
        );
        let with_creds = QuotaConfig {
            api_user: Some("u".into()),
            api_token: Some("t".into()),
            ..enabled(QuotaAuthMode::AdminToken)
        };
        assert_eq!(with_creds.validate(), Ok(()));
    }

    #[test]
    fn customers_dot_url_required_in_every_mode() {
        for mode in [QuotaAuthMode::AdminToken, QuotaAuthMode::LicenseChecksum] {
            let cfg = QuotaConfig {
                customers_dot_url: " ".into(),
                api_user: Some("u".into()),
                api_token: Some("t".into()),
                ..enabled(mode)
            };
            assert_eq!(
                cfg.validate(),
                Err(QuotaConfigError::MissingCustomersDotUrl)
            );
        }
    }

    #[test]
    fn embedded_default_is_admin_token_and_disabled() {
        let quota = AppConfig::embedded_defaults().billing.quota;
        assert_eq!(quota.auth_mode, QuotaAuthMode::AdminToken);
        assert_eq!(quota.validate(), Ok(()));
    }
}
