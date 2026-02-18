use health_check::HealthStatus;
use tracing::warn;

#[derive(Clone)]
pub struct InfrastructureHealthClient {
    url: String,
    client: reqwest::Client,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("HTTP request failed: {0}")]
    Request(#[from] reqwest::Error),
}

impl InfrastructureHealthClient {
    pub fn new(url: String) -> Self {
        Self {
            url,
            client: reqwest::Client::new(),
        }
    }

    pub async fn check(&self) -> Result<HealthStatus, Error> {
        let response = self
            .client
            .get(format!("{}/health", self.url))
            .send()
            .await?
            .json()
            .await?;

        Ok(response)
    }

    pub async fn check_or_unavailable(&self) -> HealthStatus {
        match self.check().await {
            Ok(status) => status,
            Err(e) => {
                warn!(error = %e, "Failed to reach health-check service");
                HealthStatus {
                    status: health_check::Status::Unhealthy,
                    services: vec![],
                    clickhouse: health_check::ComponentHealth {
                        status: health_check::Status::Unhealthy,
                        error: Some(format!("Health-check service unreachable: {}", e)),
                    },
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn install_crypto_provider() {
        let _ = rustls::crypto::ring::default_provider().install_default();
    }

    #[test]
    fn client_construction_does_not_panic() {
        install_crypto_provider();
        let client = InfrastructureHealthClient::new("http://localhost:9999".to_string());
        assert_eq!(client.url, "http://localhost:9999");
    }

    #[tokio::test]
    async fn check_or_unavailable_returns_unhealthy_for_unreachable_service() {
        install_crypto_provider();
        let client = InfrastructureHealthClient::new("http://127.0.0.1:1".to_string());
        let status = client.check_or_unavailable().await;

        assert_eq!(status.status, health_check::Status::Unhealthy);
        assert!(status.clickhouse.error.is_some());
        assert!(
            status
                .clickhouse
                .error
                .as_ref()
                .unwrap()
                .contains("unreachable")
        );
    }

    #[tokio::test]
    async fn check_returns_error_for_unreachable_service() {
        install_crypto_provider();
        let client = InfrastructureHealthClient::new("http://127.0.0.1:1".to_string());
        assert!(client.check().await.is_err());
    }
}
