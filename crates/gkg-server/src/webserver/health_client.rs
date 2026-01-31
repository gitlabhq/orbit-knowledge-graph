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
