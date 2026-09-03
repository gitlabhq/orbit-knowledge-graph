use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use gitlab_client::CloudConnectorTokenCache;
use labkit_events::TokenSource;
use labkit_events::oidc::SourceError;
use reqwest::header::{AUTHORIZATION, HeaderMap};

/// labkit `TokenSource` that authenticates billing emission with the Cloud
/// Connector instance token on Self-Managed / Dedicated. The token is pulled
/// from Rails and cached by [`CloudConnectorTokenCache`]; this type only reads
/// the cache and injects the `Authorization` header.
pub struct CloudConnectorTokenSource {
    cache: Arc<CloudConnectorTokenCache>,
}

impl CloudConnectorTokenSource {
    pub fn new(cache: Arc<CloudConnectorTokenCache>) -> Self {
        Self { cache }
    }
}

impl TokenSource for CloudConnectorTokenSource {
    fn enhance_header<'a>(
        &'a self,
        headers: &'a mut HeaderMap,
        _audience: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), SourceError>> + Send + 'a>> {
        Box::pin(async move {
            let token = self
                .cache
                .token()
                .await
                .map_err(|e| SourceError::RequestingOidcToken(e.to_string()))?;
            let value = format!("Bearer {token}").parse().map_err(
                |e: reqwest::header::InvalidHeaderValue| SourceError::TokenCache(e.to_string()),
            )?;
            headers.insert(AUTHORIZATION, value);
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;

    use gitlab_client::{CloudConnectorToken, CloudConnectorTokenFetcher, GitlabClientError};

    use super::*;

    struct StubFetcher;

    impl CloudConnectorTokenFetcher for StubFetcher {
        fn fetch(
            &self,
        ) -> Pin<Box<dyn Future<Output = Result<CloudConnectorToken, GitlabClientError>> + Send + '_>>
        {
            Box::pin(async {
                Ok(CloudConnectorToken {
                    token: "cc-token-abc".into(),
                    expires_at: chrono_now() + 3_600,
                })
            })
        }
    }

    fn chrono_now() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
    }

    #[tokio::test]
    async fn enhance_header_injects_bearer_token() {
        let cache = Arc::new(CloudConnectorTokenCache::new(Arc::new(StubFetcher)));
        let source = CloudConnectorTokenSource::new(cache);

        let mut headers = HeaderMap::new();
        source
            .enhance_header(&mut headers, "billing.stgsub.gitlab.net")
            .await
            .unwrap();

        assert_eq!(headers.get(AUTHORIZATION).unwrap(), "Bearer cc-token-abc");
    }

    #[tokio::test]
    async fn enhance_header_surfaces_fetch_failure() {
        struct FailingFetcher;
        impl CloudConnectorTokenFetcher for FailingFetcher {
            fn fetch(
                &self,
            ) -> Pin<
                Box<
                    dyn Future<Output = Result<CloudConnectorToken, GitlabClientError>> + Send + '_,
                >,
            > {
                Box::pin(async { Err(GitlabClientError::Unauthorized) })
            }
        }

        let cache = Arc::new(CloudConnectorTokenCache::new(Arc::new(FailingFetcher)));
        let source = CloudConnectorTokenSource::new(cache);

        let mut headers = HeaderMap::new();
        let result = source
            .enhance_header(&mut headers, "billing.stgsub.gitlab.net")
            .await;

        assert!(result.is_err());
        assert!(headers.get(AUTHORIZATION).is_none());
    }
}
