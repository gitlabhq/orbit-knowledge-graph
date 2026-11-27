use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;

use labkit_events::TokenSource;
use labkit_events::oidc::SourceError;
use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue};

pub struct FileTokenSource {
    path: PathBuf,
}

impl FileTokenSource {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

impl TokenSource for FileTokenSource {
    fn enhance_header<'a>(
        &'a self,
        headers: &'a mut HeaderMap,
        _audience: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), SourceError>> + Send + 'a>> {
        Box::pin(async move {
            let contents = tokio::fs::read_to_string(&self.path)
                .await
                .map_err(|error| {
                    SourceError::RequestingOidcToken(format!(
                        "failed to read billing token file: {error}"
                    ))
                })?;
            let token = contents.trim();
            if token.is_empty() {
                return Err(SourceError::RequestingOidcToken(
                    "billing token file is empty".into(),
                ));
            }
            let mut value = HeaderValue::from_str(&format!("Bearer {token}")).map_err(|_| {
                SourceError::RequestingOidcToken(
                    "billing token file contains an invalid authorization header".into(),
                )
            })?;
            value.set_sensitive(true);
            headers.insert(AUTHORIZATION, value);
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn trims_the_token_and_marks_the_header_sensitive() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("token");
        std::fs::write(&path, "  projected-token\n").unwrap();
        let source = FileTokenSource::new(path);
        let mut headers = HeaderMap::new();

        source
            .enhance_header(&mut headers, "billing.example")
            .await
            .unwrap();

        assert_eq!(headers[AUTHORIZATION], "Bearer projected-token");
        assert!(headers[AUTHORIZATION].is_sensitive());
    }

    #[tokio::test]
    async fn missing_file_fails_without_an_authorization_header() {
        let dir = tempfile::tempdir().unwrap();
        let source = FileTokenSource::new(dir.path().join("missing"));
        let mut headers = HeaderMap::new();

        assert!(
            source
                .enhance_header(&mut headers, "billing.example")
                .await
                .is_err()
        );
        assert!(!headers.contains_key(AUTHORIZATION));
    }

    #[tokio::test]
    async fn empty_or_invalid_tokens_fail_without_exposing_the_token() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("token");
        let source = FileTokenSource::new(&path);

        for token in [" \n", "secret-token\r\nInjected: value"] {
            std::fs::write(&path, token).unwrap();
            let mut headers = HeaderMap::new();
            let error = source
                .enhance_header(&mut headers, "billing.example")
                .await
                .unwrap_err();
            assert!(!headers.contains_key(AUTHORIZATION));
            assert!(!error.to_string().contains("secret-token"));
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn follows_projected_volume_symlinks_after_rotation() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        for (version, token) in [("v1", "first"), ("v2", "second")] {
            std::fs::create_dir(dir.path().join(version)).unwrap();
            std::fs::write(dir.path().join(version).join("token"), token).unwrap();
        }
        symlink("v1", dir.path().join("..data")).unwrap();
        symlink("..data/token", dir.path().join("token")).unwrap();
        let source = FileTokenSource::new(dir.path().join("token"));

        for (version, token) in [("v1", "first"), ("v2", "second")] {
            symlink(version, dir.path().join("..data_tmp")).unwrap();
            std::fs::rename(dir.path().join("..data_tmp"), dir.path().join("..data")).unwrap();
            let mut headers = HeaderMap::new();
            source
                .enhance_header(&mut headers, "billing.example")
                .await
                .unwrap();
            assert_eq!(headers[AUTHORIZATION], format!("Bearer {token}"));
        }
    }
}
