//! A seeded fake of the Slack sign-in a delegated check asks for.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use orbit_authorization::{Answers, AuthError, Credential, Credentials, Module, Need, Provider};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SlackOptions {
    /// The sign-in handle the Data Extraction Service holds for each caller subject.
    pub handles: BTreeMap<String, String>,
}

/// Answers `Credentials` with the caller's Slack sign-in handle; a caller without one fails
/// closed.
pub struct SlackSignIn(SlackOptions);

impl Module for SlackSignIn {
    const KIND: &'static str = "fake.slack";
    type Options = SlackOptions;

    fn build(options: SlackOptions) -> Result<Self, AuthError> {
        Ok(Self(options))
    }

    fn answers(self: Arc<Self>, _: &[Need]) -> Result<Answers, AuthError> {
        Ok(Answers {
            credentials: Some(self),
            ..Answers::default()
        })
    }
}

#[async_trait]
impl Provider<Credentials> for SlackSignIn {
    async fn call(&self, query: Credentials) -> Result<Credential, AuthError> {
        let handle = self.0.handles.get(query.caller.subject()).ok_or_else(|| {
            AuthError::Denied(format!(
                "{} has no {} sign-in",
                query.caller.subject(),
                query.service
            ))
        })?;
        Ok(Credential {
            service: query.service,
            handle: handle.clone(),
        })
    }
}
