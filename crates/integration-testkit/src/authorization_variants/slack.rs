//! A seeded fake of Slack: the sign-in a delegated check asks for, and a check of messages keyed
//! by Slack's string ids.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use orbit_authorization::{
    Answers, AuthError, Check, Credential, Credentials, Key, Module, Need, Provider,
};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SlackOptions {
    /// The sign-in handle the Data Extraction Service holds for each caller subject.
    pub handles: BTreeMap<String, String>,
    /// The message ids each caller subject may read.
    pub readable: BTreeMap<String, Vec<String>>,
}

/// Answers `Credentials` with the caller's Slack sign-in handle and `Check` with the messages
/// the caller may read; a caller without either fails closed.
pub struct SlackSignIn(SlackOptions);

impl Module for SlackSignIn {
    const KIND: &'static str = "fake.slack";
    type Options = SlackOptions;

    fn build(options: SlackOptions) -> Result<Self, AuthError> {
        Ok(Self(options))
    }

    fn answers(self: Arc<Self>, _: &[Need]) -> Result<Answers, AuthError> {
        Ok(Answers {
            credentials: Some(self.clone()),
            check: Some(self),
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

#[async_trait]
impl Provider<Check> for SlackSignIn {
    async fn call(&self, query: Check) -> Result<Vec<BTreeSet<Key>>, AuthError> {
        let readable: BTreeSet<Key> = self
            .0
            .readable
            .get(query.caller.subject())
            .into_iter()
            .flatten()
            .cloned()
            .map(Key::Text)
            .collect();
        Ok(query
            .requests
            .iter()
            .map(|request| {
                request
                    .ids
                    .iter()
                    .filter(|id| readable.contains(id))
                    .cloned()
                    .collect()
            })
            .collect())
    }
}
