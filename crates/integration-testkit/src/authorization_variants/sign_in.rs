//! A seeded fake of a delegated service such as Slack or Jira: the sign-in a delegated check asks
//! for, and a check of rows keyed by the service's string ids.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use orbit_authorization::{
    Answers, AuthError, Check, Credential, Credentials, Key, Module, Need, Provider,
};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignInOptions {
    /// The sign-in handle the Data Extraction Service holds for each caller subject.
    pub handles: BTreeMap<String, String>,
    /// The row ids each caller subject may read.
    pub readable: BTreeMap<String, Vec<String>>,
}

/// Answers `Credentials` with the caller's sign-in handle and `Check` with the rows
/// the caller may read; a caller without either fails closed.
pub struct SignIn(SignInOptions);

impl Module for SignIn {
    const KIND: &'static str = "fake.sign_in";
    type Options = SignInOptions;

    fn build(options: SignInOptions) -> Result<Self, AuthError> {
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
impl Provider<Credentials> for SignIn {
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
impl Provider<Check> for SignIn {
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
