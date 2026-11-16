//! The callers the page switches between: GitLab sessions at seeded roles, and subjects of a
//! Zanzibar-style deployment. Each signs in through its deployment's real providers.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use compiler::orbit_authorization::{
    AuthError, Caller, Check, Checks, Grants, Key, Provider, Request,
};
use integration_testkit::authorization_variants::fakes::OidcToken;
use integration_testkit::authorization_variants::registry;
use orbit_server::authorization::Deployment;
use orbit_server::authorization::gitlab::session_grants;
use orbit_server_config::AuthorizationConfig;
use query_data_model::Model;
use query_data_model::catalog::ProviderUse;
use query_engine::pipeline::{AuthorizedPath, SecurityContext};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::run::RunLog;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Callers {
    pub gitlab: AuthorizationConfig,
    pub zanzibar: AuthorizationConfig,
    pub callers: BTreeMap<String, Declared>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, tag = "deployment", rename_all = "snake_case")]
pub enum Declared {
    Gitlab {
        label: String,
        role: String,
        user_id: i64,
        paths: Vec<Membership>,
    },
    Zanzibar {
        label: String,
        role: String,
        subject: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Membership {
    pub path: String,
    pub access_level: u32,
}

/// A signed-in caller: the grants its queries compile with, and the deployment that checks its
/// remote rows and holds its sign-ins.
pub struct Session {
    pub grants: Grants,
    pub caller: Caller,
    pub deployment: Arc<Deployment>,
}

impl Callers {
    pub async fn sign_in(&self, name: &str, model: &Model) -> anyhow::Result<Session> {
        let declared = self
            .callers
            .get(name)
            .with_context(|| format!("no caller {name}"))?;
        let bind = |config: &AuthorizationConfig| {
            Deployment::bind(config, model, &registry()).map(Arc::new)
        };
        match declared {
            Declared::Gitlab { user_id, paths, .. } => {
                let deployment = bind(&self.gitlab)?;
                let paths = paths
                    .iter()
                    .map(|member| AuthorizedPath::new(member.path.as_str(), member.access_level))
                    .collect();
                let context = SecurityContext::new_with_roles(*user_id, paths)
                    .map_err(|error| anyhow!("{name}: {error}"))?;
                let grants = session_grants(&self.gitlab, model, context)?;
                let caller = Caller::new(format!("user:{user_id}"), false, ());
                Ok(Session {
                    grants,
                    caller,
                    deployment,
                })
            }
            Declared::Zanzibar { subject, .. } => {
                let deployment = bind(&self.zanzibar)?;
                let token = OidcToken {
                    subject: subject.clone(),
                    groups: Vec::new(),
                };
                let caller = deployment.identify(Request::new(token)).await?;
                let grants = deployment.grants(&caller).await?;
                Ok(Session {
                    grants,
                    caller,
                    deployment,
                })
            }
        }
    }
}

impl Session {
    /// The caller's sign-in handle at the Slack the live connection calls.
    pub async fn slack_handle(&self) -> anyhow::Result<String> {
        let credential = self
            .deployment
            .credentials(&self.caller, "slack", "slack")
            .await?;
        Ok(credential.handle)
    }

    /// The deployment's checks for every provider the model checks rows with, each logging what
    /// it was asked and what it allowed.
    pub fn checks(&self, model: &Model, log: Arc<RunLog>) -> Checks {
        model
            .access()
            .providers()
            .into_iter()
            .filter(|(_, used)| *used == ProviderUse::Check)
            .fold(Checks::default(), |checks, (name, _)| {
                let logged = Logged {
                    deployment: Arc::clone(&self.deployment),
                    caller: self.caller.clone(),
                    provider: name.to_string(),
                    log: Arc::clone(&log),
                };
                checks.with(name, Arc::new(logged))
            })
    }
}

/// Asks as the signed-in caller: a GitLab session's grants carry a placeholder caller, and its
/// checks must reach the provider as the session's user.
struct Logged {
    deployment: Arc<Deployment>,
    caller: Caller,
    provider: String,
    log: Arc<RunLog>,
}

#[async_trait]
impl Provider<Check> for Logged {
    async fn call(&self, query: Check) -> Result<Vec<BTreeSet<Key>>, AuthError> {
        let asked: Vec<(String, String, Vec<Key>)> = query
            .requests
            .iter()
            .map(|request| {
                (
                    request.permission.clone(),
                    request.resource.clone(),
                    request.ids.clone(),
                )
            })
            .collect();
        let allowed = self
            .deployment
            .checks()
            .check(&self.caller, &self.provider, query.requests)
            .await?;
        for ((permission, resource, ids), kept) in asked.iter().zip(&allowed) {
            self.log.push(json!({
                "event": "check",
                "provider": self.provider,
                "permission": permission,
                "resource": resource,
                "asked": ids,
                "allowed": kept,
            }));
        }
        Ok(allowed)
    }
}
