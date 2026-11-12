//! The callers the page switches between: GitLab sessions at seeded roles, and subjects of a
//! Zanzibar-style deployment. Each signs in through its deployment's real providers.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use compiler::orbit_authorization::{Caller, CheckRequest, Grants, Key, Request};
use compiler::remote::RowCheck;
use integration_testkit::authorization_variants::fakes::OidcToken;
use integration_testkit::authorization_variants::registry;
use orbit_server::authorization::Deployment;
use orbit_server::authorization::gitlab::session_grants;
use orbit_server_config::AuthorizationConfig;
use query_data_model::Model;
use query_engine::pipeline::{AuthorizedPath, SecurityContext};
use query_engine::shared::dag::{ExecutorError, RowChecks};
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
        user_id: i64,
        paths: Vec<Membership>,
    },
    Zanzibar {
        label: String,
        subject: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Membership {
    pub path: String,
    pub access_level: u32,
}

impl Declared {
    pub fn label(&self) -> &str {
        match self {
            Self::Gitlab { label, .. } | Self::Zanzibar { label, .. } => label,
        }
    }
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

    pub fn checks(&self, log: Arc<RunLog>) -> DeploymentChecks {
        DeploymentChecks {
            deployment: Arc::clone(&self.deployment),
            caller: self.caller.clone(),
            log,
        }
    }
}

/// Answers each remote row check with the provider the check names, as the caller, and logs
/// what each check was asked and allowed.
pub struct DeploymentChecks {
    deployment: Arc<Deployment>,
    caller: Caller,
    log: Arc<RunLog>,
}

#[async_trait]
impl RowChecks for DeploymentChecks {
    async fn allowed(
        &self,
        check: &RowCheck,
        keys: Vec<Key>,
    ) -> Result<BTreeSet<Key>, ExecutorError> {
        let asked = keys.clone();
        let request = CheckRequest {
            permission: check.permission.clone(),
            resource: check.resource.clone(),
            ids: keys,
        };
        let allowed = self
            .deployment
            .check(&self.caller, &check.provider, vec![request])
            .await
            .map_err(|error| ExecutorError(error.to_string()))?
            .pop()
            .unwrap_or_default();
        self.log.push(json!({
            "event": "check",
            "provider": check.provider,
            "permission": check.permission,
            "resource": check.resource,
            "field": check.field,
            "asked": asked,
            "allowed": allowed,
        }));
        Ok(allowed)
    }
}
