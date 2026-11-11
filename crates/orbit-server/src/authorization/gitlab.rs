//! The GitLab modules: `gitlab.session` identifies callers from the session token Rails issues
//! and answers their scopes, and `gitlab.abilities` checks rows over the Rails redaction call.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use orbit_authorization::{
    Answers, AuthError, Caller, Check, Identify, Key, Module, Need, Provider, ScopeKind, ScopeSet,
    Scopes,
};
use query_engine::pipeline::SecurityContext;
use serde::Deserialize;
use tokio::sync::{Mutex, mpsc};
use tonic::{Status, Streaming};

use crate::auth::authz::SecurityContextError;
use crate::auth::{Claims, build_security_context};
use crate::proto::ExecuteQueryMessage;
use crate::redaction::{RedactionExchangeError, RedactionService, ResourceCheck};

/// The request stream Rails holds open, over which it answers ability checks.
pub struct RailsExchange {
    tx: mpsc::Sender<Result<ExecuteQueryMessage, Status>>,
    stream: Mutex<Streaming<ExecuteQueryMessage>>,
}

impl RailsExchange {
    pub fn new(
        tx: mpsc::Sender<Result<ExecuteQueryMessage, Status>>,
        stream: Streaming<ExecuteQueryMessage>,
    ) -> Self {
        Self {
            tx,
            stream: Mutex::new(stream),
        }
    }
}

/// What the gRPC transport verified: the session's claims and, for a query, its stream.
pub struct VerifiedRequest {
    pub claims: Claims,
    pub exchange: Option<Arc<RailsExchange>>,
}

/// A caller `gitlab.session` identified.
pub struct GitLabCaller {
    pub context: SecurityContext,
    exchange: Option<Arc<RailsExchange>>,
}

/// GitLab access levels; values match `Gitlab::Access` in Rails.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Guest = 10,
    Reporter = 20,
    SecurityManager = 25,
    Developer = 30,
    Maintainer = 40,
    Owner = 50,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionOptions {
    /// The role a caller must hold on a path to hold each permission there.
    pub permissions: BTreeMap<String, Role>,
}

pub struct GitLabSession {
    permissions: BTreeMap<String, Role>,
    kinds: HashMap<String, ScopeKind>,
}

impl GitLabSession {
    pub fn role(&self, permission: &str) -> Option<Role> {
        self.permissions.get(permission).copied()
    }
}

impl Module for GitLabSession {
    const KIND: &'static str = "gitlab.session";
    type Options = SessionOptions;

    fn build(options: SessionOptions) -> Result<Self, AuthError> {
        Ok(Self {
            permissions: options.permissions,
            kinds: HashMap::new(),
        })
    }

    fn answers(self: Arc<Self>, needs: &[Need]) -> Result<Answers, AuthError> {
        let mut kinds = HashMap::new();
        for need in needs {
            if !matches!(need.kind, ScopeKind::Hierarchy | ScopeKind::Global) {
                return Err(AuthError::Invalid(format!(
                    "gitlab.session answers hierarchy and global scopes, not {:?} scope {}",
                    need.kind, need.scope
                )));
            }
            if !self.permissions.contains_key(&need.permission) {
                return Err(AuthError::Invalid(format!(
                    "gitlab.session maps no role to permission {}",
                    need.permission
                )));
            }
            kinds.insert(need.scope.clone(), need.kind);
        }
        let session = Arc::new(Self {
            permissions: self.permissions.clone(),
            kinds,
        });
        Ok(Answers {
            identify: Some(session.clone()),
            scopes: Some(session),
            scopes_exact: true,
            ..Answers::default()
        })
    }
}

#[async_trait]
impl Provider<Identify> for GitLabSession {
    async fn call(&self, query: Identify) -> Result<Caller, AuthError> {
        let request = query.request.get::<VerifiedRequest>().ok_or_else(|| {
            AuthError::Invalid("gitlab.session identifies only verified session requests".into())
        })?;
        let context = build_security_context(&request.claims).map_err(|error| match error {
            SecurityContextError::NoEnabledNamespaces => AuthError::NoScopes,
            other => AuthError::Denied(other.to_string()),
        })?;
        let session = GitLabCaller {
            context,
            exchange: request.exchange.clone(),
        };
        let subject = format!("user:{}", request.claims.user_id);
        Ok(Caller::new(subject, request.claims.admin, session))
    }
}

impl GitLabSession {
    /// The session's paths at the role `permission` maps to; a global scope is all or nothing.
    fn answer(
        &self,
        context: &SecurityContext,
        scope: &str,
        kind: ScopeKind,
        permission: &str,
    ) -> Result<ScopeSet, AuthError> {
        let role = self.role(permission).ok_or_else(|| {
            AuthError::Invalid(format!(
                "gitlab.session was not bound for permission {permission} in scope {scope}"
            ))
        })?;
        let paths: Vec<String> = context
            .paths_at_least(role as u32)
            .into_iter()
            .map(|path| path.as_str().to_string())
            .collect();
        Ok(match kind {
            ScopeKind::Global if !paths.is_empty() => ScopeSet::All,
            _ => ScopeSet::Subtrees(paths),
        })
    }
}

#[async_trait]
impl Provider<Scopes> for GitLabSession {
    async fn call(&self, query: Scopes) -> Result<ScopeSet, AuthError> {
        let session = gitlab_caller(&query.caller)?;
        let kind = self.kinds.get(&query.scope).copied().ok_or_else(|| {
            AuthError::Invalid(format!(
                "gitlab.session was not bound for scope {}",
                query.scope
            ))
        })?;
        self.answer(&session.context, &query.scope, kind, &query.permission)
    }
}

/// The grants the deployment's `gitlab.session` answers for a session that no request carries,
/// as tools and tests compile with.
pub fn session_grants(
    config: &orbit_server_config::AuthorizationConfig,
    model: &query_data_model::Model,
    context: SecurityContext,
) -> Result<orbit_authorization::Grants, AuthError> {
    let bound = config
        .providers
        .get(&config.identify)
        .ok_or_else(|| AuthError::Invalid(format!("no provider {} is bound", config.identify)))?;
    let options = serde_json::from_value(serde_json::Value::Object(bound.options.clone()))
        .map_err(|error| AuthError::Invalid(error.to_string()))?;
    let session = GitLabSession::build(options)?;
    let access = model.access();
    let admin = context.admin;
    let mut answers = Vec::new();
    for (scope, permission) in access.needs() {
        let (scope, permission) = (access.scope(scope), access.permission(permission));
        let kind = super::kind(scope.kind);
        answers.push((
            scope.name.clone(),
            permission.name.clone(),
            session.answer(&context, &scope.name, kind, &permission.name)?,
        ));
    }
    let caller = Caller::new(
        "session",
        admin,
        GitLabCaller {
            context,
            exchange: None,
        },
    );
    Ok(answers.into_iter().fold(
        orbit_authorization::Grants::new(caller),
        |grants, (scope, permission, set)| grants.with(&scope, &permission, set),
    ))
}

fn gitlab_caller(caller: &Caller) -> Result<&GitLabCaller, AuthError> {
    caller
        .session::<GitLabCaller>()
        .ok_or_else(|| AuthError::Invalid("the caller was not identified by gitlab.session".into()))
}

/// Checks rows with the Rails abilities, in one exchange per batch, and fails closed.
pub struct RailsAbilities;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AbilitiesOptions {}

impl Module for RailsAbilities {
    const KIND: &'static str = "gitlab.abilities";
    type Options = AbilitiesOptions;

    fn build(_: AbilitiesOptions) -> Result<Self, AuthError> {
        Ok(Self)
    }

    fn answers(self: Arc<Self>, _: &[Need]) -> Result<Answers, AuthError> {
        Ok(Answers {
            check: Some(self),
            ..Answers::default()
        })
    }
}

#[async_trait]
impl Provider<Check> for RailsAbilities {
    async fn call(&self, query: Check) -> Result<Vec<BTreeSet<Key>>, AuthError> {
        let resources = query
            .requests
            .iter()
            .map(|request| {
                Ok(ResourceCheck {
                    provider: String::new(),
                    resource_type: request.resource.clone(),
                    ids: request.ids.iter().map(rails_id).collect::<Result<_, _>>()?,
                    ability: request.permission.clone(),
                })
            })
            .collect::<Result<Vec<_>, AuthError>>()?;
        let exchange = gitlab_caller(&query.caller)?
            .exchange
            .clone()
            .ok_or_else(|| AuthError::Unavailable("the request holds no Rails stream".into()))?;
        let mut stream = exchange.stream.lock().await;
        let result = RedactionService::request_authorization(&resources, &exchange.tx, &mut stream)
            .await
            .map_err(|error| match error {
                RedactionExchangeError::StreamClosed | RedactionExchangeError::ReceiveFailed(_) => {
                    AuthError::Unavailable(format!("{error:?}"))
                }
                other => AuthError::Denied(format!("{other:?}")),
            })?;
        let mut authorized: HashMap<&str, HashMap<i64, bool>> = HashMap::new();
        for authorization in &result.authorizations {
            authorized
                .entry(authorization.resource_type.as_str())
                .or_default()
                .extend(authorization.authorized.iter().map(|(id, ok)| (*id, *ok)));
        }
        Ok(query
            .requests
            .iter()
            .map(|request| {
                let granted = authorized.get(request.resource.as_str());
                let allowed = |key: &&Key| {
                    matches!(key, Key::Int(id) if granted.and_then(|g| g.get(id)).copied() == Some(true))
                };
                request.ids.iter().filter(allowed).cloned().collect()
            })
            .collect())
    }
}

/// Rails keys its objects by integer id; any other key names no Rails object.
fn rails_id(key: &Key) -> Result<i64, AuthError> {
    match key {
        Key::Int(id) => Ok(*id),
        other => Err(AuthError::Invalid(format!(
            "gitlab.abilities checks integer ids, not {other:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use orbit_authorization::{Caller, CheckRequest};

    use super::*;

    #[tokio::test]
    async fn rails_abilities_fail_closed_on_keys_that_are_not_integer_ids() {
        let check = Check {
            caller: Caller::new("user:1", false, ()),
            requests: vec![CheckRequest {
                permission: "read_message".into(),
                resource: "message".into(),
                ids: vec![Key::Int(1), Key::Text("m1".into())],
            }],
        };
        let error = RailsAbilities.call(check).await.unwrap_err();
        assert!(matches!(error, AuthError::Invalid(message) if message.contains("Text(\"m1\")")));
    }
}
