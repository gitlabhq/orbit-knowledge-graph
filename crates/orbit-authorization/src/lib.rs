//! The authorization contract: the queries a provider answers, the modules that build providers
//! from deployment options, and the answers the query engine's security phase enforces.

use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use serde::de::DeserializeOwned;

pub trait Query: Send + 'static {
    type Response: Send;
}

#[async_trait]
pub trait Provider<Q: Query>: Send + Sync {
    async fn call(&self, query: Q) -> Result<Q::Response, AuthError>;
}

/// Who sent this request.
pub struct Identify {
    pub request: Request,
}

/// Which scope keys the caller holds `permission` in.
pub struct Scopes {
    pub caller: Caller,
    pub scope: String,
    pub permission: String,
}

/// Which of these rows the caller may use, one answer per request, in one batch.
pub struct Check {
    pub caller: Caller,
    pub requests: Vec<CheckRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckRequest {
    pub permission: String,
    /// The provider's name for the type of the checked objects.
    pub resource: String,
    pub ids: Vec<Key>,
}

/// The caller's sign-in for a delegated service.
pub struct Credentials {
    pub caller: Caller,
    pub service: String,
}

impl Query for Identify {
    type Response = Caller;
}

impl Query for Scopes {
    type Response = ScopeSet;
}

impl Query for Check {
    type Response = Vec<BTreeSet<Key>>;
}

impl Query for Credentials {
    type Response = Credential;
}

/// What the transport verified, in the form the identify module reads.
#[derive(Clone)]
pub struct Request(Arc<dyn Any + Send + Sync>);

impl Request {
    pub fn new<T: Any + Send + Sync>(verified: T) -> Self {
        Self(Arc::new(verified))
    }

    pub fn get<T: Any>(&self) -> Option<&T> {
        self.0.downcast_ref()
    }
}

/// The identified caller; `session` is what the identify module needs to answer later queries.
#[derive(Clone)]
pub struct Caller {
    subject: String,
    admin: bool,
    session: Arc<dyn Any + Send + Sync>,
}

impl Caller {
    pub fn new<T: Any + Send + Sync>(subject: impl Into<String>, admin: bool, session: T) -> Self {
        Self {
            subject: subject.into(),
            admin,
            session: Arc::new(session),
        }
    }

    pub fn subject(&self) -> &str {
        &self.subject
    }

    pub fn admin(&self) -> bool {
        self.admin
    }

    pub fn session<T: Any>(&self) -> Option<&T> {
        self.session.downcast_ref()
    }
}

impl fmt::Debug for Caller {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Caller")
            .field("subject", &self.subject)
            .field("admin", &self.admin)
            .finish_non_exhaustive()
    }
}

/// The scope keys a caller holds a permission in. An empty `Keys` or `Subtrees` grants nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScopeSet {
    All,
    Keys(Vec<Key>),
    Subtrees(Vec<String>),
}

impl ScopeSet {
    pub fn is_empty(&self) -> bool {
        match self {
            Self::All => false,
            Self::Keys(keys) => keys.is_empty(),
            Self::Subtrees(roots) => roots.is_empty(),
        }
    }
}

/// The key of a scope or an object, in the type its declaring property holds.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Key {
    Int(i64),
    Text(String),
    Uuid(String),
    Composite(Vec<Key>),
}

/// A handle to the caller's sign-in; the delegated service holds the secret.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Credential {
    pub service: String,
    pub handle: String,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AuthError {
    #[error("authorization denied: {0}")]
    Denied(String),
    #[error("authorization provider unavailable: {0}")]
    Unavailable(String),
    #[error("the caller holds no scope")]
    NoScopes,
    #[error("authorization provider misconfigured: {0}")]
    Invalid(String),
}

/// The rule a scope kind gives; a module declares which kinds it can answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ScopeKind {
    Hierarchy,
    Flat,
    Owner,
    Global,
}

/// One scope and permission the model asks a provider about.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Need {
    pub scope: String,
    pub kind: ScopeKind,
    pub permission: String,
}

/// The queries one built module answers.
#[derive(Clone, Default)]
pub struct Answers {
    pub identify: Option<Arc<dyn Provider<Identify>>>,
    pub scopes: Option<Arc<dyn Provider<Scopes>>>,
    pub check: Option<Arc<dyn Provider<Check>>>,
    pub credentials: Option<Arc<dyn Provider<Credentials>>>,
    /// The `Scopes` answers name exactly the scopes the caller holds, so no check must confirm them.
    pub scopes_exact: bool,
}

pub trait Module: Sized + Send + Sync + 'static {
    const KIND: &'static str;
    type Options: DeserializeOwned;

    fn build(options: Self::Options) -> Result<Self, AuthError>;

    /// The queries this module answers, once its options cover every scope it will be asked.
    fn answers(self: Arc<Self>, needs: &[Need]) -> Result<Answers, AuthError>;
}

/// The `Scopes` answers for one request, by scope and permission.
#[derive(Debug, Clone)]
pub struct Grants {
    caller: Caller,
    scopes: BTreeMap<(String, String), ScopeSet>,
}

impl Grants {
    pub fn new(caller: Caller) -> Self {
        Self {
            caller,
            scopes: BTreeMap::new(),
        }
    }

    pub fn with(mut self, scope: &str, permission: &str, set: ScopeSet) -> Self {
        self.scopes
            .insert((scope.to_string(), permission.to_string()), set);
        self
    }

    pub fn caller(&self) -> &Caller {
        &self.caller
    }

    /// No scope was answered, which means the caller was never resolved.
    pub fn is_empty(&self) -> bool {
        self.scopes.is_empty()
    }

    /// The answer for a scope and permission; one nobody asked for fails closed.
    pub fn scope_set(&self, scope: &str, permission: &str) -> Result<&ScopeSet, AuthError> {
        self.scopes
            .get(&(scope.to_string(), permission.to_string()))
            .ok_or_else(|| {
                AuthError::Invalid(format!(
                    "no scope answer for permission {permission} in scope {scope}"
                ))
            })
    }
}
