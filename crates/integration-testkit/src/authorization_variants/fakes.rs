//! Seeded fakes of authorization systems other than GitLab, each a module the deployment config
//! binds like `gitlab.session`.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use orbit_authorization::{
    Answers, AuthError, Caller, Check, Identify, Module, Need, Provider, ScopeKey, ScopeKind,
    ScopeSet, Scopes,
};
use serde::Deserialize;

/// An OIDC token the transport verified: the subject and its `groups` claim.
#[derive(Debug, Clone)]
pub struct OidcToken {
    pub subject: String,
    pub groups: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NoOptions {}

/// The global scopes a module is asked about.
fn globals(needs: &[Need]) -> BTreeSet<String> {
    needs
        .iter()
        .filter(|need| need.kind == ScopeKind::Global)
        .map(|need| need.scope.clone())
        .collect()
}

/// A global scope holds for a caller with any key, and the keys themselves for other scopes.
fn answer(globals: &BTreeSet<String>, scope: &str, keys: Vec<String>) -> ScopeSet {
    match (globals.contains(scope), keys.is_empty()) {
        (true, false) => ScopeSet::All,
        (true, true) => ScopeSet::Keys(Vec::new()),
        (false, _) => ScopeSet::Keys(keys.into_iter().map(ScopeKey::Text).collect()),
    }
}

/// Identifies callers from an OIDC token and answers flat scopes with its groups, exactly.
pub struct OidcGroups {
    globals: BTreeSet<String>,
}

impl Module for OidcGroups {
    const KIND: &'static str = "fake.oidc";
    type Options = NoOptions;

    fn build(_: NoOptions) -> Result<Self, AuthError> {
        Ok(Self {
            globals: BTreeSet::new(),
        })
    }

    fn answers(self: Arc<Self>, needs: &[Need]) -> Result<Answers, AuthError> {
        if let Some(need) = needs
            .iter()
            .find(|need| !matches!(need.kind, ScopeKind::Flat | ScopeKind::Global))
        {
            return Err(AuthError::Invalid(format!(
                "fake.oidc answers flat and global scopes, not {:?} scope {}",
                need.kind, need.scope
            )));
        }
        let bound = Arc::new(Self {
            globals: globals(needs),
        });
        Ok(Answers {
            identify: Some(bound.clone()),
            scopes: Some(bound),
            scopes_exact: true,
            ..Answers::default()
        })
    }
}

#[async_trait]
impl Provider<Identify> for OidcGroups {
    async fn call(&self, query: Identify) -> Result<Caller, AuthError> {
        let token = query
            .request
            .get::<OidcToken>()
            .ok_or_else(|| AuthError::Denied("the request carries no OIDC token".into()))?;
        Ok(Caller::new(token.subject.clone(), false, token.clone()))
    }
}

#[async_trait]
impl Provider<Scopes> for OidcGroups {
    async fn call(&self, query: Scopes) -> Result<ScopeSet, AuthError> {
        let token = query
            .caller
            .session::<OidcToken>()
            .ok_or_else(|| AuthError::Invalid("the caller carries no OIDC token".into()))?;
        Ok(answer(&self.globals, &query.scope, token.groups.clone()))
    }
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ZanzibarOptions {
    /// The scope keys each subject relates to.
    pub relations: BTreeMap<String, Vec<String>>,
    /// The object ids each subject may read.
    pub allowed: BTreeMap<String, Vec<i64>>,
}

/// Answers `Scopes` with the keys a subject relates to and `Check` with its allowed objects,
/// like `LookupResources` and `CheckBulkPermissions`.
pub struct Zanzibar {
    options: ZanzibarOptions,
    globals: BTreeSet<String>,
}

impl Module for Zanzibar {
    const KIND: &'static str = "fake.zanzibar";
    type Options = ZanzibarOptions;

    fn build(options: ZanzibarOptions) -> Result<Self, AuthError> {
        Ok(Self {
            options,
            globals: BTreeSet::new(),
        })
    }

    fn answers(self: Arc<Self>, needs: &[Need]) -> Result<Answers, AuthError> {
        let bound = Arc::new(Self {
            options: self.options.clone(),
            globals: globals(needs),
        });
        Ok(Answers {
            scopes: Some(bound.clone()),
            check: Some(bound),
            scopes_exact: true,
            ..Answers::default()
        })
    }
}

#[async_trait]
impl Provider<Scopes> for Zanzibar {
    async fn call(&self, query: Scopes) -> Result<ScopeSet, AuthError> {
        let keys = self
            .options
            .relations
            .get(query.caller.subject())
            .into_iter()
            .flatten();
        Ok(answer(&self.globals, &query.scope, keys.cloned().collect()))
    }
}

#[async_trait]
impl Provider<Check> for Zanzibar {
    async fn call(&self, query: Check) -> Result<Vec<BTreeSet<i64>>, AuthError> {
        let allowed: BTreeSet<i64> = self
            .options
            .allowed
            .get(query.caller.subject())
            .into_iter()
            .flatten()
            .copied()
            .collect();
        let pick = |ids: &[i64]| {
            ids.iter()
                .copied()
                .filter(|id| allowed.contains(id))
                .collect()
        };
        Ok(query
            .requests
            .iter()
            .map(|request| pick(&request.ids))
            .collect())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CedarOptions {
    /// The object ids every caller is denied.
    pub denied: Vec<i64>,
}

/// Answers `Check` with a policy that denies the seeded objects to everyone.
pub struct Cedar(CedarOptions);

impl Module for Cedar {
    const KIND: &'static str = "fake.cedar";
    type Options = CedarOptions;

    fn build(options: CedarOptions) -> Result<Self, AuthError> {
        Ok(Self(options))
    }

    fn answers(self: Arc<Self>, _: &[Need]) -> Result<Answers, AuthError> {
        Ok(Answers {
            check: Some(self),
            ..Answers::default()
        })
    }
}

#[async_trait]
impl Provider<Check> for Cedar {
    async fn call(&self, query: Check) -> Result<Vec<BTreeSet<i64>>, AuthError> {
        let allow = |id: &i64| !self.0.denied.contains(id);
        let pick = |ids: &[i64]| ids.iter().copied().filter(allow).collect();
        Ok(query
            .requests
            .iter()
            .map(|request| pick(&request.ids))
            .collect())
    }
}
