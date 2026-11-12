//! Binds the provider names packages use to the modules the deployment configures, and asks
//! them about each request.

pub mod gitlab;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use orbit_authorization::{
    Answers, AuthError, Caller, Checks, Credential, Credentials, Grants, Identify, Module, Need,
    Provider, Request, ScopeKind, Scopes,
};
use orbit_server_config::AuthorizationConfig;
use query_data_model::Model;
use query_data_model::catalog::{ProviderUse, ScopeKind as DeclaredKind};

type Builder = Box<dyn Fn(serde_json::Value, &[Need]) -> Result<Answers, AuthError> + Send + Sync>;

/// The module kinds a deployment may bind.
#[derive(Default)]
pub struct Registry {
    builders: BTreeMap<&'static str, Builder>,
}

impl Registry {
    #[must_use]
    pub fn with<M: Module>(mut self) -> Self {
        let build = |options: serde_json::Value, needs: &[Need]| {
            let options: M::Options = serde_json::from_value(options)
                .map_err(|error| AuthError::Invalid(format!("{}: {error}", M::KIND)))?;
            Arc::new(M::build(options)?).answers(needs)
        };
        self.builders.insert(M::KIND, Box::new(build));
        self
    }

    /// The modules that wrap what GitLab runs: its session and its Rails abilities.
    pub fn gitlab() -> Self {
        Self::default()
            .with::<gitlab::GitLabSession>()
            .with::<gitlab::RailsAbilities>()
    }
}

/// The providers one deployment binds, checked against one model.
pub struct Deployment {
    identify: Arc<dyn Provider<Identify>>,
    needs: Vec<(Need, Arc<dyn Provider<Scopes>>)>,
    checks: Checks,
    credentials: BTreeMap<String, Arc<dyn Provider<Credentials>>>,
}

#[derive(Debug, thiserror::Error)]
#[error("authorization does not bind the model: {}", .0.join("; "))]
pub struct BindError(pub Vec<String>);

impl Deployment {
    pub fn bind(
        config: &AuthorizationConfig,
        model: &Model,
        registry: &Registry,
    ) -> Result<Self, BindError> {
        let access = model.access();
        let mut errors = Vec::new();
        let mut needs: BTreeMap<&str, Vec<Need>> = BTreeMap::new();
        for (scope, permission) in access.needs() {
            let scope = access.scope(scope);
            needs
                .entry(scope.provider.as_str())
                .or_default()
                .push(Need {
                    scope: scope.name.clone(),
                    kind: kind(scope.kind),
                    permission: access.permission(permission).name.clone(),
                });
        }
        let mut uses: BTreeMap<&str, BTreeSet<ProviderUse>> = BTreeMap::new();
        for (name, used) in access.providers() {
            uses.entry(name).or_default().insert(used);
        }
        uses.entry(config.identify.as_str()).or_default();
        let mut answers = BTreeMap::new();
        for name in uses.keys() {
            let Some(bound) = config.providers.get(*name) else {
                errors.push(format!(
                    "packages name provider {name}, which the deployment does not bind"
                ));
                continue;
            };
            let Some(build) = registry.builders.get(bound.module.as_str()) else {
                errors.push(format!(
                    "provider {name} is bound to unknown module {}",
                    bound.module
                ));
                continue;
            };
            let options = serde_json::Value::Object(bound.options.clone());
            match build(options, needs.get(*name).map_or(&[], Vec::as_slice)) {
                Ok(built) => {
                    answers.insert(*name, (bound.module.as_str(), built));
                }
                Err(error) => errors.push(format!("provider {name}: {error}")),
            }
        }
        let missing = |name: &str, query: &str, errors: &mut Vec<String>| {
            let module = answers.get(name).map_or("", |(module, _)| *module);
            errors.push(format!(
                "provider {name} is bound to module {module}, which does not answer {query}"
            ));
        };
        let identify = answers
            .get(config.identify.as_str())
            .and_then(|(_, a)| a.identify.clone());
        if identify.is_none() && answers.contains_key(config.identify.as_str()) {
            missing(&config.identify, "Identify", &mut errors);
        }
        let mut bound_needs = Vec::new();
        for (name, wanted) in needs {
            match answers.get(name).map(|(_, a)| a.scopes.clone()) {
                Some(Some(scopes)) => {
                    bound_needs.extend(wanted.into_iter().map(|need| (need, scopes.clone())))
                }
                Some(None) => missing(name, "Scopes", &mut errors),
                None => {}
            }
        }
        let mut checks = Checks::default();
        let mut credentials = BTreeMap::new();
        for (name, used) in &uses {
            let Some((_, built)) = answers.get(name) else {
                continue;
            };
            if used.contains(&ProviderUse::Check) {
                match &built.check {
                    Some(check) => checks = checks.with(*name, Arc::clone(check)),
                    None => missing(name, "Check", &mut errors),
                }
            }
            if used.contains(&ProviderUse::Credentials) {
                match &built.credentials {
                    Some(provider) => {
                        credentials.insert(name.to_string(), Arc::clone(provider));
                    }
                    None => missing(name, "Credentials", &mut errors),
                }
            }
        }
        inexact_without_checks(model, &answers, &mut errors);
        match (identify, errors.is_empty()) {
            (Some(identify), true) => Ok(Self {
                identify,
                needs: bound_needs,
                checks,
                credentials,
            }),
            _ => Err(BindError(errors)),
        }
    }

    pub async fn identify(&self, request: Request) -> Result<Caller, AuthError> {
        self.identify.call(Identify { request }).await
    }

    /// Every scope answer the model's reads need, for one caller.
    pub async fn grants(&self, caller: &Caller) -> Result<Grants, AuthError> {
        let mut grants = Grants::new(caller.clone());
        for (need, provider) in &self.needs {
            let query = Scopes {
                caller: caller.clone(),
                scope: need.scope.clone(),
                permission: need.permission.clone(),
            };
            let set = provider.call(query).await?;
            grants = grants.with(&need.scope, &need.permission, set);
        }
        Ok(grants)
    }

    /// The row checks of every provider the packages name.
    pub fn checks(&self) -> &Checks {
        &self.checks
    }

    /// The caller's sign-in for a delegated service, from the provider a check names.
    pub async fn credentials(
        &self,
        caller: &Caller,
        provider: &str,
        service: &str,
    ) -> Result<Credential, AuthError> {
        let signer = self.credentials.get(provider).ok_or_else(|| {
            AuthError::Invalid(format!("no provider {provider} answers Credentials"))
        })?;
        let query = Credentials {
            caller: caller.clone(),
            service: service.to_string(),
        };
        signer.call(query).await
    }
}

/// An entity whose scope answer is not exact must declare a check to confirm its rows.
fn inexact_without_checks(
    model: &Model,
    answers: &BTreeMap<&str, (&str, Answers)>,
    errors: &mut Vec<String>,
) {
    let access = model.access();
    for entity in model.graph().entities() {
        let declared = access.entity(entity.id);
        let provider = &access.scope(declared.scope).provider;
        let exact = answers
            .get(provider.as_str())
            .is_some_and(|(_, a)| a.scopes_exact);
        if !exact && answers.contains_key(provider.as_str()) && declared.checks.is_empty() {
            errors.push(format!(
                "{}'s scope answer from {provider} is not exact, so it needs a check",
                entity.name
            ));
        }
    }
}

pub(crate) fn kind(declared: DeclaredKind) -> ScopeKind {
    match declared {
        DeclaredKind::Hierarchy => ScopeKind::Hierarchy,
        DeclaredKind::Flat => ScopeKind::Flat,
        DeclaredKind::Owner => ScopeKind::Owner,
        DeclaredKind::Global => ScopeKind::Global,
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use orbit_server_config::{AppConfig, ProviderConfig};

    use super::*;

    pub(crate) fn deployment() -> Deployment {
        let model = query_data_model::Packages::embedded().load().unwrap();
        let config = AppConfig::embedded_defaults().authorization;
        Deployment::bind(&config, &model, &Registry::gitlab()).unwrap()
    }

    fn bind_errors(edit: impl FnOnce(&mut AuthorizationConfig)) -> Vec<String> {
        let model = query_data_model::Packages::embedded().load().unwrap();
        let mut config = AppConfig::embedded_defaults().authorization;
        edit(&mut config);
        match Deployment::bind(&config, &model, &Registry::gitlab()) {
            Ok(_) => Vec::new(),
            Err(BindError(errors)) => errors,
        }
    }

    #[test]
    fn the_embedded_model_binds_to_the_default_deployment() {
        deployment();
    }

    #[test]
    fn an_unbound_provider_fails_the_bind() {
        let errors = bind_errors(|config| {
            config.providers.remove("rails");
        });
        assert_eq!(
            errors,
            ["packages name provider rails, which the deployment does not bind"]
        );
    }

    #[test]
    fn a_module_that_does_not_answer_the_query_fails_the_bind() {
        let errors = bind_errors(|config| {
            let session = config.providers["gitlab_session"].clone();
            config.providers.insert("rails".into(), session);
        });
        assert_eq!(
            errors,
            ["provider rails is bound to module gitlab.session, which does not answer Check"]
        );
    }

    #[test]
    fn the_session_maps_every_permission_it_is_asked_about() {
        let errors = bind_errors(|config| {
            let session = config.providers.get_mut("gitlab_session").unwrap();
            session.options.insert(
                "permissions".into(),
                serde_json::json!({"read": "reporter"}),
            );
        });
        assert_eq!(
            errors,
            [
                "provider gitlab_session: authorization provider misconfigured: gitlab.session maps no role to permission read_security"
            ]
        );
        let errors = bind_errors(|config| {
            config.providers.insert(
                "rails".into(),
                ProviderConfig {
                    module: "gitlab.rails".into(),
                    options: Default::default(),
                },
            );
        });
        assert_eq!(
            errors,
            ["provider rails is bound to unknown module gitlab.rails"]
        );
    }
}
