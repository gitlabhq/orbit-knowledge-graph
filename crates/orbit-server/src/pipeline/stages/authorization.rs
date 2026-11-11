use std::time::Instant;

use orbit_log_fields::info;
use std::collections::BTreeMap;
use std::sync::Arc;

use orbit_authorization::{AuthError, Caller, CheckRequest};

use super::HydrationStage;
use crate::authorization::Deployment;
use crate::redaction::{ResourceAuthorization, ResourceCheck};

use query_engine::pipeline::{
    PipelineError, PipelineObserver, PipelineStage, QueryPipelineContext,
};
use query_engine::shared::{AuthorizationOutput, ExtractionOutput};

#[derive(Clone)]
pub struct AuthorizationStage;

impl PipelineStage for AuthorizationStage {
    type Input = ExtractionOutput;
    type Output = AuthorizationOutput;

    async fn execute(
        &self,
        ctx: &mut QueryPipelineContext,
        obs: &mut dyn PipelineObserver,
    ) -> Result<Self::Output, PipelineError> {
        let mut input = ctx
            .phases
            .remove::<ExtractionOutput>()
            .ok_or_else(|| {
                PipelineError::Authorization("ExtractionOutput not found in phases".into())
            })
            .inspect_err(|e| obs.record_error(e))?;
        let t = Instant::now();

        HydrationStage::resolve_owners(ctx, &mut input.query_result)
            .await
            .inspect_err(|e| obs.record_error(e))?;
        let resources_to_check = input.query_result.resource_checks();
        let authorizations = if resources_to_check.is_empty() {
            info!("No redaction required, returning result directly");
            Vec::new()
        } else {
            check(ctx, resources_to_check)
                .await
                .inspect_err(|e| obs.record_error(e))?
        };

        obs.authorized(t.elapsed());

        Ok(AuthorizationOutput {
            query_result: input.query_result,
            authorizations,
        })
    }
}

/// Asks each provider the checks name about its rows, in one batch per provider.
async fn check(
    ctx: &QueryPipelineContext,
    resources: Vec<ResourceCheck>,
) -> Result<Vec<ResourceAuthorization>, PipelineError> {
    let missing = |what: &str| {
        PipelineError::Authorization(format!("{what} not available in server_extensions"))
    };
    let deployment = ctx
        .server_extensions
        .get::<Arc<Deployment>>()
        .ok_or_else(|| missing("authorization"))?;
    let caller = ctx
        .server_extensions
        .get::<Caller>()
        .ok_or_else(|| missing("caller"))?;
    let mut by_provider: BTreeMap<String, Vec<CheckRequest>> = BTreeMap::new();
    for resource in resources {
        by_provider
            .entry(resource.provider)
            .or_default()
            .push(CheckRequest {
                permission: resource.ability,
                resource: resource.resource_type,
                ids: resource.ids,
            });
    }
    let mut authorizations = Vec::new();
    for (provider, requests) in by_provider {
        let allowed = deployment
            .check(caller, &provider, requests.clone())
            .await
            .map_err(|error| match error {
                AuthError::Unavailable(message) => PipelineError::Streaming(message),
                AuthError::Denied(message) | AuthError::Invalid(message) => {
                    PipelineError::Authorization(message)
                }
                AuthError::NoScopes => PipelineError::NoEnabledNamespaces,
            })?;
        for (request, allowed) in requests.into_iter().zip(allowed) {
            authorizations.push(ResourceAuthorization {
                provider: provider.clone(),
                resource_type: request.resource,
                authorized: request
                    .ids
                    .into_iter()
                    .map(|id| (id, allowed.contains(&id)))
                    .collect(),
            });
        }
    }
    Ok(authorizations)
}
