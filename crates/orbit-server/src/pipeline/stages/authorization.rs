use std::time::Instant;

use orbit_log_fields::info;
use std::collections::BTreeMap;
use std::sync::Arc;

use orbit_authorization::{AuthError, Caller, CheckRequest, Key};

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
    let mut by_provider: BTreeMap<String, Vec<(CheckRequest, Vec<i64>)>> = BTreeMap::new();
    for resource in resources {
        let request = CheckRequest {
            permission: resource.ability,
            resource: resource.resource_type,
            ids: resource.ids.iter().copied().map(Key::Int).collect(),
        };
        by_provider
            .entry(resource.provider)
            .or_default()
            .push((request, resource.ids));
    }
    let mut authorizations = Vec::new();
    for (provider, batch) in by_provider {
        let (requests, ids): (Vec<CheckRequest>, Vec<Vec<i64>>) = batch.into_iter().unzip();
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
        for ((request, ids), allowed) in requests.into_iter().zip(ids).zip(allowed) {
            authorizations.push(ResourceAuthorization {
                provider: provider.clone(),
                resource_type: request.resource,
                authorized: ids
                    .into_iter()
                    .map(|id| (id, allowed.contains(&Key::Int(id))))
                    .collect(),
            });
        }
    }
    Ok(authorizations)
}
