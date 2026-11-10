//! Compiles a GQL query for a connection that is not SQL into its backend's request:
//! parse, plan, security, security check, render.

use std::sync::Arc;

use query_data_model::Model;
use query_data_model::catalog::Render;
use query_data_model::implementations::live::ExtractionRequest;
use query_data_model::implementations::orbit_code::StructuralSearchRequest;
use query_data_model::plan::logical::LogicalPlan;
use serde::Serialize;

pub use crate::passes::plan::remote::{RemotePlan, Target, plan};
pub use crate::passes::remote_security::{Namespaces, check, secure};

use crate::error::{QueryError, Result};
use crate::types::SecurityContext;

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "backend", rename_all = "snake_case")]
pub enum RemoteRequest {
    Code(StructuralSearchRequest),
    Live(ExtractionRequest),
}

pub fn parse(raw: &str, model: &Model, ctx: &SecurityContext) -> Result<LogicalPlan> {
    let catalog = match ctx.admin {
        true => model.logical().as_admin(),
        false => model.logical(),
    };
    Ok(query_data_model::surface::gql::parse(raw, catalog)?.plan)
}

pub fn render(target: &Target, plan: &RemotePlan) -> Result<RemoteRequest> {
    match (target, plan) {
        (Target::Code(code), RemotePlan::Code(plan)) => Ok(RemoteRequest::Code(code.render(plan)?)),
        (Target::Live(live), RemotePlan::Live(plan)) => Ok(RemoteRequest::Live(live.render(plan)?)),
        _ => Err(QueryError::PipelineInvariant(
            "the plan does not match its target".into(),
        )),
    }
}

pub fn compile(
    raw: &str,
    model: &Arc<Model>,
    ctx: &SecurityContext,
    namespaces: &Namespaces,
) -> Result<RemoteRequest> {
    let logical = parse(raw, model, ctx)?;
    let (target, planned) = plan(&logical, model)?;
    let emitted = secure(&target, planned, ctx, namespaces)?;
    check(&emitted)?;
    render(&target, &emitted)
}
