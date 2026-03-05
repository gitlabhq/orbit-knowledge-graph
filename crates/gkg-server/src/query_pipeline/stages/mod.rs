mod authorization;
mod compilation;
mod execution;
mod extraction;
mod formatting;
mod hydration;
mod redaction;
mod security;

use std::future::Future;

use crate::redaction::RedactionMessage;

use super::error::PipelineError;
use super::metrics::PipelineObserver;
use super::types::{PipelineRequest, QueryPipelineContext};

pub trait PipelineStage<M: RedactionMessage>: Send + Sync {
    type Input: Send;
    type Output: Send;

    fn execute(
        &self,
        input: Self::Input,
        ctx: &mut QueryPipelineContext,
        req: &mut PipelineRequest<'_, M>,
        obs: &mut PipelineObserver,
    ) -> impl Future<Output = Result<Self::Output, PipelineError>> + Send;
}

/// Chains [`PipelineStage`] calls, threading the output of one stage
/// into the input of the next while carrying the shared context, request, and observer.
pub struct PipelineRunner<'a, T, M: RedactionMessage> {
    value: T,
    ctx: &'a mut QueryPipelineContext,
    req: PipelineRequest<'a, M>,
    obs: &'a mut PipelineObserver,
}

impl<'a, M: RedactionMessage> PipelineRunner<'a, (), M> {
    pub fn start(
        ctx: &'a mut QueryPipelineContext,
        req: PipelineRequest<'a, M>,
        obs: &'a mut PipelineObserver,
    ) -> Self {
        Self {
            value: (),
            ctx,
            req,
            obs,
        }
    }
}

impl<'a, T, M: RedactionMessage> PipelineRunner<'a, T, M> {
    pub async fn then<S>(self, stage: &S) -> Result<PipelineRunner<'a, S::Output, M>, PipelineError>
    where
        S: PipelineStage<M, Input = T> + Sync,
    {
        let PipelineRunner {
            value,
            ctx,
            mut req,
            obs,
        } = self;
        let output = stage.execute(value, ctx, &mut req, obs).await?;
        Ok(PipelineRunner {
            value: output,
            ctx,
            req,
            obs,
        })
    }

    pub fn finish(self) -> T {
        self.value
    }
}

pub use authorization::AuthorizationStage;
pub use compilation::CompilationStage;
pub use execution::ExecutionStage;
pub use extraction::ExtractionStage;
pub use formatting::FormattingStage;
pub use hydration::HydrationStage;
pub use redaction::RedactionStage;
pub use security::{SecurityError, SecurityStage};
