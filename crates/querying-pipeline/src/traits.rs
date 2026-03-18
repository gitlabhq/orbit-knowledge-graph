use std::future::Future;

use crate::error::PipelineError;
use crate::observer::PipelineObserver;
use crate::types::QueryPipelineContext;

pub trait PipelineStage: Send + Sync {
    type Input: Send;
    type Output: Send;

    fn execute(
        &self,
        input: Self::Input,
        ctx: &mut QueryPipelineContext,
        obs: &mut dyn PipelineObserver,
    ) -> impl Future<Output = Result<Self::Output, PipelineError>> + Send;
}

/// Chains [`PipelineStage`] calls, threading the output of one stage
/// into the input of the next while carrying the shared context and observer.
pub struct PipelineRunner<'a, T> {
    value: T,
    ctx: &'a mut QueryPipelineContext,
    obs: &'a mut dyn PipelineObserver,
}

impl<'a> PipelineRunner<'a, ()> {
    pub fn start(ctx: &'a mut QueryPipelineContext, obs: &'a mut dyn PipelineObserver) -> Self {
        Self {
            value: (),
            ctx,
            obs,
        }
    }
}

impl<'a, T: Send> PipelineRunner<'a, T> {
    pub async fn then<S>(self, stage: &S) -> Result<PipelineRunner<'a, S::Output>, PipelineError>
    where
        S: PipelineStage<Input = T> + Sync,
    {
        let PipelineRunner { value, ctx, obs } = self;
        let output = stage.execute(value, ctx, obs).await?;
        Ok(PipelineRunner {
            value: output,
            ctx,
            obs,
        })
    }

    pub fn finish(self) -> T {
        self.value
    }
}
