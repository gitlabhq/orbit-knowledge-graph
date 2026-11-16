use std::future::Future;
use std::marker::PhantomData;

use crate::error::PipelineError;
use crate::observer::PipelineObserver;
use crate::types::QueryPipelineContext;

pub trait PipelineStage: Send + Sync {
    type Input: Send + Sync + 'static;
    type Output: Send + Sync + 'static;

    fn execute(
        &self,
        ctx: &mut QueryPipelineContext,
        obs: &mut dyn PipelineObserver,
    ) -> impl Future<Output = Result<Self::Output, PipelineError>> + Send;
}

pub struct PipelineRunner<'a, T> {
    ctx: &'a mut QueryPipelineContext,
    obs: &'a mut dyn PipelineObserver,
    _marker: PhantomData<T>,
}

impl<'a> PipelineRunner<'a, ()> {
    pub fn start(ctx: &'a mut QueryPipelineContext, obs: &'a mut dyn PipelineObserver) -> Self {
        Self::resume(ctx, obs, ())
    }
}

impl<'a, T: Send + Sync + 'static> PipelineRunner<'a, T> {
    /// Starts at the stage that takes `input`, for a caller that ran the stages before it.
    pub fn resume(
        ctx: &'a mut QueryPipelineContext,
        obs: &'a mut dyn PipelineObserver,
        input: T,
    ) -> Self {
        ctx.phases.insert(input);
        Self {
            ctx,
            obs,
            _marker: PhantomData,
        }
    }

    pub async fn then<S>(self, stage: &S) -> Result<PipelineRunner<'a, S::Output>, PipelineError>
    where
        S: PipelineStage<Input = T>,
    {
        let PipelineRunner { ctx, obs, .. } = self;
        let output = stage.execute(ctx, obs).await?;
        ctx.phases.insert(output);
        Ok(PipelineRunner {
            ctx,
            obs,
            _marker: PhantomData,
        })
    }

    pub fn finish(self) -> Option<T> {
        self.ctx.phases.remove::<T>()
    }
}
