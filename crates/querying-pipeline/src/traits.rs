use std::future::Future;
use std::marker::PhantomData;

use crate::error::PipelineError;
use crate::observer::PipelineObserver;
use crate::types::QueryPipelineContext;

pub trait PipelineStage: Send + Sync {
    type Input: Send + Sync + 'static;
    type Output: Send + Sync + 'static;

    /// Stages read prior phase output from `ctx.phases.get::<Self::Input>()`.
    /// Return the stage output — the runner inserts it into phases.
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
        ctx.phases.insert(());
        Self {
            ctx,
            obs,
            _marker: PhantomData,
        }
    }
}

impl<'a, T: Send + Sync + 'static> PipelineRunner<'a, T> {
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
