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
        input: Self::Input,
        ctx: &mut QueryPipelineContext,
        obs: &mut dyn PipelineObserver,
    ) -> impl Future<Output = Result<Self::Output, PipelineError>> + Send;
}

/// Chains [`PipelineStage`] calls. The phantom type `T` tracks what the last
/// stage produced. The runner pulls input from `ctx.phases` and pushes output
/// back, so stages stay pure functions of `(input, ctx) -> output`.
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
        let input = ctx.phases.remove::<T>().ok_or_else(|| {
            PipelineError::Execution(format!(
                "phase data not found: {}",
                std::any::type_name::<T>()
            ))
        })?;
        let output = stage.execute(input, ctx, obs).await?;
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
