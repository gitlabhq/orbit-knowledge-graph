mod error;
mod outcome;
mod pool;

use std::sync::Arc;

use async_trait::async_trait;

use crate::lease::Fence;
use crate::payload::{Key, Payload};

pub use error::TaskError;
pub use outcome::Outcome;
pub use pool::{Permit, PoolRequest};

pub(crate) use error::TaskErrorKind;
pub(crate) use outcome::{Completion, EncodedOutcome};
pub(crate) use pool::Pools;

#[async_trait]
pub trait Worker: Send + Sync + 'static {
    const NAME: &'static str;
    const EXCLUSIVE: bool = false;
    type Input: Payload;
    type Output: Payload;

    fn key(input: &Self::Input) -> Key;
    async fn run(
        &self,
        ctx: &TaskContext,
        input: Self::Input,
    ) -> Result<Outcome<Self::Output>, TaskError>;
}

pub struct TaskContext {
    pools: Arc<Pools>,
    fence: Option<Fence>,
}

impl TaskContext {
    pub(crate) fn new(pools: Arc<Pools>, fence: Option<Fence>) -> Self {
        Self { pools, fence }
    }

    pub fn pool(&self, name: &str) -> PoolRequest<'_> {
        PoolRequest::new(&self.pools, name)
    }

    pub fn fence(&self) -> Option<Fence> {
        self.fence
    }
}
