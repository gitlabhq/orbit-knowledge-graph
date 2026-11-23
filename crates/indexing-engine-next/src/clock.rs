use std::time::{Duration, SystemTime};

use async_trait::async_trait;

#[async_trait]
pub(crate) trait Clock: Send + Sync {
    async fn now(&self) -> SystemTime;
    async fn sleep(&self, duration: Duration);
}
