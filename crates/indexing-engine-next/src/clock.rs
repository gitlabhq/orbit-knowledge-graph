use std::time::{Duration, SystemTime};

use async_trait::async_trait;

#[async_trait]
pub trait Clock: Send + Sync {
    async fn now(&self) -> SystemTime;
    async fn sleep(&self, duration: Duration);
}

pub struct SystemClock;

#[async_trait]
impl Clock for SystemClock {
    async fn now(&self) -> SystemTime {
        SystemTime::now()
    }

    async fn sleep(&self, duration: Duration) {
        tokio::time::sleep(duration).await;
    }
}
