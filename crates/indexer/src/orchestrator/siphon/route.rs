use async_trait::async_trait;
use siphon_proto::LogicalReplicationEvents;
use uuid::Uuid;

use crate::orchestrator::scheduled::TaskError;

pub struct CdcContext {
    pub dispatch_id: Uuid,
    pub campaign_id: Option<String>,
}

#[derive(Default)]
pub struct RouteOutcome {
    pub dispatched: u64,
    pub skipped: u64,
}

#[async_trait]
pub trait Route: Send + Sync {
    fn source_table(&self) -> &str;

    async fn dispatch(
        &self,
        ctx: &CdcContext,
        events: &[LogicalReplicationEvents],
    ) -> Result<RouteOutcome, TaskError>;
}
