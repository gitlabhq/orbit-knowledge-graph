//! The [`Route`] contract: one CDC source table mapped to a dispatch action.

use async_trait::async_trait;
use siphon_proto::LogicalReplicationEvents;
use uuid::Uuid;

use crate::orchestrator::scheduled::TaskError;

/// Per-drain-pass correlation context shared by every route.
pub struct CdcContext {
    pub dispatch_id: Uuid,
    pub campaign_id: Option<String>,
}

#[derive(Default)]
pub struct RouteOutcome {
    pub dispatched: u64,
    pub skipped: u64,
}

/// A reactive consumer of one Siphon CDC source table. The [`Siphon`](super::Siphon)
/// trigger owns the NATS plumbing (consume, decode, ack); a route only decides
/// what to dispatch for a batch of already-decoded replication events.
#[async_trait]
pub trait Route: Send + Sync {
    fn source_table(&self) -> &str;

    async fn dispatch(
        &self,
        ctx: &CdcContext,
        events: &[LogicalReplicationEvents],
    ) -> Result<RouteOutcome, TaskError>;
}
