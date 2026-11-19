use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::ledger::JobSummary;
use crate::payload::{Key, Tenant};
use crate::planner::Planner;
use crate::worker::Worker;

#[async_trait]
pub trait Reconciler: Send + Sync + 'static {
    const NAME: &'static str;
    const WRITES: &'static [&'static str];
    const EVERY: Duration;

    async fn reconcile(&self, ctx: &ReconcileContext) -> Result<(), ReconcileError>;
}

pub struct ReconcileContext {
    backend: Arc<dyn ReconcileBackend>,
}

pub struct ReconcileError(anyhow::Error);

#[async_trait]
pub(crate) trait ReconcileBackend: Send + Sync {
    async fn last_job(
        &self,
        tenant: &Tenant,
        worker: &'static str,
        key: &Key,
    ) -> anyhow::Result<Option<JobSummary>>;
    async fn replan(
        &self,
        planner: &'static str,
        tenant: &Tenant,
        keys: Vec<Key>,
    ) -> anyhow::Result<()>;
    async fn campaign(&self, planner: &'static str, tag: &str) -> anyhow::Result<()>;
}

impl ReconcileContext {
    pub(crate) fn new(backend: Arc<dyn ReconcileBackend>) -> Self {
        Self { backend }
    }

    pub async fn last_job<W: Worker>(
        &self,
        tenant: &Tenant,
        key: &Key,
    ) -> Result<Option<JobSummary>, ReconcileError> {
        Ok(self.backend.last_job(tenant, W::NAME, key).await?)
    }

    pub async fn replan<P: Planner>(
        &self,
        tenant: &Tenant,
        keys: Vec<Key>,
    ) -> Result<(), ReconcileError> {
        Ok(self.backend.replan(P::NAME, tenant, keys).await?)
    }

    pub async fn campaign<P: Planner>(&self, tag: &str) -> Result<(), ReconcileError> {
        Ok(self.backend.campaign(P::NAME, tag).await?)
    }
}

impl<E: Into<anyhow::Error>> From<E> for ReconcileError {
    fn from(error: E) -> Self {
        Self(error.into())
    }
}

impl fmt::Display for ReconcileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:#}", self.0)
    }
}

impl fmt::Debug for ReconcileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.0, f)
    }
}
