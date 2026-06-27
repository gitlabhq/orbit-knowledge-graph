use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use uuid::Uuid;

use crate::campaign::CampaignState;
use crate::nats::NatsServices;
use crate::orchestrator::scheduled::{ScheduledTaskMetrics, TaskError};
use crate::orchestrator::siphon::decoder::decode_logical_replication_events;
use crate::orchestrator::siphon::route::{CdcContext, Route, RouteOutcome};
use crate::orchestrator::{Trigger, TriggerError};
use crate::types::Subscription;
use gkg_server_config::SiphonRouterConfig;

const METRIC_NAME: &str = "dispatch.siphon";
const POLL_INTERVAL: Duration = Duration::from_secs(1);

pub struct Siphon {
    nats: Arc<dyn NatsServices>,
    metrics: ScheduledTaskMetrics,
    config: SiphonRouterConfig,
    campaign: Arc<CampaignState>,
    routes: Vec<Arc<dyn Route>>,
}

impl Siphon {
    pub fn new(
        nats: Arc<dyn NatsServices>,
        metrics: ScheduledTaskMetrics,
        config: SiphonRouterConfig,
        campaign: Arc<CampaignState>,
        routes: Vec<Arc<dyn Route>>,
    ) -> Self {
        Self {
            nats,
            metrics,
            config,
            campaign,
            routes,
        }
    }

    fn subscription_for(&self, route: &dyn Route) -> Subscription {
        let stream = &self.config.events_stream_name;
        Subscription::new(stream.clone(), format!("{stream}.{}", route.source_table()))
            .manage_stream(false)
    }

    pub async fn drain_once(&self) -> Result<RouteOutcome, TaskError> {
        let mut total = RouteOutcome::default();
        for route in &self.routes {
            let outcome = self.drain_route(route.as_ref()).await?;
            total.dispatched += outcome.dispatched;
            total.skipped += outcome.skipped;
        }
        Ok(total)
    }

    async fn drain_route(&self, route: &dyn Route) -> Result<RouteOutcome, TaskError> {
        let subscription = self.subscription_for(route);
        let mut total = RouteOutcome::default();

        loop {
            let messages = self
                .nats
                .consume_pending(&subscription, self.config.batch_size)
                .await
                .map_err(|error| {
                    self.metrics.record_error(METRIC_NAME, "consume");
                    TaskError::new(error)
                })?;

            if messages.is_empty() {
                break;
            }

            let mut decoded = Vec::with_capacity(messages.len());
            for message in &messages {
                decoded.push(
                    decode_logical_replication_events(&message.envelope.payload).map_err(
                        |error| {
                            self.metrics.record_error(METRIC_NAME, "decode");
                            TaskError::new(error)
                        },
                    )?,
                );
            }

            let ctx = CdcContext {
                dispatch_id: Uuid::new_v4(),
                campaign_id: self.campaign.current(),
            };
            let outcome = route.dispatch(&ctx, &decoded).await?;
            total.dispatched += outcome.dispatched;
            total.skipped += outcome.skipped;

            for message in messages {
                message.ack().await.map_err(|error| {
                    self.metrics.record_error(METRIC_NAME, "ack");
                    TaskError::new(error)
                })?;
            }
        }

        Ok(total)
    }
}

#[async_trait]
impl Trigger for Siphon {
    fn name(&self) -> &str {
        METRIC_NAME
    }

    async fn run(self: Box<Self>, cancel: CancellationToken) -> Result<(), TriggerError> {
        let poll = POLL_INTERVAL;

        loop {
            if cancel.is_cancelled() {
                break;
            }

            let start = Instant::now();
            let result = self.drain_once().await;
            let duration = start.elapsed().as_secs_f64();
            let outcome = if result.is_ok() { "success" } else { "error" };
            self.metrics.record_run(METRIC_NAME, outcome, duration);

            match result {
                Ok(total) if total.dispatched > 0 || total.skipped > 0 => {
                    info!(
                        dispatched = total.dispatched,
                        skipped = total.skipped,
                        "siphon drain pass dispatched requests"
                    );
                }
                Ok(_) => {}
                Err(error) => {
                    warn!(%error, "siphon drain pass failed");
                }
            }

            tokio::select! {
                () = cancel.cancelled() => break,
                () = tokio::time::sleep(poll) => {}
            }
        }

        Ok(())
    }
}
