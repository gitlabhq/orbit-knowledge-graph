//! Drives the real SDLC handlers under the real worker pool.
//!
//! `Engine::run_handlers` fans one namespace message out to every handler
//! registered on the namespace subscription and gates each on a worker-pool
//! slot. That fan-out, not the message loop, is what sets backfill peak memory,
//! so it is reproduced here rather than stood up behind NATS.

use std::sync::Arc;
use std::time::Instant;

use indexer::IndexerConfig;
use indexer::analytics::IndexingAnalytics;
use indexer::clickhouse::ClickHouseWriter;
use indexer::handler::{HandlerContext, HandlerRegistry};
use indexer::indexing_status::IndexingStatusStore;
use indexer::metrics::EngineMetrics;
use indexer::nats::ProgressNotifier;
use indexer::testkit::{MockLockService, MockNatsServices};
use indexer::types::{Envelope, Subscription};
use indexer::worker_pool::WorkerPool;

#[derive(serde::Serialize)]
pub struct HandlerRun {
    pub handler: String,
    pub ms: u128,
    pub error: Option<String>,
}

pub struct Harness {
    registry: Arc<HandlerRegistry>,
    worker_pool: Arc<WorkerPool>,
    context: HandlerContext,
}

impl Harness {
    pub async fn new(
        config: &IndexerConfig,
        ontology: &ontology::Ontology,
        partition_min_rows: u64,
    ) -> anyhow::Result<Self> {
        let metrics = Arc::new(EngineMetrics::default());
        let writer = Arc::new(ClickHouseWriter::new(
            config.graph.clone(),
            metrics.clone(),
        )?);
        let registry = Arc::new(HandlerRegistry::default());
        indexer::modules::sdlc::register_handlers(
            &registry,
            config,
            ontology,
            writer,
            IndexingAnalytics::disabled(),
            partition_min_rows,
        )
        .await
        .map_err(|error| anyhow::anyhow!("registering SDLC handlers: {error}"))?;

        let nats = Arc::new(MockNatsServices::new());
        let context = HandlerContext::new(
            nats.clone(),
            Arc::new(MockLockService::new()),
            ProgressNotifier::noop(),
            Arc::new(IndexingStatusStore::new(nats)),
        );

        Ok(Self {
            registry,
            worker_pool: Arc::new(WorkerPool::new(&config.engine, metrics)),
            context,
        })
    }

    pub fn handler_count(&self, subscription: &Subscription) -> usize {
        self.registry.handlers_for(subscription).len()
    }

    /// Mirrors `Engine::run_handlers`: one task per handler, each awaiting its
    /// own slot, so the concurrency group is what bounds in-flight pages.
    pub async fn dispatch(
        &self,
        subscription: &Subscription,
        envelope: Envelope,
    ) -> Vec<HandlerRun> {
        let group = subscription.concurrency_group.clone();
        let mut tasks = tokio::task::JoinSet::new();

        for handler in self.registry.handlers_for(subscription) {
            let context = self.context.clone();
            let envelope = envelope.clone();
            let worker_pool = self.worker_pool.clone();
            let group = group.clone();

            tasks.spawn(async move {
                let _slot = worker_pool.acquire_handler_slot(group.as_deref()).await;
                let started = Instant::now();
                let result = handler.handle(context, envelope).await;
                HandlerRun {
                    handler: handler.name().to_string(),
                    ms: started.elapsed().as_millis(),
                    error: result.err().map(|error| error.to_string()),
                }
            });
        }

        let mut runs = Vec::new();
        while let Some(joined) = tasks.join_next().await {
            match joined {
                Ok(run) => runs.push(run),
                Err(error) => runs.push(HandlerRun {
                    handler: "<panicked>".to_string(),
                    ms: 0,
                    error: Some(error.to_string()),
                }),
            }
        }
        runs.sort_by_key(|run| std::cmp::Reverse(run.ms));
        runs
    }
}
