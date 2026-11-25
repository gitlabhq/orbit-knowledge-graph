mod run_task;

use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use tokio::task::{JoinError, JoinSet};

use crate::builder::{Engine, Registry};
use crate::event::{EngineEvent, Failure, LaneEvent, LaneStep, Observer, TaskStep};
use crate::lane::LaneId;
use crate::lease::{HolderId, LeaseRules, Leases};
use crate::ports::Ports;
use crate::worker::Pools;

const PULL_RETRY: Duration = Duration::from_secs(1);

struct Runner {
    registry: Arc<Registry>,
    pools: Arc<Pools>,
    ports: Ports,
    observer: Arc<dyn Observer>,
    leases: Leases,
}

impl Engine {
    pub async fn serve(
        &self,
        ports: Ports,
        holder: HolderId,
        rules: LeaseRules,
    ) -> anyhow::Result<()> {
        let queue = Arc::clone(&ports.queue);
        tokio::select! {
            () = serve_lanes(self, ports, holder, rules) => Ok(()),
            closed = queue.wait_until_closed() => Err(closed),
        }
    }
}

async fn serve_lanes(engine: &Engine, ports: Ports, holder: HolderId, rules: LeaseRules) {
    let observer = engine.build_observer();
    let pools = Pools::new(
        engine.registry.list_registered_pools().clone(),
        engine.config.pools.clone(),
    );
    let runner = Arc::new(Runner {
        registry: Arc::clone(&engine.registry),
        pools: Arc::new(pools),
        leases: Leases::new(&ports, Arc::clone(&observer), holder, rules),
        ports,
        observer,
    });

    let mut lanes = JoinSet::new();
    for (lane, config) in &engine.config.lanes {
        lanes.spawn(serve_lane(Arc::clone(&runner), lane.clone(), config.slots));
    }
    while let Some(joined) = lanes.join_next().await {
        resume_if_panicked(joined);
    }
}

async fn serve_lane(runner: Arc<Runner>, lane: LaneId, slots: NonZeroU32) {
    let slots = usize::try_from(slots.get()).unwrap_or(usize::MAX);
    let mut runs = JoinSet::new();
    loop {
        let free = slots.saturating_sub(runs.len());
        if free == 0 {
            if let Some(joined) = runs.join_next().await {
                resume_if_panicked(joined);
            }
            continue;
        }

        let now = runner.ports.clock.now().await;
        let deliveries = match runner.ports.queue.pull(&lane, free).await {
            Ok(deliveries) => deliveries,
            Err(error) => {
                let pull_failed = LaneEvent {
                    at: now,
                    lane: lane.clone(),
                    step: LaneStep::PullFailed(Failure::new("pull_failed", error)),
                };
                runner.observer.observe(&EngineEvent::Lane(pull_failed));
                runner.ports.clock.sleep(PULL_RETRY).await;
                continue;
            }
        };

        if deliveries.is_empty() {
            tokio::select! {
                biased;
                Some(joined) = runs.join_next() => resume_if_panicked(joined),
                () = runner.ports.queue.wait_for_ready(&lane) => {}
            }
            continue;
        }

        for delivery in deliveries {
            let delivered = delivery.message.build_event(now, TaskStep::Delivered);
            runner.observer.observe(&delivered);
            runs.spawn(Arc::clone(&runner).run_delivered_task(delivery));
        }
    }
}

pub(crate) fn resume_if_panicked(joined: Result<(), JoinError>) {
    if let Err(error) = joined
        && error.is_panic()
    {
        std::panic::resume_unwind(error.into_panic());
    }
}
