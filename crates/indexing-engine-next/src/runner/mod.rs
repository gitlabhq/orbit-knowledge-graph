mod run_task;

use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use tokio::task::{JoinError, JoinSet};

use crate::builder::{Engine, Registry};
use crate::event::ProtocolEvent;
use crate::lease::{HolderId, LeaseRules, Leases};
use crate::ports::Ports;
use crate::worker::Pools;

const PULL_RETRY: Duration = Duration::from_secs(1);

struct Runner {
    registry: Arc<Registry>,
    pools: Arc<Pools>,
    ports: Ports,
    leases: Leases,
}

pub(crate) async fn serve(engine: &Engine, ports: Ports, holder: HolderId, rules: LeaseRules) {
    let pools = Pools::new(
        engine.registry.list_registered_pools().clone(),
        engine.config.pools.clone(),
    );
    let runner = Arc::new(Runner {
        registry: Arc::clone(&engine.registry),
        pools: Arc::new(pools),
        leases: Leases::new(&ports, holder, rules),
        ports,
    });

    let mut lanes = JoinSet::new();
    for (lane_id, config) in &engine.config.lanes {
        lanes.spawn(serve_lane(
            Arc::clone(&runner),
            lane_id.clone(),
            config.slots,
        ));
    }
    while let Some(joined) = lanes.join_next().await {
        resume_if_panicked(joined);
    }
}

async fn serve_lane(runner: Arc<Runner>, lane_id: String, slots: NonZeroU32) {
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
        let deliveries = match runner.ports.tasks.pull(&lane_id, free).await {
            Ok(deliveries) => deliveries,
            Err(error) => {
                orbit_log_fields::warn!(
                    safe(ERROR_TYPE) = "pull_failed",
                    expanded(ERROR_MESSAGE) = %error,
                    "the pull of the lane failed"
                );
                runner.ports.clock.sleep_until(now + PULL_RETRY).await;
                continue;
            }
        };
        if deliveries.is_empty() {
            tokio::select! {
                biased;
                Some(joined) = runs.join_next() => resume_if_panicked(joined),
                () = runner.ports.tasks.wait_for_ready(&lane_id) => {}
            }
            continue;
        }

        for delivery in deliveries {
            runner
                .ports
                .log
                .record(now, ProtocolEvent::Delivered(delivery.message.task));
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
