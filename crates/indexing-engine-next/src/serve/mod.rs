mod run;
mod submit;

use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use tokio::task::{JoinError, JoinSet};

use crate::builder::{Engine, Registry};
use crate::port::Ports;
use crate::protocol::{HolderId, LeaseRules, ProtocolEvent};
use crate::worker::Pools;

pub(crate) use submit::submit_task;

const PULL_RETRY: Duration = Duration::from_secs(1);

pub(crate) struct Process {
    registry: Arc<Registry>,
    pools: Arc<Pools>,
    ports: Ports,
    holder: HolderId,
    rules: LeaseRules,
}

pub(crate) async fn serve(engine: &Engine, ports: Ports, holder: HolderId, rules: LeaseRules) {
    let pools = Pools::new(
        engine.registry.list_registered_pools().clone(),
        engine.config.pools.clone(),
    );
    let process = Arc::new(Process {
        registry: Arc::clone(&engine.registry),
        pools: Arc::new(pools),
        ports,
        holder,
        rules,
    });

    let mut lanes = JoinSet::new();
    for (lane_id, config) in &engine.config.lanes {
        lanes.spawn(serve_lane(
            Arc::clone(&process),
            lane_id.clone(),
            config.slots,
        ));
    }
    while let Some(joined) = lanes.join_next().await {
        resume_if_panicked(joined);
    }
}

async fn serve_lane(process: Arc<Process>, lane_id: String, slots: NonZeroU32) {
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

        let now = process.ports.clock.now().await;
        let deliveries = match process.ports.tasks.pull(&lane_id, free).await {
            Ok(deliveries) => deliveries,
            Err(error) => {
                orbit_log_fields::warn!(
                    safe(ERROR_TYPE) = "pull_failed",
                    expanded(ERROR_MESSAGE) = %error,
                    "the pull of the lane failed"
                );
                process.ports.clock.sleep_until(now + PULL_RETRY).await;
                continue;
            }
        };
        if deliveries.is_empty() {
            tokio::select! {
                biased;
                Some(joined) = runs.join_next() => resume_if_panicked(joined),
                () = process.ports.tasks.wait_for_ready(&lane_id) => {}
            }
            continue;
        }

        for delivery in deliveries {
            process
                .ports
                .log
                .record(now, ProtocolEvent::Delivered(delivery.message.task));
            runs.spawn(Arc::clone(&process).run_delivered_task(delivery));
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
