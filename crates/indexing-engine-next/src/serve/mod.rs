mod lane;
mod run;
mod submit;

use std::sync::Arc;

use tokio::task::{JoinError, JoinSet};

use crate::builder::{Engine, Registry};
use crate::port::Ports;
use crate::protocol::{HolderId, LeaseRules};
use crate::worker::Pools;

pub(crate) use submit::submit_task;

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
        lanes.spawn(lane::serve_lane(
            Arc::clone(&process),
            lane_id.clone(),
            config.slots,
        ));
    }
    while let Some(joined) = lanes.join_next().await {
        resume_panic(joined);
    }
}

pub(crate) fn resume_panic(joined: Result<(), JoinError>) {
    if let Err(error) = joined
        && error.is_panic()
    {
        std::panic::resume_unwind(error.into_panic());
    }
}
