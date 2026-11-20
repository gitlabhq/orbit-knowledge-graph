use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinSet;

use crate::port::Delivery;
use crate::protocol::ProtocolEvent;
use crate::serve::{Process, resume_panic};

const PULL_RETRY: Duration = Duration::from_secs(1);

pub(super) async fn serve_lane(process: Arc<Process>, lane_id: String, slots: NonZeroU32) {
    let slots = usize::try_from(slots.get()).unwrap_or(usize::MAX);
    let mut runs = JoinSet::new();
    loop {
        let free = slots.saturating_sub(runs.len());
        if free == 0 {
            if let Some(joined) = runs.join_next().await {
                resume_panic(joined);
            }
            continue;
        }

        let deliveries = process.deliver(&lane_id, free).await;
        if deliveries.is_empty() {
            tokio::select! {
                biased;
                Some(joined) = runs.join_next() => resume_panic(joined),
                () = process.ports.tasks.wait_for_ready(&lane_id) => {}
            }
            continue;
        }
        for delivery in deliveries {
            runs.spawn(Arc::clone(&process).run_delivery(delivery));
        }
    }
}

impl Process {
    async fn deliver(&self, lane_id: &str, max: usize) -> Vec<Delivery> {
        let now = self.ports.clock.now().await;
        match self.ports.tasks.pull(lane_id, max).await {
            Ok(deliveries) => {
                for delivery in &deliveries {
                    self.ports
                        .log
                        .record(now, ProtocolEvent::Delivered(delivery.message.task));
                }
                deliveries
            }
            Err(error) => {
                orbit_log_fields::warn!(
                    safe(ERROR_TYPE) = "pull_failed",
                    expanded(ERROR_MESSAGE) = %error,
                    "the pull of the lane failed"
                );
                self.ports.clock.sleep_until(now + PULL_RETRY).await;
                Vec::new()
            }
        }
    }
}
