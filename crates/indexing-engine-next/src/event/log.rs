use super::{EngineEvent, LaneEvent, LaneStep, Observer, TaskEvent};

pub(crate) struct LogObserver;

impl Observer for LogObserver {
    fn observe(&self, event: &EngineEvent) {
        match event {
            EngineEvent::Task(event) => log_task_event(event),
            EngineEvent::Lane(event) => log_lane_event(event),
        }
    }
}

fn log_task_event(event: &TaskEvent) {
    let task_id = event.task.id.as_uuid().to_string();
    let worker = event.task.worker.as_str();
    let step = event.step.name();
    match event.step.failure() {
        Some(failure) => orbit_log_fields::warn!(
            safe(TASK_ID) = task_id.as_str(),
            safe(TASK) = worker,
            safe(PHASE) = step,
            safe(ERROR_TYPE) = failure.kind,
            expanded(ERROR_MESSAGE) = failure.message.as_str(),
            "a task step of the engine failed"
        ),
        None => orbit_log_fields::debug!(
            safe(TASK_ID) = task_id.as_str(),
            safe(TASK) = worker,
            safe(PHASE) = step,
            "a task took a step in the engine"
        ),
    }
}

fn log_lane_event(event: &LaneEvent) {
    match &event.step {
        LaneStep::PullFailed(failure) => orbit_log_fields::warn!(
            safe(TASK) = event.lane.worker(),
            safe(ERROR_TYPE) = failure.kind,
            expanded(ERROR_MESSAGE) = failure.message.as_str(),
            "the pull of a lane failed"
        ),
    }
}
