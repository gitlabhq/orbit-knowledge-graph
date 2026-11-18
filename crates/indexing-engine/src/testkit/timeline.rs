use std::fmt;

use tokio::time::Instant;

use crate::config::format_duration;
use crate::lane::Lane;
use crate::payload::TaskInput;

pub(super) struct Timeline {
    origin: Instant,
    lines: Vec<String>,
}

pub(super) enum TaskEvent {
    Submitted,
    Spawned,
    AlreadyPending,
    Started {
        attempt: u16,
    },
    Ended {
        attempt: &'static str,
        next_step: Option<String>,
    },
}

impl Default for Timeline {
    fn default() -> Self {
        Self {
            origin: Instant::now(),
            lines: Vec::new(),
        }
    }
}

impl Timeline {
    pub(super) fn record(&mut self, input: &TaskInput, lane: &Lane, event: TaskEvent) {
        let at = format_duration(self.origin.elapsed());
        let lane = lane.format_id(input.worker);
        self.lines
            .push(format!("{at} {lane} {key} {event}", key = input.key));
    }

    pub(super) fn list_lines(&self) -> Vec<String> {
        self.lines.clone()
    }
}

impl fmt::Display for TaskEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TaskEvent::Submitted => f.write_str("submitted"),
            TaskEvent::Spawned => f.write_str("spawned"),
            TaskEvent::AlreadyPending => f.write_str("already pending"),
            TaskEvent::Started { attempt } => write!(f, "started attempt {attempt}"),
            TaskEvent::Ended {
                attempt,
                next_step: None,
            } => f.write_str(attempt),
            TaskEvent::Ended {
                attempt,
                next_step: Some(next_step),
            } => write!(f, "{attempt}, {next_step}"),
        }
    }
}
