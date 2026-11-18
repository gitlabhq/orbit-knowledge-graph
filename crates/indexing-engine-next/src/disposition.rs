use std::time::Duration;

use crate::config::{LaneConfig, format_duration};
use crate::lane::Lane;
use crate::worker::{Completion, EncodedOutcome, TaskError, TaskErrorKind};

const RETRY_BACKOFF: [Duration; 4] = [
    Duration::from_secs(1),
    Duration::from_secs(10),
    Duration::from_secs(60),
    Duration::from_secs(300),
];
const INCOMPATIBLE_REDELIVERY: Duration = Duration::from_secs(30);

pub(crate) enum Attempt {
    Finished(EncodedOutcome),
    Failed(TaskError),
    TimedOut,
}

pub(crate) enum Disposition {
    Finish(EncodedOutcome),
    Retry {
        after: Duration,
    },
    Redeliver {
        after: Duration,
    },
    PauseLane {
        after: Duration,
    },
    MoveLane {
        to: Lane,
    },
    DeadLetter {
        class: &'static str,
        message: String,
    },
}

impl Attempt {
    pub(crate) fn describe(&self) -> &'static str {
        match self {
            Attempt::Finished(outcome) => match outcome.completion {
                Completion::Done(_) => "done",
                Completion::NoChange => "no change",
            },
            Attempt::Failed(error) => error.describe(),
            Attempt::TimedOut => "timed out",
        }
    }
}

impl Disposition {
    pub(crate) fn decide(
        attempt: Attempt,
        attempts_used: u16,
        age: Duration,
        lane: &LaneConfig,
    ) -> Self {
        let failure = match attempt {
            Attempt::Finished(outcome) => return Self::Finish(outcome),
            Attempt::Failed(error) => Some(error.into_kind()),
            Attempt::TimedOut => None,
        };
        if age >= lane.max_age {
            return Self::DeadLetter {
                class: "expired",
                message: format!(
                    "the task is older than its max age of {}",
                    format_duration(lane.max_age)
                ),
            };
        }
        match failure {
            None => Self::decide_after_timeout(attempts_used, lane),
            Some(TaskErrorKind::Failed(error)) => {
                Self::retry_or_dead_letter(attempts_used, format!("{error:#}"))
            }
            Some(TaskErrorKind::Permanent(error)) => Self::DeadLetter {
                class: "permanent",
                message: format!("{error:#}"),
            },
            Some(TaskErrorKind::Defer { after, .. }) => Self::Redeliver { after },
            Some(TaskErrorKind::Throttle { after, .. }) => Self::PauseLane { after },
            Some(TaskErrorKind::Incompatible { .. }) => Self::Redeliver {
                after: INCOMPATIBLE_REDELIVERY,
            },
        }
    }

    pub(crate) fn describe_next_step(&self) -> Option<String> {
        match self {
            Self::Finish(_) => None,
            Self::Retry { after } => Some(format!("retry in {}", format_duration(*after))),
            Self::Redeliver { after } => Some(format!("redeliver in {}", format_duration(*after))),
            Self::PauseLane { after } => {
                Some(format!("pause the lane for {}", format_duration(*after)))
            }
            Self::MoveLane { to } => Some(format!("move to {to}")),
            Self::DeadLetter { class, message } => {
                Some(format!("dead-letter as {class}: {message}"))
            }
        }
    }

    fn decide_after_timeout(attempts_used: u16, lane: &LaneConfig) -> Self {
        let message = format!(
            "the task ran longer than the lane timeout of {}",
            format_duration(lane.timeout)
        );
        let next_lane = lane.on_timeout.as_deref().and_then(Lane::parse_id);
        match next_lane {
            Some((_, to)) if usize::from(attempts_used) < RETRY_BACKOFF.len() => {
                Self::MoveLane { to }
            }
            _ => Self::retry_or_dead_letter(attempts_used, message),
        }
    }

    fn retry_or_dead_letter(attempts_used: u16, message: String) -> Self {
        match RETRY_BACKOFF.get(usize::from(attempts_used)) {
            Some(after) => Self::Retry { after: *after },
            None => Self::DeadLetter {
                class: "failed",
                message,
            },
        }
    }
}
