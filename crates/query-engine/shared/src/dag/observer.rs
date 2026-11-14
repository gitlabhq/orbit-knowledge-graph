//! What a DAG run reports as it goes: plain, serializable events a caller can record or stream.

use compiler::dag::{Backend, Input, Rendered, StageId};
use compiler::orbit_authorization::Key;
use serde::Serialize;

/// The most keys of one key set an event carries.
pub const KEY_SAMPLE: usize = 8;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct StageInfo {
    pub id: StageId,
    pub backend: Backend,
    pub connection: Option<String>,
    pub inputs: Vec<Input>,
    /// The stage's logical sub-plan; the Combine stage plans from its inputs, so it has none.
    pub logical: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum Event {
    Logical {
        plan: Vec<String>,
    },
    Split {
        stages: Vec<StageInfo>,
    },
    Started {
        stage: StageId,
    },
    Planned {
        stage: StageId,
        plan: Vec<String>,
    },
    Emitted {
        stage: StageId,
        plan: Vec<String>,
    },
    Rendered {
        stage: StageId,
        requests: Vec<Rendered>,
    },
    Keys {
        from: StageId,
        to: StageId,
        binding: String,
        count: usize,
        sample: Vec<Key>,
    },
    Rows {
        stage: StageId,
        count: usize,
    },
    Finished {
        stage: StageId,
        elapsed_ms: f64,
    },
    Failed {
        stage: StageId,
        error: String,
    },
}

impl Event {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Logical { .. } => "logical",
            Self::Split { .. } => "split",
            Self::Started { .. } => "started",
            Self::Planned { .. } => "planned",
            Self::Emitted { .. } => "emitted",
            Self::Rendered { .. } => "rendered",
            Self::Keys { .. } => "keys",
            Self::Rows { .. } => "rows",
            Self::Finished { .. } => "finished",
            Self::Failed { .. } => "failed",
        }
    }
}

/// Receives a run's events. A disabled observer gets none, and the run builds none.
pub trait Observer: Send {
    fn enabled(&self) -> bool {
        true
    }

    fn event(&mut self, event: Event);
}

/// The observer production passes.
pub struct Silent;

impl Observer for Silent {
    fn enabled(&self) -> bool {
        false
    }

    fn event(&mut self, _: Event) {}
}

/// Keeps every event in order.
#[derive(Debug, Default)]
pub struct Recording(pub Vec<Event>);

impl Observer for Recording {
    fn event(&mut self, event: Event) {
        self.0.push(event);
    }
}
