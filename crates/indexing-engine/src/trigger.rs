use std::time::Duration;

use serde::Serialize;

use crate::lane::Lane;
use crate::payload::{Key, Payload, Tenant};
use crate::planner::PlanError;
use crate::worker::Worker;

const DONE_SUBJECT_PREFIX: &str = "orbit.done.";

#[derive(Clone, Debug)]
pub enum Trigger {
    Cron(&'static str),
    Stream {
        subjects: &'static [&'static str],
        batch: usize,
        linger: Duration,
    },
    Done(DoneTrigger),
}

impl Trigger {
    pub fn done<W: Worker>() -> Self {
        Self::Done(DoneTrigger { worker: W::NAME })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DoneTrigger {
    worker: &'static str,
}

impl DoneTrigger {
    pub(crate) fn worker(&self) -> &'static str {
        self.worker
    }
}

#[derive(Debug)]
pub enum Fired {
    Events(EventBatch),
    Manual { tenant: Tenant, keys: Vec<Key> },
    Sweep { cursor: Option<String>, tag: String },
    Cron,
}

#[derive(Debug)]
pub struct EventBatch {
    events: Vec<Event>,
    source_lane: Option<Lane>,
}

#[derive(Debug)]
struct Event {
    subject: String,
    bytes: Vec<u8>,
}

impl EventBatch {
    pub fn encode<T: Serialize>(subject: &str, payloads: &[T]) -> Result<Self, serde_json::Error> {
        let events = payloads
            .iter()
            .map(|payload| {
                Ok(Event {
                    subject: subject.to_owned(),
                    bytes: serde_json::to_vec(payload)?,
                })
            })
            .collect::<Result<_, serde_json::Error>>()?;
        Ok(Self {
            events,
            source_lane: None,
        })
    }

    pub fn decode<T: Payload>(&self) -> Result<Vec<T>, PlanError> {
        self.events
            .iter()
            .map(|event| Ok(serde_json::from_slice(&event.bytes)?))
            .collect()
    }

    pub fn outputs<W: Worker>(&self) -> Result<Vec<W::Output>, PlanError> {
        let subject = format_done_subject(W::NAME);
        self.events
            .iter()
            .filter(|event| event.subject == subject)
            .map(|event| Ok(serde_json::from_slice(&event.bytes)?))
            .collect()
    }

    pub(crate) fn done(worker: &'static str, output: Vec<u8>, lane: Lane) -> Self {
        Self {
            events: vec![Event {
                subject: format_done_subject(worker),
                bytes: output,
            }],
            source_lane: Some(lane),
        }
    }

    pub(crate) fn source_lane(&self) -> Option<&Lane> {
        self.source_lane.as_ref()
    }
}

fn format_done_subject(worker: &str) -> String {
    format!("{DONE_SUBJECT_PREFIX}{worker}")
}
