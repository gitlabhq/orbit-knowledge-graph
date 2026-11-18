use std::fmt;

use async_trait::async_trait;

use crate::lane::Lane;
use crate::payload::{TaskInput, Tenant};
use crate::trigger::{Fired, Trigger};
use crate::worker::Worker;

#[async_trait]
pub trait Planner: Send + Sync + 'static {
    const NAME: &'static str;

    fn triggers() -> Vec<Trigger>;
    async fn plan(&self, ctx: &mut PlanContext, fired: Fired) -> Result<(), PlanError>;
}

pub struct PlanContext {
    submissions: Vec<Submission>,
    routing: Routing,
    next_page: Option<String>,
}

pub struct Submit<'a> {
    submission: &'a mut Submission,
}

pub struct PlanError(anyhow::Error);

pub(crate) struct Plan {
    pub(crate) tasks: Vec<PlannedTask>,
    pub(crate) next_page: Option<String>,
}

pub(crate) struct PlannedTask {
    pub(crate) tenant: Tenant,
    pub(crate) input: TaskInput,
    pub(crate) lane: Lane,
}

struct Submission {
    tenant: Tenant,
    input: Result<TaskInput, serde_json::Error>,
    lane: Option<Lane>,
}

enum Routing {
    Live { source_lane: Lane },
    Sweep,
}

impl PlanContext {
    pub fn submit<W: Worker>(&mut self, tenant: Tenant, input: W::Input) -> Submit<'_> {
        let index = self.submissions.len();
        self.submissions.push(Submission {
            tenant,
            input: TaskInput::encode::<W>(&input),
            lane: None,
        });
        Submit {
            submission: &mut self.submissions[index],
        }
    }

    pub fn continue_from(&mut self, cursor: Option<String>) {
        self.next_page = cursor;
    }

    pub(crate) fn for_fired(fired: &Fired) -> Self {
        let routing = match fired {
            Fired::Sweep { .. } => Routing::Sweep,
            Fired::Events(batch) => Routing::Live {
                source_lane: batch.source_lane().cloned().unwrap_or(Lane::DEFAULT),
            },
            Fired::Manual { .. } | Fired::Cron => Routing::Live {
                source_lane: Lane::DEFAULT,
            },
        };
        Self {
            submissions: Vec::new(),
            routing,
            next_page: None,
        }
    }

    pub(crate) fn into_plan(self) -> Result<Plan, PlanError> {
        let tasks = self
            .submissions
            .into_iter()
            .map(|submission| {
                Ok(PlannedTask {
                    lane: self.routing.pick_lane(submission.lane),
                    tenant: submission.tenant,
                    input: submission.input?,
                })
            })
            .collect::<Result<_, serde_json::Error>>()?;
        Ok(Plan {
            tasks,
            next_page: self.next_page,
        })
    }
}

impl Submit<'_> {
    pub fn lane(self, lane: Lane) {
        self.submission.lane = Some(lane);
    }
}

impl Routing {
    fn pick_lane(&self, requested: Option<Lane>) -> Lane {
        match self {
            Routing::Sweep => Lane::BACKFILL,
            Routing::Live { source_lane } => requested.unwrap_or_else(|| source_lane.clone()),
        }
    }
}

impl<E: Into<anyhow::Error>> From<E> for PlanError {
    fn from(error: E) -> Self {
        Self(error.into())
    }
}

impl fmt::Display for PlanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:#}", self.0)
    }
}

impl fmt::Debug for PlanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.0, f)
    }
}
