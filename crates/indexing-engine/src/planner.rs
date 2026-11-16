use std::fmt;

use async_trait::async_trait;

use crate::lane::Lane;
use crate::payload::{TaskInput, Tenant};
use crate::resources::FromResources;
use crate::trigger::{Fired, Trigger};
use crate::worker::Worker;

#[async_trait]
pub trait Planner: FromResources + Send + Sync + 'static {
    const NAME: &'static str;

    fn triggers() -> Vec<Trigger>;
    async fn plan(&self, ctx: &mut PlanContext, fired: Fired) -> Result<(), PlanError>;
}

pub struct PlanContext {
    submissions: Vec<Submission>,
    encode_error: Option<serde_json::Error>,
    routing: Routing,
    next_page: Option<String>,
}

pub struct Submit<'a> {
    lane: Option<&'a mut Option<Lane>>,
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
    input: TaskInput,
    lane: Option<Lane>,
}

enum Routing {
    Live { source_lane: Lane },
    Sweep,
}

impl PlanContext {
    pub fn submit<W: Worker>(&mut self, tenant: Tenant, input: W::Input) -> Submit<'_> {
        match TaskInput::encode::<W>(&input) {
            Ok(input) => {
                self.submissions.push(Submission {
                    tenant,
                    input,
                    lane: None,
                });
                Submit {
                    lane: self
                        .submissions
                        .last_mut()
                        .map(|submission| &mut submission.lane),
                }
            }
            Err(error) => {
                self.encode_error.get_or_insert(error);
                Submit { lane: None }
            }
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
            encode_error: None,
            routing,
            next_page: None,
        }
    }

    pub(crate) fn into_plan(self) -> Result<Plan, PlanError> {
        if let Some(error) = self.encode_error {
            return Err(error.into());
        }
        let tasks = self
            .submissions
            .into_iter()
            .map(|submission| PlannedTask {
                lane: self.routing.pick_lane(submission.lane),
                tenant: submission.tenant,
                input: submission.input,
            })
            .collect();
        Ok(Plan {
            tasks,
            next_page: self.next_page,
        })
    }
}

impl Submit<'_> {
    pub fn lane(self, lane: Lane) {
        if let Some(slot) = self.lane {
            *slot = Some(lane);
        }
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
