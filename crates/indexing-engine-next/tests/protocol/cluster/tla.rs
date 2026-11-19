use std::fmt::{self, Write};

use indexing_engine_next::protocol::TaskId;
use indexing_engine_next::testkit::simulator::MessageState;

use super::{Cluster, Holder, MAX_EVENTS, RandomRun};

pub(super) struct Tla<'a>(pub(super) &'a Cluster);

impl Tla<'_> {
    fn per_holder(
        &self,
        f: &mut fmt::Formatter<'_>,
        value: impl Fn(&Holder) -> String,
    ) -> fmt::Result {
        let entries: Vec<String> = self
            .0
            .holders
            .iter()
            .map(|holder| format!("{} :> {}", holder.id.as_str(), value(holder)))
            .collect();
        write!(f, "({})", entries.join(" @@ "))
    }
}

impl fmt::Display for Tla<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let cluster = self.0;
        let holder_name =
            |h: Option<usize>| h.map_or("NoPod".to_string(), |h| format!("p{}", h + 1));

        write!(f, "[now |-> {}, offset |-> ", cluster.clock.ticks())?;
        self.per_holder(f, |holder| holder.skew.to_string())?;

        let (holder, expires_at) = match cluster.lease.get() {
            Some(lease) => (
                lease.holder.as_str().to_string(),
                cluster.clock.ticks_at(lease.expires_at),
            ),
            None => ("NoPod".to_string(), 0),
        };
        write!(
            f,
            ", lease |-> [holder |-> {holder}, expiresAt |-> {expires_at}, rev |-> {}]",
            cluster.lease.revision().0
        )?;
        write!(
            f,
            ", marker |-> {}, nextId |-> {}",
            cluster.marker.get().copied().map_or(0, spec_id),
            cluster.next_id
        )?;

        let copies: Vec<String> = cluster
            .broker
            .messages()
            .iter()
            .map(|message| {
                let state = match message.state {
                    MessageState::Ready => "ready",
                    MessageState::Inflight => "inflight",
                    MessageState::Acknowledged => "acked",
                };
                format!(
                    "[id |-> {}, state |-> \"{state}\", deadline |-> {}, pod |-> {}]",
                    spec_id(message.task),
                    message.deadline,
                    holder_name(message.consumer)
                )
            })
            .collect();
        write!(f, ", copies |-> <<{}>>", copies.join(", "))?;

        let planner = match cluster.publishing {
            Some(task) => format!("[phase |-> \"publishing\", id |-> {}]", spec_id(task)),
            None => "[phase |-> \"idle\", id |-> 0]".to_string(),
        };
        let mut cover: Vec<String> = cluster
            .covers
            .iter()
            .map(|task| spec_id(*task).to_string())
            .collect();
        cover.resize(MAX_EVENTS, "0".to_string());
        let started: Vec<String> = cluster
            .started
            .iter()
            .map(|task| spec_id(*task).to_string())
            .collect();
        write!(
            f,
            ", planner |-> {planner}, submitted |-> {}, cover |-> <<{}>>, started |-> {{{}}}",
            cluster.covers.len(),
            cover.join(", "),
            started.join(", ")
        )?;

        f.write_str(", pc |-> ")?;
        self.per_holder(f, |holder| format!("\"{}\"", holder.step.name()))?;
        f.write_str(", held |-> ")?;
        self.per_holder(f, |holder| holder.held.map_or(0, |c| c + 1).to_string())?;
        f.write_str(", token |-> ")?;
        self.per_holder(f, |holder| holder.last_hold.token.to_string())?;
        f.write_str(", seenRev |-> ")?;
        self.per_holder(f, |holder| holder.last_hold.seen.to_string())?;
        f.write_str(", expiry |-> ")?;
        self.per_holder(f, |holder| holder.last_hold.expiry.to_string())?;
        f.write_str(", renewAt |-> ")?;
        self.per_holder(f, |holder| holder.last_hold.renew_at.to_string())?;
        f.write_str(", stalled |-> ")?;
        self.per_holder(f, |holder| tla_bool(holder.stalled).to_string())?;

        let enabled: Vec<String> = cluster
            .enabled_actions()
            .iter()
            .map(|action| format!("\"{action}\""))
            .collect();
        write!(
            f,
            ", stallsLeft |-> {}, crashesLeft |-> {}, storeFence |-> {}, staleWrite |-> FALSE, enabled |-> {{{}}}]",
            cluster.faults.stalls_left(),
            cluster.faults.crashes_left(),
            cluster.store.highest_fence().0,
            enabled.join(", ")
        )
    }
}

fn spec_id(task: TaskId) -> u128 {
    task.as_uuid().as_u128()
}

fn tla_bool(value: bool) -> &'static str {
    if value { "TRUE" } else { "FALSE" }
}

pub fn write_runs_module(runs: &[RandomRun]) -> String {
    let mut module = String::new();
    module.push_str(
        "---------------------------- MODULE LeaseDedupeRuns ----------------------------\n",
    );
    module.push_str("EXTENDS LeaseDedupe\n\nCONSTANTS p1, p2, p3\n\nRuns == <<\n");
    for (n, run) in runs.iter().enumerate() {
        let separator = if n + 1 == runs.len() { "" } else { "," };
        let _ = writeln!(module, "<<\n{}\n>>{separator}", run.states.join(",\n"));
    }
    module.push_str(
        ">>\n=============================================================================\n",
    );
    module
}
