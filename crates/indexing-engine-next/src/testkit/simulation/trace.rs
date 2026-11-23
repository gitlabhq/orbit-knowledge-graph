use std::fmt::Write;
use std::path::Path;

use super::MessageState;

use super::world::{ACKNOWLEDGE_WAIT_TICKS, Stall, StepOrder, World, ticks_at};
use super::{Simulation, SimulationReport};

const MODULE_FOOTER: &str =
    "=============================================================================\n";

impl World {
    pub(super) fn format_spec_state(&self) -> String {
        let pod =
            |process: Option<usize>| process.map_or("NoPod".to_owned(), |p| format!("p{}", p + 1));
        let mut state = format!("[now |-> {}", self.seen_tick());
        state.push_str(", offset |-> ");
        state.push_str(&self.format_per_pod(|process| process.offset.to_string()));

        let (holder, expires_at) = match self.lease.get() {
            Some(lease) => (pod(self.find_lease_holder()), ticks_at(lease.expires_at)),
            None => ("NoPod".to_owned(), 0),
        };
        let _ = write!(
            state,
            ", lease |-> [holder |-> {holder}, expiresAt |-> {expires_at}, rev |-> {}]",
            self.lease.revision().0
        );
        let pending = self
            .pending
            .get()
            .map_or(0, |message| self.find_spec_id(message.task));
        let _ = write!(
            state,
            ", pending |-> {pending}, nextId |-> {}",
            self.ids.len() + 1
        );

        let copies: Vec<String> = self
            .broker
            .messages()
            .iter()
            .map(|message| {
                let copy_state = match message.state {
                    MessageState::Ready => "ready",
                    MessageState::Inflight => "inflight",
                    MessageState::Acknowledged => "acked",
                    MessageState::Lost => "lost",
                };
                format!(
                    "[id |-> {}, state |-> \"{copy_state}\", deadline |-> {}, pod |-> {}]",
                    self.find_spec_id(message.task),
                    message.deadline,
                    pod(message.consumer)
                )
            })
            .collect();
        let _ = write!(state, ", copies |-> <<{}>>", copies.join(", "));

        let planner = match self.publishing {
            Some(task) => format!(
                "[phase |-> \"publishing\", id |-> {}]",
                self.find_spec_id(task)
            ),
            None => "[phase |-> \"idle\", id |-> 0]".to_owned(),
        };
        let mut cover: Vec<String> = self
            .covers
            .iter()
            .map(|task| self.find_spec_id(*task).to_string())
            .collect();
        cover.resize(self.submits.max(cover.len()), "0".to_owned());
        let _ = write!(
            state,
            ", planner |-> {planner}, submitted |-> {}, cover |-> <<{}>>, started |-> {}",
            self.covers.len(),
            cover.join(", "),
            self.format_id_set(self.started.iter().copied())
        );

        state.push_str(", pc |-> ");
        state
            .push_str(&self.format_per_pod(|process| format!("\"{}\"", process.phase.spec_name())));
        state.push_str(", held |-> ");
        state.push_str(
            &self.format_per_pod(|process| process.held.map_or(0, |copy| copy + 1).to_string()),
        );
        state.push_str(", token |-> ");
        state.push_str(&self.format_per_pod(|process| {
            format!(
                "<<{}, {}>>",
                process.token.generation.0, process.token.revision.0
            )
        }));
        state.push_str(", seenRev |-> ");
        state.push_str(&self.format_per_pod(|process| process.seen.0.to_string()));
        state.push_str(", expiry |-> ");
        state.push_str(&self.format_per_pod(|process| process.expiry.to_string()));
        state.push_str(", renewAt |-> ");
        state.push_str(&self.format_per_pod(|process| process.renew_at.to_string()));
        state.push_str(", stalled |-> ");
        state.push_str(&self.format_per_pod(|process| {
            let stalled = matches!(process.stall, Stall::Stalled { .. });
            format_bool(stalled).to_owned()
        }));

        let fence = self.database.highest_fence();
        let faults = self.faults_left;
        let _ = write!(
            state,
            ", stallsLeft |-> {}, crashesLeft |-> {}, storeFence |-> <<{}, {}>>, staleWrite |-> FALSE",
            faults.stalls, faults.crashes, fence.generation.0, fence.revision.0
        );
        let _ = write!(
            state,
            ", rejectedCurrent |-> {}, generation |-> {}, natsGeneration |-> {}",
            format_bool(self.rejected_current),
            self.database.generation().0,
            self.nats_generation.map_or(0, |generation| generation.0)
        );
        let _ = write!(
            state,
            ", wipesLeft |-> {}, duplicatesLeft |-> {}, lossesLeft |-> {}, lostInWipe |-> {}]",
            faults.wipes,
            faults.duplicates,
            faults.losses,
            self.format_id_set(self.lost_in_wipe.iter().copied())
        );
        state
    }

    fn format_per_pod(&self, value: impl Fn(&super::world::ProcessState) -> String) -> String {
        let entries: Vec<String> = self
            .processes
            .iter()
            .enumerate()
            .map(|(p, process)| format!("p{} :> {}", p + 1, value(process)))
            .collect();
        format!("({})", entries.join(" @@ "))
    }

    fn format_id_set(&self, tasks: impl Iterator<Item = crate::task::TaskId>) -> String {
        let mut ids: Vec<usize> = tasks.map(|task| self.find_spec_id(task)).collect();
        ids.sort_unstable();
        let ids: Vec<String> = ids.iter().map(usize::to_string).collect();
        format!("{{{}}}", ids.join(", "))
    }
}

impl Simulation {
    pub fn write_trace_check(
        &self,
        folder: &Path,
        reports: &[SimulationReport],
    ) -> std::io::Result<()> {
        if self.step_order == StepOrder::Interleaved {
            return Err(std::io::Error::other(
                "an interleaved simulation splits steps, so TLC can't follow its trace",
            ));
        }
        std::fs::create_dir_all(folder)?;
        std::fs::write(
            folder.join("LeaseDedupeTraces.tla"),
            self.format_traces_module(reports),
        )?;
        std::fs::write(
            folder.join("LeaseDedupeTraceCheck.cfg"),
            self.format_trace_config(),
        )
    }

    fn format_traces_module(&self, reports: &[SimulationReport]) -> String {
        let pods = self.format_pod_names();
        let mut module = format!(
            "---------------------------- MODULE LeaseDedupeTraces ----------------------------\nEXTENDS LeaseDedupe\n\nCONSTANTS {}\n\nTraces == <<\n",
            pods.join(", ")
        );
        let traces: Vec<String> = reports
            .iter()
            .map(|report| format!("<<\n{}\n>>", report.trace.join(",\n")))
            .collect();
        module.push_str(&traces.join(",\n"));
        module.push_str("\n>>\n");
        module.push_str(MODULE_FOOTER);
        module
    }

    fn format_trace_config(&self) -> String {
        let pods = self.format_pod_names();
        let mut config = String::from("CONSTANTS\n");
        for pod in &pods {
            let _ = writeln!(config, "    {pod} = {pod}");
        }
        let _ = writeln!(config, "    Pods = {{{}}}", pods.join(", "));
        let constants = [
            ("NoPod", "NoPod".to_owned()),
            ("Offsets", "{0, 1}".to_owned()),
            ("Ttl", "3".to_owned()),
            ("RenewEvery", "1".to_owned()),
            ("SkewMargin", "1".to_owned()),
            ("AckWait", ACKNOWLEDGE_WAIT_TICKS.to_string()),
            ("GiveBackDelay", "1".to_owned()),
            ("MaxTime", self.count_ticks().to_string()),
            ("MaxEvents", self.submits.to_string()),
            ("MaxStalls", self.faults.stalls.to_string()),
            ("MaxCrashes", self.faults.crashes.to_string()),
            ("MaxWipes", self.faults.wipes.to_string()),
            ("MaxDuplicates", self.faults.duplicates.to_string()),
            ("MaxLosses", self.faults.losses.to_string()),
            ("StartClearsOnlyOwnPending", "TRUE".to_owned()),
            ("SubmitRepublishesFoundPending", "TRUE".to_owned()),
            ("HolderStopsBeforeExpiry", "FALSE".to_owned()),
            ("WritesFencedByLeaseRevision", "TRUE".to_owned()),
            ("FenceHasGeneration", "TRUE".to_owned()),
        ];
        for (name, value) in constants {
            let _ = writeln!(config, "    {name} = {value}");
        }
        config.push_str("\nSPECIFICATION TraceSpec\n");
        config.push_str("INVARIANTS StartsAtInit NoStaleWrite NoRejectedCurrentHolder AtMostOnePending EverySubmitCovered\n");
        config
    }

    fn format_pod_names(&self) -> Vec<String> {
        (1..=self.processes).map(|n| format!("p{n}")).collect()
    }
}

fn format_bool(value: bool) -> &'static str {
    if value { "TRUE" } else { "FALSE" }
}
