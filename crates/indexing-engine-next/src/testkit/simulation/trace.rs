use std::fmt::Write;
use std::path::Path;

use super::MessageState;

use super::world::{ACK_WAIT_TICKS, ProcessState, Stall, StepOrder, World, ticks_at};
use super::{Simulation, SimulationReport};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraceStep {
    action: String,
    state: Vec<(&'static str, String)>,
}

impl World {
    pub(super) fn record_trace_step(&mut self, action: String) {
        let state = self.format_quint_state();
        if self.trace.last().map(|step| &step.state) != Some(&state) {
            self.trace.push(TraceStep { action, state });
        }
    }

    pub(super) fn format_initial_action(&self) -> String {
        format!(
            "initWith({})",
            self.format_per_pod(|process| process.offset.to_string())
        )
    }

    fn format_quint_state(&self) -> Vec<(&'static str, String)> {
        let pod = |process: Option<usize>| {
            process.map_or("\"NoPod\"".to_owned(), |p| format!("\"p{}\"", p + 1))
        };
        let (holder, expires_at) = match self.lease.get() {
            Some(lease) => (pod(self.find_lease_holder()), ticks_at(lease.expires_at)),
            None => ("\"NoPod\"".to_owned(), 0),
        };
        let pending = self
            .pending
            .get()
            .map_or(0, |message| self.find_spec_id(message.task));
        let copies: Vec<String> = self
            .broker
            .messages()
            .iter()
            .map(|message| {
                let copy_state = match message.state {
                    MessageState::Ready => "ready",
                    MessageState::Inflight => "inflight",
                    MessageState::Acked => "acked",
                    MessageState::Lost => "lost",
                };
                format!(
                    "{{ id: {}, state: \"{copy_state}\", deadline: {}, pod: {} }}",
                    self.find_spec_id(message.task),
                    message.deadline,
                    pod(message.consumer)
                )
            })
            .collect();
        let planner = match self.publishing {
            Some(task) => format!(
                "{{ phase: \"publishing\", id: {} }}",
                self.find_spec_id(task)
            ),
            None => "{ phase: \"idle\", id: 0 }".to_owned(),
        };
        let mut cover: Vec<usize> = self
            .covers
            .iter()
            .map(|task| self.find_spec_id(*task))
            .collect();
        cover.resize(self.submits.max(cover.len()), 0);
        let cover: Vec<String> = cover
            .iter()
            .enumerate()
            .map(|(index, id)| format!("{} -> {id}", index + 1))
            .collect();
        let faults = self.faults_left;

        vec![
            ("now", self.seen_tick().to_string()),
            (
                "offset",
                self.format_per_pod(|process| process.offset.to_string()),
            ),
            (
                "lease",
                format!(
                    "{{ holder: {holder}, expiresAt: {expires_at}, rev: {} }}",
                    self.lease.revision().0
                ),
            ),
            ("pending", pending.to_string()),
            ("nextId", (self.ids.len() + 1).to_string()),
            ("copies", format!("[{}]", copies.join(", "))),
            ("planner", planner),
            ("submitted", self.covers.len().to_string()),
            ("cover", format!("Map({})", cover.join(", "))),
            ("started", self.format_id_set(self.started.iter().copied())),
            (
                "pc",
                self.format_per_pod(|process| format!("\"{}\"", process.phase.spec_name())),
            ),
            (
                "held",
                self.format_per_pod(|process| process.held.map_or(0, |copy| copy + 1).to_string()),
            ),
            (
                "seenRev",
                self.format_per_pod(|process| process.seen.0.to_string()),
            ),
            (
                "renewAt",
                self.format_per_pod(|process| process.renew_at.to_string()),
            ),
            (
                "stalled",
                self.format_per_pod(|process| {
                    matches!(process.stall, Stall::Stalled { .. }).to_string()
                }),
            ),
            ("stallsLeft", faults.stalls.to_string()),
            ("crashesLeft", faults.crashes.to_string()),
            ("wipesLeft", faults.wipes.to_string()),
            ("duplicatesLeft", faults.duplicates.to_string()),
            ("lossesLeft", faults.losses.to_string()),
            (
                "lostInWipe",
                self.format_id_set(self.lost_in_wipe.iter().copied()),
            ),
        ]
    }

    fn format_per_pod(&self, value: impl Fn(&ProcessState) -> String) -> String {
        let entries: Vec<String> = self
            .processes
            .iter()
            .enumerate()
            .map(|(p, process)| format!("\"p{}\" -> {}", p + 1, value(process)))
            .collect();
        format!("Map({})", entries.join(", "))
    }

    fn format_id_set(&self, tasks: impl Iterator<Item = crate::task::TaskId>) -> String {
        let mut ids: Vec<usize> = tasks.map(|task| self.find_spec_id(task)).collect();
        ids.sort_unstable();
        let ids: Vec<String> = ids.iter().map(usize::to_string).collect();
        format!("Set({})", ids.join(", "))
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
                "an interleaved simulation splits steps, so a trace can't follow the spec",
            ));
        }
        std::fs::create_dir_all(folder)?;
        std::fs::write(
            folder.join("traces.qnt"),
            self.format_traces_module(reports),
        )
    }

    fn format_traces_module(&self, reports: &[SimulationReport]) -> String {
        let mut module = format!(
            "module traces {{\n  import leaseDedupeProtocol({}).* from \"./leaseDedupeProtocol\"\n",
            self.format_constants()
        );
        for report in reports {
            let _ = write!(module, "\n  run seed{}Test =", report.seed);
            let last = report.trace.len().saturating_sub(1);
            for (index, step) in report.trace.iter().enumerate() {
                let previous = index
                    .checked_sub(1)
                    .map(|before| &report.trace[before].state);
                let full = previous.is_none() || index == last;
                let checks: Vec<String> = step
                    .state
                    .iter()
                    .enumerate()
                    .filter(|(position, pair)| {
                        full || previous.is_some_and(|before| before[*position] != **pair)
                    })
                    .map(|(_, (variable, value))| format!("{variable} == {value}"))
                    .collect();
                let action = if index == 0 {
                    format!(" {}", step.action)
                } else {
                    format!("\n    .then({})", step.action)
                };
                let _ = write!(
                    module,
                    "{action}\n    .expect(and {{ {}, AtMostOnePending, EverySubmitCovered }})",
                    checks.join(", ")
                );
            }
            module.push('\n');
        }
        module.push_str("}\n");
        module
    }

    fn format_constants(&self) -> String {
        let pods: Vec<String> = (1..=self.processes).map(|n| format!("\"p{n}\"")).collect();
        let constants = [
            ("Pods", format!("Set({})", pods.join(", "))),
            ("NoPod", "\"NoPod\"".to_owned()),
            ("Offsets", "Set(0, 1)".to_owned()),
            ("Ttl", "3".to_owned()),
            ("RenewEvery", "1".to_owned()),
            ("SkewMargin", "1".to_owned()),
            ("AckWait", ACK_WAIT_TICKS.to_string()),
            ("GiveBackDelay", "1".to_owned()),
            ("MaxTime", self.count_ticks().to_string()),
            ("MaxEvents", self.submits.to_string()),
            ("MaxStalls", self.faults.stalls.to_string()),
            ("MaxCrashes", self.faults.crashes.to_string()),
            ("MaxWipes", self.faults.wipes.to_string()),
            ("MaxDuplicates", self.faults.duplicates.to_string()),
            ("MaxLosses", self.faults.losses.to_string()),
            ("StartClearsOnlyOwnPending", "true".to_owned()),
            ("SubmitRepublishesFoundPending", "true".to_owned()),
        ];
        let constants: Vec<String> = constants
            .iter()
            .map(|(name, value)| format!("{name} = {value}"))
            .collect();
        constants.join(", ")
    }
}

pub(super) fn format_quint_action(name: &str, detail: &str) -> String {
    let arguments = detail
        .strip_prefix('(')
        .and_then(|rest| rest.split_once(')'))
        .map(|(inside, _)| inside);
    let Some(arguments) = arguments else {
        return name.to_owned();
    };
    let arguments: Vec<String> = arguments
        .split(", ")
        .map(|argument| match argument.parse::<usize>() {
            Ok(copy) => copy.saturating_sub(1).to_string(),
            Err(_) => format!("\"{argument}\""),
        })
        .collect();
    format!("{name}({})", arguments.join(", "))
}
