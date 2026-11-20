use rand::RngExt;

use crate::protocol::{Lease, TaskId};
use crate::testkit::simulator::MessageState;

use super::world::{Stall, StepKind, World};

const MAX_STALL_TICKS: u64 = 8;
const MAX_RECOVERY_TICKS: u64 = 2;

#[derive(Clone, Copy, Debug)]
pub(super) enum Fault {
    Stall {
        process: usize,
        ticks: u64,
        at: StepKind,
    },
    Crash {
        process: usize,
    },
    RenewalFailure {
        process: usize,
    },
    PlannerCrash,
    Wipe {
        recover_after: u64,
    },
    Duplicate {
        message: usize,
    },
}

impl World {
    pub(super) fn draw_fault(&mut self) -> Option<Fault> {
        if !self.rng.random_bool(0.5) {
            return None;
        }
        let process = self.rng.random_range(0..self.processes.len());
        let left = self.faults_left;
        let mut faults = Vec::new();
        if left.stalls > 0 {
            let ticks = self.rng.random_range(1..=MAX_STALL_TICKS);
            let at = if self.rng.random_bool(0.5) {
                StepKind::Any
            } else {
                StepKind::Commit
            };
            faults.push(Fault::Stall { process, ticks, at });
        }
        if left.crashes > 0 {
            faults.extend([
                Fault::Crash { process },
                Fault::RenewalFailure { process },
                Fault::PlannerCrash,
            ]);
        }
        if left.wipes > 0 {
            let recover_after = self.rng.random_range(0..=MAX_RECOVERY_TICKS);
            faults.push(Fault::Wipe { recover_after });
        }
        let messages = self.broker.messages().len();
        if left.duplicates > 0 && messages > 0 {
            let message = self.rng.random_range(0..messages);
            faults.push(Fault::Duplicate { message });
        }
        if faults.is_empty() {
            return None;
        }
        let index = self.rng.random_range(0..faults.len());
        Some(faults.swap_remove(index))
    }

    pub(super) fn inject_fault(&mut self, fault: Fault) -> Option<usize> {
        match fault {
            Fault::Stall { process, ticks, at } => {
                if self.processes[process].stall == Stall::Awake {
                    self.processes[process].stall = Stall::Armed { ticks, at };
                }
                None
            }
            Fault::Crash { process } => self.crash(process).then_some(process),
            Fault::RenewalFailure { process } => {
                self.processes[process].renewal_fails = true;
                None
            }
            Fault::PlannerCrash => {
                self.planner_crash_armed = true;
                None
            }
            Fault::Wipe { recover_after } => {
                self.wipe_nats(recover_after);
                None
            }
            Fault::Duplicate { message } => {
                self.duplicate_message(message);
                None
            }
        }
    }

    pub(super) fn heal(&mut self) -> bool {
        self.planner_crash_armed = false;
        for process in &mut self.processes {
            process.renewal_fails = false;
        }
        self.recover_at = Some(self.now_tick());
        self.resume_stalls(true)
    }

    pub(super) fn resume_stalls(&mut self, every: bool) -> bool {
        let now = self.now_tick();
        let mut resumed = false;
        for p in 0..self.processes.len() {
            match self.processes[p].stall {
                Stall::Stalled { until } if every || until <= now => {
                    self.processes[p].stall = Stall::Awake;
                    self.record_step(format!("Resume(p{})", p + 1));
                    resumed = true;
                }
                Stall::Armed { .. } if every => self.processes[p].stall = Stall::Awake,
                Stall::Stalled { .. } | Stall::Armed { .. } | Stall::Awake => {}
            }
        }
        resumed
    }

    pub(super) fn recover_nats_when_due(&mut self) {
        let due = self.recover_at.is_some_and(|at| self.now_tick() >= at);
        if !due || self.nats_generation.is_some() {
            return;
        }
        self.nats_generation = Some(self.database.start_next_generation());
        self.record_step("RecoverNats".to_owned());
    }

    pub(super) fn stall_if_armed(&mut self, process: usize, step: StepKind) -> bool {
        match self.processes[process].stall {
            Stall::Awake => false,
            Stall::Stalled { .. } => true,
            Stall::Armed { ticks, at } => {
                let holding = self.processes[process].phase.is_holding();
                let reached = at == StepKind::Any || at == step;
                if !holding || !reached || self.faults_left.stalls == 0 {
                    return false;
                }
                self.faults_left.stalls -= 1;
                let until = self.now_tick() + ticks;
                self.processes[process].stall = Stall::Stalled { until };
                self.record_step(format!("Stall(p{}) for {ticks} ticks", process + 1));
                true
            }
        }
    }

    pub(super) fn take_renewal_failure(&mut self, process: usize, lease: Option<&Lease>) -> bool {
        let state = &self.processes[process];
        let fails = state.renewal_fails
            && state.phase.is_holding()
            && lease.is_some()
            && self.faults_left.crashes > 0;
        if fails {
            self.faults_left.crashes -= 1;
            self.processes[process].renewal_fails = false;
        }
        fails
    }

    pub(super) fn take_planner_crash(&mut self) -> bool {
        let crashes = self.planner_crash_armed && self.faults_left.crashes > 0;
        if crashes {
            self.faults_left.crashes -= 1;
            self.planner_crash_armed = false;
        }
        crashes
    }

    pub(super) fn record_planner_crash(&mut self) {
        self.publishing = None;
        self.record_step("PlannerCrash".to_owned());
    }

    fn crash(&mut self, process: usize) -> bool {
        if self.processes[process].phase == super::world::Phase::Idle
            || self.faults_left.crashes == 0
        {
            return false;
        }
        self.faults_left.crashes -= 1;
        self.drop_run(process);
        let state = &mut self.processes[process];
        state.incarnation += 1;
        state.stall = Stall::Awake;
        state.renewal_fails = false;
        self.record_step(format!("Crash(p{})", process + 1));
        true
    }

    fn wipe_nats(&mut self, recover_after: u64) {
        if self.faults_left.wipes == 0 {
            return;
        }
        self.faults_left.wipes -= 1;
        self.lease.wipe();
        self.marker.wipe();
        self.broker.wipe();
        self.nats_generation = None;
        self.recover_at = Some(self.now_tick() + recover_after);
        let not_done: Vec<TaskId> = self
            .ids
            .iter()
            .copied()
            .filter(|task| !self.broker.is_done(*task))
            .collect();
        self.lost_in_wipe.extend(not_done);
        self.record_step("NatsWipe".to_owned());
    }

    fn duplicate_message(&mut self, message: usize) {
        let lost = self.broker.messages()[message].state == MessageState::Lost;
        if self.faults_left.duplicates == 0 || lost {
            return;
        }
        self.faults_left.duplicates -= 1;
        self.broker.duplicate(message);
        self.record_step(format!("DuplicateCopy({})", message + 1));
    }
}
