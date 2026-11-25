use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::{Mutex, MutexGuard};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use tokio::sync::Notify;
use tokio::time::Instant;

use super::{FaultBudget, SimulatedBroker, SimulatedKeyValue};
use crate::event::{TaskEvent, TaskStep};
use crate::lease::Lease;
use crate::task::{TaskId, TaskMessage};
use crate::versioned::Revision;

pub(super) const TICK: Duration = Duration::from_secs(20);
pub(super) const ACK_WAIT_TICKS: u64 = 2;
const MAX_CALLS_PER_TICK: u64 = 10_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Actor {
    Planner,
    Process { index: usize, incarnation: usize },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum StepOrder {
    Atomic,
    Interleaved,
}

#[derive(Clone)]
pub(super) struct SharedWorld {
    world: Arc<Mutex<World>>,
    changed: Arc<Notify>,
}

pub(super) struct World {
    seed: u64,
    started_at: Instant,
    seen_tick: u64,
    calls_this_tick: u64,
    pub(super) rng: StdRng,
    step_order: StepOrder,
    pub(super) lease: SimulatedKeyValue<Lease>,
    pub(super) pending: SimulatedKeyValue<TaskMessage>,
    pub(super) broker: SimulatedBroker,
    payloads: HashMap<TaskId, TaskMessage>,
    pub(super) processes: Vec<ProcessState>,
    pub(super) publishing: Option<TaskId>,
    pub(super) ids: Vec<TaskId>,
    pub(super) covers: Vec<TaskId>,
    pub(super) started: BTreeSet<TaskId>,
    pub(super) lost_in_wipe: BTreeSet<TaskId>,
    pub(super) faults_left: FaultBudget,
    pub(super) planner_crash_armed: bool,
    pub(super) submits: usize,
    timeline: Vec<String>,
    broken: Option<String>,
    counts: BTreeMap<String, usize>,
}

pub(super) struct ProcessState {
    pub(super) offset: u64,
    pub(super) incarnation: usize,
    pub(super) phase: Phase,
    pub(super) held: Option<usize>,
    pub(super) seen: Revision,
    pub(super) renew_at: u64,
    pub(super) stall: Stall,
    pub(super) renewal_fails: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Phase {
    Idle,
    Leasing,
    Leased,
    Running,
    Done,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Stall {
    Awake,
    Armed { ticks: u64, at: StepKind },
    Stalled { until: u64 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum StepKind {
    Any,
    Work,
}

pub(super) struct WorldOutcome {
    pub(super) seed: u64,
    pub(super) broken: Option<String>,
    pub(super) timeline: Vec<String>,
    pub(super) counts: BTreeMap<String, usize>,
}

impl SharedWorld {
    pub(super) fn new(
        seed: u64,
        processes: usize,
        submits: usize,
        faults: FaultBudget,
        step_order: StepOrder,
    ) -> Self {
        let mut rng = StdRng::seed_from_u64(seed);
        let processes = (0..processes)
            .map(|_| ProcessState::idle(rng.random_range(0..=1)))
            .collect();
        let world = World {
            seed,
            started_at: Instant::now(),
            seen_tick: 0,
            calls_this_tick: 0,
            rng,
            step_order,
            lease: SimulatedKeyValue::default(),
            pending: SimulatedKeyValue::default(),
            broker: SimulatedBroker::new(ACK_WAIT_TICKS),
            payloads: HashMap::new(),
            processes,
            publishing: None,
            ids: Vec::new(),
            covers: Vec::new(),
            started: BTreeSet::new(),
            lost_in_wipe: BTreeSet::new(),
            faults_left: faults,
            planner_crash_armed: false,
            submits,
            timeline: Vec::new(),
            broken: None,
            counts: BTreeMap::new(),
        };
        let shared = Self {
            world: Arc::new(Mutex::new(world)),
            changed: Arc::new(Notify::new()),
        };
        shared.lock().record_initial_state();
        shared
    }

    pub(super) fn lock(&self) -> MutexGuard<'_, World> {
        let mut world = self.world.lock();
        world.record_passed_ticks();
        world
    }

    pub(super) fn notify_changed(&self) {
        self.changed.notify_waiters();
    }

    pub(super) async fn yield_randomly(&self) {
        let yields = self.lock().rng.random_range(0..3);
        for _ in 0..yields {
            tokio::task::yield_now().await;
        }
    }

    pub(super) async fn begin_step(&self, actor: Actor, kind: StepKind) -> SystemTime {
        self.yield_randomly().await;
        self.wait_while_stalled(actor, kind).await;
        self.stop_unless_allowed(actor).await;
        self.lock().local_time(actor)
    }

    pub(super) async fn stop_unless_allowed(&self, actor: Actor) {
        if !self.lock().allow_port_call(actor) {
            std::future::pending::<()>().await;
        }
    }

    pub(super) async fn interleave_inside_step(&self, actor: Actor) {
        self.stop_unless_allowed(actor).await;
        let interleave = {
            let mut world = self.lock();
            world.step_order == StepOrder::Interleaved && world.rng.random_bool(0.5)
        };
        if interleave {
            tokio::task::yield_now().await;
        }
    }

    pub(super) async fn wait_for_ready_message(&self) {
        loop {
            let changed = self.changed.notified();
            if self.lock().broker.ready_messages().next().is_some() {
                return;
            }
            changed.await;
        }
    }

    pub(super) fn into_outcome(self) -> WorldOutcome {
        let mut world = self.world.lock();
        WorldOutcome {
            seed: world.seed,
            broken: world.broken.take(),
            timeline: std::mem::take(&mut world.timeline),
            counts: std::mem::take(&mut world.counts),
        }
    }

    async fn wait_while_stalled(&self, actor: Actor, kind: StepKind) {
        let Actor::Process { index, .. } = actor else {
            return;
        };
        loop {
            let changed = self.changed.notified();
            if !self.lock().stall_if_armed(index, kind) {
                return;
            }
            changed.await;
            self.yield_randomly().await;
        }
    }
}

impl World {
    pub(super) fn start_of_tick(&self, tick: u64) -> Instant {
        self.started_at + TICK * u32::try_from(tick).unwrap_or(u32::MAX)
    }

    pub(super) fn now_tick(&self) -> u64 {
        let elapsed = Instant::now().duration_since(self.started_at);
        u64::try_from(elapsed.as_millis() / TICK.as_millis()).unwrap_or(u64::MAX)
    }

    pub(super) fn local_time(&self, actor: Actor) -> SystemTime {
        UNIX_EPOCH + Instant::now().duration_since(self.started_at) + TICK * self.offset_of(actor)
    }

    pub(super) fn draw_number(&mut self, range: std::ops::RangeInclusive<u64>) -> u64 {
        self.rng.random_range(range)
    }

    pub(super) fn is_broken(&self) -> bool {
        self.broken.is_some()
    }

    pub(super) fn start_process(&self, index: usize) -> Actor {
        Actor::Process {
            index,
            incarnation: self.processes[index].incarnation,
        }
    }

    pub(super) fn count(&mut self, name: &str) {
        *self.counts.entry(name.to_owned()).or_default() += 1;
    }

    pub(super) fn publish(&mut self, message: &TaskMessage) {
        self.payloads.insert(message.task, message.clone());
        self.broker.publish(message.task);
    }

    pub(super) fn deliver(&mut self, process: usize, message: usize) -> Option<TaskMessage> {
        let now = self.now_tick();
        self.processes[process].held = Some(message);
        let task = self.broker.deliver(message, process, now);
        self.payloads.get(&task).cloned()
    }

    pub(super) fn has_submits_left(&self) -> bool {
        self.covers.len() < self.submits && self.publishing.is_none()
    }

    pub(super) fn are_all_submits_done(&self) -> bool {
        self.publishing.is_none()
            && self.covers.len() == self.submits
            && self
                .covers
                .iter()
                .all(|task| self.lost_in_wipe.contains(task) || self.broker.is_done(*task))
    }

    pub(super) fn apply_task_event(&mut self, actor: Actor, event: &TaskEvent) {
        if event.at != self.local_time(actor) {
            self.break_rule(format!(
                "{actor:?} logged {:?}, which is not the time of its step",
                event.at
            ));
        }
        let task = event.task.id;
        match (actor, &event.step) {
            (Actor::Planner, TaskStep::Submitted { already_pending }) => {
                if !already_pending {
                    self.ids.push(task);
                }
                self.publishing = Some(task);
                let id = self.find_spec_id(task);
                self.record_step("Submit", format!(", task {id}"));
            }
            (Actor::Planner, TaskStep::Published) => {
                self.publishing = None;
                self.covers.push(task);
                let id = self.find_spec_id(task);
                self.record_step("Publish", format!(", task {id}"));
            }
            (Actor::Process { index, .. }, TaskStep::Delivered) => {
                self.processes[index].phase = Phase::Leasing;
                let copy = self.processes[index].held.map_or(0, |copy| copy + 1);
                let id = self.find_spec_id(task);
                self.record_step("Deliver", format!("(p{}, {copy}), task {id}", index + 1));
            }
            (Actor::Process { index, .. }, TaskStep::LeaseAcquired(hold)) => {
                let process = &mut self.processes[index];
                process.phase = Phase::Leased;
                process.seen = hold.seen_revision();
                process.renew_at = ticks_at(hold.renew_at());
                self.record_step("AcquireLease", format!("(p{})", index + 1));
            }
            (Actor::Process { index, .. }, TaskStep::GivenBack { .. }) => {
                self.drop_run(index);
                self.record_step("GiveBack", format!("(p{})", index + 1));
            }
            (Actor::Process { index, .. }, TaskStep::Started { .. }) => {
                self.started.insert(task);
                self.processes[index].phase = Phase::Running;
                self.record_step("StartTask", format!("(p{})", index + 1));
            }
            (Actor::Process { index, .. }, TaskStep::Renewed(hold)) => {
                let process = &mut self.processes[index];
                process.seen = hold.seen_revision();
                process.renew_at = ticks_at(hold.renew_at());
                self.record_step("Renew", format!("(p{})", index + 1));
            }
            (Actor::Process { index, .. }, TaskStep::RenewalLost) => {
                self.drop_run(index);
                self.record_step("Renew", format!("(p{}), lost", index + 1));
            }
            (Actor::Process { index, .. }, TaskStep::RenewalFailed(_)) => {
                self.drop_run(index);
                self.record_step("RenewFails", format!("(p{})", index + 1));
            }
            (Actor::Process { index, .. }, TaskStep::Finished { .. }) => {
                self.drop_run(index);
                self.record_step("Finish", format!("(p{})", index + 1));
            }
            (Actor::Process { index, .. }, TaskStep::Abandoned(_)) => {
                self.drop_run(index);
                self.record_step("Abandon", format!("(p{})", index + 1));
            }
            (_, TaskStep::PortCallFailed(_)) => {}
            (actor, step) => {
                self.break_rule(format!(
                    "{actor:?} logged {step:?}, which only the other actor sends"
                ));
            }
        }
    }

    pub(super) fn finish_work(&mut self, process: usize) {
        self.processes[process].phase = Phase::Done;
        self.record_step("Work", format!("(p{})", process + 1));
    }

    pub(super) fn redeliver_expired_messages(&mut self) -> bool {
        let now = self.now_tick();
        let expired: Vec<usize> = self.broker.expired_messages(now).collect();
        for message in &expired {
            self.broker.redeliver(*message);
            self.record_step("Redeliver", format!("({})", message + 1));
        }
        !expired.is_empty()
    }

    pub(super) fn break_rule(&mut self, rule: String) {
        if self.broken.is_none() {
            self.timeline
                .push(format!("t{} broken: {rule}", self.seen_tick));
            self.broken = Some(rule);
        }
    }

    pub(super) fn find_spec_id(&self, task: TaskId) -> usize {
        self.ids
            .iter()
            .position(|id| *id == task)
            .map_or(0, |index| index + 1)
    }

    pub(super) fn drop_run(&mut self, process: usize) {
        self.processes[process].phase = Phase::Idle;
        self.processes[process].held = None;
    }

    pub(super) fn record_step(&mut self, name: &str, detail: String) {
        self.count(name);
        if name != "Tick" {
            self.timeline
                .push(format!("t{} {name}{detail}", self.seen_tick));
        }
        if let Some(rule) = self.find_broken_rule() {
            self.break_rule(rule.to_owned());
        }
    }

    pub(super) fn allow_port_call(&mut self, actor: Actor) -> bool {
        self.calls_this_tick += 1;
        if self.calls_this_tick > MAX_CALLS_PER_TICK {
            self.break_rule(format!(
                "{actor:?} made {MAX_CALLS_PER_TICK} port calls in one tick, so time can't pass"
            ));
        }
        match actor {
            Actor::Planner => true,
            Actor::Process { index, incarnation } => {
                self.processes[index].incarnation == incarnation && !self.is_broken()
            }
        }
    }

    fn offset_of(&self, actor: Actor) -> u32 {
        match actor {
            Actor::Planner => 0,
            Actor::Process { index, .. } => {
                u32::try_from(self.processes[index].offset).unwrap_or(u32::MAX)
            }
        }
    }

    fn record_initial_state(&mut self) {
        self.timeline.push("t0 Init".to_owned());
    }

    fn record_passed_ticks(&mut self) {
        while self.seen_tick < self.now_tick() {
            self.seen_tick += 1;
            self.calls_this_tick = 0;
            self.record_step("Tick", String::new());
        }
    }

    fn find_broken_rule(&self) -> Option<&'static str> {
        let pending_task = self.pending.get().map(|message| message.task);
        let another_task_waits = self
            .ids
            .iter()
            .filter(|task| self.is_pending(**task))
            .any(|task| Some(*task) != pending_task);
        if another_task_waits {
            return Some("AtMostOnePending");
        }
        if !self.covers.iter().all(|task| self.is_covered(*task)) {
            return Some("EverySubmitCovered");
        }
        None
    }

    fn is_pending(&self, task: TaskId) -> bool {
        !self.started.contains(&task)
            && !self.lost_in_wipe.contains(&task)
            && self.broker.has_live_message(task)
    }

    fn is_covered(&self, task: TaskId) -> bool {
        self.started.contains(&task)
            || self.broker.has_live_message(task)
            || self.publishing == Some(task)
            || self.lost_in_wipe.contains(&task)
    }
}

impl ProcessState {
    fn idle(offset: u64) -> Self {
        Self {
            offset,
            incarnation: 0,
            phase: Phase::Idle,
            held: None,
            seen: Revision(0),
            renew_at: 0,
            stall: Stall::Awake,
            renewal_fails: false,
        }
    }
}

impl Phase {
    pub(super) fn is_holding(self) -> bool {
        match self {
            Phase::Leased | Phase::Running | Phase::Done => true,
            Phase::Idle | Phase::Leasing => false,
        }
    }
}

pub(super) fn ticks_at(time: SystemTime) -> u64 {
    ticks_in(time.duration_since(UNIX_EPOCH).unwrap_or_default())
}

pub(super) fn ticks_in(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis() / TICK.as_millis()).unwrap_or(u64::MAX)
}
